"""Unit tests for the local FastAPI presentation layer and /ws/ui event listener."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from bluered_client.application import (
    DiscoveryAndReviewService,
    PeerUploadListener,
    PrivateChatService,
    TransferOrchestrator,
)
from bluered_client.domain import (
    FileReview,
    LocalPeer,
    PresenceStatus,
    PrivateMessage,
    RemotePeer,
    SearchMatch,
    SharedFile,
    TransferState,
    TransferTask,
)
from bluered_client.infrastructure import FairShareQueuePolicy, Sha256IntegrityVerifier
from bluered_client.presentation import (
    ClientAppContext,
    WebSocketTransferEventListener,
    create_client_app,
)
from fastapi.testclient import TestClient


class _FakeFileIndex:
    def __init__(self) -> None:
        self._files: dict[str, SharedFile] = {}
        self.bloom_hits = 1
        self.bloom_rejects = 4
        self.sqlite_queries = 1

    async def upsert(self, shared_file: SharedFile) -> None:
        self._files[shared_file.file_hash] = shared_file

    async def remove_by_hash(self, file_hash: str) -> bool:
        return self._files.pop(file_hash, None) is not None

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        return self._files.get(file_hash)

    async def search_local(self, query: str) -> list[SharedFile]:
        q = query.lower()
        return [f for f in self._files.values() if q in f.file_name.lower()]

    async def list_all(self) -> list[SharedFile]:
        return list(self._files.values())


class _FakeServerCoordinator:
    def __init__(self, search_results: list[SearchMatch]) -> None:
        self.is_connected = True
        self.user_id: str | None = "user-alice"
        self.presence_status = PresenceStatus.ONLINE
        self._search_results = search_results
        self._reviews: dict[str, list[FileReview]] = {}
        self.communities: list[tuple[str, str]] = []
        self.library_saved: list[tuple[str, str]] = []
        self.rooms_joined: list[str] = []

    async def connect(self, host: str, port: int) -> None:
        del host, port
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False
        self.presence_status = PresenceStatus.OFFLINE

    async def register(self, username: str, password: str) -> str:
        del password
        return f"user-{username}"

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        del password, tcp_port
        self.user_id = f"user-{username}"
        self.presence_status = PresenceStatus.ONLINE
        return (self.user_id, "tok-123")

    async def send_heartbeat(self, user_id: str, tcp_port: int) -> None:
        del user_id, tcp_port

    async def sync_bloom_filter(
        self, user_id: str, size_bits: int, hash_functions_k: int, bitset_base64: str
    ) -> None:
        del user_id, size_bits, hash_functions_k, bitset_base64

    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]:
        del max_results
        return [m for m in self._search_results if query.lower() in m.file.file_name.lower()]

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None:
        review = FileReview(
            review_id=f"{file_hash}:{self.user_id}",
            file_hash=file_hash,
            author_id=self.user_id or "local",
            rating=rating,
            comment=comment,
        )
        existing = [r for r in self._reviews.get(file_hash, []) if r.author_id != review.author_id]
        existing.append(review)
        self._reviews[file_hash] = existing

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        return list(self._reviews.get(file_hash, []))

    async def create_community(self, name: str, description: str = "") -> None:
        self.communities.append((name, description))

    async def post_debate(self, community_id: str, title: str, body: str) -> None:
        del community_id, title, body

    async def reply_debate(self, post_id: str, body: str) -> None:
        del post_id, body

    async def save_to_library(self, file_hash: str, title: str) -> None:
        self.library_saved.append((file_hash, title))

    async def join_room(self, room_id: str) -> None:
        self.rooms_joined.append(room_id)

    async def send_room_message(
        self, room_id: str, sender_id: str, sender_username: str, content: str
    ) -> None:
        del room_id, sender_id, sender_username, content


class _FakePeerTransport:
    def __init__(self, payloads: dict[str, bytes], browse_catalog: list[SharedFile]) -> None:
        self._payloads = payloads
        self._browse_catalog = browse_catalog
        self.sent_messages: list[PrivateMessage] = []

    async def request_file_chunks(
        self, peer: RemotePeer, file_hash: str, offset: int = 0
    ) -> AsyncIterator[bytes]:
        del peer
        data = self._payloads[file_hash][offset:]
        midpoint = max(1, len(data) // 2)
        if data[:midpoint]:
            yield data[:midpoint]
        if data[midpoint:]:
            yield data[midpoint:]

    async def browse_peer_files(self, peer: RemotePeer) -> list[SharedFile]:
        del peer
        return list(self._browse_catalog)

    async def send_private_message(self, peer: RemotePeer, message: PrivateMessage) -> bool:
        del peer
        self.sent_messages.append(message)
        return True

    async def close(self) -> None:
        return None


class _FakePendingStore:
    def __init__(self) -> None:
        self.pending: list[PrivateMessage] = []

    async def enqueue_pending_message(self, message: PrivateMessage) -> None:
        self.pending.append(message)

    async def list_pending_messages(self, recipient_id: str | None = None) -> list[PrivateMessage]:
        if recipient_id is None:
            return list(self.pending)
        return [m for m in self.pending if m.recipient_id == recipient_id]

    async def mark_message_delivered(self, message_id: str) -> bool:
        del message_id
        return True


def _build_test_client(tmp_path: Path) -> tuple[TestClient, ClientAppContext, bytes, str]:
    sample_bytes = b"BlueRed P2P Presentation Layer Sample Payload!" * 4
    sample_hash = hashlib.sha256(sample_bytes).hexdigest()

    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    sample_file = shared_dir / "architecture_notes.txt"
    sample_file.write_bytes(sample_bytes)

    download_dir = tmp_path / "downloads"
    download_dir.mkdir()

    remote_peer = RemotePeer(
        peer_id="peer-bob",
        username="bob",
        ip_address="127.0.0.1",
        tcp_port=2235,
        status=PresenceStatus.ONLINE,
    )
    remote_file = SharedFile(
        file_hash=sample_hash,
        file_name="architecture_notes.txt",
        size_bytes=len(sample_bytes),
        mime_type="text/plain",
        category="document",
    )

    local_peer = LocalPeer(
        peer_id="user-alice",
        username="alice",
        ip_address="127.0.0.1",
        tcp_port=2234,
        shared_directories=[shared_dir],
        max_upload_slots=2,
    )
    file_index = _FakeFileIndex()
    integrity = Sha256IntegrityVerifier()
    coordinator = _FakeServerCoordinator(
        search_results=[
            SearchMatch(file=remote_file, peer=remote_peer, avg_rating=4.5, review_count=2)
        ]
    )
    transport = _FakePeerTransport(
        payloads={sample_hash: sample_bytes},
        browse_catalog=[remote_file],
    )
    event_bus = WebSocketTransferEventListener()
    discovery = DiscoveryAndReviewService(coordinator, file_index, integrity)
    transfers = TransferOrchestrator(
        local_peer=local_peer,
        peer_transport=transport,
        integrity_verifier=integrity,
        download_dir=download_dir,
        listeners=[event_bus],
    )
    private_chat = PrivateChatService(local_peer, transport, _FakePendingStore())
    upload_listener = PeerUploadListener(
        local_peer=local_peer,
        file_index=file_index,
        queue_policy=FairShareQueuePolicy(max_concurrent_slots=2),
        integrity_verifier=integrity,
    )

    ctx = ClientAppContext(
        local_peer=local_peer,
        discovery=discovery,
        transfers=transfers,
        private_chat=private_chat,
        upload_listener=upload_listener,
        server_coordinator=coordinator,
        file_index=file_index,
        peer_transport=transport,
        event_bus=event_bus,
        advertised_host="127.0.0.1",
    )
    app = create_client_app(ctx)
    return TestClient(app), ctx, sample_bytes, sample_hash


def test_spa_index_health_and_session_endpoints(tmp_path: Path) -> None:
    client, _, _, _ = _build_test_client(tmp_path)
    with client:
        index_res = client.get("/")
        assert index_res.status_code == 200
        assert "BlueRed" in index_res.text

        health = client.get("/health")
        assert health.status_code == 200
        body = health.json()
        assert body["status"] == "ok"
        assert body["peer_username"] == "alice"

        login_res = client.post(
            "/api/session/login",
            json={"username": "alice", "password": "secret"},
        )
        assert login_res.status_code == 200
        assert login_res.json()["presence_status"] == "ONLINE"

        away_res = client.post("/api/session/presence", json={"status": "AWAY"})
        assert away_res.status_code == 200
        assert away_res.json()["presence_status"] == "AWAY"

        session_res = client.get("/api/session")
        assert session_res.status_code == 200
        assert session_res.json()["bloom"]["bloom_rejects"] == 4


def test_folders_search_browse_reviews_and_download_flow(tmp_path: Path) -> None:
    client, _, sample_bytes, sample_hash = _build_test_client(tmp_path)
    with client:
        reindex = client.post("/api/folders/reindex")
        assert reindex.status_code == 200
        assert reindex.json()["total_files"] == 1

        search = client.get("/api/search", params={"q": "architecture"})
        assert search.status_code == 200
        results = search.json()["results"]
        assert len(results) == 1
        assert results[0]["file_hash"] == sample_hash
        assert results[0]["avg_rating"] == 4.5

        browse = client.post(
            "/api/peers/browse",
            json={
                "peer_id": "peer-bob",
                "username": "bob",
                "ip_address": "127.0.0.1",
                "tcp_port": 2235,
            },
        )
        assert browse.status_code == 200
        assert len(browse.json()["files"]) == 1

        pub = client.post(
            f"/api/reviews/{sample_hash}",
            json={"rating": 5, "comment": "Verificado por SHA-256"},
        )
        assert pub.status_code == 201

        revs = client.get(f"/api/reviews/{sample_hash}")
        assert revs.status_code == 200
        assert revs.json()["review_count"] == 1
        assert revs.json()["avg_rating"] == 5.0

        dl = client.post(
            "/api/transfers/download",
            json={
                "peer_id": "peer-bob",
                "peer_username": "bob",
                "peer_ip": "127.0.0.1",
                "peer_port": 2235,
                "file_hash": sample_hash,
                "file_name": "architecture_notes.txt",
                "size_bytes": len(sample_bytes),
                "wait": True,
            },
        )
        assert dl.status_code == 200
        dl_body = dl.json()
        assert dl_body["state"] == "COMPLETED"
        assert dl_body["bytes_transferred"] == len(sample_bytes)
        assert Path(dl_body["target_path"]).read_bytes() == sample_bytes

        lib = client.get("/api/library")
        assert lib.status_code == 200
        assert any(i["file_hash"] == sample_hash for i in lib.json()["items"])


def test_communities_rooms_private_chat_and_blocklist(tmp_path: Path) -> None:
    client, _, _, sample_hash = _build_test_client(tmp_path)
    with client:
        comm = client.post(
            "/api/communities",
            json={"name": "Sistemas Distribuidos", "description": "Papers P2P"},
        )
        assert comm.status_code == 201
        comm_id = comm.json()["id"]

        debate = client.post(
            f"/api/communities/{comm_id}/debates",
            json={"title": "Bloom Filters", "body": "Reducen tráfico en 90%"},
        )
        assert debate.status_code == 201

        join = client.post("/api/rooms/general/join")
        assert join.status_code == 200
        room_msg = client.post("/api/rooms/general/messages", json={"content": "Hola red!"})
        assert room_msg.status_code == 201

        pm = client.post(
            "/api/chat/private",
            json={
                "peer_id": "peer-bob",
                "peer_username": "bob",
                "peer_ip": "127.0.0.1",
                "peer_port": 2235,
                "content": "Hola Bob",
            },
        )
        assert pm.status_code == 201
        assert pm.json()["delivered"] is True

        blk = client.post("/api/peers/peer-bob/block")
        assert blk.status_code == 200
        assert "peer-bob" in blk.json()["blocked_peers"]

        unblk = client.post("/api/peers/peer-bob/unblock")
        assert unblk.status_code == 200
        assert "peer-bob" not in unblk.json()["blocked_peers"]

        saved = client.post(
            "/api/library",
            json={"file_hash": sample_hash, "title": "Notas Arquitectura"},
        )
        assert saved.status_code == 201


class _FakeUiWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.sent: list[dict[str, Any]] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, Any]) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        del code, reason


@pytest.mark.asyncio
async def test_websocket_transfer_event_listener_unit_broadcasts(tmp_path: Path) -> None:
    _, ctx, _, sample_hash = _build_test_client(tmp_path)
    fake_ws = _FakeUiWebSocket()
    await ctx.event_bus.connect(fake_ws)  # type: ignore[arg-type]
    assert fake_ws.accepted is True
    assert ctx.event_bus.active_connections_count == 1

    task = TransferTask(
        task_id="tx-ws-1",
        file_hash=sample_hash,
        file_name="demo.bin",
        total_bytes=100,
        remote_peer_id="peer-bob",
        state=TransferState.TRANSFERRING,
        bytes_transferred=50,
    )
    await ctx.event_bus.on_state_changed(task, TransferState.HANDSHAKING)
    await ctx.event_bus.on_progress(task)
    await ctx.event_bus.on_error(task, "Peer disconnected")

    assert [evt["event"] for evt in fake_ws.sent] == [
        "transfer_state_changed",
        "transfer_progress",
        "transfer_error",
    ]
    assert fake_ws.sent[0]["data"]["previous_state"] == "HANDSHAKING"
    assert fake_ws.sent[1]["data"]["progress_percent"] == 50.0
    assert fake_ws.sent[2]["data"]["error_message"] == "Peer disconnected"


def test_websocket_ui_endpoint_streams_live_transfer_events(tmp_path: Path) -> None:
    client, _, sample_bytes, sample_hash = _build_test_client(tmp_path)
    with client, client.websocket_connect("/ws/ui") as ws:
        initial: dict[str, Any] = ws.receive_json()
        assert initial["event"] == "snapshot"

        ws.send_text("ping")
        assert ws.receive_json()["event"] == "pong"

        dl = client.post(
            "/api/transfers/download",
            json={
                "peer_id": "peer-bob",
                "peer_username": "bob",
                "peer_ip": "127.0.0.1",
                "peer_port": 2235,
                "file_hash": sample_hash,
                "file_name": "architecture_notes.txt",
                "size_bytes": len(sample_bytes),
                "wait": True,
            },
        )
        assert dl.status_code == 200

        received_events: list[str] = []
        while True:
            evt: dict[str, Any] = ws.receive_json()
            received_events.append(evt["event"])
            if (
                evt["event"] == "transfer_state_changed"
                and evt["data"].get("state") == "COMPLETED"
            ):
                break

        assert "transfer_state_changed" in received_events
        assert "transfer_progress" in received_events
