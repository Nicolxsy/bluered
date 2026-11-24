"""Unit tests for the local FastAPI presentation layer and /ws/ui event listener."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from bluered_client.application import (
    CommunityClientService,
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
from bluered_client.infrastructure import (
    FairShareQueuePolicy,
    RawTcpUploadServer,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
)
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
        self.is_authenticated = True
        self.user_id: str | None = "user-alice"
        self.presence_status = PresenceStatus.ONLINE
        self._search_results = search_results
        self._reviews: dict[str, list[FileReview]] = {}
        self.communities: list[dict[str, Any]] = []
        self.debates_by_comm: dict[str, list[dict[str, Any]]] = {}
        self.library_saved: list[dict[str, Any]] = []
        self.rooms_joined: list[str] = []
        self.rooms: list[dict[str, Any]] = [
            {"id": "general", "name": "General", "member_count": 1},
            {"id": "music-lossless", "name": "Audio & Vinilos", "member_count": 1},
            {"id": "papers-open", "name": "Documentos Abiertos", "member_count": 1},
        ]

    async def connect(self, host: str, port: int) -> None:
        del host, port
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False
        self.is_authenticated = False
        self.presence_status = PresenceStatus.OFFLINE

    async def register(self, username: str, password: str) -> str:
        del password
        return f"user-{username}"

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        del password, tcp_port
        self.user_id = f"user-{username}"
        self.is_authenticated = True
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

    async def fetch_communities(self) -> list[dict[str, Any]]:
        return list(self.communities)

    async def create_community(self, name: str, description: str = "") -> dict[str, Any] | None:
        item = {
            "id": f"comm-{len(self.communities) + 1}",
            "name": name.strip(),
            "description": description.strip(),
        }
        self.communities.append(item)
        return item

    async def fetch_debates(self, community_id: str) -> list[dict[str, Any]]:
        return list(self.debates_by_comm.get(community_id, []))

    async def post_debate(
        self, community_id: str, title: str, body: str
    ) -> dict[str, Any] | None:
        posts = self.debates_by_comm.setdefault(community_id, [])
        post = {
            "post_id": f"post-{len(posts) + 1}",
            "community_id": community_id,
            "author_id": self.user_id or "user-alice",
            "title": title.strip(),
            "body": body.strip(),
            "parent_post_id": None,
            "created_at_iso": datetime.now(UTC).isoformat(),
        }
        posts.append(post)
        return post

    async def reply_debate(self, post_id: str, body: str) -> dict[str, Any] | None:
        for cid, posts in self.debates_by_comm.items():
            parent = next((p for p in posts if p["post_id"] == post_id), None)
            if parent is not None:
                reply = {
                    "post_id": f"post-{len(posts) + 1}",
                    "community_id": cid,
                    "author_id": self.user_id or "user-alice",
                    "title": (
                        parent["title"]
                        if str(parent["title"]).startswith("Re: ")
                        else f"Re: {parent['title']}"
                    ),
                    "body": body.strip(),
                    "parent_post_id": post_id,
                    "created_at_iso": datetime.now(UTC).isoformat(),
                }
                posts.append(reply)
                return reply
        return None

    async def fetch_library(self) -> list[dict[str, Any]]:
        return list(self.library_saved)

    async def save_to_library(self, file_hash: str, title: str) -> dict[str, Any] | None:
        item = {
            "file_hash": file_hash,
            "title": title.strip(),
            "saved_at_iso": datetime.now(UTC).isoformat(),
        }
        self.library_saved = [i for i in self.library_saved if i["file_hash"] != file_hash]
        self.library_saved.insert(0, item)
        return item

    async def fetch_rooms(self) -> list[dict[str, Any]]:
        return list(self.rooms)

    async def join_room(self, room_id: str) -> list[dict[str, Any]] | None:
        self.rooms_joined.append(room_id)
        if not any(r["id"] == room_id for r in self.rooms):
            self.rooms.append({"id": room_id, "name": room_id, "member_count": 1})
        return list(self.rooms)

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
        upload_server=RawTcpUploadServer(local_peer),
    )
    known_peers: dict[str, dict[str, Any]] = {}
    community = CommunityClientService(local_peer, coordinator, known_peers=known_peers)

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
        community=community,
        advertised_host="127.0.0.1",
        known_peers=known_peers,
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


def test_debate_replies_cross_node_sync_and_folders_crud(tmp_path: Path) -> None:
    client, ctx, _, _ = _build_test_client(tmp_path)
    coord = ctx.server_coordinator
    assert isinstance(coord, _FakeServerCoordinator)
    ctx.known_peers["user-bob"] = {
        "peer_id": "user-bob",
        "username": "bob",
        "ip_address": "127.0.0.1",
        "tcp_port": 2235,
        "status": "ONLINE",
    }

    with client:
        # Cross-node community & multi-level Cas_04 debate reply sync
        coord.communities.append({"id": "c-remote", "name": "Remota", "description": "De Bob"})
        now_iso = datetime.now(UTC).isoformat()
        coord.debates_by_comm["c-remote"] = [
            {
                "post_id": "p-root",
                "community_id": "c-remote",
                "author_id": "user-bob",
                "title": "Hilo de Bob",
                "body": "Inicio",
                "parent_post_id": None,
                "created_at_iso": now_iso,
            },
            {
                "post_id": "p-r1",
                "community_id": "c-remote",
                "author_id": "user-bob",
                "title": "Re: Hilo de Bob",
                "body": "Nivel 2",
                "parent_post_id": "p-root",
                "created_at_iso": now_iso,
            },
        ]

        # Reply to level-2 reply via REST (Cas_04)
        rep = client.post("/api/debates/p-r1/replies", json={"body": "Nivel 3 de Alice"})
        assert rep.status_code == 201
        assert rep.json()["post_id"] == "p-r1"

        listed = client.get("/api/communities", params={"community_id": "c-remote"})
        assert listed.status_code == 200
        debates = listed.json()["debates"]
        assert len(debates) == 1
        assert debates[0]["id"] == "p-root"
        assert debates[0]["author_username"] == "bob"
        assert len(debates[0]["replies"]) == 2
        assert debates[0]["replies"][1]["body"] == "Nivel 3 de Alice"

        # Health reflects live is_authenticated drop cleanly
        coord.is_authenticated = False
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["authenticated"] is False

        # Folder add & delete
        extra_dir = tmp_path / "extra_shared"
        extra_dir.mkdir()
        (extra_dir / "doc.txt").write_text("hello", encoding="utf-8")
        add_res = client.post("/api/folders", json={"path": str(extra_dir)})
        assert add_res.status_code == 201
        assert add_res.json()["indexed_count"] == 1

        del_res = client.request("DELETE", "/api/folders", json={"path": str(extra_dir)})
        assert del_res.status_code == 200
        assert str(extra_dir) not in [
            d for d in client.get("/api/folders").json()["directories"]
        ]


async def test_resume_endpoint_hydrates_sqlite_checkpoint_after_restart(tmp_path: Path) -> None:
    payload = b"BlueRed-Resume-Checkpoint-Payload-" * 16
    file_hash = hashlib.sha256(payload).hexdigest()
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()
    target_path = downloads_dir / "restarted.bin"
    part_path = downloads_dir / "restarted.bin.part"
    half = len(payload) // 2
    part_path.write_bytes(payload[:half])

    async with SqliteFileRepository(tmp_path / "client.db") as repo:
        await repo.save_download_checkpoint(
            task_id="tx-restart-1",
            file_hash=file_hash,
            file_name="restarted.bin",
            target_path=target_path,
            total_bytes=len(payload),
            bytes_transferred=half,
            state="TRANSFERRING",
            source_peer_id="peer-bob",
        )

        local_peer = LocalPeer("user-alice", "alice", "127.0.0.1", 2234)
        integrity = Sha256IntegrityVerifier()
        transport = _FakePeerTransport(payloads={file_hash: payload}, browse_catalog=[])
        coordinator = _FakeServerCoordinator(search_results=[])
        event_bus = WebSocketTransferEventListener()
        transfers = TransferOrchestrator(
            local_peer=local_peer,
            peer_transport=transport,
            integrity_verifier=integrity,
            download_dir=downloads_dir,
            checkpoint_store=repo,
            listeners=[event_bus],
        )
        known_peers: dict[str, dict[str, Any]] = {}
        ctx = ClientAppContext(
            local_peer=local_peer,
            discovery=DiscoveryAndReviewService(coordinator, repo, integrity),
            transfers=transfers,
            private_chat=PrivateChatService(local_peer, transport, repo),
            upload_listener=PeerUploadListener(
                local_peer, repo, FairShareQueuePolicy(), integrity,
                upload_server=RawTcpUploadServer(local_peer),
            ),
            server_coordinator=coordinator,
            file_index=repo,
            peer_transport=transport,
            event_bus=event_bus,
            known_peers=known_peers,
        )
        app = create_client_app(ctx)

        with TestClient(app) as tc:
            # 1. GET /api/transfers hydrates incomplete SQLite checkpoint as PAUSED
            listed = tc.get("/api/transfers")
            assert listed.status_code == 200
            txs = listed.json()["transfers"]
            assert len(txs) == 1
            assert txs[0]["task_id"] == "tx-restart-1"
            assert txs[0]["state"] == "PAUSED"

            # 2. Missing task -> 404
            assert tc.post("/api/transfers/nonexistent/resume").status_code == 404

            # 3. Unknown/offline peer -> 409
            assert tc.post("/api/transfers/tx-restart-1/resume").status_code == 409

            # 4. Known ONLINE peer -> wait=False resumes from SQLite checkpoint cleanly
            known_peers["peer-bob"] = {
                "peer_id": "peer-bob",
                "username": "bob",
                "ip_address": "127.0.0.1",
                "tcp_port": 2235,
                "status": "ONLINE",
            }
            res = tc.post("/api/transfers/tx-restart-1/resume")
            assert res.status_code == 200
            import time

            for _ in range(20):
                st = tc.get("/api/transfers/tx-restart-1").json()["state"]
                if st == "COMPLETED":
                    break
                time.sleep(0.02)
            assert tc.get("/api/transfers/tx-restart-1").json()["state"] == "COMPLETED"
            assert target_path.read_bytes() == payload


async def test_main_build_client_app_presence_offline_and_pending_pm_flush(tmp_path: Path) -> None:
    import socket

    from bluered_client.main import ClientSettings, build_client_app
    from bluered_contracts.control_plane import PresenceUpdateMessage

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        alice_port = probe.getsockname()[1]

    cfg = ClientSettings(
        peer_username="alice",
        peer_password="secret",
        peer_tcp_port=alice_port,
        peer_bind_host="127.0.0.1",
        peer_advertised_host="127.0.0.1",
        ui_host="127.0.0.1",
        ui_port=8001,
        shared_dir=tmp_path / "shared",
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "alice.db",
        max_upload_slots=2,
        server_ws_url="ws://127.0.0.1:8000/ws",
        auto_login=False,
    )
    app = build_client_app(cfg)
    with TestClient(app) as tc:
        ctx: ClientAppContext = app.state.ctx
        coord = ctx.server_coordinator
        on_presence = getattr(coord, "_on_presence", None)
        assert on_presence is not None

        # Queue offline PM for peer-bob while bob is OFFLINE
        pm = tc.post(
            "/api/chat/private",
            json={
                "peer_id": "peer-bob",
                "peer_username": "bob",
                "peer_ip": "127.0.0.1",
                "peer_port": 2235,
                "status": "OFFLINE",
                "content": "Hola cuando regreses!",
            },
        )
        assert pm.status_code == 201
        assert pm.json()["delivered"] is False

        # Simulate bob coming ONLINE with unreachable port -> must not raise
        await on_presence(
            PresenceUpdateMessage(
                user_id="peer-bob",
                username="bob",
                online=True,
                ip="127.0.0.1",
                tcp_port=65432,
            )
        )
        assert ctx.known_peers["peer-bob"]["status"] == "ONLINE"
        await asyncio.sleep(0.05)

        # Simulate bob going OFFLINE with ip=None, tcp_port=None -> known_peers MUST flip to OFFLINE
        await on_presence(
            PresenceUpdateMessage(
                user_id="peer-bob",
                username="bob",
                online=False,
                ip=None,
                tcp_port=None,
            )
        )
        assert ctx.known_peers["peer-bob"]["status"] == "OFFLINE"

        # Now start a real peer upload listener for bob and bring bob ONLINE -> pending PM flushes
        bob_received: list[PrivateMessage] = []
        bob_peer = LocalPeer("peer-bob", "bob", "127.0.0.1", 1)
        bob_repo = SqliteFileRepository(tmp_path / "bob.db")
        await bob_repo.initialize()

        async def _on_bob_pm(m: PrivateMessage) -> None:
            bob_received.append(m)

        bob_listener = PeerUploadListener(
            bob_peer,
            bob_repo,
            FairShareQueuePolicy(),
            Sha256IntegrityVerifier(),
            upload_server=RawTcpUploadServer(bob_peer),
            on_private_message=_on_bob_pm,
            bind_port=0,
        )
        bob_port = await bob_listener.start()
        try:
            await on_presence(
                PresenceUpdateMessage(
                    user_id="peer-bob",
                    username="bob",
                    online=True,
                    ip="127.0.0.1",
                    tcp_port=bob_port,
                )
            )
            for _ in range(100):
                if len(bob_received) == 1:
                    break
                await asyncio.sleep(0.02)
            assert len(bob_received) == 1
            assert bob_received[0].content == "Hola cuando regreses!"
            msgs = tc.get("/api/chat/private").json()["messages"]
            assert any(m["delivered"] is True for m in msgs)
        finally:
            await bob_listener.stop()
            await bob_repo.close()


async def test_presence_deliver_pending_pm_does_not_block_ws_recv_loop(tmp_path: Path) -> None:
    import asyncio
    import socket

    from bluered_client.main import ClientSettings, build_client_app
    from bluered_contracts.control_plane import (
        PresenceUpdateMessage,
        SearchQueryMessage,
        SearchResultsMessage,
        parse_control_message,
        serialize_control_message,
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        alice_port = probe.getsockname()[1]

    class _FakeWs:
        def __init__(self) -> None:
            self.sent: list[str] = []
            self.incoming: asyncio.Queue[str] = asyncio.Queue()

        async def send(self, message: str | bytes) -> None:
            self.sent.append(message.decode("utf-8") if isinstance(message, bytes) else message)

        async def recv(self) -> str | bytes:
            return await self.incoming.get()

        async def close(self) -> None:
            return None

    fake_ws = _FakeWs()

    cfg = ClientSettings(
        peer_username="alice",
        peer_password="secret",
        peer_tcp_port=alice_port,
        peer_bind_host="127.0.0.1",
        peer_advertised_host="127.0.0.1",
        ui_host="127.0.0.1",
        ui_port=8001,
        shared_dir=tmp_path / "shared",
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "alice_nonblock.db",
        max_upload_slots=2,
        server_ws_url="ws://127.0.0.1:8000/ws",
        auto_login=False,
    )
    app = build_client_app(cfg)

    async def _connect_fn(_uri: str) -> _FakeWs:
        return fake_ws

    async with app.router.lifespan_context(app):
        ctx: ClientAppContext = app.state.ctx
        coord = ctx.server_coordinator
        coord._connect_fn = _connect_fn  # type: ignore[attr-defined]
        await coord.connect("127.0.0.1", 8000)

        bob_offline = RemotePeer(
            peer_id="peer-bob",
            username="bob",
            ip_address="127.0.0.1",
            tcp_port=2235,
            status=PresenceStatus.OFFLINE,
        )
        queued_pm = await ctx.private_chat.send_message(bob_offline, "Mensaje bloqueado")
        assert queued_pm.delivered is False

        delivery_started = asyncio.Event()
        release_delivery = asyncio.Event()
        delivery_calls = 0

        async def _slow_deliver(peer: RemotePeer) -> list[PrivateMessage]:
            nonlocal delivery_calls
            delivery_calls += 1
            delivery_started.set()
            await release_delivery.wait()
            delivered_pm = PrivateMessage(
                message_id=queued_pm.message_id,
                sender_id=queued_pm.sender_id,
                recipient_id=peer.peer_id,
                content=queued_pm.content,
                delivered=True,
                created_at=queued_pm.created_at,
            )
            return [delivered_pm]

        ctx.private_chat.deliver_pending_messages = _slow_deliver  # type: ignore[method-assign]

        search_task = asyncio.create_task(coord.search("symphony"))
        await asyncio.sleep(0)
        sent_search = parse_control_message(fake_ws.sent[-1])
        assert isinstance(sent_search, SearchQueryMessage)

        # Push two rapid PresenceUpdateMessage(online=True) followed by SearchResultsMessage
        presence_frame = serialize_control_message(
            PresenceUpdateMessage(
                user_id="peer-bob",
                username="bob",
                online=True,
                ip="127.0.0.1",
                tcp_port=2235,
            )
        )
        fake_ws.incoming.put_nowait(presence_frame)
        fake_ws.incoming.put_nowait(presence_frame)
        fake_ws.incoming.put_nowait(
            serialize_control_message(
                SearchResultsMessage(search_token=sent_search.search_token, results=[])
            )
        )

        # _recv_loop MUST process SearchResultsMessage without waiting for _slow_deliver
        results = await asyncio.wait_for(search_task, timeout=0.5)
        assert results == []
        await asyncio.wait_for(delivery_started.wait(), timeout=0.5)
        assert delivery_calls == 1

        release_delivery.set()
        await asyncio.sleep(0.05)
        assert any(
            m["message_id"] == queued_pm.message_id and m["delivered"] is True
            for m in ctx.private_messages_cache
        )
        await coord.disconnect()


async def test_community_service_reply_debate_deduplicates_when_on_debates_fires_first() -> None:
    local_peer = LocalPeer("u-alice", "alice", "127.0.0.1", 2234)
    now_iso = datetime.now(UTC).isoformat()

    class _SyncingPort:
        def __init__(self, svc_ref: CommunityClientService | None = None) -> None:
            self.svc = svc_ref

        async def create_community(
            self, name: str, description: str = ""
        ) -> dict[str, Any] | None:
            return {"id": "c1", "name": name, "description": description}

        async def post_debate(
            self, community_id: str, title: str, body: str
        ) -> dict[str, Any] | None:
            return None

        async def reply_debate(self, post_id: str, body: str) -> dict[str, Any] | None:
            reply_row = {
                "post_id": "p-reply-1",
                "community_id": "c1",
                "author_id": "u-alice",
                "title": "",
                "body": body,
                "parent_post_id": post_id,
                "created_at_iso": now_iso,
            }
            assert self.svc is not None
            self.svc.apply_server_debates(
                "c1",
                [
                    {
                        "post_id": "p-root",
                        "community_id": "c1",
                        "author_id": "u-bob",
                        "title": "Root",
                        "body": "Body",
                        "parent_post_id": None,
                        "created_at_iso": now_iso,
                    },
                    reply_row,
                ],
            )
            return reply_row

        async def save_to_library(
            self, file_hash: str, title: str
        ) -> dict[str, Any] | None:
            return None

        async def join_room(self, room_id: str) -> list[dict[str, Any]]:
            return []

        async def send_room_message(
            self,
            room_id: str,
            sender_id: str,
            sender_username: str,
            content: str,
        ) -> None:
            return None

    port = _SyncingPort()
    svc = CommunityClientService(local_peer, port)
    port.svc = svc
    svc.apply_server_communities([{"id": "c1", "name": "General", "description": ""}])
    svc.apply_server_debates(
        "c1",
        [
            {
                "post_id": "p-root",
                "community_id": "c1",
                "author_id": "u-bob",
                "title": "Root",
                "body": "Body",
                "parent_post_id": None,
                "created_at_iso": now_iso,
            }
        ],
    )

    reply = await svc.reply_debate("p-root", "Sin duplicar")
    assert reply["id"] == "p-reply-1"
    listed = await svc.list_communities(community_id="c1", refresh=False)
    assert [r["id"] for r in listed["debates"][0]["replies"]] == ["p-reply-1"]


async def test_resume_endpoint_wait_false_returns_live_state_and_guards_concurrent_resume(
    tmp_path: Path,
) -> None:
    import httpx

    payload = b"BlueRed-Resume-Concurrent-Race-" * 16
    file_hash = hashlib.sha256(payload).hexdigest()
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()
    target_path = downloads_dir / "race.bin"
    part_path = downloads_dir / "race.bin.part"
    half = len(payload) // 2
    part_path.write_bytes(payload[:half])

    class _ControlledTransport(_FakePeerTransport):
        def __init__(self) -> None:
            super().__init__(payloads={file_hash: payload}, browse_catalog=[])
            self.stream_calls = 0
            self.gate = asyncio.Event()

        async def request_file_chunks(
            self,
            peer: RemotePeer,
            file_hash: str,
            offset: int = 0,
        ) -> AsyncIterator[bytes]:
            self.stream_calls += 1
            await self.gate.wait()
            async for chunk in super().request_file_chunks(peer, file_hash, offset=offset):
                yield chunk

    class _FakeCheckpointStore:
        def __init__(self) -> None:
            self._items: dict[str, Any] = {}

        async def save_download_checkpoint(
            self,
            task_id: str,
            file_hash: str,
            file_name: str,
            target_path: Path,
            total_bytes: int,
            bytes_transferred: int,
            state: str,
            source_peer_id: str,
        ) -> None:
            from datetime import UTC, datetime

            from bluered_client.domain import DownloadCheckpoint

            self._items[task_id] = DownloadCheckpoint(
                task_id=task_id,
                file_hash=file_hash,
                file_name=file_name,
                target_path=target_path,
                total_bytes=total_bytes,
                bytes_transferred=bytes_transferred,
                state=state,
                source_peer_id=source_peer_id,
                updated_at=datetime.now(UTC),
            )

        async def get_download_checkpoint(self, task_id: str) -> Any:
            return self._items.get(task_id)

        async def list_incomplete_checkpoints(self) -> list[Any]:
            return [
                cp
                for cp in self._items.values()
                if cp.state not in {"COMPLETED", "CANCELLED"}
            ]

    repo = _FakeFileIndex()
    ckpt_store = _FakeCheckpointStore()
    await ckpt_store.save_download_checkpoint(
        task_id="tx-race-1",
        file_hash=file_hash,
        file_name="race.bin",
        target_path=target_path,
        total_bytes=len(payload),
        bytes_transferred=half,
        state="PAUSED",
        source_peer_id="peer-bob",
    )

    local_peer = LocalPeer("user-alice", "alice", "127.0.0.1", 2234)
    integrity = Sha256IntegrityVerifier()
    transport = _ControlledTransport()
    coordinator = _FakeServerCoordinator(search_results=[])
    event_bus = WebSocketTransferEventListener()
    transfers = TransferOrchestrator(
        local_peer=local_peer,
        peer_transport=transport,
        integrity_verifier=integrity,
        download_dir=downloads_dir,
        checkpoint_store=ckpt_store,
        listeners=[event_bus],
    )
    known_peers = {
        "peer-bob": {
            "peer_id": "peer-bob",
            "username": "bob",
            "ip_address": "127.0.0.1",
            "tcp_port": 2235,
            "status": "ONLINE",
        }
    }
    ctx = ClientAppContext(
        local_peer=local_peer,
        discovery=DiscoveryAndReviewService(coordinator, repo, integrity),
        transfers=transfers,
        private_chat=PrivateChatService(local_peer, transport, _FakePendingStore()),
        upload_listener=PeerUploadListener(
            local_peer,
            repo,
            FairShareQueuePolicy(),
            integrity,
            upload_server=RawTcpUploadServer(local_peer),
        ),
        server_coordinator=coordinator,
        file_index=repo,
        peer_transport=transport,
        event_bus=event_bus,
        known_peers=known_peers,
    )
    app = create_client_app(ctx)

    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        r1 = await ac.post("/api/transfers/tx-race-1/resume")
        assert r1.status_code == 200
        # Live state must not regress to PAUSED while background resume is active
        assert r1.json()["state"] in {"RESUMING", "TRANSFERRING", "COMPLETED"}

        # Second concurrent resume while first is in flight must NOT spawn a 2nd stream
        r2 = await ac.post("/api/transfers/tx-race-1/resume")
        assert r2.status_code == 200
        assert transport.stream_calls == 1

        transport.gate.set()
        for _ in range(50):
            st = (await ac.get("/api/transfers/tx-race-1")).json()["state"]
            if st == "COMPLETED":
                break
            await asyncio.sleep(0.01)
        assert (await ac.get("/api/transfers/tx-race-1")).json()["state"] == "COMPLETED"
        assert transport.stream_calls == 1
        assert target_path.read_bytes() == payload


async def test_logout_stays_offline_across_keepalive_ticks_and_re_login_re_enables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import httpx
    from bluered_client.main import ClientSettings, build_client_app
    from bluered_contracts.control_plane import (
        AuthLoginMessage,
        AuthOkMessage,
        AuthRegisterMessage,
        parse_control_message,
        serialize_control_message,
    )

    monkeypatch.setenv("HEARTBEAT_INTERVAL_S", "0.05")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        alice_port = probe.getsockname()[1]

    login_calls = 0

    class _AutoAuthWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str] = asyncio.Queue()

        async def send(self, message: str | bytes) -> None:
            nonlocal login_calls
            raw = message.decode("utf-8") if isinstance(message, bytes) else message
            parsed = parse_control_message(raw)
            if isinstance(parsed, AuthRegisterMessage):
                self.incoming.put_nowait(
                    serialize_control_message(
                        AuthOkMessage(user_id="u-alice", username="alice", session_token="reg")
                    )
                )
            elif isinstance(parsed, AuthLoginMessage):
                login_calls += 1
                self.incoming.put_nowait(
                    serialize_control_message(
                        AuthOkMessage(
                            user_id="u-alice",
                            username="alice",
                            session_token=f"tok-{login_calls}",
                        )
                    )
                )

        async def recv(self) -> str | bytes:
            return await self.incoming.get()

        async def close(self) -> None:
            return None

    async def _connect_fn(_uri: str) -> _AutoAuthWs:
        return _AutoAuthWs()

    monkeypatch.setattr(
        "bluered_client.infrastructure.ws_coordinator.WebSocketServerCoordinator._default_connect",
        staticmethod(_connect_fn),
    )

    cfg = ClientSettings(
        peer_username="alice",
        peer_password="secret",
        peer_tcp_port=alice_port,
        peer_bind_host="127.0.0.1",
        peer_advertised_host="127.0.0.1",
        ui_host="127.0.0.1",
        ui_port=8001,
        shared_dir=tmp_path / "shared",
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "alice_logout.db",
        max_upload_slots=2,
        server_ws_url="ws://127.0.0.1:8000/ws",
        auto_login=True,
    )
    app = build_client_app(cfg)

    async with app.router.lifespan_context(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as ac:
        h1 = (await ac.get("/health")).json()
        assert h1["authenticated"] is True
        assert h1["presence_status"] == "ONLINE"
        assert login_calls == 1

        out = await ac.post("/api/session/logout")
        assert out.status_code == 200
        assert out.json()["presence_status"] == "OFFLINE"

        await asyncio.sleep(0.22)
        h2 = (await ac.get("/health")).json()
        assert h2["connected"] is False
        assert h2["authenticated"] is False
        assert h2["presence_status"] == "OFFLINE"
        assert login_calls == 1

        rel = await ac.post(
            "/api/session/login",
            json={"username": "alice", "password": "secret"},
        )
        assert rel.status_code == 200
        h3 = (await ac.get("/health")).json()
        assert h3["authenticated"] is True
        assert h3["presence_status"] == "ONLINE"
        assert login_calls == 2
