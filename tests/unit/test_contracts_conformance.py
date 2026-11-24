"""Structural and runtime_checkable conformance tests for client & server contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bluered_client.application import PeerUploadListener
from bluered_client.contracts import (
    IBloomSyncSource,
    ICommunityActionPort,
    ICommunitySyncPort,
    IDownloadCheckpointStore,
    IFileIndexRepository,
    IIncrementalIndexRepository,
    IIntegrityVerifier,
    INatSignalingCoordinator,
    INatTraversalStrategy,
    IPeerTransport,
    IPeerUploadHandler,
    IPeerUploadServer,
    IPeerUploadSession,
    IPendingMessageStore,
    IServerCoordinator,
    IStreamingPeerTransport,
    ITransferEventListener,
    ITransferQueuePolicy,
)
from bluered_client.domain import (
    DownloadCheckpoint,
    FileReview,
    LocalPeer,
    PrivateMessage,
    RemotePeer,
    SearchMatch,
    SharedFile,
    TransferState,
    TransferTask,
)
from bluered_client.infrastructure import (
    BloomFilterRepositoryDecorator,
    CompositeNatTraversalStrategy,
    DirectSocketNatStrategy,
    FairShareQueuePolicy,
    FifoQueuePolicy,
    RawTcpPeerTransport,
    RawTcpUploadServer,
    RelayNatStrategy,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
    TcpHolePunchingStrategy,
    UpnpNatStrategy,
    WebSocketServerCoordinator,
)
from bluered_client.presentation import WebSocketTransferEventListener
from bluered_server.contracts import (
    IClientPushGateway,
    ICommunityRepository,
    IFileReviewRepository,
    IPasswordHasher,
    ISearchRoutingStrategy,
    ISessionStore,
    ITokenProvider,
    IUserRepository,
    IWebSocketGateway,
)
from bluered_server.domain import (
    ChatRoom,
    Community,
    DebatePost,
    FileReputationSummary,
    LibraryItem,
    PeerSession,
    ReviewEntry,
    SearchRequestPacket,
    UserAccount,
)

# --- Minimal In-Memory Client Contract Fakes ---


class FakeServerCoordinator:
    async def connect(self, host: str, port: int) -> None: ...
    async def disconnect(self) -> None: ...
    async def register(self, username: str, password: str) -> str:
        return "u1"

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        return ("u1", "tok")

    async def send_heartbeat(self, user_id: str, tcp_port: int) -> None: ...
    async def sync_bloom_filter(
        self, user_id: str, size_bits: int, hash_functions_k: int, bitset_base64: str
    ) -> None: ...
    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]:
        return []

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None: ...
    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        return []


class FakeFileIndexRepository:
    async def upsert(self, shared_file: SharedFile) -> None: ...
    async def remove_by_hash(self, file_hash: str) -> bool:
        return True

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        return None

    async def search_local(self, query: str) -> list[SharedFile]:
        return []

    async def list_all(self) -> list[SharedFile]:
        return []


class FakeIncrementalIndexRepository(FakeFileIndexRepository):
    async def add_shared_directory(self, dir_path: Path) -> int:
        return 1

    async def get_file_stat_by_path(self, local_path: Path) -> tuple[str, float, int] | None:
        return None

    async def upsert_indexed_file(
        self,
        shared_file: SharedFile,
        *,
        mtime: float = 0.0,
        directory_id: int | None = None,
    ) -> None: ...


class FakeBloomSyncSource:
    def export_bloom_sync_payload(self) -> tuple[int, int, str]:
        return (1024, 3, "AAAA")


class FakeDownloadCheckpointStore:
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
    ) -> None: ...

    async def get_download_checkpoint(self, task_id: str) -> DownloadCheckpoint | None:
        return DownloadCheckpoint(
            task_id=task_id,
            file_hash="a" * 64,
            file_name="f.bin",
            target_path=Path("f.bin"),
            total_bytes=10,
            bytes_transferred=0,
            state="PAUSED",
            source_peer_id="p2",
            updated_at=datetime.now(UTC),
        )

    async def list_incomplete_checkpoints(self) -> list[DownloadCheckpoint]:
        return []

    async def delete_download_checkpoint(self, task_id: str) -> bool:
        return True


class FakePendingMessageStore:
    async def enqueue_pending_message(self, message: PrivateMessage) -> None: ...
    async def list_pending_messages(
        self, recipient_id: str | None = None
    ) -> list[PrivateMessage]:
        return []

    async def mark_message_delivered(self, message_id: str) -> bool:
        return True


class FakeNatSignalingCoordinator:
    async def request_nat_punch(
        self, target_peer_id: str, requester_ip: str, requester_port: int
    ) -> None: ...
    async def request_nat_relay(self, target_peer_id: str, session_id: str) -> None: ...
    def set_nat_signal_handler(self, handler: Callable[[Any], Awaitable[None]]) -> None: ...


class FakePeerTransport:
    async def request_file_chunks(
        self, peer: RemotePeer, file_hash: str, offset: int = 0
    ) -> AsyncIterator[bytes]:
        if False:
            yield b""

    async def browse_peer_files(self, peer: RemotePeer) -> list[SharedFile]:
        return []

    async def send_private_message(self, peer: RemotePeer, message: PrivateMessage) -> bool:
        return True

    async def close(self) -> None: ...


class FakeStreamingPeerTransport(FakePeerTransport):
    async def stream_file_download(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
        *,
        on_queue_status: Callable[[int], Awaitable[None]] | None = None,
        on_slot_available: Callable[[], Awaitable[None]] | None = None,
    ) -> AsyncIterator[tuple[int, bytes, str]]:
        if False:
            yield (0, b"", "")


class FakePeerUploadSession:
    @property
    def remote_peer_id(self) -> str:
        return "peer-remote"

    async def wait_disconnected(self) -> None: ...
    async def send_browse_response(self, files: list[SharedFile]) -> None: ...
    async def send_private_chat_ack(
        self, status: str, error_detail: str | None = None
    ) -> None: ...
    async def send_queue_status(self, queue_position: int, file_hash: str) -> None: ...
    async def send_slot_available(self, file_hash: str) -> None: ...
    async def send_file_chunk(
        self,
        file_hash: str,
        offset: int,
        chunk_index: int,
        chunk_sha256: str,
        payload: bytes,
    ) -> None: ...
    async def send_transfer_complete(self, file_hash: str, final_offset: int) -> None: ...
    async def send_transfer_error(
        self, status: str, detail: str, *, file_hash: str | None = None
    ) -> None: ...


class FakePeerUploadHandler:
    def is_peer_blocked(self, peer_id: str) -> bool:
        return False

    async def handle_browse(self, session: IPeerUploadSession) -> None: ...
    async def handle_download(
        self, session: IPeerUploadSession, file_hash: str | None, offset: int
    ) -> None: ...
    async def handle_private_chat(
        self,
        session: IPeerUploadSession,
        *,
        sender_id: str,
        recipient_id: str,
        message_id: str,
        content: str,
    ) -> None: ...


class FakePeerUploadServer:
    @property
    def bound_port(self) -> int | None:
        return 2234

    async def start(self, host: str, port: int, handler: IPeerUploadHandler) -> int:
        return port

    async def stop(self) -> None: ...


class FakeCommunityActionPort:
    async def create_community(
        self, name: str, description: str = ""
    ) -> dict[str, Any] | None:
        return None

    async def post_debate(
        self, community_id: str, title: str, body: str
    ) -> dict[str, Any] | None:
        return None

    async def reply_debate(self, post_id: str, body: str) -> dict[str, Any] | None:
        return None

    async def save_to_library(self, file_hash: str, title: str) -> dict[str, Any] | None:
        return None

    async def join_room(self, room_id: str) -> list[dict[str, Any]] | None:
        return None

    async def send_room_message(
        self, room_id: str, sender_id: str, sender_username: str, content: str
    ) -> None: ...


class FakeCommunitySyncPort(FakeCommunityActionPort):
    async def fetch_communities(self) -> list[dict[str, Any]]:
        return []

    async def fetch_debates(self, community_id: str) -> list[dict[str, Any]]:
        return []

    async def fetch_rooms(self) -> list[dict[str, Any]]:
        return []

    async def fetch_library(self) -> list[dict[str, Any]]:
        return []


class FakeNatTraversalStrategy:
    @property
    def strategy_name(self) -> str:
        return "direct"

    async def establish_connection(
        self, local_peer: LocalPeer, remote_peer: RemotePeer
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise NotImplementedError


class FakeTransferQueuePolicy:
    def enqueue(self, task: TransferTask) -> int:
        return 0

    def dequeue_next(self) -> TransferTask | None:
        return None

    def remove(self, task_id: str) -> bool:
        return False

    def position_of(self, task_id: str) -> int | None:
        return None

    def pending_count(self) -> int:
        return 0


class FakeTransferEventListener:
    async def on_state_changed(
        self, task: TransferTask, previous_state: TransferState
    ) -> None: ...
    async def on_progress(self, task: TransferTask) -> None: ...
    async def on_error(self, task: TransferTask, error_message: str) -> None: ...


class FakeIntegrityVerifier:
    def compute_chunk_hash(self, data: bytes) -> str:
        return ""

    def verify_chunk(self, data: bytes, expected_hash: str) -> bool:
        return True

    async def compute_file_hash(self, file_path: Path) -> str:
        return ""

    async def verify_file(self, file_path: Path, expected_hash: str) -> bool:
        return True


# --- Minimal In-Memory Server Contract Fakes ---


class FakeSessionStore:
    async def upsert_session(self, session: PeerSession) -> None: ...
    async def get_session(self, user_id: str) -> PeerSession | None:
        return None

    async def delete_session(self, user_id: str) -> None: ...
    async def list_active_sessions(self) -> list[PeerSession]:
        return []

    async def save_bloom_filter(
        self, user_id: str, bitset: bytes, size_bits: int, hash_functions_k: int
    ) -> None: ...
    async def get_bloom_filter(self, user_id: str) -> tuple[bytes, int, int] | None:
        return None

    async def register_search_route(
        self, search_token: str, requester_id: str, ttl_seconds: int = 60
    ) -> None: ...
    async def get_search_route(self, search_token: str) -> str | None:
        return None


class FakeFileReviewRepository:
    async def upsert_review(self, entry: ReviewEntry) -> FileReputationSummary:
        return FileReputationSummary(file_hash=entry.file_hash)

    async def get_reputation_summary(self, file_hash: str) -> FileReputationSummary | None:
        return None

    async def get_reputation_summaries(
        self, file_hashes: Sequence[str]
    ) -> dict[str, FileReputationSummary]:
        return {}

    async def list_reviews_for_file(self, file_hash: str) -> list[ReviewEntry]:
        return []


class FakeSearchRoutingStrategy:
    def select_target_peers(
        self, packet: SearchRequestPacket, active_sessions: Sequence[PeerSession]
    ) -> list[PeerSession]:
        return list(active_sessions)


class FakeClientPushGateway:
    async def send_to_peer(self, user_id: str, payload_json: str) -> bool:
        return True

    async def broadcast(self, user_ids: Sequence[str], payload_json: str) -> int:
        return len(user_ids)

    async def is_connected(self, user_id: str) -> bool:
        return False


class FakeUserRepository:
    async def create(self, account: UserAccount) -> None: ...
    async def get_by_id(self, user_id: str) -> UserAccount | None:
        return None

    async def get_by_username(self, username: str) -> UserAccount | None:
        return None

    async def update(self, account: UserAccount) -> None: ...


class FakeCommunityRepository:
    async def create_community(self, community: Community) -> None: ...
    async def list_communities(self) -> list[Community]:
        return []

    async def get_community(self, community_id: str) -> Community | None:
        return None

    async def create_debate_post(self, post: DebatePost) -> None: ...
    async def get_debate_post(self, post_id: str) -> DebatePost | None:
        return None

    async def list_debate_posts(self, community_id: str) -> list[DebatePost]:
        return []

    async def list_chat_rooms(self) -> list[ChatRoom]:
        return []

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom | None:
        return None

    async def save_library_item(self, item: LibraryItem) -> None: ...
    async def list_library_items(self, owner_id: str) -> list[LibraryItem]:
        return []


class FakeWebSocketGateway(FakeClientPushGateway):
    def register_connection(self, user_id: str, websocket: Any) -> None: ...
    def unregister_connection(self, user_id: str, websocket: Any | None = None) -> None: ...
    async def close_all(self) -> None: ...


class FakePasswordHasher:
    def hash_password(self, password: str) -> str:
        return f"hashed:{password}"

    def verify_password(self, password: str, password_hash: str) -> bool:
        return password_hash == f"hashed:{password}"


class FakeTokenProvider:
    def issue_token(self, user_id: str, username: str) -> str:
        return f"{user_id}:{username}"

    def verify_token(self, token: str) -> tuple[str, str] | None:
        if ":" not in token:
            return None
        uid, uname = token.split(":", 1)
        return uid, uname


def test_client_contracts_runtime_and_static_conformance() -> None:
    coordinator: IServerCoordinator = FakeServerCoordinator()
    repo: IFileIndexRepository = FakeFileIndexRepository()
    inc_repo: IIncrementalIndexRepository = FakeIncrementalIndexRepository()
    bloom_src: IBloomSyncSource = FakeBloomSyncSource()
    ckpt_store: IDownloadCheckpointStore = FakeDownloadCheckpointStore()
    pm_store: IPendingMessageStore = FakePendingMessageStore()
    nat_sig: INatSignalingCoordinator = FakeNatSignalingCoordinator()
    transport: IPeerTransport = FakePeerTransport()
    stream_transport: IStreamingPeerTransport = FakeStreamingPeerTransport()
    upload_session: IPeerUploadSession = FakePeerUploadSession()
    upload_handler: IPeerUploadHandler = FakePeerUploadHandler()
    upload_server: IPeerUploadServer = FakePeerUploadServer()
    comm_action: ICommunityActionPort = FakeCommunityActionPort()
    comm_sync: ICommunitySyncPort = FakeCommunitySyncPort()
    nat: INatTraversalStrategy = FakeNatTraversalStrategy()
    queue_policy: ITransferQueuePolicy = FakeTransferQueuePolicy()
    listener: ITransferEventListener = FakeTransferEventListener()
    verifier: IIntegrityVerifier = FakeIntegrityVerifier()

    assert isinstance(coordinator, IServerCoordinator)
    assert isinstance(repo, IFileIndexRepository)
    assert isinstance(inc_repo, IIncrementalIndexRepository)
    assert isinstance(bloom_src, IBloomSyncSource)
    assert isinstance(ckpt_store, IDownloadCheckpointStore)
    assert isinstance(pm_store, IPendingMessageStore)
    assert isinstance(nat_sig, INatSignalingCoordinator)
    assert isinstance(transport, IPeerTransport)
    assert isinstance(stream_transport, IStreamingPeerTransport)
    assert isinstance(upload_session, IPeerUploadSession)
    assert isinstance(upload_handler, IPeerUploadHandler)
    assert isinstance(upload_server, IPeerUploadServer)
    assert isinstance(comm_action, ICommunityActionPort)
    assert isinstance(comm_sync, ICommunitySyncPort)
    assert isinstance(nat, INatTraversalStrategy)
    assert isinstance(queue_policy, ITransferQueuePolicy)
    assert isinstance(listener, ITransferEventListener)
    assert isinstance(verifier, IIntegrityVerifier)


def test_real_client_adapters_conform_to_ports(tmp_path: Path) -> None:
    local = LocalPeer("p1", "alice", "127.0.0.1", 2234)
    ws_coord = WebSocketServerCoordinator()
    sqlite_repo = SqliteFileRepository(tmp_path / "test.db")
    bloom_repo = BloomFilterRepositoryDecorator(sqlite_repo)
    direct_nat = DirectSocketNatStrategy()
    upnp_nat = UpnpNatStrategy()
    punch_nat = TcpHolePunchingStrategy(ws_coord)
    relay_nat = RelayNatStrategy(ws_coord)
    comp_nat = CompositeNatTraversalStrategy([direct_nat])
    tcp_transport = RawTcpPeerTransport(local, comp_nat)
    tcp_upload_server = RawTcpUploadServer(local)
    fifo = FifoQueuePolicy()
    fair = FairShareQueuePolicy()
    verifier = Sha256IntegrityVerifier()
    upload_listener = PeerUploadListener(
        local, bloom_repo, fair, verifier, upload_server=tcp_upload_server
    )
    event_bus = WebSocketTransferEventListener()

    assert isinstance(ws_coord, IServerCoordinator)
    assert isinstance(ws_coord, INatSignalingCoordinator)
    assert isinstance(ws_coord, ICommunityActionPort)
    assert isinstance(ws_coord, ICommunitySyncPort)
    assert isinstance(sqlite_repo, IFileIndexRepository)
    assert isinstance(sqlite_repo, IIncrementalIndexRepository)
    assert isinstance(sqlite_repo, IDownloadCheckpointStore)
    assert isinstance(sqlite_repo, IPendingMessageStore)
    assert isinstance(bloom_repo, IFileIndexRepository)
    assert isinstance(bloom_repo, IIncrementalIndexRepository)
    assert isinstance(bloom_repo, IBloomSyncSource)
    assert isinstance(tcp_transport, IPeerTransport)
    assert isinstance(tcp_transport, IStreamingPeerTransport)
    assert isinstance(tcp_upload_server, IPeerUploadServer)
    assert isinstance(upload_listener, IPeerUploadHandler)
    assert isinstance(verifier, IIntegrityVerifier)
    assert isinstance(fifo, ITransferQueuePolicy)
    assert isinstance(fair, ITransferQueuePolicy)
    assert isinstance(direct_nat, INatTraversalStrategy)
    assert isinstance(upnp_nat, INatTraversalStrategy)
    assert isinstance(punch_nat, INatTraversalStrategy)
    assert isinstance(relay_nat, INatTraversalStrategy)
    assert isinstance(comp_nat, INatTraversalStrategy)
    assert isinstance(event_bus, ITransferEventListener)


def test_server_contracts_runtime_and_static_conformance() -> None:
    sessions: ISessionStore = FakeSessionStore()
    reviews: IFileReviewRepository = FakeFileReviewRepository()
    routing: ISearchRoutingStrategy = FakeSearchRoutingStrategy()
    gateway: IClientPushGateway = FakeClientPushGateway()
    ws_gateway: IWebSocketGateway = FakeWebSocketGateway()
    users: IUserRepository = FakeUserRepository()
    communities: ICommunityRepository = FakeCommunityRepository()
    hasher: IPasswordHasher = FakePasswordHasher()
    tokens: ITokenProvider = FakeTokenProvider()

    assert isinstance(sessions, ISessionStore)
    assert isinstance(reviews, IFileReviewRepository)
    assert isinstance(routing, ISearchRoutingStrategy)
    assert isinstance(gateway, IClientPushGateway)
    assert isinstance(ws_gateway, IWebSocketGateway)
    assert isinstance(users, IUserRepository)
    assert isinstance(communities, ICommunityRepository)
    assert isinstance(hasher, IPasswordHasher)
    assert isinstance(tokens, ITokenProvider)
