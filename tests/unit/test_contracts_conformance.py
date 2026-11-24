"""Structural and runtime_checkable conformance tests for client & server contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

from bluered_client.contracts import (
    IFileIndexRepository,
    IIntegrityVerifier,
    INatTraversalStrategy,
    IPeerTransport,
    IServerCoordinator,
    ITransferEventListener,
    ITransferQueuePolicy,
)
from bluered_client.domain import (
    FileReview,
    LocalPeer,
    PrivateMessage,
    RemotePeer,
    SearchMatch,
    SharedFile,
    TransferState,
    TransferTask,
)
from bluered_server.contracts import (
    IClientPushGateway,
    ICommunityRepository,
    IFileReviewRepository,
    ISearchRoutingStrategy,
    ISessionStore,
    IUserRepository,
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
    async def list_debate_posts(self, community_id: str) -> list[DebatePost]:
        return []

    async def list_chat_rooms(self) -> list[ChatRoom]:
        return []

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom | None:
        return None

    async def save_library_item(self, item: LibraryItem) -> None: ...
    async def list_library_items(self, owner_id: str) -> list[LibraryItem]:
        return []


def test_client_contracts_runtime_and_static_conformance() -> None:
    coordinator: IServerCoordinator = FakeServerCoordinator()
    repo: IFileIndexRepository = FakeFileIndexRepository()
    transport: IPeerTransport = FakePeerTransport()
    nat: INatTraversalStrategy = FakeNatTraversalStrategy()
    queue_policy: ITransferQueuePolicy = FakeTransferQueuePolicy()
    listener: ITransferEventListener = FakeTransferEventListener()
    verifier: IIntegrityVerifier = FakeIntegrityVerifier()

    assert isinstance(coordinator, IServerCoordinator)
    assert isinstance(repo, IFileIndexRepository)
    assert isinstance(transport, IPeerTransport)
    assert isinstance(nat, INatTraversalStrategy)
    assert isinstance(queue_policy, ITransferQueuePolicy)
    assert isinstance(listener, ITransferEventListener)
    assert isinstance(verifier, IIntegrityVerifier)


def test_server_contracts_runtime_and_static_conformance() -> None:
    sessions: ISessionStore = FakeSessionStore()
    reviews: IFileReviewRepository = FakeFileReviewRepository()
    routing: ISearchRoutingStrategy = FakeSearchRoutingStrategy()
    gateway: IClientPushGateway = FakeClientPushGateway()
    users: IUserRepository = FakeUserRepository()
    communities: ICommunityRepository = FakeCommunityRepository()

    assert isinstance(sessions, ISessionStore)
    assert isinstance(reviews, IFileReviewRepository)
    assert isinstance(routing, ISearchRoutingStrategy)
    assert isinstance(gateway, IClientPushGateway)
    assert isinstance(users, IUserRepository)
    assert isinstance(communities, ICommunityRepository)
