"""Abstract port contracts for BlueRed central server (Figure 54)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

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


@runtime_checkable
class ISessionStore(Protocol):
    """Volatile Redis store for active sessions, Bloom filters, and search routes."""

    async def upsert_session(self, session: PeerSession) -> None: ...

    async def get_session(self, user_id: str) -> PeerSession | None: ...

    async def delete_session(self, user_id: str) -> None: ...

    async def list_active_sessions(self) -> list[PeerSession]: ...

    async def save_bloom_filter(
        self,
        user_id: str,
        bitset: bytes,
        size_bits: int,
        hash_functions_k: int,
    ) -> None: ...

    async def get_bloom_filter(self, user_id: str) -> tuple[bytes, int, int] | None: ...

    async def register_search_route(
        self,
        search_token: str,
        requester_id: str,
        ttl_seconds: int = 60,
    ) -> None: ...

    async def get_search_route(self, search_token: str) -> str | None: ...


@runtime_checkable
class IFileReviewRepository(Protocol):
    """Transactional ACID repository for centralized file reviews and summaries."""

    async def upsert_review(self, entry: ReviewEntry) -> FileReputationSummary: ...

    async def get_reputation_summary(self, file_hash: str) -> FileReputationSummary | None: ...

    async def get_reputation_summaries(
        self, file_hashes: Sequence[str]
    ) -> dict[str, FileReputationSummary]: ...

    async def list_reviews_for_file(self, file_hash: str) -> list[ReviewEntry]: ...


@runtime_checkable
class ISearchRoutingStrategy(Protocol):
    """OCP strategy for selecting which active peers receive a search broadcast."""

    def select_target_peers(
        self,
        packet: SearchRequestPacket,
        active_sessions: Sequence[PeerSession],
    ) -> list[PeerSession]: ...


@runtime_checkable
class IClientPushGateway(Protocol):
    """WebSocket control-plane push gateway to connected peers."""

    async def send_to_peer(self, user_id: str, payload_json: str) -> bool: ...

    async def broadcast(self, user_ids: Sequence[str], payload_json: str) -> int: ...

    async def is_connected(self, user_id: str) -> bool: ...


@runtime_checkable
class IUserRepository(Protocol):
    """Persistent repository for UserAccount records (PostgreSQL)."""

    async def create(self, account: UserAccount) -> None: ...

    async def get_by_id(self, user_id: str) -> UserAccount | None: ...

    async def get_by_username(self, username: str) -> UserAccount | None: ...

    async def update(self, account: UserAccount) -> None: ...


@runtime_checkable
class ICommunityRepository(Protocol):
    """Persistent repository for communities, debate posts, chat rooms, and library items."""

    async def create_community(self, community: Community) -> None: ...

    async def list_communities(self) -> list[Community]: ...

    async def get_community(self, community_id: str) -> Community | None: ...

    async def create_debate_post(self, post: DebatePost) -> None: ...

    async def list_debate_posts(self, community_id: str) -> list[DebatePost]: ...

    async def list_chat_rooms(self) -> list[ChatRoom]: ...

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom | None: ...

    async def save_library_item(self, item: LibraryItem) -> None: ...

    async def list_library_items(self, owner_id: str) -> list[LibraryItem]: ...


@runtime_checkable
class IPasswordHasher(Protocol):
    """Password hashing and verification port (e.g. Argon2id)."""

    def hash_password(self, password: str) -> str: ...

    def verify_password(self, password: str, password_hash: str) -> bool: ...


@runtime_checkable
class ITokenProvider(Protocol):
    """Stateless session token issuer and verifier (e.g. JWT)."""

    def issue_token(self, user_id: str, username: str) -> str: ...

    def verify_token(self, token: str) -> tuple[str, str] | None: ...
