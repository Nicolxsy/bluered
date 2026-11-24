"""In-memory fakes for server unit tests."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from bluered_server.domain import (
    ChatRoom,
    Community,
    DebatePost,
    FileReputationSummary,
    LibraryItem,
    PeerSession,
    ReviewEntry,
    UserAccount,
)


class InMemoryUserRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, UserAccount] = {}
        self._by_username: dict[str, str] = {}

    async def create(self, account: UserAccount) -> None:
        self._by_id[account.user_id] = account
        self._by_username[account.username] = account.user_id

    async def get_by_id(self, user_id: str) -> UserAccount | None:
        return self._by_id.get(user_id)

    async def get_by_username(self, username: str) -> UserAccount | None:
        uid = self._by_username.get(username)
        return self._by_id.get(uid) if uid is not None else None

    async def update(self, account: UserAccount) -> None:
        self._by_id[account.user_id] = account
        self._by_username[account.username] = account.user_id


class InMemorySessionStore:
    def __init__(self) -> None:
        self.sessions: dict[str, PeerSession] = {}
        self.blooms: dict[str, tuple[bytes, int, int]] = {}
        self.search_routes: dict[str, tuple[str, datetime]] = {}
        self.last_route_ttl: int | None = None

    async def upsert_session(self, session: PeerSession) -> None:
        self.sessions[session.user_id] = session

    async def get_session(self, user_id: str) -> PeerSession | None:
        return self.sessions.get(user_id)

    async def delete_session(self, user_id: str) -> None:
        self.sessions.pop(user_id, None)
        self.blooms.pop(user_id, None)

    async def list_active_sessions(self) -> list[PeerSession]:
        return list(self.sessions.values())

    async def save_bloom_filter(
        self,
        user_id: str,
        bitset: bytes,
        size_bits: int,
        hash_functions_k: int,
    ) -> None:
        self.blooms[user_id] = (bitset, size_bits, hash_functions_k)
        if user_id in self.sessions:
            s = self.sessions[user_id]
            s.bloom_filter_bytes = bitset
            s.bloom_size_bits = size_bits
            s.bloom_hash_functions_k = hash_functions_k

    async def get_bloom_filter(self, user_id: str) -> tuple[bytes, int, int] | None:
        return self.blooms.get(user_id)

    async def register_search_route(
        self,
        search_token: str,
        requester_id: str,
        ttl_seconds: int = 60,
    ) -> None:
        self.last_route_ttl = ttl_seconds
        self.search_routes[search_token] = (
            requester_id,
            datetime.now(UTC) + timedelta(seconds=ttl_seconds),
        )

    async def get_search_route(self, search_token: str) -> str | None:
        item = self.search_routes.get(search_token)
        if item is None:
            return None
        requester_id, expires_at = item
        if datetime.now(UTC) >= expires_at:
            self.search_routes.pop(search_token, None)
            return None
        return requester_id


class InMemoryFileReviewRepository:
    def __init__(self) -> None:
        self.summaries: dict[str, FileReputationSummary] = {}
        self.reviews: dict[tuple[str, str], ReviewEntry] = {}
        self.batch_calls: int = 0

    async def upsert_review(self, entry: ReviewEntry) -> FileReputationSummary:
        summary = self.summaries.get(entry.file_hash)
        if summary is None:
            summary = FileReputationSummary(file_hash=entry.file_hash)
            self.summaries[entry.file_hash] = summary

        key = (entry.file_hash, entry.author_id)
        existing = self.reviews.get(key)
        if existing is None:
            summary.apply_new_review(entry.rating)
        else:
            summary.apply_updated_review(old_rating=existing.rating, new_rating=entry.rating)
        self.reviews[key] = entry
        return summary

    async def get_reputation_summary(self, file_hash: str) -> FileReputationSummary | None:
        return self.summaries.get(file_hash)

    async def get_reputation_summaries(
        self, file_hashes: Sequence[str]
    ) -> dict[str, FileReputationSummary]:
        self.batch_calls += 1
        return {h: self.summaries[h] for h in file_hashes if h in self.summaries}

    async def list_reviews_for_file(self, file_hash: str) -> list[ReviewEntry]:
        items = [r for (fh, _), r in self.reviews.items() if fh == file_hash]
        items.sort(key=lambda r: r.updated_at, reverse=True)
        return items


class InMemoryCommunityRepository:
    def __init__(self) -> None:
        self.communities: dict[str, Community] = {}
        self.posts: list[DebatePost] = []
        self.rooms: dict[str, ChatRoom] = {
            "general": ChatRoom(room_id="general", name="General"),
            "music-lossless": ChatRoom(room_id="music-lossless", name="Audio & Vinilos"),
            "papers-open": ChatRoom(room_id="papers-open", name="Documentos Abiertos"),
        }
        self.library: list[LibraryItem] = []

    async def create_community(self, community: Community) -> None:
        self.communities[community.community_id] = community

    async def list_communities(self) -> list[Community]:
        return list(self.communities.values())

    async def get_community(self, community_id: str) -> Community | None:
        return self.communities.get(community_id)

    async def create_debate_post(self, post: DebatePost) -> None:
        self.posts.append(post)

    async def get_debate_post(self, post_id: str) -> DebatePost | None:
        for post in self.posts:
            if post.post_id == post_id:
                return post
        return None

    async def list_debate_posts(self, community_id: str) -> list[DebatePost]:
        return [p for p in self.posts if p.community_id == community_id]

    async def list_chat_rooms(self) -> list[ChatRoom]:
        return list(self.rooms.values())

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom | None:
        room = self.rooms.get(room_id)
        if room is None:
            return None
        room.member_ids.add(user_id)
        return room

    async def save_library_item(self, item: LibraryItem) -> None:
        self.library = [
            existing
            for existing in self.library
            if not (existing.owner_id == item.owner_id and existing.file_hash == item.file_hash)
        ]
        self.library.append(item)

    async def list_library_items(self, owner_id: str) -> list[LibraryItem]:
        return [i for i in self.library if i.owner_id == owner_id]


class InMemoryPushGateway:
    def __init__(self, connected_users: set[str] | None = None) -> None:
        self.connected = connected_users if connected_users is not None else set()
        self.sent: list[tuple[str, str]] = []

    async def send_to_peer(self, user_id: str, payload_json: str) -> bool:
        self.sent.append((user_id, payload_json))
        return True

    async def broadcast(self, user_ids: Sequence[str], payload_json: str) -> int:
        for uid in user_ids:
            self.sent.append((uid, payload_json))
        return len(user_ids)

    async def is_connected(self, user_id: str) -> bool:
        return user_id in self.connected
