"""PostgreSQL ACID repositories backed by asyncpg connection pools."""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg  # type: ignore[import-untyped]

from bluered_server.contracts import (
    ICommunityRepository,
    IFileReviewRepository,
    IUserRepository,
)
from bluered_server.domain import (
    ChatRoom,
    Community,
    DebatePost,
    FileReputationSummary,
    LibraryItem,
    ReviewEntry,
    UserAccount,
)


class PostgresUserRepository(IUserRepository):
    """Persistent repository for UserAccount records."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(self, account: UserAccount) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (
                    user_id, username, password_hash,
                    failed_login_attempts, locked_until, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                account.user_id,
                account.username,
                account.password_hash,
                account.failed_login_attempts,
                account.locked_until,
                account.created_at,
            )

    async def get_by_id(self, user_id: str) -> UserAccount | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id, username, password_hash,
                       failed_login_attempts, locked_until, created_at
                FROM users
                WHERE user_id = $1
                """,
                user_id,
            )
        return self._row_to_account(row) if row is not None else None

    async def get_by_username(self, username: str) -> UserAccount | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id, username, password_hash,
                       failed_login_attempts, locked_until, created_at
                FROM users
                WHERE username = $1
                """,
                username,
            )
        return self._row_to_account(row) if row is not None else None

    async def update(self, account: UserAccount) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users
                SET username = $2,
                    password_hash = $3,
                    failed_login_attempts = $4,
                    locked_until = $5
                WHERE user_id = $1
                """,
                account.user_id,
                account.username,
                account.password_hash,
                account.failed_login_attempts,
                account.locked_until,
            )

    @staticmethod
    def _row_to_account(row: asyncpg.Record) -> UserAccount:
        return UserAccount(
            user_id=str(row["user_id"]),
            username=str(row["username"]),
            password_hash=str(row["password_hash"]),
            failed_login_attempts=int(row["failed_login_attempts"]),
            locked_until=row["locked_until"],
            created_at=row["created_at"],
        )


class PostgresFileReviewRepository(IFileReviewRepository):
    """ACID transactional repository for file_catalog and file_reviews (Figure 55)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert_review(self, entry: ReviewEntry) -> FileReputationSummary:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """
                INSERT INTO file_catalog (file_hash, review_count, rating_sum, avg_rating)
                VALUES ($1, 0, 0, 0.0)
                ON CONFLICT (file_hash) DO NOTHING
                """,
                entry.file_hash,
            )
            catalog_row = await conn.fetchrow(
                """
                SELECT review_count, rating_sum, avg_rating
                FROM file_catalog
                WHERE file_hash = $1
                FOR UPDATE
                """,
                entry.file_hash,
            )
            assert catalog_row is not None
            summary = FileReputationSummary(
                file_hash=entry.file_hash,
                review_count=int(catalog_row["review_count"]),
                rating_sum=int(catalog_row["rating_sum"]),
                avg_rating=float(catalog_row["avg_rating"]),
            )

            existing_row = await conn.fetchrow(
                """
                SELECT rating
                FROM file_reviews
                WHERE file_hash = $1 AND author_id = $2
                """,
                entry.file_hash,
                entry.author_id,
            )

            await conn.execute(
                """
                INSERT INTO file_reviews (
                    review_id, file_hash, author_id, author_username,
                    rating, comment, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (file_hash, author_id)
                DO UPDATE SET
                    author_username = EXCLUDED.author_username,
                    rating = EXCLUDED.rating,
                    comment = EXCLUDED.comment,
                    updated_at = EXCLUDED.updated_at
                """,
                entry.review_id,
                entry.file_hash,
                entry.author_id,
                entry.author_username,
                entry.rating,
                entry.comment,
                entry.created_at,
                entry.updated_at,
            )

            if existing_row is None:
                summary.apply_new_review(entry.rating)
            else:
                summary.apply_updated_review(
                    old_rating=int(existing_row["rating"]),
                    new_rating=entry.rating,
                )

            await conn.execute(
                """
                UPDATE file_catalog
                SET review_count = $2,
                    rating_sum = $3,
                    avg_rating = $4,
                    updated_at = NOW()
                WHERE file_hash = $1
                """,
                summary.file_hash,
                summary.review_count,
                summary.rating_sum,
                summary.avg_rating,
            )
            return summary

    async def get_reputation_summary(self, file_hash: str) -> FileReputationSummary | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT file_hash, review_count, rating_sum, avg_rating
                FROM file_catalog
                WHERE file_hash = $1
                """,
                file_hash,
            )
        if row is None:
            return None
        return FileReputationSummary(
            file_hash=str(row["file_hash"]).strip(),
            review_count=int(row["review_count"]),
            rating_sum=int(row["rating_sum"]),
            avg_rating=float(row["avg_rating"]),
        )

    async def get_reputation_summaries(
        self, file_hashes: Sequence[str]
    ) -> dict[str, FileReputationSummary]:
        if not file_hashes:
            return {}
        hash_list = list(dict.fromkeys(file_hashes))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT file_hash, review_count, rating_sum, avg_rating
                FROM file_catalog
                WHERE file_hash = ANY($1::char(64)[])
                """,
                hash_list,
            )
        result: dict[str, FileReputationSummary] = {}
        for row in rows:
            clean_hash = str(row["file_hash"]).strip()
            result[clean_hash] = FileReputationSummary(
                file_hash=clean_hash,
                review_count=int(row["review_count"]),
                rating_sum=int(row["rating_sum"]),
                avg_rating=float(row["avg_rating"]),
            )
        return result

    async def list_reviews_for_file(self, file_hash: str) -> list[ReviewEntry]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT review_id, file_hash, author_id, author_username,
                       rating, comment, created_at, updated_at
                FROM file_reviews
                WHERE file_hash = $1
                ORDER BY updated_at DESC
                """,
                file_hash,
            )
        return [
            ReviewEntry(
                review_id=str(row["review_id"]),
                file_hash=str(row["file_hash"]).strip(),
                author_id=str(row["author_id"]),
                author_username=str(row["author_username"]),
                rating=int(row["rating"]),
                comment=str(row["comment"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]


class PostgresCommunityRepository(ICommunityRepository):
    """Persistent PostgreSQL repository for communities, debates, chat rooms, and library."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_community(self, community: Community) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """
                INSERT INTO communities (
                    community_id, name, description, creator_id, created_at
                )
                VALUES ($1, $2, $3, $4, $5)
                """,
                community.community_id,
                community.name,
                community.description,
                community.creator_id,
                community.created_at,
            )
            for member_id in community.member_ids:
                await conn.execute(
                    """
                    INSERT INTO community_members (community_id, user_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    community.community_id,
                    member_id,
                )

    async def list_communities(self) -> list[Community]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT community_id, name, description, creator_id, created_at
                FROM communities
                ORDER BY created_at ASC
                """
            )
            member_rows = await conn.fetch(
                "SELECT community_id, user_id FROM community_members"
            )
        members_by_comm: dict[str, set[str]] = {}
        for m in member_rows:
            members_by_comm.setdefault(str(m["community_id"]), set()).add(str(m["user_id"]))
        return [
            Community(
                community_id=str(r["community_id"]),
                name=str(r["name"]),
                description=str(r["description"]),
                creator_id=str(r["creator_id"]),
                member_ids=members_by_comm.get(str(r["community_id"]), set()),
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def get_community(self, community_id: str) -> Community | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT community_id, name, description, creator_id, created_at
                FROM communities
                WHERE community_id = $1
                """,
                community_id,
            )
            if row is None:
                return None
            member_rows = await conn.fetch(
                "SELECT user_id FROM community_members WHERE community_id = $1",
                community_id,
            )
        return Community(
            community_id=str(row["community_id"]),
            name=str(row["name"]),
            description=str(row["description"]),
            creator_id=str(row["creator_id"]),
            member_ids={str(m["user_id"]) for m in member_rows},
            created_at=row["created_at"],
        )

    async def create_debate_post(self, post: DebatePost) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO debate_posts (
                    post_id, community_id, author_id, title, body, parent_post_id, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                post.post_id,
                post.community_id,
                post.author_id,
                post.title,
                post.body,
                post.parent_post_id,
                post.created_at,
            )

    async def get_debate_post(self, post_id: str) -> DebatePost | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT post_id, community_id, author_id, title, body, parent_post_id, created_at
                FROM debate_posts
                WHERE post_id = $1
                """,
                post_id,
            )
        if row is None:
            return None
        return DebatePost(
            post_id=str(row["post_id"]),
            community_id=str(row["community_id"]),
            author_id=str(row["author_id"]),
            title=str(row["title"]),
            body=str(row["body"]),
            parent_post_id=str(row["parent_post_id"]) if row["parent_post_id"] else None,
            created_at=row["created_at"],
        )

    async def list_debate_posts(self, community_id: str) -> list[DebatePost]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT post_id, community_id, author_id, title, body, parent_post_id, created_at
                FROM debate_posts
                WHERE community_id = $1
                ORDER BY created_at ASC
                """,
                community_id,
            )
        return [
            DebatePost(
                post_id=str(r["post_id"]),
                community_id=str(r["community_id"]),
                author_id=str(r["author_id"]),
                title=str(r["title"]),
                body=str(r["body"]),
                parent_post_id=str(r["parent_post_id"]) if r["parent_post_id"] else None,
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def list_chat_rooms(self) -> list[ChatRoom]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT room_id, name, community_id, created_at
                FROM chat_rooms
                ORDER BY created_at ASC
                """
            )
            member_rows = await conn.fetch(
                "SELECT room_id, user_id FROM chat_room_members"
            )
        members_by_room: dict[str, set[str]] = {}
        for m in member_rows:
            members_by_room.setdefault(str(m["room_id"]), set()).add(str(m["user_id"]))
        return [
            ChatRoom(
                room_id=str(r["room_id"]),
                name=str(r["name"]),
                community_id=str(r["community_id"]) if r["community_id"] else None,
                member_ids=members_by_room.get(str(r["room_id"]), set()),
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom | None:
        async with self._pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT room_id, name, community_id, created_at
                FROM chat_rooms
                WHERE room_id = $1
                """,
                room_id,
            )
            if row is None:
                return None
            await conn.execute(
                """
                INSERT INTO chat_room_members (room_id, user_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                room_id,
                user_id,
            )
            member_rows = await conn.fetch(
                "SELECT user_id FROM chat_room_members WHERE room_id = $1",
                room_id,
            )
        return ChatRoom(
            room_id=str(row["room_id"]),
            name=str(row["name"]),
            community_id=str(row["community_id"]) if row["community_id"] else None,
            member_ids={str(m["user_id"]) for m in member_rows},
            created_at=row["created_at"],
        )

    async def save_library_item(self, item: LibraryItem) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO personal_library (item_id, owner_id, file_hash, title, saved_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (owner_id, file_hash)
                DO UPDATE SET title = EXCLUDED.title, saved_at = EXCLUDED.saved_at
                """,
                item.item_id,
                item.owner_id,
                item.file_hash,
                item.title,
                item.saved_at,
            )

    async def list_library_items(self, owner_id: str) -> list[LibraryItem]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT item_id, owner_id, file_hash, title, saved_at
                FROM personal_library
                WHERE owner_id = $1
                ORDER BY saved_at DESC
                """,
                owner_id,
            )
        return [
            LibraryItem(
                item_id=str(r["item_id"]),
                owner_id=str(r["owner_id"]),
                file_hash=str(r["file_hash"]).strip(),
                title=str(r["title"]),
                saved_at=r["saved_at"],
            )
            for r in rows
        ]
