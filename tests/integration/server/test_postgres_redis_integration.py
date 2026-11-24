"""Integration tests against real PostgreSQL and Redis from docker-compose.yml."""

from __future__ import annotations

import hashlib
from pathlib import Path

import asyncpg
import pytest
import redis.asyncio as redis
from bluered_contracts.bloom import BloomFilter
from bluered_server.domain import (
    Community,
    DebatePost,
    LibraryItem,
    PeerSession,
    ReviewEntry,
    UserAccount,
)
from bluered_server.infrastructure import (
    PostgresCommunityRepository,
    PostgresFileReviewRepository,
    PostgresUserRepository,
    RedisSessionStore,
)

PG_DSN = "postgresql://bluered:bluered_dev@127.0.0.1:5432/bluered"
REDIS_URL = "redis://127.0.0.1:6379/0"
MIGRATION_FILE = Path(__file__).resolve().parents[3] / "server" / "migrations" / "001_init.sql"
SAMPLE_HASH = hashlib.sha256(b"integration-test-file").hexdigest()


async def _connect_or_skip() -> tuple[asyncpg.Pool, redis.Redis]:
    try:
        pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=3, timeout=2.0)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable on 127.0.0.1:5432: {exc}")

    r = redis.from_url(REDIS_URL, decode_responses=False, socket_connect_timeout=2.0)
    try:
        await r.ping()
    except Exception as exc:
        await pool.close()
        await r.aclose()
        pytest.skip(f"Redis not reachable on 127.0.0.1:6379: {exc}")

    return pool, r


async def test_redis_session_store_ttl_and_bloom_bytes() -> None:
    pool, r = await _connect_or_skip()
    await pool.close()
    try:
        store = RedisSessionStore(r, default_session_ttl=45)
        session = PeerSession(
            user_id="integ-u1",
            username="integ_alice",
            ip_address="127.0.0.1",
            tcp_port=9001,
        )
        await store.upsert_session(session)
        ttl = await r.ttl("active_sessions:integ-u1")
        assert 0 < ttl <= 45

        bloom = BloomFilter.optimal(100)
        bloom.add_filename("sample.flac")
        await store.save_bloom_filter(
            "integ-u1", bloom.to_bytes(), bloom.size_bits, bloom.hash_functions_k
        )
        loaded_bloom = await store.get_bloom_filter("integ-u1")
        assert loaded_bloom is not None
        assert loaded_bloom[0] == bloom.to_bytes()

        await store.register_search_route("integ-tok-1", "integ-u1", ttl_seconds=60)
        route_ttl = await r.ttl("active_search_routes:integ-tok-1")
        assert 0 < route_ttl <= 60
        assert await store.get_search_route("integ-tok-1") == "integ-u1"

        await store.delete_session("integ-u1")
        assert await store.get_session("integ-u1") is None
    finally:
        await r.delete(
            "active_sessions:integ-u1",
            "peer_bloom_filters:integ-u1",
            "active_search_routes:integ-tok-1",
        )
        await r.aclose()


async def test_postgres_acid_reviews_and_communities() -> None:
    pool, r = await _connect_or_skip()
    await r.aclose()
    try:
        sql = MIGRATION_FILE.read_text(encoding="utf-8")
        async with pool.acquire() as conn:
            await conn.execute(sql)
            await conn.execute("DELETE FROM personal_library WHERE owner_id LIKE 'integ-%'")
            await conn.execute("DELETE FROM debate_posts WHERE author_id LIKE 'integ-%'")
            await conn.execute("DELETE FROM communities WHERE creator_id LIKE 'integ-%'")
            await conn.execute("DELETE FROM file_reviews WHERE author_id LIKE 'integ-%'")
            await conn.execute("DELETE FROM file_catalog WHERE file_hash = $1", SAMPLE_HASH)
            await conn.execute("DELETE FROM users WHERE user_id LIKE 'integ-%'")

        user_repo = PostgresUserRepository(pool)
        review_repo = PostgresFileReviewRepository(pool)
        comm_repo = PostgresCommunityRepository(pool)

        u1 = UserAccount(user_id="integ-u1", username="integ_alice", password_hash="hash1")
        u2 = UserAccount(user_id="integ-u2", username="integ_bob", password_hash="hash2")
        await user_repo.create(u1)
        await user_repo.create(u2)
        assert (await user_repo.get_by_username("integ_alice")) is not None

        s1 = await review_repo.upsert_review(
            ReviewEntry(
                review_id="integ-r1",
                file_hash=SAMPLE_HASH,
                author_id="integ-u1",
                author_username="integ_alice",
                rating=5,
                comment="Great",
            )
        )
        assert s1.review_count == 1
        assert s1.avg_rating == pytest.approx(5.0)

        s2 = await review_repo.upsert_review(
            ReviewEntry(
                review_id="integ-r2",
                file_hash=SAMPLE_HASH,
                author_id="integ-u1",
                author_username="integ_alice",
                rating=3,
                comment="Updated",
            )
        )
        assert s2.review_count == 1
        assert s2.avg_rating == pytest.approx(3.0)

        batch = await review_repo.get_reputation_summaries([SAMPLE_HASH])
        assert SAMPLE_HASH in batch
        assert batch[SAMPLE_HASH].review_count == 1

        comm = Community(
            community_id="integ-c1",
            name="Integ Community",
            description="Test",
            creator_id="integ-u1",
            member_ids={"integ-u1"},
        )
        await comm_repo.create_community(comm)
        await comm_repo.create_debate_post(
            DebatePost(
                post_id="integ-p1",
                community_id="integ-c1",
                author_id="integ-u1",
                title="Topic",
                body="Body",
            )
        )
        await comm_repo.save_library_item(
            LibraryItem(
                item_id="integ-l1",
                owner_id="integ-u1",
                file_hash=SAMPLE_HASH,
                title="Saved",
            )
        )
        assert len(await comm_repo.list_debate_posts("integ-c1")) == 1
        assert len(await comm_repo.list_library_items("integ-u1")) == 1
    finally:
        await pool.close()
