"""Composition Root for BlueRed Central Server."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]
import redis.asyncio as redis
from fastapi import FastAPI

from bluered_server.application import (
    AuthService,
    CommunityAndChatService,
    FileReputationService,
    PresenceAndSessionService,
    SearchCoordinatorService,
)
from bluered_server.infrastructure import (
    Argon2PasswordHasher,
    BloomFilterRoutingStrategy,
    JwtTokenProvider,
    PostgresCommunityRepository,
    PostgresFileReviewRepository,
    PostgresUserRepository,
    RedisSessionStore,
    ServerServices,
    WebSocketClientGateway,
    create_fastapi_app,
)

_MIGRATION_PATH = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"


@dataclass(frozen=True, slots=True)
class ServerSettings:
    """Environment-driven settings for the central server."""

    postgres_dsn: str
    redis_url: str
    jwt_secret: str
    run_migrations: bool = True

    @classmethod
    def from_env(cls) -> ServerSettings:
        return cls(
            postgres_dsn=os.getenv(
                "BLUERED_POSTGRES_DSN",
                "postgresql://bluered:bluered_dev@127.0.0.1:5432/bluered",
            ),
            redis_url=os.getenv("BLUERED_REDIS_URL", "redis://127.0.0.1:6379/0"),
            jwt_secret=os.getenv("BLUERED_JWT_SECRET", "bluered-dev-secret-change-me"),
            run_migrations=os.getenv("BLUERED_RUN_MIGRATIONS", "1") == "1",
        )


async def build_server_app(settings: ServerSettings | None = None) -> FastAPI:
    """Wire PostgreSQL pool, Redis client, repositories, strategies, and services."""
    cfg = settings or ServerSettings.from_env()
    pool = await asyncpg.create_pool(dsn=cfg.postgres_dsn, min_size=2, max_size=10)
    redis_client = redis.from_url(cfg.redis_url, decode_responses=False)

    if cfg.run_migrations and _MIGRATION_PATH.is_file():
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        async with pool.acquire() as conn:
            await conn.execute(sql)

    user_repo = PostgresUserRepository(pool)
    review_repo = PostgresFileReviewRepository(pool)
    community_repo = PostgresCommunityRepository(pool)
    session_store = RedisSessionStore(redis_client)
    gateway = WebSocketClientGateway()
    password_hasher = Argon2PasswordHasher()
    token_provider = JwtTokenProvider(cfg.jwt_secret)
    routing_strategy = BloomFilterRoutingStrategy()

    services = ServerServices(
        auth=AuthService(user_repo, session_store, password_hasher, token_provider),
        presence=PresenceAndSessionService(session_store, gateway),
        search=SearchCoordinatorService(session_store, review_repo, routing_strategy, gateway),
        reputation=FileReputationService(review_repo),
        community=CommunityAndChatService(community_repo, gateway),
        gateway=gateway,
    )

    async def _shutdown() -> None:
        await redis_client.aclose()
        await pool.close()

    return create_fastapi_app(services, on_shutdown=_shutdown)
