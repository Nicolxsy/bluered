"""End-to-end integration tests for the BlueRed WebSocket Control Plane (Phase 2 + Phase 3)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest
import uvicorn
from bluered_client.application import DiscoveryAndReviewService
from bluered_client.infrastructure import (
    BloomFilterRepositoryDecorator,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
    WebSocketServerCoordinator,
)
from bluered_contracts.control_plane import SearchBroadcastMessage, SearchHitItem
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
    WebSocketClientGateway,
)
from bluered_server.main import ServerSettings, build_server_app
from bluered_server.presentation import ServerServices, create_fastapi_app
from fastapi import FastAPI

from tests.unit.server.fakes import (
    InMemoryCommunityRepository,
    InMemoryFileReviewRepository,
    InMemorySessionStore,
    InMemoryUserRepository,
)


def _is_tcp_reachable(host: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _build_inmemory_app() -> FastAPI:
    user_repo = InMemoryUserRepository()
    review_repo = InMemoryFileReviewRepository()
    community_repo = InMemoryCommunityRepository()
    session_store = InMemorySessionStore()
    gateway = WebSocketClientGateway()
    password_hasher = Argon2PasswordHasher()
    token_provider = JwtTokenProvider("bluered-e2e-test-secret-key-32-bytes-min")
    routing_strategy = BloomFilterRoutingStrategy()

    services = ServerServices(
        auth=AuthService(user_repo, session_store, password_hasher, token_provider),
        presence=PresenceAndSessionService(session_store, gateway),
        search=SearchCoordinatorService(
            session_store,
            review_repo,
            routing_strategy,
            gateway,
            default_timeout_seconds=1.5,
        ),
        reputation=FileReputationService(review_repo),
        community=CommunityAndChatService(community_repo, gateway),
        gateway=gateway,
    )
    return create_fastapi_app(services)


@asynccontextmanager
async def _running_uvicorn_server() -> AsyncIterator[tuple[str, int, bool]]:
    """Start the real FastAPI app on 127.0.0.1 with an ephemeral port."""
    pg_port_default = os.getenv("BLUERED_PG_HOST_PORT", "5433")
    pg_dsn = os.getenv(
        "BLUERED_POSTGRES_DSN",
        f"postgresql://bluered:bluered_dev@127.0.0.1:{pg_port_default}/bluered",
    )
    redis_url = os.getenv("BLUERED_REDIS_URL", "redis://127.0.0.1:6379/0")
    parsed_pg = urlparse(pg_dsn)
    parsed_redis = urlparse(redis_url)
    use_real_db = _is_tcp_reachable(
        parsed_pg.hostname or "127.0.0.1", parsed_pg.port or 5433
    ) and _is_tcp_reachable(
        parsed_redis.hostname or "127.0.0.1", parsed_redis.port or 6379
    )
    app = (
        await build_server_app(
            ServerSettings(
                postgres_dsn=pg_dsn,
                redis_url=redis_url,
                jwt_secret=os.getenv(
                    "BLUERED_JWT_SECRET",
                    "bluered-e2e-test-secret-key-32-bytes-min",
                ),
                run_migrations=True,
            )
        )
        if use_real_db
        else _build_inmemory_app()
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    sock.setblocking(False)
    host, port = sock.getsockname()

    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]), name="bluered-uvicorn-e2e")
    try:
        while not server.started:
            if task.done():
                await task
            await asyncio.sleep(0.02)
        yield (host, port, use_real_db)
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5.0)
        sock.close()


@pytest.mark.asyncio
async def test_control_plane_end_to_end(tmp_path: Path) -> None:
    """Verify auth, Bloom sync, O(1) negative reject, federated search, and review UPSERT."""
    async with _running_uvicorn_server() as (host, port, _use_real_db):
        repo_a = SqliteFileRepository(tmp_path / "peer_a.db")
        repo_b = SqliteFileRepository(tmp_path / "peer_b.db")
        await repo_a.initialize()
        await repo_b.initialize()

        bloom_a = BloomFilterRepositoryDecorator(repo_a)
        bloom_b = BloomFilterRepositoryDecorator(repo_b)

        coord_a = WebSocketServerCoordinator(auto_reconnect=False, request_timeout_s=5.0)
        coord_b = WebSocketServerCoordinator(auto_reconnect=False, request_timeout_s=5.0)

        service_a = DiscoveryAndReviewService(coord_a, bloom_a, Sha256IntegrityVerifier())
        service_b = DiscoveryAndReviewService(coord_b, bloom_b, Sha256IntegrityVerifier())

        try:
            # Verify SQLite WAL mode is active on both peers
            conn_a = await repo_a._get_conn()
            async with conn_a.execute("PRAGMA journal_mode;") as cursor:
                mode_row = await cursor.fetchone()
            assert mode_row is not None
            assert str(mode_row[0]).lower() == "wal"

            await coord_a.connect(host, port)
            await coord_b.connect(host, port)

            # a) Register & login of Peer A and Peer B
            suffix = uuid4().hex[:8]
            user_a_name = f"peer_a_{suffix}"
            user_b_name = f"peer_b_{suffix}"
            peer_a_port = 19101
            peer_b_port = 19102

            reg_a = await coord_a.register(user_a_name, "StrongPassA123!")
            uid_a, token_a = await coord_a.login(user_a_name, "StrongPassA123!", peer_a_port)
            assert uid_a == reg_a
            assert token_a

            reg_b = await coord_b.register(user_b_name, "StrongPassB123!")
            uid_b, token_b = await coord_b.login(user_b_name, "StrongPassB123!", peer_b_port)
            assert uid_b == reg_b
            assert token_b

            async def _on_broadcast_a(msg: SearchBroadcastMessage) -> list[SearchHitItem]:
                local_matches = await service_a.handle_incoming_search_broadcast(msg.query)
                return [
                    SearchHitItem(
                        file_hash=sf.file_hash,
                        filename=sf.file_name,
                        size_bytes=sf.size_bytes,
                        peer_id=uid_a,
                        peer_username=user_a_name,
                        peer_ip="127.0.0.1",
                        peer_port=peer_a_port,
                    )
                    for sf in local_matches
                ]

            coord_a.set_search_broadcast_handler(_on_broadcast_a)

            # b) Indexing local files on Peer A + automatic Bloom filter sync
            shared_dir_a = tmp_path / "shared_a"
            shared_dir_a.mkdir()
            sample_path = shared_dir_a / "quantum_computing_notes.pdf"
            sample_bytes = (
                f"BlueRed E2E test payload for quantum computing notes ({suffix}).\n".encode()
            )
            sample_path.write_bytes(sample_bytes)
            expected_hash = hashlib.sha256(sample_bytes).hexdigest()

            indexed_count = await service_a.configure_and_index([shared_dir_a], user_id=uid_a)
            assert indexed_count == 1
            await asyncio.sleep(0.05)

            # c) Negative search query rejected in O(1) by Bloom filter
            bloom_a.reset_metrics()
            neg_remote = await service_b.execute_global_search("xylophone_nonexistent_999")
            assert neg_remote == []

            neg_local = await service_a.handle_incoming_search_broadcast(
                "xylophone_nonexistent_999"
            )
            assert neg_local == []
            assert bloom_a.bloom_rejects >= 1
            assert bloom_a.sqlite_queries == 0
            assert bloom_a.bloom_hits == 0

            # d) Positive federated search from Peer B returning Peer A's file
            bloom_a.reset_metrics()
            pos_results = await service_b.execute_global_search("quantum")
            assert len(pos_results) == 1
            hit = pos_results[0]
            assert hit.file.file_hash == expected_hash
            assert hit.file.file_name == "quantum_computing_notes.pdf"
            assert hit.file.size_bytes == len(sample_bytes)
            assert hit.peer.peer_id == uid_a
            assert hit.peer.tcp_port == peer_a_port
            assert hit.review_count == 0
            assert hit.avg_rating is None
            assert bloom_a.bloom_hits >= 1
            assert bloom_a.sqlite_queries >= 1

            # e) Review publish + UPSERT update (Cas_14 / Cas_24)
            await service_b.submit_file_review(expected_hash, 3, "Initial solid draft")
            await service_a.submit_file_review(expected_hash, 5, "Author verified checksum")

            reviews_1, avg_1, count_1 = await service_b.get_file_reputation(expected_hash)
            assert count_1 == 2
            assert len(reviews_1) == 2
            assert avg_1 == pytest.approx(4.0)

            # Peer B updates its review (UPSERT -> count stays 2, avg becomes 5.0)
            await service_b.submit_file_review(expected_hash, 5, "Updated after full read")

            reviews_2, avg_2, count_2 = await service_b.get_file_reputation(expected_hash)
            assert count_2 == 2
            assert len(reviews_2) == 2
            assert avg_2 == pytest.approx(5.0)

            # Federated search now returns enriched reputation metadata from file_catalog
            enriched_results = await service_b.execute_global_search("quantum")
            assert len(enriched_results) == 1
            assert enriched_results[0].review_count == 2
            assert enriched_results[0].avg_rating == pytest.approx(5.0)
        finally:
            await coord_a.disconnect()
            await coord_b.disconnect()
            await repo_a.close()
            await repo_b.close()
