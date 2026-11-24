"""E2E smoke test against the live 6-container BlueRed Docker Compose stack.

Verifies:
1. All 6 services (postgres, redis, bluered-server, peer-alice, peer-bob,
   peer-carol) are healthy and authenticated.
2. Global federated search from peer-carol discovers a file shared by peer-alice.
3. Real P2P binary TCP download peer-alice -> peer-carol completes with exact
   SHA-256 and byte-length integrity.
4. Review submission from peer-carol persists in PostgreSQL (``file_reviews``
   and denormalized ``file_catalog``) and is returned by ``GET /api/reviews``.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
from pathlib import Path
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import httpx
import pytest
import redis.asyncio as redis

SERVER_URL = os.getenv("BLUERED_SERVER_URL", "http://127.0.0.1:8000")
ALICE_URL = os.getenv("BLUERED_ALICE_URL", "http://127.0.0.1:8001")
BOB_URL = os.getenv("BLUERED_BOB_URL", "http://127.0.0.1:8002")
CAROL_URL = os.getenv("BLUERED_CAROL_URL", "http://127.0.0.1:8003")
PG_DSN = os.getenv(
    "BLUERED_POSTGRES_DSN",
    "postgresql://bluered:bluered_dev@127.0.0.1:5433/bluered",
)
REDIS_URL = os.getenv("BLUERED_REDIS_URL", "redis://127.0.0.1:6379/0")
REQUIRE_DOCKER = os.getenv("BLUERED_REQUIRE_DOCKER", "0") == "1"

ALICE_SAMPLE_FILE = (
    Path(__file__).resolve().parents[2]
    / "docker"
    / "samples"
    / "alice"
    / "arquitectura_bluered_guia.txt"
)


async def _ensure_stack_reachable(client: httpx.AsyncClient) -> None:
    """Skip when Docker stack is not running locally unless explicitly required."""
    try:
        resp = await client.get(f"{SERVER_URL}/health", timeout=3.0)
        if resp.status_code != 200:
            raise RuntimeError(f"Unexpected status {resp.status_code} from {SERVER_URL}/health")
    except Exception as exc:
        if REQUIRE_DOCKER:
            pytest.fail(f"Docker stack not reachable at {SERVER_URL}: {exc}")
        pytest.skip(f"BlueRed Docker stack not reachable at {SERVER_URL}: {exc}")


async def _wait_peer_ready(
    client: httpx.AsyncClient,
    base_url: str,
    expected_username: str,
    *,
    min_indexed_files: int = 0,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """Poll peer /health until connected, authenticated, and indexed."""
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] = {}
    last_error: str | None = None

    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{base_url}/health", timeout=3.0)
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, dict):
                    last_payload = payload
                    if (
                        payload.get("status") == "ok"
                        and payload.get("peer_username") == expected_username
                        and payload.get("connected") is True
                        and payload.get("authenticated") is True
                        and int(payload.get("indexed_files", 0)) >= min_indexed_files
                    ):
                        return payload
        except Exception as exc:
            last_error = str(exc)
        await asyncio.sleep(0.5)

    raise AssertionError(
        f"Peer {expected_username} at {base_url} did not become ready within "
        f"{timeout_seconds}s. last_payload={last_payload!r}, last_error={last_error!r}"
    )


async def _wait_transfer_terminal(
    client: httpx.AsyncClient,
    base_url: str,
    task_id: str,
    *,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """Poll GET /api/transfers/{task_id} until transfer reaches a terminal state."""
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] = {}
    while time.monotonic() < deadline:
        resp = await client.get(f"{base_url}/api/transfers/{task_id}", timeout=5.0)
        assert resp.status_code == 200, f"GET transfer failed: {resp.status_code} {resp.text}"
        payload = resp.json()
        assert isinstance(payload, dict)
        last_payload = payload
        if payload.get("state") in {"COMPLETED", "FAILED", "CANCELLED"}:
            return payload
        await asyncio.sleep(0.3)

    raise AssertionError(
        f"Transfer {task_id} did not finish within {timeout_seconds}s: {last_payload!r}"
    )


@pytest.mark.asyncio
async def test_docker_multi_peer_smoke_e2e() -> None:
    """Full end-to-end smoke test across the 6-service Docker Compose stack."""
    assert ALICE_SAMPLE_FILE.is_file(), f"Missing sample file: {ALICE_SAMPLE_FILE}"
    expected_bytes = ALICE_SAMPLE_FILE.read_bytes()
    expected_size = len(expected_bytes)
    expected_sha256 = hashlib.sha256(expected_bytes).hexdigest()

    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Verify all 6 services are healthy
        await _ensure_stack_reachable(client)

        server_health = await client.get(f"{SERVER_URL}/health")
        assert server_health.status_code == 200
        assert server_health.json().get("status") == "ok"

        pg_conn = await asyncpg.connect(dsn=PG_DSN, timeout=5.0)
        try:
            one = await pg_conn.fetchval("SELECT 1")
            assert one == 1
        finally:
            await pg_conn.close()

        redis_client = redis.from_url(REDIS_URL, decode_responses=False, socket_connect_timeout=5.0)
        try:
            assert await redis_client.ping() is True
            alice_health = await _wait_peer_ready(
                client, ALICE_URL, "alice", min_indexed_files=1
            )
            bob_health = await _wait_peer_ready(
                client, BOB_URL, "bob", min_indexed_files=1
            )
            carol_health = await _wait_peer_ready(
                client, CAROL_URL, "carol", min_indexed_files=0
            )
            assert alice_health["indexed_files"] >= 1
            assert bob_health["indexed_files"] >= 1
            assert carol_health["authenticated"] is True

            # Verify Polyglot DB invariant: active Redis sessions have positive TTL
            session_keys = await redis_client.keys("active_sessions:*")
            assert len(session_keys) >= 3, f"Expected >=3 active sessions in Redis: {session_keys}"
            for key in session_keys:
                ttl = await redis_client.ttl(key)
                assert ttl > 0, f"Redis session key {key!r} must have TTL > 0, got {ttl}"
        finally:
            await redis_client.aclose()

        # 2. Global federated search from peer-carol finding peer-alice's file
        search_resp = await client.get(
            f"{CAROL_URL}/api/search",
            params={"q": "arquitectura", "max_results": 20},
        )
        assert search_resp.status_code == 200, (
            f"Search failed: {search_resp.status_code} {search_resp.text}"
        )
        search_data = search_resp.json()
        results: list[dict[str, Any]] = search_data.get("results", [])
        matching_hits = [
            hit
            for hit in results
            if hit.get("file_name") == ALICE_SAMPLE_FILE.name
            and hit.get("peer_username") == "alice"
        ]
        assert matching_hits, (
            f"Expected {ALICE_SAMPLE_FILE.name} from alice in search results: {search_data!r}"
        )
        target_hit = matching_hits[0]
        assert target_hit["file_hash"] == expected_sha256
        assert int(target_hit["size_bytes"]) == expected_size

        # 3. Real P2P download peer-alice -> peer-carol with SHA-256 verification
        download_payload = {
            "peer_id": target_hit["peer_id"],
            "peer_username": target_hit["peer_username"],
            "peer_ip": target_hit["peer_ip"],
            "peer_port": int(target_hit["peer_port"]),
            "file_hash": target_hit["file_hash"],
            "file_name": target_hit["file_name"],
            "size_bytes": int(target_hit["size_bytes"]),
            "conflict_policy": "keep_both",
            "wait": True,
        }
        download_resp = await client.post(
            f"{CAROL_URL}/api/transfers/download",
            json=download_payload,
        )
        assert download_resp.status_code in {200, 201, 202}, (
            f"Download failed: {download_resp.status_code} {download_resp.text}"
        )
        transfer = download_resp.json()
        if transfer.get("state") not in {"COMPLETED", "FAILED", "CANCELLED"}:
            transfer = await _wait_transfer_terminal(
                client, CAROL_URL, str(transfer["task_id"])
            )

        assert transfer.get("state") == "COMPLETED", f"Transfer did not complete: {transfer!r}"
        assert transfer.get("file_hash") == expected_sha256
        assert int(transfer.get("total_bytes", -1)) == expected_size
        assert int(transfer.get("bytes_transferred", -1)) == expected_size
        assert transfer.get("target_path"), f"Missing target_path in transfer: {transfer!r}"

        # 4. Publish review from peer-carol and verify real persistence in PostgreSQL
        review_comment = "Excelente guia de arquitectura BlueRed verificada en E2E Docker"
        review_resp = await client.post(
            f"{CAROL_URL}/api/reviews/{expected_sha256}",
            json={"rating": 5, "comment": review_comment},
        )
        assert review_resp.status_code in {200, 201}, (
            f"Review submission failed: {review_resp.status_code} {review_resp.text}"
        )

        reviews_get = await client.get(f"{CAROL_URL}/api/reviews/{expected_sha256}")
        assert reviews_get.status_code == 200
        reviews_payload = reviews_get.json()
        assert int(reviews_payload.get("review_count", 0)) >= 1
        assert float(reviews_payload.get("avg_rating", 0.0)) > 0.0

        pg_conn = await asyncpg.connect(dsn=PG_DSN, timeout=5.0)
        try:
            catalog_row = await pg_conn.fetchrow(
                "SELECT review_count, rating_sum, avg_rating "
                "FROM file_catalog WHERE file_hash = $1",
                expected_sha256,
            )
            assert catalog_row is not None, "Expected file_catalog row in PostgreSQL"
            assert int(catalog_row["review_count"]) >= 1
            assert int(catalog_row["rating_sum"]) >= 5
            assert float(catalog_row["avg_rating"]) > 0.0

            review_row = await pg_conn.fetchrow(
                "SELECT author_username, rating, comment "
                "FROM file_reviews WHERE file_hash = $1 AND author_username = $2",
                expected_sha256,
                "carol",
            )
            assert review_row is not None, "Expected carol's review row in PostgreSQL file_reviews"
            assert int(review_row["rating"]) == 5
            assert str(review_row["comment"]) == review_comment
        finally:
            await pg_conn.close()
