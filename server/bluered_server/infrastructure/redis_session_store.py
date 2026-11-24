"""Redis-backed volatile ISessionStore implementation."""

from __future__ import annotations

from datetime import UTC, datetime

import redis.asyncio as redis

from bluered_server.contracts import ISessionStore
from bluered_server.domain import PeerSession

_SESSION_PREFIX = "active_sessions:"
_BLOOM_PREFIX = "peer_bloom_filters:"
_SEARCH_ROUTE_PREFIX = "active_search_routes:"


class RedisSessionStore(ISessionStore):
    """Stores active peer sessions (EX=45), Bloom filters (bytes), and search routes (EX=60)."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        default_session_ttl: int = 45,
    ) -> None:
        self._redis = client
        self._default_session_ttl = default_session_ttl

    async def upsert_session(self, session: PeerSession) -> None:
        key = f"{_SESSION_PREFIX}{session.user_id}"
        ttl = session.ttl_seconds or self._default_session_ttl
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                key,
                mapping={
                    "user_id": session.user_id,
                    "username": session.username,
                    "ip_address": session.ip_address,
                    "tcp_port": str(session.tcp_port),
                    "last_heartbeat_at": session.last_heartbeat_at.isoformat(),
                    "ttl_seconds": str(ttl),
                },
            )
            pipe.expire(key, ttl)
            if (
                session.bloom_filter_bytes is not None
                and session.bloom_size_bits is not None
                and session.bloom_hash_functions_k is not None
            ):
                bloom_key = f"{_BLOOM_PREFIX}{session.user_id}"
                pipe.hset(
                    bloom_key,
                    mapping={
                        "bitset_payload": session.bloom_filter_bytes,
                        "size_bits": str(session.bloom_size_bits),
                        "hash_functions_k": str(session.bloom_hash_functions_k),
                        "updated_at": datetime.now(UTC).isoformat(),
                    },
                )
                pipe.expire(bloom_key, ttl)
            await pipe.execute()

    async def get_session(self, user_id: str) -> PeerSession | None:
        key = f"{_SESSION_PREFIX}{user_id}"
        raw = await self._redis.hgetall(key)
        if not raw:
            return None
        bloom = await self.get_bloom_filter(user_id)
        return self._decode_session(raw, bloom)

    async def delete_session(self, user_id: str) -> None:
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.delete(f"{_SESSION_PREFIX}{user_id}")
            pipe.delete(f"{_BLOOM_PREFIX}{user_id}")
            await pipe.execute()

    async def list_active_sessions(self) -> list[PeerSession]:
        sessions: list[PeerSession] = []
        async for key in self._redis.scan_iter(match=f"{_SESSION_PREFIX}*"):
            raw = await self._redis.hgetall(key)
            if not raw:
                continue
            uid = self._decode_str(raw.get(b"user_id") or raw.get("user_id"))
            if not uid:
                continue
            bloom = await self.get_bloom_filter(uid)
            sessions.append(self._decode_session(raw, bloom))
        return sessions

    async def save_bloom_filter(
        self,
        user_id: str,
        bitset: bytes,
        size_bits: int,
        hash_functions_k: int,
    ) -> None:
        bloom_key = f"{_BLOOM_PREFIX}{user_id}"
        session_key = f"{_SESSION_PREFIX}{user_id}"
        ttl = await self._redis.ttl(session_key)
        effective_ttl = ttl if isinstance(ttl, int) and ttl > 0 else self._default_session_ttl
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                bloom_key,
                mapping={
                    "bitset_payload": bitset,
                    "size_bits": str(size_bits),
                    "hash_functions_k": str(hash_functions_k),
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
            pipe.expire(bloom_key, effective_ttl)
            await pipe.execute()

    async def get_bloom_filter(self, user_id: str) -> tuple[bytes, int, int] | None:
        bloom_key = f"{_BLOOM_PREFIX}{user_id}"
        raw = await self._redis.hgetall(bloom_key)
        if not raw:
            return None
        bitset = raw.get(b"bitset_payload") or raw.get("bitset_payload")
        size_raw = raw.get(b"size_bits") or raw.get("size_bits")
        k_raw = raw.get(b"hash_functions_k") or raw.get("hash_functions_k")
        if bitset is None or size_raw is None or k_raw is None:
            return None
        bitset_bytes = bitset.encode("latin1") if isinstance(bitset, str) else bytes(bitset)
        return (
            bitset_bytes,
            int(self._decode_str(size_raw)),
            int(self._decode_str(k_raw)),
        )

    async def register_search_route(
        self,
        search_token: str,
        requester_id: str,
        ttl_seconds: int = 60,
    ) -> None:
        key = f"{_SEARCH_ROUTE_PREFIX}{search_token}"
        await self._redis.set(key, requester_id, ex=ttl_seconds)

    async def get_search_route(self, search_token: str) -> str | None:
        key = f"{_SEARCH_ROUTE_PREFIX}{search_token}"
        val = await self._redis.get(key)
        if val is None:
            return None
        return self._decode_str(val)

    @staticmethod
    def _decode_str(value: bytes | str | None) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def _decode_session(
        self,
        raw: dict[bytes | str, bytes | str],
        bloom: tuple[bytes, int, int] | None,
    ) -> PeerSession:
        def _get(field: str) -> str:
            val = raw.get(field.encode("utf-8"))
            if val is None:
                val = raw.get(field)
            return self._decode_str(val)

        return PeerSession(
            user_id=_get("user_id"),
            username=_get("username"),
            ip_address=_get("ip_address"),
            tcp_port=int(_get("tcp_port")),
            bloom_filter_bytes=bloom[0] if bloom is not None else None,
            bloom_size_bits=bloom[1] if bloom is not None else None,
            bloom_hash_functions_k=bloom[2] if bloom is not None else None,
            last_heartbeat_at=datetime.fromisoformat(_get("last_heartbeat_at")),
            ttl_seconds=int(_get("ttl_seconds") or self._default_session_ttl),
        )
