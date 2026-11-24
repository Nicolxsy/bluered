"""Presence, heartbeat, Bloom filter sync, and NAT signaling service (Cas_08, Cas_20)."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

from bluered_server.contracts import IClientPushGateway, ISessionStore
from bluered_server.domain import PeerSession

from .errors import NotFoundError, ValidationError

_PROTOCOL_VERSION = "1.0"


class PresenceAndSessionService:
    """Manages peer sessions, ONLINE/AWAY/OFFLINE status, Bloom sync, and NAT signaling."""

    def __init__(
        self,
        session_store: ISessionStore,
        push_gateway: IClientPushGateway,
        *,
        session_ttl_seconds: int = 45,
        away_threshold_seconds: int = 30,
    ) -> None:
        self._session_store = session_store
        self._push_gateway = push_gateway
        self._session_ttl_seconds = session_ttl_seconds
        self._away_threshold_seconds = away_threshold_seconds

    async def register_session(
        self,
        user_id: str,
        username: str,
        ip_address: str,
        tcp_port: int,
        *,
        now: datetime | None = None,
    ) -> PeerSession:
        """Register or refresh an active peer session and broadcast PRESENCE_UPDATE."""
        session = PeerSession(
            user_id=user_id,
            username=username,
            ip_address=ip_address,
            tcp_port=tcp_port,
            last_heartbeat_at=now or datetime.now(UTC),
            ttl_seconds=self._session_ttl_seconds,
        )
        await self._session_store.upsert_session(session)
        await self._broadcast_presence(session, online=True)
        return session

    async def heartbeat(
        self,
        user_id: str,
        *,
        tcp_port: int | None = None,
        ip_address: str | None = None,
        now: datetime | None = None,
    ) -> PeerSession:
        """Refresh a peer's session TTL on heartbeat."""
        session = await self._session_store.get_session(user_id)
        if session is None:
            raise NotFoundError(f"Active session not found for user '{user_id}'")

        if ip_address is not None:
            session.ip_address = ip_address
        if tcp_port is not None:
            if isinstance(tcp_port, bool) or not (1 <= tcp_port <= 65535):
                raise ValidationError(f"tcp_port must be between 1 and 65535, got {tcp_port}")
            session.tcp_port = tcp_port

        session.refresh_heartbeat(now=now or datetime.now(UTC))
        await self._session_store.upsert_session(session)
        return session

    async def get_presence_status(
        self,
        user_id: str,
        *,
        now: datetime | None = None,
    ) -> str:
        """Return ONLINE, AWAY, or OFFLINE based on heartbeat recency and 45s TTL."""
        current = now or datetime.now(UTC)
        session = await self._session_store.get_session(user_id)
        if session is None or session.is_expired(now=current):
            return "OFFLINE"
        elapsed = (current - session.last_heartbeat_at).total_seconds()
        if elapsed >= self._away_threshold_seconds:
            return "AWAY"
        return "ONLINE"

    async def sync_bloom_filter(
        self,
        user_id: str,
        bitset: bytes,
        size_bits: int,
        hash_functions_k: int,
    ) -> None:
        """Persist a peer's Bloom filter bitset in volatile session storage."""
        session = await self._session_store.get_session(user_id)
        if session is None:
            raise NotFoundError(f"Active session not found for user '{user_id}'")
        expected_bytes = (size_bits + 7) // 8
        if len(bitset) != expected_bytes:
            raise ValidationError(
                f"bitset length {len(bitset)} does not match size_bits {size_bits}"
            )
        await self._session_store.save_bloom_filter(
            user_id=user_id,
            bitset=bitset,
            size_bits=size_bits,
            hash_functions_k=hash_functions_k,
        )

    async def disconnect(self, user_id: str) -> None:
        """Remove active session and Bloom filter, then notify online peers."""
        session = await self._session_store.get_session(user_id)
        await self._session_store.delete_session(user_id)
        if session is not None:
            await self._broadcast_presence(session, online=False)

    async def coordinate_nat_punch(
        self,
        requester_id: str,
        target_peer_id: str,
        requester_ip: str,
        requester_port: int,
        *,
        rendezvous_delay_ms: int = 500,
    ) -> bool:
        """Send synchronized NAT_PUNCH_COORDINATE messages to both peers (Cas_20)."""
        target = await self._session_store.get_session(target_peer_id)
        if target is None or target.is_expired():
            return False

        rendezvous_ms = int(time.time() * 1000) + rendezvous_delay_ms
        to_target = json.dumps(
            {
                "protocol_version": _PROTOCOL_VERSION,
                "type": "NAT_PUNCH_COORDINATE",
                "peer_id": requester_id,
                "peer_ip": requester_ip,
                "peer_port": requester_port,
                "rendezvous_at_epoch_ms": rendezvous_ms,
            }
        )
        to_requester = json.dumps(
            {
                "protocol_version": _PROTOCOL_VERSION,
                "type": "NAT_PUNCH_COORDINATE",
                "peer_id": target.user_id,
                "peer_ip": target.ip_address,
                "peer_port": target.tcp_port,
                "rendezvous_at_epoch_ms": rendezvous_ms,
            }
        )
        sent_target = await self._push_gateway.send_to_peer(target_peer_id, to_target)
        sent_requester = await self._push_gateway.send_to_peer(requester_id, to_requester)
        return sent_target and sent_requester

    async def request_nat_relay(
        self,
        requester_id: str,
        target_peer_id: str,
        session_id: str,
    ) -> bool:
        """Forward a NAT_RELAY_REQUEST signaling packet to the target peer (Cas_20)."""
        target = await self._session_store.get_session(target_peer_id)
        if target is None or target.is_expired():
            return False

        payload = json.dumps(
            {
                "protocol_version": _PROTOCOL_VERSION,
                "type": "NAT_RELAY_REQUEST",
                "target_peer_id": requester_id,
                "session_id": session_id,
            }
        )
        return await self._push_gateway.send_to_peer(target_peer_id, payload)

    async def _broadcast_presence(self, session: PeerSession, *, online: bool) -> None:
        active = await self._session_store.list_active_sessions()
        recipients = [peer.user_id for peer in active if peer.user_id != session.user_id]
        if not recipients:
            return
        payload = json.dumps(
            {
                "protocol_version": _PROTOCOL_VERSION,
                "type": "PRESENCE_UPDATE",
                "user_id": session.user_id,
                "username": session.username,
                "online": online,
                "ip": session.ip_address if online else None,
                "tcp_port": session.tcp_port if online else None,
            }
        )
        await self._push_gateway.broadcast(recipients, payload)
