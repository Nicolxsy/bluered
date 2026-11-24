"""Unit tests for OCP NAT traversal strategies (Cas_20, Fig. 24)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from bluered_client.contracts import INatTraversalStrategy
from bluered_client.domain import LocalPeer, NatTraversalError, PresenceStatus, RemotePeer
from bluered_client.infrastructure import (
    CompositeNatTraversalStrategy,
    DirectSocketNatStrategy,
    RelayNatStrategy,
    TcpHolePunchingStrategy,
    UpnpNatStrategy,
)
from bluered_contracts.control_plane import NatPunchCoordinateMessage


class _FakeNatCoordinator:
    def __init__(self, *, emit_coordinate: bool = True, peer_port: int = 0) -> None:
        self._handler: Callable[[Any], Awaitable[None]] | None = None
        self._emit_coordinate = emit_coordinate
        self._peer_port = peer_port
        self.punch_requests: list[tuple[str, str, int]] = []
        self.relay_requests: list[tuple[str, str]] = []

    def set_nat_signal_handler(self, handler: Callable[[Any], Awaitable[None]]) -> None:
        self._handler = handler

    async def request_nat_punch(
        self, target_peer_id: str, requester_ip: str, requester_port: int
    ) -> None:
        self.punch_requests.append((target_peer_id, requester_ip, requester_port))
        if self._emit_coordinate and self._handler is not None:
            await self._handler(
                NatPunchCoordinateMessage(
                    peer_id=target_peer_id,
                    peer_ip="127.0.0.1",
                    peer_port=self._peer_port,
                    rendezvous_at_epoch_ms=0,
                )
            )

    async def request_nat_relay(self, target_peer_id: str, session_id: str) -> None:
        self.relay_requests.append((target_peer_id, session_id))


def _local_peer() -> LocalPeer:
    return LocalPeer(peer_id="local-1", username="alice", ip_address="127.0.0.1", tcp_port=9001)


def _remote_peer(port: int) -> RemotePeer:
    return RemotePeer(
        peer_id="remote-1",
        username="bob",
        ip_address="127.0.0.1",
        tcp_port=port,
        status=PresenceStatus.ONLINE,
    )


async def test_direct_socket_strategy_connects_and_fails_cleanly() -> None:
    async def _noop_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        del reader
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_noop_handler, "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    try:
        strategy: INatTraversalStrategy = DirectSocketNatStrategy(connect_timeout_s=2.0)
        reader, writer = await strategy.establish_connection(_local_peer(), _remote_peer(port))
        del reader
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()

    with pytest.raises(NatTraversalError):
        await DirectSocketNatStrategy(connect_timeout_s=0.5).establish_connection(
            _local_peer(), _remote_peer(port)
        )


async def test_upnp_strategy_degrades_cleanly_when_unavailable() -> None:
    strategy = UpnpNatStrategy(discover_timeout_s=0.3)
    with pytest.raises(NatTraversalError):
        await strategy.establish_connection(_local_peer(), _remote_peer(65432))


async def test_tcp_hole_punching_strategy_success_and_timeout() -> None:
    async def _noop_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        del reader
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_noop_handler, "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    try:
        coord = _FakeNatCoordinator(emit_coordinate=True, peer_port=port)
        strategy = TcpHolePunchingStrategy(coord, timeout_s=2.0)
        reader, writer = await strategy.establish_connection(_local_peer(), _remote_peer(port))
        del reader
        writer.close()
        await writer.wait_closed()
        assert len(coord.punch_requests) == 1
    finally:
        server.close()
        await server.wait_closed()

    timeout_coord = _FakeNatCoordinator(emit_coordinate=False, peer_port=port)
    with pytest.raises(NatTraversalError):
        await TcpHolePunchingStrategy(timeout_coord, timeout_s=0.1).establish_connection(
            _local_peer(), _remote_peer(port)
        )


async def test_relay_and_composite_nat_fallback_chain() -> None:
    async def _noop_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        del reader
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_noop_handler, "127.0.0.1", 0)
    relay_port = int(server.sockets[0].getsockname()[1])
    try:
        coord = _FakeNatCoordinator(emit_coordinate=False)
        composite = CompositeNatTraversalStrategy(
            [
                UpnpNatStrategy(discover_timeout_s=0.2),
                TcpHolePunchingStrategy(coord, timeout_s=0.1),
                RelayNatStrategy(
                    coord,
                    relay_host="127.0.0.1",
                    relay_port=relay_port,
                    timeout_s=2.0,
                ),
            ]
        )
        reader, writer = await composite.establish_connection(
            _local_peer(), _remote_peer(65431)
        )
        del reader
        writer.close()
        await writer.wait_closed()
        assert composite.last_used_strategy == "relay"
        assert len(coord.relay_requests) == 1
    finally:
        server.close()
        await server.wait_closed()

    failing = CompositeNatTraversalStrategy([RelayNatStrategy(None)])
    with pytest.raises(NatTraversalError):
        await failing.establish_connection(_local_peer(), _remote_peer(65431))
