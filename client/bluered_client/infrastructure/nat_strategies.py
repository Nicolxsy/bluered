"""OCP NAT traversal strategies implementing INatTraversalStrategy (Cas_20, Fig. 24)."""

from __future__ import annotations

import asyncio
import importlib
import time
import uuid
from collections.abc import Sequence
from typing import Any

from bluered_contracts.control_plane import NatPunchCoordinateMessage

from bluered_client.contracts import INatSignalingCoordinator, INatTraversalStrategy
from bluered_client.domain import LocalPeer, NatTraversalError, RemotePeer


class DirectSocketNatStrategy:
    """Direct TCP connection strategy for LAN, loopback, and Docker networks."""

    def __init__(self, *, connect_timeout_s: float = 5.0) -> None:
        if connect_timeout_s <= 0:
            raise ValueError("connect_timeout_s must be positive")
        self._connect_timeout_s = connect_timeout_s

    @property
    def strategy_name(self) -> str:
        return "direct"

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        del local_peer
        try:
            async with asyncio.timeout(self._connect_timeout_s):
                return await asyncio.open_connection(
                    remote_peer.ip_address,
                    remote_peer.tcp_port,
                )
        except (OSError, TimeoutError) as exc:
            raise NatTraversalError(
                f"Direct TCP connection to {remote_peer.ip_address}:{remote_peer.tcp_port} "
                f"failed: {exc}"
            ) from exc


class UpnpNatStrategy:
    """UPnP IGD port-mapping strategy via optional miniupnpc with clean degradation."""

    def __init__(
        self,
        *,
        connect_timeout_s: float = 5.0,
        discover_timeout_s: float = 2.0,
    ) -> None:
        self._connect_timeout_s = connect_timeout_s
        self._discover_timeout_s = discover_timeout_s

    @property
    def strategy_name(self) -> str:
        return "upnp"

    @staticmethod
    def _open_upnp_mapping(local_port: int) -> bool:
        try:
            miniupnpc: Any = importlib.import_module("miniupnpc")
        except ImportError:
            return False
        upnp = miniupnpc.UPnP()
        upnp.discoverdelay = 200
        discovered = int(upnp.discover())
        if discovered <= 0:
            return False
        upnp.selectigd()
        upnp.addportmapping(
            local_port,
            "TCP",
            upnp.lanaddr,
            local_port,
            "BlueRed P2P",
            "",
        )
        return True

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            async with asyncio.timeout(self._discover_timeout_s):
                mapped = await asyncio.to_thread(self._open_upnp_mapping, local_peer.tcp_port)
        except Exception as exc:
            raise NatTraversalError(f"UPnP IGD discovery failed: {exc}") from exc

        if not mapped:
            raise NatTraversalError("UPnP IGD router or miniupnpc module unavailable")

        try:
            async with asyncio.timeout(self._connect_timeout_s):
                return await asyncio.open_connection(
                    remote_peer.ip_address,
                    remote_peer.tcp_port,
                )
        except (OSError, TimeoutError) as exc:
            raise NatTraversalError(f"UPnP-mapped TCP connection failed: {exc}") from exc


class TcpHolePunchingStrategy:
    """Coordinated TCP hole punching via IServerCoordinator signaling (Cas_20)."""

    def __init__(
        self,
        coordinator: INatSignalingCoordinator | None = None,
        *,
        timeout_s: float = 5.0,
    ) -> None:
        self._coordinator = coordinator
        self._timeout_s = timeout_s

    @property
    def strategy_name(self) -> str:
        return "tcp_hole_punching"

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._coordinator is None:
            raise NatTraversalError("TcpHolePunchingStrategy requires an INatSignalingCoordinator")

        loop = asyncio.get_running_loop()
        coord_future: asyncio.Future[NatPunchCoordinateMessage] = loop.create_future()

        async def _on_nat_signal(signal_msg: Any) -> None:
            if (
                isinstance(signal_msg, NatPunchCoordinateMessage)
                and signal_msg.peer_id == remote_peer.peer_id
                and not coord_future.done()
            ):
                coord_future.set_result(signal_msg)

        self._coordinator.set_nat_signal_handler(_on_nat_signal)
        try:
            async with asyncio.timeout(self._timeout_s):
                await self._coordinator.request_nat_punch(
                    remote_peer.peer_id,
                    local_peer.ip_address,
                    local_peer.tcp_port,
                )
                coord = await coord_future
                now_ms = int(time.time() * 1000)
                if coord.rendezvous_at_epoch_ms > now_ms:
                    delay_s = min((coord.rendezvous_at_epoch_ms - now_ms) / 1000.0, self._timeout_s)
                    await asyncio.sleep(delay_s)
                return await asyncio.open_connection(coord.peer_ip, coord.peer_port)
        except (OSError, TimeoutError) as exc:
            raise NatTraversalError(
                f"TCP hole punching to peer '{remote_peer.peer_id}' failed: {exc}"
            ) from exc


class RelayNatStrategy:
    """Fallback relay strategy using NAT_RELAY_REQUEST signaling when direct TCP fails."""

    def __init__(
        self,
        coordinator: INatSignalingCoordinator | None = None,
        *,
        relay_host: str | None = None,
        relay_port: int | None = None,
        timeout_s: float = 5.0,
    ) -> None:
        self._coordinator = coordinator
        self._relay_host = relay_host
        self._relay_port = relay_port
        self._timeout_s = timeout_s

    @property
    def strategy_name(self) -> str:
        return "relay"

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        del local_peer
        if self._coordinator is not None:
            try:
                async with asyncio.timeout(self._timeout_s):
                    await self._coordinator.request_nat_relay(
                        remote_peer.peer_id,
                        uuid.uuid4().hex,
                    )
            except (OSError, TimeoutError, RuntimeError) as exc:
                raise NatTraversalError(f"Relay signaling failed: {exc}") from exc

        if self._relay_host is None or self._relay_port is None:
            raise NatTraversalError("No relay endpoint configured for RelayNatStrategy")

        try:
            async with asyncio.timeout(self._timeout_s):
                return await asyncio.open_connection(self._relay_host, self._relay_port)
        except (OSError, TimeoutError) as exc:
            raise NatTraversalError(
                f"Relay TCP connection to {self._relay_host}:{self._relay_port} failed: {exc}"
            ) from exc


class CompositeNatTraversalStrategy:
    """Extensible chain of INatTraversalStrategy instances (OCP, Cas_20)."""

    def __init__(self, strategies: Sequence[INatTraversalStrategy]) -> None:
        if not strategies:
            raise ValueError("CompositeNatTraversalStrategy requires at least one strategy")
        self._strategies = list(strategies)
        self._last_used_strategy: str | None = None

    @property
    def strategy_name(self) -> str:
        return "composite"

    @property
    def last_used_strategy(self) -> str | None:
        return self._last_used_strategy

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        errors: list[str] = []
        for strategy in self._strategies:
            try:
                streams = await strategy.establish_connection(local_peer, remote_peer)
                self._last_used_strategy = strategy.strategy_name
                return streams
            except NatTraversalError as exc:
                errors.append(f"{strategy.strategy_name}: {exc}")
        raise NatTraversalError(
            f"All NAT traversal strategies failed for peer '{remote_peer.peer_id}': "
            + "; ".join(errors)
        )
