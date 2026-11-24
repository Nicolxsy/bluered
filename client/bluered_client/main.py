"""Composition Root for the BlueRed P2P Client Node."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import uvicorn
from bluered_contracts.control_plane import (
    PresenceUpdateMessage,
    RoomMessage,
    SearchBroadcastMessage,
    SearchHitItem,
)
from fastapi import FastAPI

from bluered_client.application import (
    CommunityClientService,
    DiscoveryAndReviewService,
    PeerUploadListener,
    PrivateChatService,
    TransferOrchestrator,
)
from bluered_client.domain import (
    LocalPeer,
    PeerBlockedError,
    PresenceStatus,
    PrivateMessage,
    RemotePeer,
    TransferState,
    TransferTask,
)
from bluered_client.infrastructure import (
    BloomFilterRepositoryDecorator,
    CompositeNatTraversalStrategy,
    ControlPlaneError,
    DirectSocketNatStrategy,
    FairShareQueuePolicy,
    RawTcpPeerTransport,
    RawTcpUploadServer,
    RelayNatStrategy,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
    TcpHolePunchingStrategy,
    WebSocketServerCoordinator,
)
from bluered_client.presentation import (
    ClientAppContext,
    WebSocketTransferEventListener,
    create_client_app,
)

logger = logging.getLogger("bluered.client")


@dataclass(frozen=True, slots=True)
class ClientSettings:
    """Environment-driven settings for a BlueRed client peer."""

    peer_username: str
    peer_password: str
    peer_tcp_port: int
    peer_bind_host: str
    peer_advertised_host: str
    ui_host: str
    ui_port: int
    shared_dir: Path
    download_dir: Path
    db_path: Path
    max_upload_slots: int
    server_ws_url: str
    auto_login: bool

    @classmethod
    def from_env(cls) -> ClientSettings:
        username = os.getenv("PEER_USERNAME", "peer")
        return cls(
            peer_username=username,
            peer_password=os.getenv("PEER_PASSWORD", "bluered123"),
            peer_tcp_port=int(os.getenv("PEER_TCP_PORT", "2234")),
            peer_bind_host=os.getenv("PEER_BIND_HOST", "0.0.0.0"),
            peer_advertised_host=os.getenv("PEER_ADVERTISED_HOST", "127.0.0.1"),
            ui_host=os.getenv("UI_HOST", "0.0.0.0"),
            ui_port=int(os.getenv("UI_PORT", "8001")),
            shared_dir=Path(os.getenv("SHARED_DIR", "./data/shared")),
            download_dir=Path(os.getenv("DOWNLOAD_DIR", "./data/downloads")),
            db_path=Path(os.getenv("DB_PATH", "./data/bluered_client.db")),
            max_upload_slots=int(os.getenv("MAX_UPLOAD_SLOTS", "3")),
            server_ws_url=os.getenv("SERVER_WS_URL", "ws://127.0.0.1:8000/ws"),
            auto_login=os.getenv("AUTO_LOGIN", "1") == "1",
        )


def _parse_ws_url(url: str) -> tuple[str, int, str]:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    default_port = 443 if parsed.scheme == "wss" else 8000
    port = parsed.port or default_port
    path = parsed.path or "/ws"
    return host, port, path


def _upsert_private_message_cache(
    cache: list[dict[str, Any]],
    msg: PrivateMessage,
) -> None:
    record = {
        "message_id": msg.message_id,
        "sender_id": msg.sender_id,
        "recipient_id": msg.recipient_id,
        "content": msg.content,
        "delivered": msg.delivered,
        "created_at": msg.created_at.isoformat(),
    }
    for idx, existing in enumerate(cache):
        if existing.get("message_id") == msg.message_id:
            cache[idx] = record
            return
    cache.append(record)


def build_client_app(settings: ClientSettings | None = None) -> FastAPI:
    """Wire all Clean Architecture layers and return the FastAPI application."""
    cfg = settings or ClientSettings.from_env()
    ws_host, ws_port, ws_path = _parse_ws_url(cfg.server_ws_url)

    local_peer = LocalPeer(
        peer_id=f"local-{cfg.peer_username}",
        username=cfg.peer_username,
        ip_address=cfg.peer_bind_host,
        tcp_port=cfg.peer_tcp_port,
        shared_directories=[cfg.shared_dir],
        max_upload_slots=cfg.max_upload_slots,
    )

    sqlite_repo = SqliteFileRepository(cfg.db_path)
    bloom_repo = BloomFilterRepositoryDecorator(sqlite_repo)
    integrity = Sha256IntegrityVerifier()
    coordinator = WebSocketServerCoordinator(ws_path=ws_path)

    nat_strategy = CompositeNatTraversalStrategy(
        [
            DirectSocketNatStrategy(),
            TcpHolePunchingStrategy(coordinator),
            RelayNatStrategy(coordinator),
        ]
    )
    peer_transport = RawTcpPeerTransport(local_peer, nat_strategy)
    queue_policy = FairShareQueuePolicy(max_concurrent_slots=max(1, cfg.max_upload_slots))

    event_bus = WebSocketTransferEventListener()
    discovery = DiscoveryAndReviewService(coordinator, bloom_repo, integrity)
    transfers = TransferOrchestrator(
        local_peer=local_peer,
        peer_transport=peer_transport,
        integrity_verifier=integrity,
        download_dir=cfg.download_dir,
        checkpoint_store=sqlite_repo,
        listeners=[event_bus],
    )
    private_chat = PrivateChatService(local_peer, peer_transport, sqlite_repo)

    known_peers: dict[str, dict[str, Any]] = {}
    community = CommunityClientService(local_peer, coordinator, known_peers=known_peers)

    ctx: ClientAppContext

    async def _on_incoming_private_message(msg: PrivateMessage) -> None:
        _upsert_private_message_cache(ctx.private_messages_cache, msg)
        await event_bus.publish_private_message(msg)

    upload_server = RawTcpUploadServer(local_peer)
    upload_listener = PeerUploadListener(
        local_peer=local_peer,
        file_index=bloom_repo,
        queue_policy=queue_policy,
        integrity_verifier=integrity,
        upload_server=upload_server,
        on_private_message=_on_incoming_private_message,
    )

    async def _handle_search_broadcast(msg: SearchBroadcastMessage) -> list[SearchHitItem]:
        matches = await discovery.handle_incoming_search_broadcast(msg.query)
        return [
            SearchHitItem(
                file_hash=sf.file_hash,
                filename=sf.file_name,
                size_bytes=sf.size_bytes,
                peer_id=local_peer.peer_id,
                peer_username=local_peer.username,
                peer_ip=cfg.peer_advertised_host,
                peer_port=local_peer.tcp_port,
            )
            for sf in matches
        ]

    async def _handle_room_message(msg: RoomMessage) -> None:
        payload = {
            "room_id": msg.room_id,
            "sender_id": msg.sender_id,
            "sender_username": msg.sender_username,
            "content": msg.content,
        }
        community.record_incoming_room_message(payload)
        await event_bus.publish_room_message(payload)

    pending_pm_tasks: dict[str, asyncio.Task[None]] = {}
    pending_pm_locks: dict[str, asyncio.Lock] = {}

    async def _deliver_pending_for_peer(remote_peer: RemotePeer) -> None:
        lock = pending_pm_locks.setdefault(remote_peer.peer_id, asyncio.Lock())
        async with lock:
            try:
                delivered = await private_chat.deliver_pending_messages(remote_peer)
            except PeerBlockedError as exc:
                logger.warning("Pending PM delivery blocked by %s: %s", remote_peer.peer_id, exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Pending PM delivery failed for %s: %s", remote_peer.peer_id, exc)
            else:
                for pm in delivered:
                    _upsert_private_message_cache(ctx.private_messages_cache, pm)
                    await event_bus.publish_private_message(pm)

    async def _handle_presence(msg: PresenceUpdateMessage) -> None:
        presence_val = (
            PresenceStatus.ONLINE.value if msg.online else PresenceStatus.OFFLINE.value
        )
        previous = ctx.known_peers.get(msg.user_id, {})
        ctx.known_peers[msg.user_id] = {
            "peer_id": msg.user_id,
            "username": msg.username,
            "ip_address": msg.ip if msg.ip is not None else previous.get("ip_address"),
            "tcp_port": msg.tcp_port if msg.tcp_port is not None else previous.get("tcp_port"),
            "status": presence_val,
        }
        await event_bus.publish_presence_update(
            {
                "user_id": msg.user_id,
                "username": msg.username,
                "online": msg.online,
                "ip": msg.ip,
                "tcp_port": msg.tcp_port,
            }
        )
        if not msg.online:
            stale_pm_task = pending_pm_tasks.pop(msg.user_id, None)
            if stale_pm_task is not None and not stale_pm_task.done():
                stale_pm_task.cancel()
            return
        if msg.ip is not None and msg.tcp_port is not None:
            existing_pm_task = pending_pm_tasks.get(msg.user_id)
            if existing_pm_task is not None and not existing_pm_task.done():
                return
            try:
                remote_peer = RemotePeer(
                    peer_id=msg.user_id,
                    username=msg.username,
                    ip_address=msg.ip,
                    tcp_port=msg.tcp_port,
                    status=PresenceStatus.ONLINE,
                )
            except Exception as exc:
                logger.warning("Invalid peer presence payload for %s: %s", msg.user_id, exc)
                return

            pm_task = asyncio.create_task(
                _deliver_pending_for_peer(remote_peer),
                name=f"bluered-pm-flush-{msg.user_id[:8]}",
            )
            pending_pm_tasks[msg.user_id] = pm_task

            def _clear_pm_task(
                done_task: asyncio.Task[None],
                uid: str = msg.user_id,
            ) -> None:
                if pending_pm_tasks.get(uid) is done_task:
                    pending_pm_tasks.pop(uid, None)

            pm_task.add_done_callback(_clear_pm_task)

    async def _on_communities_synced(items: list[dict[str, Any]]) -> None:
        community.apply_server_communities(items)
        await event_bus.publish_community_update({"action": "communities_synced"})

    async def _on_debates_synced(community_id: str, posts: list[dict[str, Any]]) -> None:
        community.apply_server_debates(community_id, posts)
        await event_bus.publish_community_update(
            {"action": "debates_synced", "community_id": community_id}
        )

    async def _on_rooms_synced(rooms: list[dict[str, Any]]) -> None:
        community.apply_server_rooms(rooms)

    async def _on_library_synced(items: list[dict[str, Any]]) -> None:
        community.apply_server_library(items)
        await event_bus.publish_library_update({"action": "library_synced"})

    async def _on_reauthenticated(user_id: str) -> None:
        local_peer.peer_id = user_id
        await community.sync_all()

    coordinator.set_search_broadcast_handler(_handle_search_broadcast)
    coordinator.set_room_message_handler(_handle_room_message)
    coordinator.set_presence_handler(_handle_presence)
    coordinator.set_community_list_handler(_on_communities_synced)
    coordinator.set_debate_list_handler(_on_debates_synced)
    coordinator.set_room_list_handler(_on_rooms_synced)
    coordinator.set_library_list_handler(_on_library_synced)
    coordinator.set_reauthenticated_handler(_on_reauthenticated)

    def _get_bloom_metrics() -> dict[str, Any]:
        size_bits, hash_k, _ = bloom_repo.export_bloom_sync_payload()
        return {
            "size_bits": size_bits,
            "hash_functions_k": hash_k,
            "bloom_hits": bloom_repo.bloom_hits,
            "bloom_rejects": bloom_repo.bloom_rejects,
            "sqlite_queries": bloom_repo.sqlite_queries,
        }

    async def _rebuild_bloom_and_sync(user_id: str | None) -> int:
        count = await bloom_repo.rebuild_from_repository()
        if user_id is not None and coordinator.is_connected and coordinator.is_authenticated:
            size_bits, hash_k, bitset_b64 = bloom_repo.export_bloom_sync_payload()
            await coordinator.sync_bloom_filter(user_id, size_bits, hash_k, bitset_b64)
        return count

    async def _connect_server() -> None:
        if not coordinator.is_connected:
            await coordinator.connect(ws_host, ws_port)

    manual_offline = False

    async def _on_logout() -> None:
        nonlocal manual_offline
        manual_offline = True

    async def _on_login() -> None:
        nonlocal manual_offline
        manual_offline = False

    async def _set_presence_status(target_status: PresenceStatus) -> PresenceStatus:
        nonlocal manual_offline
        manual_offline = target_status == PresenceStatus.OFFLINE
        if (
            target_status == PresenceStatus.ONLINE
            and not coordinator.is_connected
            and not coordinator.has_credentials
            and cfg.auto_login
        ):
            await _auto_connect_and_login(
                cfg=cfg,
                local_peer=local_peer,
                coordinator=coordinator,
                bloom_repo=bloom_repo,
                community=community,
                ws_host=ws_host,
                ws_port=ws_port,
            )
            return coordinator.presence_status
        return await coordinator.set_presence_status(target_status)

    ctx = ClientAppContext(
        local_peer=local_peer,
        discovery=discovery,
        transfers=transfers,
        private_chat=private_chat,
        upload_listener=upload_listener,
        server_coordinator=coordinator,
        file_index=bloom_repo,
        peer_transport=peer_transport,
        event_bus=event_bus,
        community=community,
        advertised_host=cfg.peer_advertised_host,
        get_bloom_metrics=_get_bloom_metrics,
        list_shared_directories=sqlite_repo.list_shared_directories,
        remove_shared_directory=sqlite_repo.remove_shared_directory,
        rebuild_bloom_and_sync=_rebuild_bloom_and_sync,
        connect_server=_connect_server,
        set_presence_status=_set_presence_status,
        on_logout=_on_logout,
        on_login=_on_login,
        known_peers=known_peers,
    )

    heartbeat_task: asyncio.Task[None] | None = None
    heartbeat_interval_s = float(os.getenv("HEARTBEAT_INTERVAL_S", "15.0"))

    async def _keepalive_loop() -> None:
        while True:
            await asyncio.sleep(heartbeat_interval_s)
            try:
                if coordinator.is_connected and coordinator.is_authenticated:
                    await coordinator.send_heartbeat(local_peer.peer_id, local_peer.tcp_port)
                elif cfg.auto_login and not manual_offline:
                    if coordinator.is_connected and coordinator.has_credentials:
                        await coordinator.set_presence_status(PresenceStatus.ONLINE)
                    else:
                        await _auto_connect_and_login(
                            cfg=cfg,
                            local_peer=local_peer,
                            coordinator=coordinator,
                            bloom_repo=bloom_repo,
                            community=community,
                            ws_host=ws_host,
                            ws_port=ws_port,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Keepalive heartbeat failed for %s: %s", local_peer.username, exc)

    async def _on_startup() -> None:
        nonlocal heartbeat_task
        if not cfg.shared_dir.exists():
            cfg.shared_dir.mkdir(parents=True, exist_ok=True)
        cfg.download_dir.mkdir(parents=True, exist_ok=True)
        await sqlite_repo.initialize()
        await bloom_repo.rebuild_from_repository()

        for cp in await transfers.list_incomplete_checkpoints():
            restored_state = TransferState(cp.state)
            if restored_state in {
                TransferState.QUEUED,
                TransferState.HANDSHAKING,
                TransferState.TRANSFERRING,
                TransferState.RESUMING,
            }:
                restored_state = TransferState.PAUSED
            event_bus.record_task_snapshot(
                TransferTask(
                    task_id=cp.task_id,
                    file_hash=cp.file_hash,
                    file_name=cp.file_name,
                    total_bytes=cp.total_bytes,
                    remote_peer_id=cp.source_peer_id,
                    state=restored_state,
                    bytes_transferred=cp.bytes_transferred,
                    updated_at=cp.updated_at,
                ),
                target_path=str(cp.target_path),
            )
        for pm in await private_chat.list_pending_messages():
            _upsert_private_message_cache(ctx.private_messages_cache, pm)

        bound_port = await upload_listener.start()
        local_peer.tcp_port = bound_port

        if cfg.shared_dir.is_dir():
            await discovery.configure_and_index([cfg.shared_dir])

        if cfg.auto_login:
            await _auto_connect_and_login(
                cfg=cfg,
                local_peer=local_peer,
                coordinator=coordinator,
                bloom_repo=bloom_repo,
                community=community,
                ws_host=ws_host,
                ws_port=ws_port,
            )
            heartbeat_task = asyncio.create_task(
                _keepalive_loop(),
                name=f"bluered-keepalive-{local_peer.username}",
            )

    async def _on_shutdown() -> None:
        if heartbeat_task is not None and not heartbeat_task.done():
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
        for pm_task in list(pending_pm_tasks.values()):
            if not pm_task.done():
                pm_task.cancel()
        if pending_pm_tasks:
            await asyncio.gather(*pending_pm_tasks.values(), return_exceptions=True)
        pending_pm_tasks.clear()
        await coordinator.disconnect()
        await peer_transport.close()
        await upload_listener.stop()
        await sqlite_repo.close()

    return create_client_app(ctx, on_startup=_on_startup, on_shutdown=_on_shutdown)


async def _auto_connect_and_login(
    *,
    cfg: ClientSettings,
    local_peer: LocalPeer,
    coordinator: WebSocketServerCoordinator,
    bloom_repo: BloomFilterRepositoryDecorator,
    community: CommunityClientService,
    ws_host: str,
    ws_port: int,
) -> None:
    """Connect to the central server with retries, register if needed, login, and sync Bloom."""
    if not coordinator.is_connected:
        for attempt in range(1, 11):
            try:
                await coordinator.connect(ws_host, ws_port)
                break
            except Exception as exc:
                logger.warning(
                    "Attempt %d/10 to connect to %s:%d failed: %s",
                    attempt,
                    ws_host,
                    ws_port,
                    exc,
                )
                await asyncio.sleep(1.0)
        else:
            logger.error("Could not connect to central server at %s:%d", ws_host, ws_port)
            return

    try:
        await coordinator.register(cfg.peer_username, cfg.peer_password)
    except ControlPlaneError:
        # Already registered — proceed directly to login.
        pass
    except Exception as exc:
        logger.warning("Auto-register skipped for %s: %s", cfg.peer_username, exc)

    try:
        user_id, _ = await coordinator.login(
            cfg.peer_username,
            cfg.peer_password,
            local_peer.tcp_port,
        )
        local_peer.peer_id = user_id
        size_bits, hash_k, bitset_b64 = bloom_repo.export_bloom_sync_payload()
        await coordinator.sync_bloom_filter(user_id, size_bits, hash_k, bitset_b64)
        await community.sync_all()
        logger.info(
            "Peer '%s' (%s) online on TCP :%d, Bloom synced (%d bits)",
            local_peer.username,
            user_id,
            local_peer.tcp_port,
            size_bits,
        )
    except Exception as exc:
        logger.error("Auto-login failed for %s: %s", cfg.peer_username, exc)


def main() -> None:
    """Run the BlueRed peer UI & P2P node via Uvicorn."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    cfg = ClientSettings.from_env()
    app = build_client_app(cfg)
    uvicorn.run(app, host=cfg.ui_host, port=cfg.ui_port)


if __name__ == "__main__":
    main()
