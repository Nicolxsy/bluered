"""FastAPI presentation application for the BlueRed local peer web UI."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from bluered_client.application import (
    CommunityClientService,
    DiscoveryAndReviewService,
    PeerUploadListener,
    PrivateChatService,
    TransferOrchestrator,
)
from bluered_client.contracts import (
    IBloomSyncSource,
    ICommunityActionPort,
    IFileIndexRepository,
    IPeerTransport,
    IServerCoordinator,
)
from bluered_client.domain import (
    DomainError,
    DomainValidationError,
    DownloadCheckpoint,
    InsufficientDiskSpaceError,
    InvalidStateTransitionError,
    LocalPeer,
    PeerBlockedError,
    PresenceStatus,
    PrivateMessage,
    QueueFullError,
    RemoteFileChangedError,
    RemotePeer,
    SharedFile,
    TransferIntegrityError,
    TransferState,
    TransferTask,
)

from .event_bus import WebSocketTransferEventListener
from .schemas import (
    AuthCredentialsRequest,
    BrowsePeerRequest,
    CreateCommunityRequest,
    CreateDebateReplyRequest,
    CreateDebateRequest,
    DownloadRequest,
    FolderPathRequest,
    LibrarySaveRequest,
    PresenceUpdateRequest,
    PrivateChatRequest,
    PublishReviewRequest,
    ResumeTransferRequest,
    RoomMessageRequest,
)

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_logger = logging.getLogger("bluered.client.presentation")


@dataclass(slots=True)
class ClientAppContext:
    """Dependency container injected by the Composition Root (main.py)."""

    local_peer: LocalPeer
    discovery: DiscoveryAndReviewService
    transfers: TransferOrchestrator
    private_chat: PrivateChatService
    upload_listener: PeerUploadListener
    server_coordinator: IServerCoordinator
    file_index: IFileIndexRepository
    peer_transport: IPeerTransport
    event_bus: WebSocketTransferEventListener
    community: CommunityClientService | None = None
    advertised_host: str = "127.0.0.1"
    get_bloom_metrics: Callable[[], dict[str, Any]] | None = None
    list_shared_directories: Callable[[], Awaitable[list[Path]]] | None = None
    remove_shared_directory: Callable[[Path], Awaitable[bool]] | None = None
    rebuild_bloom_and_sync: Callable[[str | None], Awaitable[int]] | None = None
    connect_server: Callable[[], Awaitable[None]] | None = None
    get_session_state: Callable[[], dict[str, Any]] | None = None
    set_presence_status: Callable[[PresenceStatus], Awaitable[PresenceStatus]] | None = None
    on_logout: Callable[[], Awaitable[None]] | None = None
    on_login: Callable[[], Awaitable[None]] | None = None
    private_messages_cache: list[dict[str, Any]] = field(default_factory=list)
    known_peers: dict[str, dict[str, Any]] = field(default_factory=dict)


def _serialize_shared_file(sf: SharedFile) -> dict[str, Any]:
    return {
        "file_hash": sf.file_hash,
        "file_name": sf.file_name,
        "size_bytes": sf.size_bytes,
        "bitrate_kbps": sf.bitrate_kbps,
        "mime_type": sf.mime_type,
        "category": sf.category,
        "local_path": str(sf.local_path) if sf.local_path is not None else None,
    }


def _default_session_state(ctx: ClientAppContext) -> dict[str, Any]:
    if ctx.get_session_state is not None:
        return ctx.get_session_state()
    connected = bool(getattr(ctx.server_coordinator, "is_connected", False))
    user_id = getattr(ctx.server_coordinator, "user_id", None)
    is_auth_attr = getattr(ctx.server_coordinator, "is_authenticated", None)
    authenticated = (
        bool(is_auth_attr) if is_auth_attr is not None else (connected and user_id is not None)
    )
    status_val = getattr(ctx.server_coordinator, "presence_status", PresenceStatus.OFFLINE)
    presence_str = (
        status_val.value if isinstance(status_val, PresenceStatus) else str(status_val)
    )
    return {
        "connected": connected,
        "authenticated": authenticated,
        "user_id": user_id or ctx.local_peer.peer_id,
        "username": ctx.local_peer.username,
        "presence_status": presence_str,
    }


def _default_bloom_metrics(ctx: ClientAppContext) -> dict[str, Any]:
    if ctx.get_bloom_metrics is not None:
        return ctx.get_bloom_metrics()
    size_bits = 0
    hash_k = 0
    if isinstance(ctx.file_index, IBloomSyncSource):
        size_bits, hash_k, _ = ctx.file_index.export_bloom_sync_payload()
    return {
        "size_bits": size_bits,
        "hash_functions_k": hash_k,
        "bloom_hits": int(getattr(ctx.file_index, "bloom_hits", 0)),
        "bloom_rejects": int(getattr(ctx.file_index, "bloom_rejects", 0)),
        "sqlite_queries": int(getattr(ctx.file_index, "sqlite_queries", 0)),
    }


MAX_CONTENT_PREVIEW_BYTES = 64 * 1024


def _read_content_preview(path: Path) -> dict[str, Any]:
    """Read up to MAX_CONTENT_PREVIEW_BYTES of a downloaded file for inline viewing."""
    with path.open("rb") as fh:
        data = fh.read(MAX_CONTENT_PREVIEW_BYTES + 1)
    truncated = len(data) > MAX_CONTENT_PREVIEW_BYTES
    data = data[:MAX_CONTENT_PREVIEW_BYTES]
    is_text = b"\x00" not in data
    return {
        "file_name": path.name,
        "size_bytes": path.stat().st_size,
        "is_text": is_text,
        "truncated": truncated,
        "content": data.decode("utf-8", errors="replace") if is_text else "",
    }


def _checkpoint_to_task(cp: DownloadCheckpoint) -> TransferTask:
    restored_state = TransferState(cp.state)
    if restored_state in {
        TransferState.QUEUED,
        TransferState.HANDSHAKING,
        TransferState.TRANSFERRING,
        TransferState.RESUMING,
    }:
        restored_state = TransferState.PAUSED
    return TransferTask(
        task_id=cp.task_id,
        file_hash=cp.file_hash,
        file_name=cp.file_name,
        total_bytes=cp.total_bytes,
        remote_peer_id=cp.source_peer_id,
        state=restored_state,
        bytes_transferred=cp.bytes_transferred,
        updated_at=cp.updated_at,
    )


def _resolve_resume_peer(
    ctx: ClientAppContext,
    task_id: str,
    source_peer_id: str,
    body: ResumeTransferRequest | None,
) -> RemotePeer | None:
    if body is not None and body.peer_ip is not None and body.peer_port is not None:
        return RemotePeer(
            peer_id=body.peer_id or source_peer_id,
            username=body.peer_username or source_peer_id,
            ip_address=body.peer_ip,
            tcp_port=body.peer_port,
            status=PresenceStatus.ONLINE,
        )
    in_mem_peer = ctx.transfers.get_peer(task_id)
    if in_mem_peer is not None:
        known = ctx.known_peers.get(in_mem_peer.peer_id)
        if known is not None and known.get("status") == PresenceStatus.OFFLINE.value:
            return None
        return in_mem_peer
    known = ctx.known_peers.get(source_peer_id)
    if (
        known is not None
        and known.get("status") != PresenceStatus.OFFLINE.value
        and known.get("ip_address")
        and known.get("tcp_port")
    ):
        return RemotePeer(
            peer_id=source_peer_id,
            username=str(known.get("username") or source_peer_id),
            ip_address=str(known["ip_address"]),
            tcp_port=int(known["tcp_port"]),
            status=PresenceStatus(str(known.get("status") or PresenceStatus.ONLINE.value)),
        )
    return None


def create_client_app(
    ctx: ClientAppContext,
    *,
    on_startup: Callable[[], Awaitable[None]] | None = None,
    on_shutdown: Callable[[], Awaitable[None]] | None = None,
) -> FastAPI:
    """Build the local FastAPI application for a BlueRed peer."""
    if ctx.community is None:
        port = (
            ctx.server_coordinator
            if isinstance(ctx.server_coordinator, ICommunityActionPort)
            else None
        )
        ctx.community = CommunityClientService(
            ctx.local_peer,
            port,
            known_peers=ctx.known_peers,
        )
    community_svc = ctx.community

    background_transfers: dict[str, asyncio.Task[TransferTask | None]] = {}
    session_override: dict[str, Any] = {
        "authenticated": False,
        "presence_status": None,
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ctx = ctx
        ctx.transfers.add_listener(ctx.event_bus)
        if on_startup is not None:
            await on_startup()
        yield
        for bg_task in list(background_transfers.values()):
            if not bg_task.done():
                bg_task.cancel()
        if background_transfers:
            await asyncio.gather(*background_transfers.values(), return_exceptions=True)
        await ctx.event_bus.close_all()
        if on_shutdown is not None:
            await on_shutdown()

    app = FastAPI(
        title=f"BlueRed Peer UI ({ctx.local_peer.username})",
        version="0.1.0",
        lifespan=lifespan,
    )

    if _STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    def _current_session() -> dict[str, Any]:
        base = _default_session_state(ctx)
        has_live_auth = getattr(ctx.server_coordinator, "is_authenticated", None) is not None
        if not has_live_auth and session_override["authenticated"]:
            base["authenticated"] = True
        if session_override["presence_status"] is not None:
            base["presence_status"] = str(session_override["presence_status"])
        return base

    def _current_user_id() -> str | None:
        state = _current_session()
        if state.get("authenticated"):
            return str(state.get("user_id") or ctx.local_peer.peer_id)
        return None

    async def _ensure_connected() -> None:
        if ctx.connect_server is not None and not bool(
            getattr(ctx.server_coordinator, "is_connected", False)
        ):
            await ctx.connect_server()

    def _task_to_dict(task: TransferTask) -> dict[str, Any]:
        target_path = ctx.transfers.get_target_path(task.task_id)
        return ctx.event_bus.record_task_snapshot(
            task,
            target_path=str(target_path) if target_path is not None else None,
        )

    async def _hydrate_incomplete_checkpoints() -> None:
        checkpoints = await ctx.transfers.list_incomplete_checkpoints()
        for cp in checkpoints:
            if ctx.transfers.get_task(cp.task_id) is None and (
                ctx.event_bus.get_task_snapshot(cp.task_id) is None
            ):
                ctx.event_bus.record_task_snapshot(
                    _checkpoint_to_task(cp),
                    target_path=str(cp.target_path),
                )

    async def _hydrate_pending_private_messages() -> None:
        pending = await ctx.private_chat.list_pending_messages()
        existing_ids = {m["message_id"] for m in ctx.private_messages_cache}
        for msg in pending:
            if msg.message_id not in existing_ids:
                ctx.private_messages_cache.append(
                    {
                        "message_id": msg.message_id,
                        "sender_id": msg.sender_id,
                        "recipient_id": msg.recipient_id,
                        "content": msg.content,
                        "delivered": msg.delivered,
                        "created_at": msg.created_at.isoformat(),
                    }
                )
                existing_ids.add(msg.message_id)

    # --- SPA Entry & Health ---

    @app.get("/", include_in_schema=False)
    async def index_page() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        session = _current_session()
        indexed = await ctx.file_index.list_all()
        return {
            "status": "ok",
            "peer_username": ctx.local_peer.username,
            "peer_id": ctx.local_peer.peer_id,
            "tcp_port": ctx.local_peer.tcp_port,
            "connected": bool(session.get("connected", False)),
            "authenticated": bool(session.get("authenticated", False)),
            "presence_status": str(session.get("presence_status", PresenceStatus.OFFLINE.value)),
            "indexed_files": len(indexed),
        }

    # --- 1. Session & Presence (Cas_07, Cas_08) ---

    @app.get("/api/session")
    async def get_session() -> dict[str, Any]:
        session = _current_session()
        return {
            "peer_id": ctx.local_peer.peer_id,
            "username": ctx.local_peer.username,
            "connected": bool(session.get("connected", False)),
            "authenticated": bool(session.get("authenticated", False)),
            "presence_status": str(session.get("presence_status", PresenceStatus.OFFLINE.value)),
            "tcp_port": ctx.local_peer.tcp_port,
            "advertised_host": ctx.advertised_host,
            "max_upload_slots": ctx.local_peer.max_upload_slots,
            "active_uploads": ctx.upload_listener.active_uploads,
            "blocked_peers": community_svc.list_blocked_peers(),
            "bloom": _default_bloom_metrics(ctx),
            "known_peers": list(ctx.known_peers.values()),
        }

    @app.post("/api/session/register", status_code=status.HTTP_201_CREATED)
    async def register_session(body: AuthCredentialsRequest) -> dict[str, Any]:
        try:
            await _ensure_connected()
            user_id = await ctx.server_coordinator.register(body.username, body.password)
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        ctx.local_peer.peer_id = user_id
        ctx.local_peer.username = body.username
        return {"user_id": user_id, "username": body.username}

    @app.post("/api/session/login")
    async def login_session(body: AuthCredentialsRequest) -> dict[str, Any]:
        try:
            await _ensure_connected()
            user_id, session_token = await ctx.server_coordinator.login(
                body.username,
                body.password,
                ctx.local_peer.tcp_port,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc),
            ) from exc
        ctx.local_peer.peer_id = user_id
        ctx.local_peer.username = body.username
        if ctx.on_login is not None:
            await ctx.on_login()
        has_live_auth = getattr(ctx.server_coordinator, "is_authenticated", None) is not None
        session_override["authenticated"] = not has_live_auth
        session_override["presence_status"] = (
            None if ctx.set_presence_status is not None else PresenceStatus.ONLINE.value
        )
        if ctx.local_peer.shared_directories:
            await ctx.discovery.configure_and_index(
                list(ctx.local_peer.shared_directories),
                user_id=user_id,
            )
        elif isinstance(ctx.file_index, IBloomSyncSource):
            size_bits, k, bitset_b64 = ctx.file_index.export_bloom_sync_payload()
            await ctx.server_coordinator.sync_bloom_filter(user_id, size_bits, k, bitset_b64)
        await community_svc.sync_all()
        await ctx.event_bus.publish_presence_update(
            {
                "user_id": user_id,
                "username": body.username,
                "status": PresenceStatus.ONLINE.value,
            }
        )
        return {
            "user_id": user_id,
            "username": body.username,
            "session_token": session_token,
            "presence_status": PresenceStatus.ONLINE.value,
        }

    @app.post("/api/session/logout")
    async def logout_session() -> dict[str, str]:
        if ctx.on_logout is not None:
            await ctx.on_logout()
        await ctx.server_coordinator.disconnect()
        session_override["authenticated"] = False
        session_override["presence_status"] = (
            None if ctx.set_presence_status is not None else PresenceStatus.OFFLINE.value
        )
        await ctx.event_bus.publish_presence_update(
            {
                "user_id": ctx.local_peer.peer_id,
                "username": ctx.local_peer.username,
                "status": PresenceStatus.OFFLINE.value,
            }
        )
        return {"presence_status": PresenceStatus.OFFLINE.value}

    @app.post("/api/session/presence")
    async def update_presence(body: PresenceUpdateRequest) -> dict[str, str]:
        target_status = PresenceStatus(body.status)
        if ctx.set_presence_status is not None:
            resolved = await ctx.set_presence_status(target_status)
            session_override["presence_status"] = None
            resolved_str = resolved.value
        else:
            if target_status == PresenceStatus.OFFLINE:
                await ctx.server_coordinator.disconnect()
                session_override["authenticated"] = False
            elif target_status == PresenceStatus.ONLINE and _current_user_id() is not None:
                await ctx.server_coordinator.send_heartbeat(
                    ctx.local_peer.peer_id,
                    ctx.local_peer.tcp_port,
                )
            session_override["presence_status"] = target_status.value
            resolved_str = target_status.value
        await ctx.event_bus.publish_presence_update(
            {
                "user_id": ctx.local_peer.peer_id,
                "username": ctx.local_peer.username,
                "status": resolved_str,
            }
        )
        return {"presence_status": resolved_str}

    # --- 2. Shared Folders & Local Index (Cas_09, Cas_10, Cas_05) ---

    @app.get("/api/folders")
    async def list_folders() -> dict[str, Any]:
        if ctx.list_shared_directories is not None:
            dirs = await ctx.list_shared_directories()
        else:
            dirs = list(ctx.local_peer.shared_directories)
        files = await ctx.file_index.list_all()
        return {
            "directories": [str(d) for d in dirs],
            "files": [_serialize_shared_file(f) for f in files],
            "bloom": _default_bloom_metrics(ctx),
        }

    @app.post("/api/folders", status_code=status.HTTP_201_CREATED)
    async def add_folder(body: FolderPathRequest) -> dict[str, Any]:
        folder_path = Path(body.path).expanduser()
        if not folder_path.is_dir():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Directory does not exist: {folder_path}",
            )
        ctx.local_peer.add_shared_directory(folder_path)
        indexed_count = await ctx.discovery.configure_and_index(
            [folder_path],
            user_id=_current_user_id(),
        )
        files = await ctx.file_index.list_all()
        return {
            "path": str(folder_path),
            "indexed_count": indexed_count,
            "total_files": len(files),
        }

    @app.delete("/api/folders")
    async def remove_folder(body: FolderPathRequest) -> dict[str, Any]:
        folder_path = Path(body.path).expanduser()
        removed = False
        if ctx.remove_shared_directory is not None:
            removed = await ctx.remove_shared_directory(folder_path)
        ctx.local_peer.shared_directories = [
            d for d in ctx.local_peer.shared_directories if d.resolve() != folder_path.resolve()
        ]
        if ctx.rebuild_bloom_and_sync is not None:
            await ctx.rebuild_bloom_and_sync(_current_user_id())
        return {"path": str(folder_path), "removed": removed}

    @app.post("/api/folders/reindex")
    async def reindex_folders() -> dict[str, Any]:
        if ctx.list_shared_directories is not None:
            dirs = await ctx.list_shared_directories()
        else:
            dirs = list(ctx.local_peer.shared_directories)
        if not dirs and ctx.local_peer.shared_directories:
            dirs = list(ctx.local_peer.shared_directories)
        indexed_count = await ctx.discovery.configure_and_index(
            dirs,
            user_id=_current_user_id(),
        )
        files = await ctx.file_index.list_all()
        return {
            "indexed_count": indexed_count,
            "total_files": len(files),
            "bloom": _default_bloom_metrics(ctx),
        }

    # --- 3. Search, Peer Browse & Reviews (Cas_11, Cas_12, Cas_14-16, Cas_24) ---

    @app.get("/api/search")
    async def search_files(
        q: str = Query(min_length=1, max_length=256),
        max_results: int = Query(default=50, ge=1, le=200),
    ) -> dict[str, Any]:
        try:
            matches = await ctx.discovery.execute_global_search(q, max_results=max_results)
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Federated search failed: {detail}",
            ) from exc

        results: list[dict[str, Any]] = []
        for match in matches:
            ctx.known_peers[match.peer.peer_id] = {
                "peer_id": match.peer.peer_id,
                "username": match.peer.username,
                "ip_address": match.peer.ip_address,
                "tcp_port": match.peer.tcp_port,
                "status": match.peer.status.value,
            }
            results.append(
                {
                    "file_hash": match.file.file_hash,
                    "file_name": match.file.file_name,
                    "size_bytes": match.file.size_bytes,
                    "category": match.file.category,
                    "peer_id": match.peer.peer_id,
                    "peer_username": match.peer.username,
                    "peer_ip": match.peer.ip_address,
                    "peer_port": match.peer.tcp_port,
                    "avg_rating": match.avg_rating,
                    "review_count": match.review_count,
                }
            )
        return {"query": q, "results": results}

    @app.post("/api/peers/browse")
    async def browse_peer(body: BrowsePeerRequest) -> dict[str, Any]:
        try:
            remote_peer = RemotePeer(
                peer_id=body.peer_id,
                username=body.username,
                ip_address=body.ip_address,
                tcp_port=body.tcp_port,
                status=PresenceStatus.ONLINE,
            )
            files = await ctx.peer_transport.browse_peer_files(remote_peer)
        except PeerBlockedError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Peer browse failed: {exc}",
            ) from exc
        return {
            "peer_id": remote_peer.peer_id,
            "username": remote_peer.username,
            "ip_address": remote_peer.ip_address,
            "tcp_port": remote_peer.tcp_port,
            "files": [_serialize_shared_file(f) for f in files],
        }

    @app.get("/api/reviews/{file_hash}")
    async def get_reviews(file_hash: str) -> dict[str, Any]:
        try:
            reviews, avg_rating, review_count = await ctx.discovery.get_file_reputation(file_hash)
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to fetch reviews: {exc}",
            ) from exc
        return {
            "file_hash": file_hash,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "file_hash": r.file_hash,
                    "author_id": r.author_id,
                    "rating": r.rating,
                    "comment": r.comment,
                    "created_at": r.created_at.isoformat(),
                }
                for r in reviews
            ],
        }

    @app.post("/api/reviews/{file_hash}", status_code=status.HTTP_201_CREATED)
    async def publish_review(file_hash: str, body: PublishReviewRequest) -> dict[str, Any]:
        try:
            await ctx.discovery.submit_file_review(
                file_hash=file_hash,
                rating=body.rating,
                comment=body.comment,
            )
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to publish review: {exc}",
            ) from exc
        return {
            "file_hash": file_hash,
            "rating": body.rating,
            "comment": body.comment.strip(),
            "status": "published",
        }

    # --- 4. Transfers & Queues (Cas_17, Cas_18, Cas_21, Cas_22) ---

    @app.get("/api/transfers")
    async def list_transfers() -> dict[str, Any]:
        await _hydrate_incomplete_checkpoints()
        return {
            "transfers": ctx.event_bus.list_task_snapshots(),
            "active_uploads": ctx.upload_listener.active_uploads,
            "max_upload_slots": ctx.local_peer.max_upload_slots,
        }

    @app.get("/api/transfers/{task_id}")
    async def get_transfer(task_id: str) -> dict[str, Any]:
        task = ctx.transfers.get_task(task_id)
        if task is not None:
            return _task_to_dict(task)
        snapshot = ctx.event_bus.get_task_snapshot(task_id)
        if snapshot is not None:
            return snapshot
        cp = await ctx.transfers.get_checkpoint(task_id)
        if cp is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transfer '{task_id}' not found",
            )
        return ctx.event_bus.record_task_snapshot(
            _checkpoint_to_task(cp),
            target_path=str(cp.target_path),
        )

    async def _completed_download_content(snapshot: dict[str, Any] | None) -> dict[str, Any]:
        if snapshot is None or snapshot.get("state") != TransferState.COMPLETED.value:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Completed download not found",
            )
        raw_path = snapshot.get("target_path")
        target = Path(str(raw_path)) if raw_path else ctx.transfers.get_target_path(
            str(snapshot["task_id"])
        )
        if target is None or not target.is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Downloaded file is no longer available on disk",
            )
        return await asyncio.to_thread(_read_content_preview, target)

    @app.get("/api/transfers/{task_id}/content")
    async def get_transfer_content(task_id: str) -> dict[str, Any]:
        return await _completed_download_content(ctx.event_bus.get_task_snapshot(task_id))

    @app.get("/api/downloads/{file_hash}/content")
    async def get_download_content(file_hash: str) -> dict[str, Any]:
        for snap in ctx.event_bus.list_task_snapshots():
            if snap.get("file_hash") == file_hash and snap.get("state") == "COMPLETED":
                return await _completed_download_content(snap)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No completed download found for this file",
        )

    @app.post("/api/transfers/download")
    async def start_download(body: DownloadRequest, response: Response) -> dict[str, Any]:
        try:
            remote_peer = RemotePeer(
                peer_id=body.peer_id,
                username=body.peer_username,
                ip_address=body.peer_ip,
                tcp_port=body.peer_port,
                status=PresenceStatus.ONLINE,
            )
            shared_file = SharedFile(
                file_hash=body.file_hash,
                file_name=body.file_name,
                size_bytes=body.size_bytes,
            )
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        task_id = uuid.uuid4().hex
        if body.wait:
            try:
                task = await ctx.transfers.start_download(
                    remote_peer,
                    shared_file,
                    conflict_policy=body.conflict_policy,
                    task_id=task_id,
                )
            except InsufficientDiskSpaceError as exc:
                raise HTTPException(
                    status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
                    detail=str(exc),
                ) from exc
            except (
                PeerBlockedError,
                QueueFullError,
                RemoteFileChangedError,
                TransferIntegrityError,
                DomainError,
            ) as exc:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=str(exc),
                ) from exc
            response.status_code = status.HTTP_200_OK
            payload = _task_to_dict(task)
            if task.state.value == "COMPLETED":
                community_svc.remember_downloaded_file(shared_file)
            return payload

        async def _run_download() -> TransferTask | None:
            try:
                result = await ctx.transfers.start_download(
                    remote_peer,
                    shared_file,
                    conflict_policy=body.conflict_policy,
                    task_id=task_id,
                )
            except Exception as exc:
                _logger.warning("Background download %s failed: %s", task_id, exc)
                return None
            _task_to_dict(result)
            if result.state.value == "COMPLETED":
                community_svc.remember_downloaded_file(shared_file)
            return result

        bg_task = asyncio.create_task(_run_download(), name=f"bluered-dl-{task_id[:8]}")
        background_transfers[task_id] = bg_task
        bg_task.add_done_callback(lambda _t: background_transfers.pop(task_id, None))
        await asyncio.sleep(0)

        response.status_code = status.HTTP_202_ACCEPTED
        live = ctx.transfers.get_task(task_id)
        if live is not None:
            return _task_to_dict(live)
        return {
            "task_id": task_id,
            "file_hash": shared_file.file_hash,
            "file_name": shared_file.file_name,
            "total_bytes": shared_file.size_bytes,
            "bytes_transferred": 0,
            "progress_ratio": 0.0,
            "progress_percent": 0.0,
            "state": "QUEUED",
            "remote_peer_id": remote_peer.peer_id,
            "queue_position": None,
            "error_message": None,
            "target_path": None,
        }

    @app.post("/api/transfers/{task_id}/pause")
    async def pause_transfer(task_id: str) -> dict[str, Any]:
        try:
            task = await ctx.transfers.pause_download(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InvalidStateTransitionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return _task_to_dict(task)

    @app.post("/api/transfers/{task_id}/resume")
    async def resume_transfer(
        task_id: str,
        wait: bool = Query(default=False),
        body: ResumeTransferRequest | None = None,
    ) -> dict[str, Any]:
        existing = ctx.transfers.get_task(task_id)
        checkpoint = None if existing is not None else await ctx.transfers.get_checkpoint(task_id)

        if existing is None and checkpoint is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transfer '{task_id}' not found",
            )

        current_task = existing if existing is not None else _checkpoint_to_task(checkpoint)  # type: ignore[arg-type]
        if current_task.state.is_terminal:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Transfer '{task_id}' is already in terminal state "
                    f"{current_task.state.value}"
                ),
            )

        try:
            resolved_peer = _resolve_resume_peer(
                ctx,
                task_id,
                current_task.remote_peer_id,
                body,
            )
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        if resolved_peer is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Source peer '{current_task.remote_peer_id}' is unknown or offline",
            )

        if wait:
            try:
                task = await ctx.transfers.resume_download(task_id, peer=resolved_peer)
            except KeyError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
            except (ValueError, DomainError) as exc:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
            payload = _task_to_dict(task)
            if task.state.value == "COMPLETED":
                community_svc.remember_downloaded_file(
                    SharedFile(
                        file_hash=task.file_hash,
                        file_name=task.file_name,
                        size_bytes=task.total_bytes,
                    )
                )
            return payload

        async def _run_resume() -> TransferTask | None:
            try:
                resumed = await ctx.transfers.resume_download(task_id, peer=resolved_peer)
            except Exception as exc:
                _logger.warning("Background resume %s failed: %s", task_id, exc)
                return None
            _task_to_dict(resumed)
            if resumed.state.value == "COMPLETED":
                community_svc.remember_downloaded_file(
                    SharedFile(
                        file_hash=resumed.file_hash,
                        file_name=resumed.file_name,
                        size_bytes=resumed.total_bytes,
                    )
                )
            return resumed

        in_flight = background_transfers.get(task_id)
        if in_flight is not None and not in_flight.done():
            live = ctx.transfers.get_task(task_id)
            if live is not None:
                return _task_to_dict(live)
            if existing is not None:
                return _task_to_dict(existing)
            snap = ctx.event_bus.get_task_snapshot(task_id)
            if snap is not None:
                return snap
            assert checkpoint is not None
            return ctx.event_bus.record_task_snapshot(
                current_task,
                target_path=str(checkpoint.target_path),
            )

        bg_task = asyncio.create_task(_run_resume(), name=f"bluered-resume-{task_id[:8]}")
        background_transfers[task_id] = bg_task
        bg_task.add_done_callback(lambda _t: background_transfers.pop(task_id, None))
        await asyncio.sleep(0)
        live = ctx.transfers.get_task(task_id)
        if live is not None:
            return _task_to_dict(live)
        if existing is not None:
            return _task_to_dict(existing)
        assert checkpoint is not None
        return ctx.event_bus.record_task_snapshot(
            current_task,
            target_path=str(checkpoint.target_path),
        )

    @app.post("/api/transfers/{task_id}/cancel")
    async def cancel_transfer(task_id: str) -> dict[str, Any]:
        try:
            task = await ctx.transfers.cancel_download(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InvalidStateTransitionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return _task_to_dict(task)

    # --- 5. Communities, Debates, Rooms & Private P2P Chat (Cas_01-Cas_06, Cas_13, Cas_23) ---

    @app.get("/api/communities")
    async def list_communities(
        community_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return await community_svc.list_communities(community_id=community_id)

    @app.post("/api/communities", status_code=status.HTTP_201_CREATED)
    async def create_community(body: CreateCommunityRequest) -> dict[str, Any]:
        try:
            item = await community_svc.create_community(
                body.name,
                body.description,
                parent_id=body.parent_id,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to create community: {exc}",
            ) from exc
        await ctx.event_bus.publish_community_update({"action": "community_created", "item": item})
        return item

    @app.post("/api/communities/{community_id}/debates", status_code=status.HTTP_201_CREATED)
    async def create_debate(community_id: str, body: CreateDebateRequest) -> dict[str, Any]:
        try:
            post = await community_svc.post_debate(community_id, body.title, body.body)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to post debate: {exc}",
            ) from exc
        await ctx.event_bus.publish_community_update({"action": "debate_posted", "post": post})
        return post

    @app.post("/api/debates/{post_id}/replies", status_code=status.HTTP_201_CREATED)
    async def reply_debate(post_id: str, body: CreateDebateReplyRequest) -> dict[str, Any]:
        try:
            reply = await community_svc.reply_debate(post_id, body.body)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to post reply: {exc}",
            ) from exc
        await ctx.event_bus.publish_community_update({"action": "debate_replied", "reply": reply})
        return reply

    @app.get("/api/rooms")
    async def list_rooms() -> dict[str, Any]:
        return await community_svc.list_rooms()

    @app.post("/api/rooms/{room_id}/join")
    async def join_room(room_id: str) -> dict[str, Any]:
        try:
            return await community_svc.join_room(room_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to join room: {exc}",
            ) from exc

    @app.post("/api/rooms/{room_id}/messages", status_code=status.HTTP_201_CREATED)
    async def send_room_message(room_id: str, body: RoomMessageRequest) -> dict[str, Any]:
        try:
            payload = await community_svc.send_room_message(room_id, body.content)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to send room message: {exc}",
            ) from exc
        await ctx.event_bus.publish_room_message(payload)
        return payload

    @app.get("/api/chat/private")
    async def list_private_messages() -> dict[str, Any]:
        await _hydrate_pending_private_messages()
        return {
            "messages": list(ctx.private_messages_cache[-100:]),
            "blocked_peers": community_svc.list_blocked_peers(),
        }

    @app.post("/api/chat/private", status_code=status.HTTP_201_CREATED)
    async def send_private_message(body: PrivateChatRequest) -> dict[str, Any]:
        try:
            remote_peer = RemotePeer(
                peer_id=body.peer_id,
                username=body.peer_username,
                ip_address=body.peer_ip,
                tcp_port=body.peer_port,
                status=PresenceStatus(body.status),
            )
            msg: PrivateMessage = await ctx.private_chat.send_message(remote_peer, body.content)
        except PeerBlockedError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        record = {
            "message_id": msg.message_id,
            "sender_id": msg.sender_id,
            "recipient_id": msg.recipient_id,
            "content": msg.content,
            "delivered": msg.delivered,
            "created_at": msg.created_at.isoformat(),
        }
        ctx.private_messages_cache.append(record)
        await ctx.event_bus.publish_private_message(msg)
        return record

    @app.post("/api/peers/{peer_id}/block")
    async def block_peer(peer_id: str) -> dict[str, Any]:
        try:
            blocked = community_svc.block_peer(peer_id)
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"blocked": True, "blocked_peers": blocked}

    @app.post("/api/peers/{peer_id}/unblock")
    async def unblock_peer(peer_id: str) -> dict[str, Any]:
        blocked = community_svc.unblock_peer(peer_id)
        return {"blocked": False, "blocked_peers": blocked}

    # --- 6. Personal Library (Cas_04) ---

    @app.get("/api/library")
    async def list_library(refresh: bool = Query(True)) -> dict[str, Any]:
        return {"items": await community_svc.list_library(refresh=refresh)}

    @app.post("/api/library", status_code=status.HTTP_201_CREATED)
    async def save_library_item(body: LibrarySaveRequest) -> dict[str, Any]:
        try:
            entry = await community_svc.save_to_library(
                body.file_hash,
                body.title,
                category=body.category,
            )
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to save to library: {exc}",
            ) from exc
        await ctx.event_bus.publish_library_update({"action": "library_saved", "item": entry})
        return entry

    # --- Real-time WebSocket UI Channel (/ws/ui) ---

    @app.websocket("/ws/ui")
    async def ui_websocket(websocket: WebSocket) -> None:
        await ctx.event_bus.connect(websocket)
        try:
            await _hydrate_incomplete_checkpoints()
            await websocket.send_json(
                {
                    "event": "snapshot",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "data": {
                        "transfers": ctx.event_bus.list_task_snapshots(),
                        "session": _current_session(),
                    },
                }
            )
            while True:
                raw = await websocket.receive_text()
                if raw == "ping":
                    await websocket.send_json({"event": "pong"})
        except WebSocketDisconnect:
            await ctx.event_bus.disconnect(websocket)
        except Exception:
            await ctx.event_bus.disconnect(websocket)

    return app
