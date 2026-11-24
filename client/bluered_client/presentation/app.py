"""FastAPI presentation application for the BlueRed local peer web UI."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from fastapi import FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from bluered_client.application import (
    DiscoveryAndReviewService,
    PeerUploadListener,
    PrivateChatService,
    TransferOrchestrator,
)
from bluered_client.contracts import (
    IBloomSyncSource,
    IFileIndexRepository,
    IPeerTransport,
    IServerCoordinator,
)
from bluered_client.domain import (
    DomainError,
    DomainValidationError,
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
    RoomMessageRequest,
)

_STATIC_DIR = Path(__file__).resolve().parent / "static"


@runtime_checkable
class ICommunityActionPort(Protocol):
    """Optional control-plane extensions for communities, debates, rooms, and library."""

    async def create_community(self, name: str, description: str = "") -> None: ...

    async def post_debate(self, community_id: str, title: str, body: str) -> None: ...

    async def reply_debate(self, post_id: str, body: str) -> None: ...

    async def save_to_library(self, file_hash: str, title: str) -> None: ...

    async def join_room(self, room_id: str) -> None: ...

    async def send_room_message(
        self,
        room_id: str,
        sender_id: str,
        sender_username: str,
        content: str,
    ) -> None: ...


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
    advertised_host: str = "127.0.0.1"
    get_bloom_metrics: Callable[[], dict[str, Any]] | None = None
    list_shared_directories: Callable[[], Awaitable[list[Path]]] | None = None
    remove_shared_directory: Callable[[Path], Awaitable[bool]] | None = None
    rebuild_bloom_and_sync: Callable[[str | None], Awaitable[int]] | None = None
    connect_server: Callable[[], Awaitable[None]] | None = None
    get_session_state: Callable[[], dict[str, Any]] | None = None
    set_presence_status: Callable[[PresenceStatus], Awaitable[PresenceStatus]] | None = None
    communities_cache: list[dict[str, Any]] = field(default_factory=list)
    debates_cache: list[dict[str, Any]] = field(default_factory=list)
    rooms_cache: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {
                "id": "general",
                "name": "General",
                "visibility": "public",
                "member_count": 1,
            },
            {
                "id": "music-lossless",
                "name": "Audio & Vinilos",
                "visibility": "public",
                "member_count": 1,
            },
            {
                "id": "papers-open",
                "name": "Documentos Abiertos",
                "visibility": "public",
                "member_count": 1,
            },
        ]
    )
    room_messages_cache: list[dict[str, Any]] = field(default_factory=list)
    private_messages_cache: list[dict[str, Any]] = field(default_factory=list)
    library_cache: list[dict[str, Any]] = field(default_factory=list)
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
    status_val = getattr(ctx.server_coordinator, "presence_status", PresenceStatus.OFFLINE)
    presence_str = (
        status_val.value if isinstance(status_val, PresenceStatus) else str(status_val)
    )
    return {
        "connected": connected,
        "authenticated": user_id is not None,
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


def create_client_app(
    ctx: ClientAppContext,
    *,
    on_startup: Callable[[], Awaitable[None]] | None = None,
    on_shutdown: Callable[[], Awaitable[None]] | None = None,
) -> FastAPI:
    """Build the local FastAPI application for a BlueRed peer."""
    background_transfers: dict[str, asyncio.Task[TransferTask]] = {}
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
        if session_override["authenticated"]:
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
            "blocked_peers": sorted(ctx.local_peer.blocked_peer_ids),
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
        session_override["authenticated"] = True
        session_override["presence_status"] = PresenceStatus.ONLINE.value
        if ctx.local_peer.shared_directories:
            await ctx.discovery.configure_and_index(
                list(ctx.local_peer.shared_directories),
                user_id=user_id,
            )
        elif isinstance(ctx.file_index, IBloomSyncSource):
            size_bits, k, bitset_b64 = ctx.file_index.export_bloom_sync_payload()
            await ctx.server_coordinator.sync_bloom_filter(user_id, size_bits, k, bitset_b64)
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
        await ctx.server_coordinator.disconnect()
        session_override["authenticated"] = False
        session_override["presence_status"] = PresenceStatus.OFFLINE.value
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
            session_override["presence_status"] = resolved.value
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
        await ctx.event_bus.publish_presence_update(
            {
                "user_id": ctx.local_peer.peer_id,
                "username": ctx.local_peer.username,
                "status": str(session_override["presence_status"]),
            }
        )
        return {"presence_status": str(session_override["presence_status"])}

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
        if snapshot is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transfer '{task_id}' not found",
            )
        return snapshot

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
                _remember_library_download(ctx, shared_file)
            return payload

        async def _run_download() -> TransferTask:
            result = await ctx.transfers.start_download(
                remote_peer,
                shared_file,
                conflict_policy=body.conflict_policy,
                task_id=task_id,
            )
            _task_to_dict(result)
            if result.state.value == "COMPLETED":
                _remember_library_download(ctx, shared_file)
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
    async def resume_transfer(task_id: str, wait: bool = Query(default=False)) -> dict[str, Any]:
        if wait:
            try:
                task = await ctx.transfers.resume_download(task_id)
            except KeyError as exc:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
            except (ValueError, DomainError) as exc:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
            return _task_to_dict(task)

        existing = ctx.transfers.get_task(task_id)
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transfer '{task_id}' not found",
            )

        async def _run_resume() -> TransferTask:
            resumed = await ctx.transfers.resume_download(task_id)
            _task_to_dict(resumed)
            return resumed

        bg_task = asyncio.create_task(_run_resume(), name=f"bluered-resume-{task_id[:8]}")
        background_transfers[task_id] = bg_task
        bg_task.add_done_callback(lambda _t: background_transfers.pop(task_id, None))
        await asyncio.sleep(0)
        return _task_to_dict(existing)

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
    async def list_communities() -> dict[str, Any]:
        return {
            "communities": list(ctx.communities_cache),
            "debates": list(ctx.debates_cache),
        }

    @app.post("/api/communities", status_code=status.HTTP_201_CREATED)
    async def create_community(body: CreateCommunityRequest) -> dict[str, Any]:
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.create_community(body.name, body.description)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to create community: {exc}",
                ) from exc
        item = {
            "id": uuid.uuid4().hex[:12],
            "name": body.name.strip(),
            "description": body.description.strip(),
            "parent_id": body.parent_id,
            "created_at": datetime.now(UTC).isoformat(),
        }
        ctx.communities_cache.append(item)
        return item

    @app.post("/api/communities/{community_id}/debates", status_code=status.HTTP_201_CREATED)
    async def create_debate(community_id: str, body: CreateDebateRequest) -> dict[str, Any]:
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.post_debate(community_id, body.title, body.body)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to post debate: {exc}",
                ) from exc
        post = {
            "id": uuid.uuid4().hex[:12],
            "community_id": community_id,
            "author_username": ctx.local_peer.username,
            "title": body.title.strip(),
            "body": body.body.strip(),
            "replies": [],
            "created_at": datetime.now(UTC).isoformat(),
        }
        ctx.debates_cache.insert(0, post)
        return post

    @app.post("/api/debates/{post_id}/replies", status_code=status.HTTP_201_CREATED)
    async def reply_debate(post_id: str, body: CreateDebateReplyRequest) -> dict[str, Any]:
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.reply_debate(post_id, body.body)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to post reply: {exc}",
                ) from exc
        reply = {
            "id": uuid.uuid4().hex[:12],
            "post_id": post_id,
            "author_username": ctx.local_peer.username,
            "body": body.body.strip(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        for debate in ctx.debates_cache:
            if debate["id"] == post_id:
                debate["replies"].append(reply)
                break
        return reply

    @app.get("/api/rooms")
    async def list_rooms() -> dict[str, Any]:
        return {
            "rooms": list(ctx.rooms_cache),
            "messages": list(ctx.room_messages_cache[-100:]),
        }

    @app.post("/api/rooms/{room_id}/join")
    async def join_room(room_id: str) -> dict[str, Any]:
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.join_room(room_id)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to join room: {exc}",
                ) from exc
        if not any(r["id"] == room_id for r in ctx.rooms_cache):
            ctx.rooms_cache.append(
                {"id": room_id, "name": room_id, "visibility": "private", "member_count": 1}
            )
        return {"room_id": room_id, "joined": True}

    @app.post("/api/rooms/{room_id}/messages", status_code=status.HTTP_201_CREATED)
    async def send_room_message(room_id: str, body: RoomMessageRequest) -> dict[str, Any]:
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.send_room_message(
                    room_id=room_id,
                    sender_id=ctx.local_peer.peer_id,
                    sender_username=ctx.local_peer.username,
                    content=body.content,
                )
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to send room message: {exc}",
                ) from exc
        payload = {
            "room_id": room_id,
            "sender_id": ctx.local_peer.peer_id,
            "sender_username": ctx.local_peer.username,
            "content": body.content.strip(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        ctx.room_messages_cache.append(payload)
        await ctx.event_bus.publish_room_message(payload)
        return payload

    @app.get("/api/chat/private")
    async def list_private_messages() -> dict[str, Any]:
        return {
            "messages": list(ctx.private_messages_cache[-100:]),
            "blocked_peers": sorted(ctx.local_peer.blocked_peer_ids),
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
            ctx.local_peer.block_peer(peer_id)
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"blocked": True, "blocked_peers": sorted(ctx.local_peer.blocked_peer_ids)}

    @app.post("/api/peers/{peer_id}/unblock")
    async def unblock_peer(peer_id: str) -> dict[str, Any]:
        ctx.local_peer.unblock_peer(peer_id)
        return {"blocked": False, "blocked_peers": sorted(ctx.local_peer.blocked_peer_ids)}

    # --- 6. Personal Library (Cas_04) ---

    @app.get("/api/library")
    async def list_library() -> dict[str, Any]:
        return {"items": list(ctx.library_cache)}

    @app.post("/api/library", status_code=status.HTTP_201_CREATED)
    async def save_library_item(body: LibrarySaveRequest) -> dict[str, Any]:
        try:
            SharedFile(file_hash=body.file_hash, file_name=body.title, size_bytes=0)
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        if isinstance(ctx.server_coordinator, ICommunityActionPort):
            try:
                await ctx.server_coordinator.save_to_library(body.file_hash, body.title)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Failed to save to library: {exc}",
                ) from exc
        entry = {
            "file_hash": body.file_hash,
            "title": body.title.strip(),
            "category": body.category,
            "saved_at_iso": datetime.now(UTC).isoformat(),
            "source": "preserved",
        }
        ctx.library_cache = [i for i in ctx.library_cache if i["file_hash"] != body.file_hash]
        ctx.library_cache.insert(0, entry)
        return entry

    # --- Real-time WebSocket UI Channel (/ws/ui) ---

    @app.websocket("/ws/ui")
    async def ui_websocket(websocket: WebSocket) -> None:
        await ctx.event_bus.connect(websocket)
        try:
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


def _remember_library_download(ctx: ClientAppContext, shared_file: SharedFile) -> None:
    if any(i["file_hash"] == shared_file.file_hash for i in ctx.library_cache):
        return
    ctx.library_cache.insert(
        0,
        {
            "file_hash": shared_file.file_hash,
            "title": shared_file.file_name,
            "category": shared_file.category,
            "saved_at_iso": datetime.now(UTC).isoformat(),
            "source": "downloaded",
        },
    )
