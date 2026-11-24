"""FastAPI WebSocket & REST presentation layer for BlueRed central server."""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from bluered_contracts.control_plane import (
    AuthErrorMessage,
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    BloomSyncMessage,
    CommunityCreateMessage,
    CommunityFetchMessage,
    CommunityListMessage,
    CommunitySummary,
    ControlPlaneEnvelope,
    DebateFetchMessage,
    DebateItem,
    DebateListMessage,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryFetchMessage,
    LibraryListMessage,
    LibrarySaveMessage,
    NatPunchRequestMessage,
    NatRelayRequestMessage,
    ReviewAckMessage,
    ReviewItem,
    ReviewPublishMessage,
    ReviewsFetchMessage,
    ReviewsListMessage,
    RoomFetchMessage,
    RoomJoinMessage,
    RoomListMessage,
    RoomMessage,
    RoomSummary,
    SearchHitReportMessage,
    SearchQueryMessage,
    parse_control_message,
    serialize_control_message,
)
from bluered_contracts.control_plane import (
    LibraryItem as ContractLibraryItem,
)
from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from pydantic import ValidationError as PydanticValidationError

from bluered_server.application import (
    AccountLockedError,
    AuthenticationError,
    AuthService,
    CommunityAndChatService,
    DuplicateSessionError,
    DuplicateUserError,
    FileReputationService,
    NotFoundError,
    PresenceAndSessionService,
    SearchCoordinatorService,
    ValidationError,
)
from bluered_server.contracts import IWebSocketGateway
from bluered_server.domain import DebatePost, DomainValidationError

from .schemas import (
    CreateCommunityRequest,
    LoginRequest,
    LoginResponse,
    PublishReviewRequest,
    RegisterRequest,
    RegisterResponse,
)

_logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ServerServices:
    """Container of wired application services injected by the Composition Root."""

    auth: AuthService
    presence: PresenceAndSessionService
    search: SearchCoordinatorService
    reputation: FileReputationService
    community: CommunityAndChatService
    gateway: IWebSocketGateway


def _build_debate_list_message(
    community_id: str, posts: Sequence[DebatePost]
) -> DebateListMessage:
    return DebateListMessage(
        community_id=community_id,
        posts=[
            DebateItem(
                post_id=p.post_id,
                community_id=p.community_id,
                author_id=p.author_id,
                title=p.title,
                body=p.body,
                parent_post_id=p.parent_post_id,
                created_at_iso=p.created_at.isoformat(),
            )
            for p in posts
        ],
    )


def create_fastapi_app(
    services: ServerServices,
    *,
    on_startup: Callable[[], Awaitable[None]] | None = None,
    on_shutdown: Callable[[], Awaitable[None]] | None = None,
) -> FastAPI:
    """Create the FastAPI application with lifespan, REST endpoints, and /ws gateway."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.services = services
        if on_startup is not None:
            await on_startup()
        yield
        await services.gateway.close_all()
        if on_shutdown is not None:
            await on_shutdown()

    app = FastAPI(title="BlueRed Central Server", version="0.1.0", lifespan=lifespan)

    def _require_bearer_user(authorization: str | None = Header(default=None)) -> tuple[str, str]:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid Authorization header",
            )
        token = authorization.removeprefix("Bearer ").strip()
        verified = services.auth.verify_token(token)
        if verified is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )
        return verified

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/auth/register",
        response_model=RegisterResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def register_endpoint(body: RegisterRequest) -> RegisterResponse:
        try:
            account = await services.auth.register(body.username, body.password)
        except DuplicateUserError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except (ValidationError, DomainValidationError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return RegisterResponse(user_id=account.user_id, username=account.username)

    @app.post("/auth/login", response_model=LoginResponse)
    async def login_endpoint(body: LoginRequest) -> LoginResponse:
        try:
            account, token = await services.auth.login(
                username=body.username,
                password=body.password,
                ip_address=body.ip_address,
                tcp_port=body.tcp_port,
            )
        except AccountLockedError as exc:
            raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc)) from exc
        except DuplicateSessionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except AuthenticationError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
        except (ValidationError, DomainValidationError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return LoginResponse(
            user_id=account.user_id,
            username=account.username,
            session_token=token,
        )

    @app.get("/communities")
    async def list_communities_endpoint() -> list[dict[str, str]]:
        items = await services.community.list_communities()
        return [
            {
                "id": c.community_id,
                "name": c.name,
                "description": c.description,
                "creator_id": c.creator_id,
            }
            for c in items
        ]

    @app.post("/communities", status_code=status.HTTP_201_CREATED)
    async def create_community_endpoint(
        body: CreateCommunityRequest,
        user: tuple[str, str] = Depends(_require_bearer_user),
    ) -> dict[str, str]:
        user_id, _ = user
        try:
            c = await services.community.create_community(
                name=body.name,
                description=body.description,
                creator_id=user_id,
            )
        except DomainValidationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"id": c.community_id, "name": c.name, "description": c.description}

    @app.get("/communities/{community_id}/debates")
    async def list_community_debates_endpoint(community_id: str) -> list[dict[str, Any]]:
        try:
            await services.community.get_community(community_id)
            posts = await services.community.list_debate_posts(community_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return [
            {
                "post_id": p.post_id,
                "community_id": p.community_id,
                "author_id": p.author_id,
                "title": p.title,
                "body": p.body,
                "parent_post_id": p.parent_post_id,
                "created_at_iso": p.created_at.isoformat(),
            }
            for p in posts
        ]

    @app.get("/rooms")
    async def list_rooms_endpoint() -> list[dict[str, Any]]:
        rooms = await services.community.list_chat_rooms()
        return [
            {
                "id": r.room_id,
                "name": r.name,
                "community_id": r.community_id,
                "member_count": r.member_count,
            }
            for r in rooms
        ]

    @app.get("/library/{user_id}")
    async def list_library_endpoint(
        user_id: str,
        user: tuple[str, str] = Depends(_require_bearer_user),
    ) -> list[dict[str, str]]:
        token_user_id, _ = user
        if token_user_id != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cannot access another user's personal library",
            )
        items = await services.community.list_library_items(user_id)
        return [
            {
                "item_id": i.item_id,
                "owner_id": i.owner_id,
                "file_hash": i.file_hash,
                "title": i.title,
                "saved_at_iso": i.saved_at.isoformat(),
            }
            for i in items
        ]

    @app.get("/reviews/{file_hash}")
    async def get_reviews_endpoint(
        file_hash: str,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=100),
    ) -> dict[str, Any]:
        try:
            summary, reviews, total = await services.reputation.get_reviews(
                file_hash, page=page, page_size=page_size
            )
        except (ValidationError, DomainValidationError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {
            "file_hash": summary.file_hash,
            "review_count": summary.review_count,
            "avg_rating": summary.avg_rating,
            "total": total,
            "page": page,
            "page_size": page_size,
            "reviews": [
                {
                    "review_id": r.review_id,
                    "author_id": r.author_id,
                    "author_username": r.author_username,
                    "rating": r.rating,
                    "comment": r.comment,
                    "updated_at_iso": r.updated_at.isoformat(),
                }
                for r in reviews
            ],
        }

    @app.post("/reviews/{file_hash}", status_code=status.HTTP_201_CREATED)
    async def publish_review_endpoint(
        file_hash: str,
        body: PublishReviewRequest,
        user: tuple[str, str] = Depends(_require_bearer_user),
    ) -> dict[str, Any]:
        user_id, username = user
        try:
            summary = await services.reputation.publish_review(
                file_hash=file_hash,
                author_id=user_id,
                author_username=username,
                rating=body.rating,
                comment=body.comment,
            )
        except (ValidationError, DomainValidationError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {
            "file_hash": summary.file_hash,
            "review_count": summary.review_count,
            "avg_rating": summary.avg_rating,
        }

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        user_id: str | None = None
        username: str | None = None

        token = websocket.query_params.get("token")
        if token:
            verified = services.auth.verify_token(token)
            if verified is None:
                await websocket.send_text(
                    serialize_control_message(
                        AuthErrorMessage(code="INVALID_TOKEN", detail="Invalid or expired token")
                    )
                )
                await websocket.close(code=1008)
                return
            user_id, username = verified
            services.gateway.register_connection(user_id, websocket)

        client_ip = websocket.client.host if websocket.client else "127.0.0.1"
        search_tasks: set[asyncio.Task[Any]] = set()

        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    msg = parse_control_message(raw)
                except (PydanticValidationError, ValueError) as exc:
                    await websocket.send_text(
                        serialize_control_message(
                            AuthErrorMessage(code="INVALID_MESSAGE", detail=str(exc))
                        )
                    )
                    continue
                user_id, username = await _dispatch_ws_message(
                    websocket=websocket,
                    services=services,
                    msg=msg,
                    user_id=user_id,
                    username=username,
                    client_ip=client_ip,
                    search_tasks=search_tasks,
                )
        except WebSocketDisconnect:
            pass
        finally:
            for task in list(search_tasks):
                task.cancel()
            if search_tasks:
                await asyncio.gather(*search_tasks, return_exceptions=True)
            if user_id is not None:
                services.gateway.unregister_connection(user_id, websocket)
                await services.presence.disconnect(user_id)

    return app


async def _dispatch_ws_message(
    *,
    websocket: WebSocket,
    services: ServerServices,
    msg: ControlPlaneEnvelope,
    user_id: str | None,
    username: str | None,
    client_ip: str,
    search_tasks: set[asyncio.Task[Any]],
) -> tuple[str | None, str | None]:
    if isinstance(msg, AuthRegisterMessage):
        try:
            account = await services.auth.register(msg.username, msg.password)
            await websocket.send_text(
                serialize_control_message(
                    AuthOkMessage(
                        user_id=account.user_id,
                        username=account.username,
                        session_token="registered",
                    )
                )
            )
        except (DuplicateUserError, ValidationError, DomainValidationError) as exc:
            await websocket.send_text(
                serialize_control_message(AuthErrorMessage(code="REGISTER_FAILED", detail=str(exc)))
            )
        return user_id, username

    if isinstance(msg, AuthLoginMessage):
        try:
            account, session_token = await services.auth.login(
                username=msg.username,
                password=msg.password,
                ip_address=client_ip,
                tcp_port=msg.tcp_port,
            )
            services.gateway.register_connection(account.user_id, websocket)
            await websocket.send_text(
                serialize_control_message(
                    AuthOkMessage(
                        user_id=account.user_id,
                        username=account.username,
                        session_token=session_token,
                    )
                )
            )
            return account.user_id, account.username
        except AccountLockedError as exc:
            await websocket.send_text(
                serialize_control_message(AuthErrorMessage(code="ACCOUNT_LOCKED", detail=str(exc)))
            )
        except DuplicateSessionError as exc:
            await websocket.send_text(
                serialize_control_message(
                    AuthErrorMessage(code="DUPLICATE_SESSION", detail=str(exc))
                )
            )
        except AuthenticationError as exc:
            await websocket.send_text(
                serialize_control_message(
                    AuthErrorMessage(code="INVALID_CREDENTIALS", detail=str(exc))
                )
            )
        except (ValidationError, DomainValidationError) as exc:
            await websocket.send_text(
                serialize_control_message(
                    AuthErrorMessage(code="VALIDATION_ERROR", detail=str(exc))
                )
            )
        return user_id, username

    if user_id is None or username is None:
        await websocket.send_text(
            serialize_control_message(
                AuthErrorMessage(
                    code="UNAUTHENTICATED",
                    detail="Login required before sending commands",
                )
            )
        )
        return user_id, username

    try:
        await _dispatch_authenticated_message(
            websocket=websocket,
            services=services,
            msg=msg,
            user_id=user_id,
            username=username,
            client_ip=client_ip,
            search_tasks=search_tasks,
        )
    except NotFoundError as exc:
        if isinstance(msg, (HeartbeatMessage, BloomSyncMessage)):
            services.gateway.unregister_connection(user_id, websocket)
            await websocket.send_text(
                serialize_control_message(
                    AuthErrorMessage(code="SESSION_EXPIRED", detail=str(exc))
                )
            )
            return None, None
        await websocket.send_text(
            serialize_control_message(AuthErrorMessage(code="NOT_FOUND", detail=str(exc)))
        )
    except (ValidationError, DomainValidationError, binascii.Error, ValueError) as exc:
        await websocket.send_text(
            serialize_control_message(AuthErrorMessage(code="VALIDATION_ERROR", detail=str(exc)))
        )

    return user_id, username


async def _dispatch_authenticated_message(
    *,
    websocket: WebSocket,
    services: ServerServices,
    msg: ControlPlaneEnvelope,
    user_id: str,
    username: str,
    client_ip: str,
    search_tasks: set[asyncio.Task[Any]],
) -> None:
    if isinstance(msg, HeartbeatMessage):
        await services.presence.heartbeat(user_id, tcp_port=msg.tcp_port, ip_address=client_ip)
    elif isinstance(msg, BloomSyncMessage):
        bitset = base64.b64decode(msg.bitset_base64.encode("ascii"), validate=True)
        await services.presence.sync_bloom_filter(
            user_id=user_id,
            bitset=bitset,
            size_bits=msg.size_bits,
            hash_functions_k=msg.hash_functions_k,
        )
    elif isinstance(msg, SearchQueryMessage):
        task = asyncio.create_task(
            services.search.search_and_wait(
                requester_id=user_id,
                query=msg.query,
                search_token=msg.search_token,
                max_results=msg.max_results,
            )
        )
        search_tasks.add(task)

        def _on_search_done(done_task: asyncio.Task[Any]) -> None:
            search_tasks.discard(done_task)
            if done_task.cancelled():
                return
            exc = done_task.exception()
            if exc is not None:
                _logger.error("Background search task failed: %s", exc, exc_info=exc)

        task.add_done_callback(_on_search_done)
    elif isinstance(msg, SearchHitReportMessage):
        await services.search.handle_hit_report(
            search_token=msg.search_token,
            hits=[h.model_dump() for h in msg.hits],
        )
    elif isinstance(msg, ReviewPublishMessage):
        summary = await services.reputation.publish_review(
            file_hash=msg.file_hash,
            author_id=user_id,
            author_username=username,
            rating=msg.rating,
            comment=msg.comment,
        )
        await websocket.send_text(
            serialize_control_message(
                ReviewAckMessage(
                    file_hash=summary.file_hash,
                    review_count=summary.review_count,
                    avg_rating=summary.avg_rating,
                )
            )
        )
    elif isinstance(msg, ReviewsFetchMessage):
        summary, reviews, _ = await services.reputation.get_reviews(msg.file_hash)
        await websocket.send_text(
            serialize_control_message(
                ReviewsListMessage(
                    file_hash=summary.file_hash,
                    review_count=summary.review_count,
                    avg_rating=summary.avg_rating,
                    reviews=[
                        ReviewItem(
                            file_hash=r.file_hash,
                            author_id=r.author_id,
                            author_username=r.author_username,
                            rating=r.rating,
                            comment=r.comment,
                            updated_at_iso=r.updated_at.isoformat(),
                        )
                        for r in reviews
                    ],
                )
            )
        )
    elif isinstance(msg, CommunityCreateMessage):
        await services.community.create_community(
            name=msg.name,
            description=msg.description,
            creator_id=user_id,
        )
        communities = await services.community.list_communities()
        await websocket.send_text(
            serialize_control_message(
                CommunityListMessage(
                    communities=[
                        CommunitySummary(id=c.community_id, name=c.name, description=c.description)
                        for c in communities
                    ]
                )
            )
        )
    elif isinstance(msg, CommunityFetchMessage):
        communities = await services.community.list_communities()
        await websocket.send_text(
            serialize_control_message(
                CommunityListMessage(
                    communities=[
                        CommunitySummary(id=c.community_id, name=c.name, description=c.description)
                        for c in communities
                    ]
                )
            )
        )
    elif isinstance(msg, DebatePostMessage):
        await services.community.create_debate_post(
            community_id=msg.community_id,
            author_id=user_id,
            title=msg.title,
            body=msg.body,
        )
        posts = await services.community.list_debate_posts(msg.community_id)
        await websocket.send_text(
            serialize_control_message(_build_debate_list_message(msg.community_id, posts))
        )
    elif isinstance(msg, DebateReplyMessage):
        reply = await services.community.reply_debate(
            post_id=msg.post_id,
            author_id=user_id,
            body=msg.body,
        )
        posts = await services.community.list_debate_posts(reply.community_id)
        await websocket.send_text(
            serialize_control_message(_build_debate_list_message(reply.community_id, posts))
        )
    elif isinstance(msg, DebateFetchMessage):
        await services.community.get_community(msg.community_id)
        posts = await services.community.list_debate_posts(msg.community_id)
        await websocket.send_text(
            serialize_control_message(_build_debate_list_message(msg.community_id, posts))
        )
    elif isinstance(msg, LibrarySaveMessage):
        await services.community.save_library_item(
            owner_id=user_id,
            file_hash=msg.file_hash,
            title=msg.title,
        )
        items = await services.community.list_library_items(user_id)
        await websocket.send_text(
            serialize_control_message(
                LibraryListMessage(
                    items=[
                        ContractLibraryItem(
                            file_hash=i.file_hash,
                            title=i.title,
                            saved_at_iso=i.saved_at.isoformat(),
                        )
                        for i in items
                    ]
                )
            )
        )
    elif isinstance(msg, LibraryFetchMessage):
        items = await services.community.list_library_items(user_id)
        await websocket.send_text(
            serialize_control_message(
                LibraryListMessage(
                    items=[
                        ContractLibraryItem(
                            file_hash=i.file_hash,
                            title=i.title,
                            saved_at_iso=i.saved_at.isoformat(),
                        )
                        for i in items
                    ]
                )
            )
        )
    elif isinstance(msg, RoomFetchMessage):
        rooms = await services.community.list_chat_rooms()
        await websocket.send_text(
            serialize_control_message(
                RoomListMessage(
                    rooms=[
                        RoomSummary(id=r.room_id, name=r.name, member_count=r.member_count)
                        for r in rooms
                    ]
                )
            )
        )
    elif isinstance(msg, RoomJoinMessage):
        await services.community.join_chat_room(msg.room_id, user_id)
        rooms = await services.community.list_chat_rooms()
        await websocket.send_text(
            serialize_control_message(
                RoomListMessage(
                    rooms=[
                        RoomSummary(id=r.room_id, name=r.name, member_count=r.member_count)
                        for r in rooms
                    ]
                )
            )
        )
    elif isinstance(msg, RoomMessage):
        await services.community.send_room_message(
            room_id=msg.room_id,
            sender_id=user_id,
            sender_username=username,
            content=msg.content,
        )
    elif isinstance(msg, NatPunchRequestMessage):
        await services.presence.coordinate_nat_punch(
            requester_id=user_id,
            target_peer_id=msg.target_peer_id,
            requester_ip=msg.requester_ip,
            requester_port=msg.requester_port,
        )
    elif isinstance(msg, NatRelayRequestMessage):
        await services.presence.request_nat_relay(
            requester_id=user_id,
            target_peer_id=msg.target_peer_id,
            session_id=msg.session_id,
        )
