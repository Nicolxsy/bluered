"""FastAPI WebSocket & REST gateway for BlueRed central server."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any

from bluered_contracts.control_plane import (
    AuthErrorMessage,
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    BloomSyncMessage,
    CommunityCreateMessage,
    CommunityListMessage,
    CommunitySummary,
    ControlPlaneEnvelope,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryListMessage,
    LibrarySaveMessage,
    NatPunchRequestMessage,
    NatRelayRequestMessage,
    ReviewAckMessage,
    ReviewItem,
    ReviewPublishMessage,
    ReviewsFetchMessage,
    ReviewsListMessage,
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
from pydantic import BaseModel, Field

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
from bluered_server.contracts import IClientPushGateway
from bluered_server.domain import DomainValidationError


class WebSocketClientGateway(IClientPushGateway):
    """Tracks active WebSocket connections per user_id and pushes control-plane payloads."""

    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}

    def register_connection(self, user_id: str, websocket: WebSocket) -> None:
        self._connections[user_id] = websocket

    def unregister_connection(self, user_id: str, websocket: WebSocket | None = None) -> None:
        current = self._connections.get(user_id)
        if current is None:
            return
        if websocket is None or current is websocket:
            self._connections.pop(user_id, None)

    async def send_to_peer(self, user_id: str, payload_json: str) -> bool:
        ws = self._connections.get(user_id)
        if ws is None:
            return False
        try:
            await ws.send_text(payload_json)
            return True
        except Exception:
            self.unregister_connection(user_id, ws)
            return False

    async def broadcast(self, user_ids: Sequence[str], payload_json: str) -> int:
        sent = 0
        for user_id in user_ids:
            if await self.send_to_peer(user_id, payload_json):
                sent += 1
        return sent

    async def is_connected(self, user_id: str) -> bool:
        return user_id in self._connections

    async def close_all(self) -> None:
        for user_id, ws in list(self._connections.items()):
            try:
                await ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
            finally:
                self._connections.pop(user_id, None)


# --- REST Request/Response Schemas ---


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=6, max_length=256)


class RegisterResponse(BaseModel):
    user_id: str
    username: str


class LoginRequest(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=256)
    tcp_port: int = Field(ge=1, le=65535)
    ip_address: str = "127.0.0.1"


class LoginResponse(BaseModel):
    user_id: str
    username: str
    session_token: str


class CreateCommunityRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)


class PublishReviewRequest(BaseModel):
    rating: int = Field(ge=1, le=5)
    comment: str = Field(min_length=1, max_length=2000)


@dataclass(slots=True)
class ServerServices:
    """Container of wired application services injected by the Composition Root."""

    auth: AuthService
    presence: PresenceAndSessionService
    search: SearchCoordinatorService
    reputation: FileReputationService
    community: CommunityAndChatService
    gateway: WebSocketClientGateway


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
                msg = parse_control_message(raw)
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
        task.add_done_callback(search_tasks.discard)
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
    elif isinstance(msg, DebatePostMessage):
        await services.community.create_debate_post(
            community_id=msg.community_id,
            author_id=user_id,
            title=msg.title,
            body=msg.body,
        )
    elif isinstance(msg, DebateReplyMessage):
        # Reply routing requires community_id lookup from post_id.
        pass
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
    elif isinstance(msg, RoomJoinMessage):
        with suppress(NotFoundError):
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

    return user_id, username
