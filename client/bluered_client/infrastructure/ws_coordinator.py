"""WebSocket control-plane client coordinator using websockets.asyncio.client."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

from bluered_contracts.control_plane import (
    AuthErrorMessage,
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    BloomSyncMessage,
    CommunityCreateMessage,
    CommunityListMessage,
    ControlPlaneEnvelope,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryListMessage,
    LibrarySaveMessage,
    NatPunchCoordinateMessage,
    NatPunchRequestMessage,
    NatRelayRequestMessage,
    PresenceUpdateMessage,
    ReviewAckMessage,
    ReviewPublishMessage,
    ReviewsFetchMessage,
    ReviewsListMessage,
    RoomJoinMessage,
    RoomListMessage,
    RoomMessage,
    SearchBroadcastMessage,
    SearchHitItem,
    SearchHitReportMessage,
    SearchQueryMessage,
    SearchResultsMessage,
    parse_control_message,
    serialize_control_message,
)
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from bluered_client.domain import FileReview, PresenceStatus, RemotePeer, SearchMatch, SharedFile

SearchBroadcastHandler = Callable[
    [SearchBroadcastMessage],
    Awaitable[list[SearchHitItem]],
]
RoomMessageHandler = Callable[[RoomMessage], Awaitable[None]]
NatSignalHandler = Callable[
    [NatPunchCoordinateMessage | NatRelayRequestMessage],
    Awaitable[None],
]
PresenceHandler = Callable[[PresenceUpdateMessage], Awaitable[None]]


class IWebSocketTransport(Protocol):
    """Minimal async WebSocket transport interface for dependency injection/testing."""

    async def send(self, message: str | bytes) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


ConnectFactory = Callable[[str], Awaitable[IWebSocketTransport]]


class ControlPlaneError(RuntimeError):
    """Raised when the central server returns an error or times out."""


class WebSocketServerCoordinator:
    """Persistent WebSocket control-plane client implementing IServerCoordinator."""

    def __init__(
        self,
        *,
        ws_path: str = "/ws",
        heartbeat_interval_s: float = 15.0,
        inactivity_timeout_s: float = 300.0,
        request_timeout_s: float = 10.0,
        auto_reconnect: bool = True,
        connect_fn: ConnectFactory | None = None,
    ) -> None:
        self._ws_path = ws_path
        self._heartbeat_interval_s = heartbeat_interval_s
        self._inactivity_timeout_s = inactivity_timeout_s
        self._request_timeout_s = request_timeout_s
        self._auto_reconnect = auto_reconnect
        self._connect_fn = connect_fn or self._default_connect

        self._ws: IWebSocketTransport | None = None
        self._uri: str | None = None
        self._closing: bool = False
        self._recv_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

        self._user_id: str | None = None
        self._username: str | None = None
        self._session_token: str | None = None
        self._tcp_port: int | None = None
        self._status: PresenceStatus = PresenceStatus.OFFLINE
        self._last_activity: float = time.monotonic()

        self._auth_future: asyncio.Future[AuthOkMessage] | None = None
        self._review_ack_future: asyncio.Future[ReviewAckMessage] | None = None
        self._search_futures: dict[str, asyncio.Future[SearchResultsMessage]] = {}
        self._reviews_futures: dict[str, asyncio.Future[ReviewsListMessage]] = {}

        self._on_search_broadcast: SearchBroadcastHandler | None = None
        self._on_room_message: RoomMessageHandler | None = None
        self._on_nat_signal: NatSignalHandler | None = None
        self._on_presence: PresenceHandler | None = None
        self._last_bloom_payload: tuple[int, int, str] | None = None

    @staticmethod
    async def _default_connect(uri: str) -> IWebSocketTransport:
        return await ws_connect(uri, ping_interval=20.0, ping_timeout=20.0)

    @property
    def is_connected(self) -> bool:
        return self._ws is not None

    @property
    def presence_status(self) -> PresenceStatus:
        return self._status

    @property
    def user_id(self) -> str | None:
        return self._user_id

    def record_activity(self) -> None:
        """Reset inactivity timer and restore ONLINE status if currently AWAY."""
        self._last_activity = time.monotonic()
        if self._ws is not None and self._status == PresenceStatus.AWAY:
            self._status = PresenceStatus.ONLINE

    def set_search_broadcast_handler(self, handler: SearchBroadcastHandler) -> None:
        self._on_search_broadcast = handler

    def set_room_message_handler(self, handler: RoomMessageHandler) -> None:
        self._on_room_message = handler

    def set_nat_signal_handler(self, handler: NatSignalHandler) -> None:
        self._on_nat_signal = handler

    def set_presence_handler(self, handler: PresenceHandler) -> None:
        self._on_presence = handler

    async def connect(self, host: str, port: int) -> None:
        """Open persistent WebSocket connection and start recv + heartbeat tasks."""
        self._closing = False
        self._uri = f"ws://{host}:{port}{self._ws_path}"
        self._ws = await self._connect_fn(self._uri)
        self._status = PresenceStatus.ONLINE
        self._last_activity = time.monotonic()
        self._recv_task = asyncio.create_task(self._recv_loop(), name="bluered-ws-recv")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="bluered-ws-heartbeat"
        )

    async def disconnect(self) -> None:
        """Gracefully close WebSocket and cancel background tasks."""
        self._closing = True
        self._status = PresenceStatus.OFFLINE
        for task in (self._heartbeat_task, self._recv_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._heartbeat_task = None
        self._recv_task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        self._fail_pending_futures(ControlPlaneError("Disconnected from server"))

    def _fail_pending_futures(self, exc: BaseException) -> None:
        if self._auth_future is not None and not self._auth_future.done():
            self._auth_future.set_exception(exc)
        if self._review_ack_future is not None and not self._review_ack_future.done():
            self._review_ack_future.set_exception(exc)
        for search_fut in self._search_futures.values():
            if not search_fut.done():
                search_fut.set_exception(exc)
        self._search_futures.clear()
        for rev_fut in self._reviews_futures.values():
            if not rev_fut.done():
                rev_fut.set_exception(exc)
        self._reviews_futures.clear()

    async def _send_model(self, msg: ControlPlaneEnvelope) -> None:
        if self._ws is None:
            raise ControlPlaneError("WebSocket is not connected")
        await self._ws.send(serialize_control_message(msg))

    async def _recv_loop(self) -> None:
        backoff = 0.5
        while not self._closing:
            ws = self._ws
            if ws is None:
                break
            try:
                raw = await ws.recv()
                backoff = 0.5
                msg = parse_control_message(raw)
                await self._dispatch_message(msg)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, EOFError, ConnectionError):
                self._ws = None
                if self._closing or not self._auto_reconnect or self._uri is None:
                    self._status = PresenceStatus.OFFLINE
                    self._fail_pending_futures(ControlPlaneError("Connection closed"))
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 15.0)
                try:
                    self._ws = await self._connect_fn(self._uri)
                    if self._user_id is not None and self._tcp_port is not None:
                        await self.send_heartbeat(self._user_id, self._tcp_port)
                        if self._last_bloom_payload is not None:
                            size_bits, k, bitset_b64 = self._last_bloom_payload
                            await self.sync_bloom_filter(
                                self._user_id, size_bits, k, bitset_b64
                            )
                except Exception:
                    self._ws = None
            except Exception as exc:
                self._fail_pending_futures(exc)

    async def _heartbeat_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self._heartbeat_interval_s)
            if self._closing or self._ws is None:
                continue
            elapsed = time.monotonic() - self._last_activity
            if elapsed >= self._inactivity_timeout_s and self._status == PresenceStatus.ONLINE:
                self._status = PresenceStatus.AWAY
            if self._user_id is not None and self._tcp_port is not None:
                with contextlib.suppress(Exception):
                    await self.send_heartbeat(self._user_id, self._tcp_port)

    async def _dispatch_message(self, msg: ControlPlaneEnvelope) -> None:
        if isinstance(msg, AuthOkMessage):
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_result(msg)
        elif isinstance(msg, AuthErrorMessage):
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_exception(
                    ControlPlaneError(f"{msg.code}: {msg.detail}")
                )
        elif isinstance(msg, SearchResultsMessage):
            fut = self._search_futures.pop(msg.search_token, None)
            if fut is not None and not fut.done():
                fut.set_result(msg)
        elif isinstance(msg, ReviewAckMessage):
            if self._review_ack_future is not None and not self._review_ack_future.done():
                self._review_ack_future.set_result(msg)
        elif isinstance(msg, ReviewsListMessage):
            rev_fut = self._reviews_futures.pop(msg.file_hash, None)
            if rev_fut is not None and not rev_fut.done():
                rev_fut.set_result(msg)
        elif isinstance(msg, SearchBroadcastMessage):
            if self._on_search_broadcast is not None:
                hits = await self._on_search_broadcast(msg)
                await self._send_model(
                    SearchHitReportMessage(search_token=msg.search_token, hits=hits)
                )
        elif isinstance(msg, RoomMessage):
            if self._on_room_message is not None:
                await self._on_room_message(msg)
        elif isinstance(msg, (NatPunchCoordinateMessage, NatRelayRequestMessage)):
            if self._on_nat_signal is not None:
                await self._on_nat_signal(msg)
        elif isinstance(msg, PresenceUpdateMessage):
            if self._on_presence is not None:
                await self._on_presence(msg)
        elif isinstance(msg, (CommunityListMessage, LibraryListMessage, RoomListMessage)):
            pass

    # --- IServerCoordinator ---

    async def register(self, username: str, password: str) -> str:
        """Register a new account and return the assigned user_id."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()
        await self._send_model(AuthRegisterMessage(username=username, password=password))
        auth_ok = await asyncio.wait_for(self._auth_future, timeout=self._request_timeout_s)
        self._user_id = auth_ok.user_id
        self._username = auth_ok.username
        self._session_token = auth_ok.session_token
        return auth_ok.user_id

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        """Authenticate and return (user_id, session_token)."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()
        self._tcp_port = tcp_port
        await self._send_model(
            AuthLoginMessage(username=username, password=password, tcp_port=tcp_port)
        )
        auth_ok = await asyncio.wait_for(self._auth_future, timeout=self._request_timeout_s)
        self._user_id = auth_ok.user_id
        self._username = auth_ok.username
        self._session_token = auth_ok.session_token
        self._status = PresenceStatus.ONLINE
        return (auth_ok.user_id, auth_ok.session_token)

    async def send_heartbeat(self, user_id: str, tcp_port: int) -> None:
        """Send periodic HEARTBEAT frame."""
        await self._send_model(HeartbeatMessage(user_id=user_id, tcp_port=tcp_port))

    async def sync_bloom_filter(
        self,
        user_id: str,
        size_bits: int,
        hash_functions_k: int,
        bitset_base64: str,
    ) -> None:
        """Send BLOOM_SYNC frame to update server-side routing filter."""
        self._last_bloom_payload = (size_bits, hash_functions_k, bitset_base64)
        await self._send_model(
            BloomSyncMessage(
                user_id=user_id,
                size_bits=size_bits,
                hash_functions_k=hash_functions_k,
                bitset_base64=bitset_base64,
            )
        )

    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]:
        """Execute a federated search and map SearchHitItem results to domain SearchMatch."""
        self.record_activity()
        token = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[SearchResultsMessage] = loop.create_future()
        self._search_futures[token] = fut
        try:
            await self._send_model(
                SearchQueryMessage(search_token=token, query=query, max_results=max_results)
            )
            results_msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
        finally:
            self._search_futures.pop(token, None)

        return [
            SearchMatch(
                file=SharedFile(
                    file_hash=item.file_hash,
                    file_name=item.filename,
                    size_bytes=item.size_bytes,
                ),
                peer=RemotePeer(
                    peer_id=item.peer_id,
                    username=item.peer_username,
                    ip_address=item.peer_ip,
                    tcp_port=item.peer_port,
                    status=PresenceStatus.ONLINE,
                ),
                avg_rating=item.avg_rating,
                review_count=item.review_count,
            )
            for item in results_msg.results
        ]

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None:
        """Publish a file review and await REVIEW_ACK."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        self._review_ack_future = loop.create_future()
        await self._send_model(
            ReviewPublishMessage(file_hash=file_hash, rating=rating, comment=comment)
        )
        await asyncio.wait_for(self._review_ack_future, timeout=self._request_timeout_s)

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        """Fetch all reviews for file_hash and map to domain FileReview entities."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[ReviewsListMessage] = loop.create_future()
        self._reviews_futures[file_hash] = fut
        try:
            await self._send_model(ReviewsFetchMessage(file_hash=file_hash))
            reviews_msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
        finally:
            self._reviews_futures.pop(file_hash, None)

        reviews: list[FileReview] = []
        for item in reviews_msg.reviews:
            comment_text = item.comment.strip() or "(no comment)"
            created = datetime.fromisoformat(item.updated_at_iso)
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            reviews.append(
                FileReview(
                    review_id=f"{item.file_hash}:{item.author_id}",
                    file_hash=item.file_hash,
                    author_id=item.author_id,
                    rating=item.rating,
                    comment=comment_text,
                    created_at=created,
                )
            )
        return reviews

    # --- Communities, Rooms & NAT Signaling (Cas_01-Cas_06, Cas_13, Cas_20) ---

    async def create_community(self, name: str, description: str = "") -> None:
        self.record_activity()
        await self._send_model(CommunityCreateMessage(name=name, description=description))

    async def post_debate(self, community_id: str, title: str, body: str) -> None:
        self.record_activity()
        await self._send_model(DebatePostMessage(community_id=community_id, title=title, body=body))

    async def reply_debate(self, post_id: str, body: str) -> None:
        self.record_activity()
        await self._send_model(DebateReplyMessage(post_id=post_id, body=body))

    async def save_to_library(self, file_hash: str, title: str) -> None:
        self.record_activity()
        await self._send_model(LibrarySaveMessage(file_hash=file_hash, title=title))

    async def join_room(self, room_id: str) -> None:
        self.record_activity()
        await self._send_model(RoomJoinMessage(room_id=room_id))

    async def send_room_message(
        self,
        room_id: str,
        sender_id: str,
        sender_username: str,
        content: str,
    ) -> None:
        self.record_activity()
        await self._send_model(
            RoomMessage(
                room_id=room_id,
                sender_id=sender_id,
                sender_username=sender_username,
                content=content,
            )
        )

    async def request_nat_punch(
        self, target_peer_id: str, requester_ip: str, requester_port: int
    ) -> None:
        self.record_activity()
        await self._send_model(
            NatPunchRequestMessage(
                target_peer_id=target_peer_id,
                requester_ip=requester_ip,
                requester_port=requester_port,
            )
        )

    async def request_nat_relay(self, target_peer_id: str, session_id: str) -> None:
        self.record_activity()
        await self._send_model(
            NatRelayRequestMessage(target_peer_id=target_peer_id, session_id=session_id)
        )
