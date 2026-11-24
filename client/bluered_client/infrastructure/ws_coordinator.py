"""WebSocket control-plane client coordinator using websockets.asyncio.client."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlparse

from bluered_contracts.control_plane import (
    AuthErrorMessage,
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    BloomSyncMessage,
    CommunityCreateMessage,
    CommunityFetchMessage,
    CommunityListMessage,
    ControlPlaneEnvelope,
    DebateFetchMessage,
    DebateListMessage,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryFetchMessage,
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
    RoomFetchMessage,
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
ReauthenticatedHandler = Callable[[str], Awaitable[None]]
CommunityListHandler = Callable[[list[dict[str, Any]]], Awaitable[None]]
DebateListHandler = Callable[[str, list[dict[str, Any]]], Awaitable[None]]
RoomListHandler = Callable[[list[dict[str, Any]]], Awaitable[None]]
LibraryListHandler = Callable[[list[dict[str, Any]]], Awaitable[None]]


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
        self._reauth_task: asyncio.Task[None] | None = None
        self._reauth_lock = asyncio.Lock()
        self._rpc_lock = asyncio.Lock()

        self._user_id: str | None = None
        self._username: str | None = None
        self._password: str | None = None
        self._session_token: str | None = None
        self._tcp_port: int | None = None
        self._authenticated: bool = False
        self._status: PresenceStatus = PresenceStatus.OFFLINE
        self._last_activity: float = time.monotonic()

        self._auth_future: asyncio.Future[AuthOkMessage] | None = None
        self._review_ack_future: asyncio.Future[ReviewAckMessage] | None = None
        self._search_futures: dict[str, asyncio.Future[SearchResultsMessage]] = {}
        self._reviews_futures: dict[str, asyncio.Future[ReviewsListMessage]] = {}
        self._community_future: asyncio.Future[CommunityListMessage] | None = None
        self._debate_futures: dict[str, asyncio.Future[DebateListMessage]] = {}
        self._debate_action_future: asyncio.Future[DebateListMessage] | None = None
        self._room_future: asyncio.Future[RoomListMessage] | None = None
        self._library_future: asyncio.Future[LibraryListMessage] | None = None

        self._on_search_broadcast: SearchBroadcastHandler | None = None
        self._on_room_message: RoomMessageHandler | None = None
        self._on_nat_signal: NatSignalHandler | None = None
        self._on_presence: PresenceHandler | None = None
        self._on_reauthenticated: ReauthenticatedHandler | None = None
        self._on_communities: CommunityListHandler | None = None
        self._on_debates: DebateListHandler | None = None
        self._on_rooms: RoomListHandler | None = None
        self._on_library: LibraryListHandler | None = None
        self._last_bloom_payload: tuple[int, int, str] | None = None

    @staticmethod
    async def _default_connect(uri: str) -> IWebSocketTransport:
        return await ws_connect(uri, ping_interval=20.0, ping_timeout=20.0)

    @property
    def is_connected(self) -> bool:
        return self._ws is not None

    @property
    def is_authenticated(self) -> bool:
        return self._ws is not None and self._authenticated

    @property
    def presence_status(self) -> PresenceStatus:
        return self._status

    @property
    def user_id(self) -> str | None:
        return self._user_id

    @property
    def has_credentials(self) -> bool:
        return bool(self._username and self._password and self._tcp_port is not None)

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

    def set_reauthenticated_handler(self, handler: ReauthenticatedHandler) -> None:
        self._on_reauthenticated = handler

    def set_community_list_handler(self, handler: CommunityListHandler) -> None:
        self._on_communities = handler

    def set_debate_list_handler(self, handler: DebateListHandler) -> None:
        self._on_debates = handler

    def set_room_list_handler(self, handler: RoomListHandler) -> None:
        self._on_rooms = handler

    def set_library_list_handler(self, handler: LibraryListHandler) -> None:
        self._on_library = handler

    async def _close_transport_only(self) -> None:
        self._closing = True
        self._authenticated = False
        self._session_token = None
        for task in (self._heartbeat_task, self._recv_task, self._reauth_task):
            if task is not None and not task.done() and task is not asyncio.current_task():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._heartbeat_task = None
        self._recv_task = None
        self._reauth_task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        self._fail_pending_futures(ControlPlaneError("Disconnected from server"))

    async def connect(self, host: str, port: int) -> None:
        """Open persistent WebSocket connection and start recv + heartbeat tasks."""
        if (
            self._ws is not None
            or (self._recv_task is not None and not self._recv_task.done())
            or (self._heartbeat_task is not None and not self._heartbeat_task.done())
        ):
            await self._close_transport_only()
        self._closing = False
        self._authenticated = False
        self._uri = f"ws://{host}:{port}{self._ws_path}"
        self._ws = await self._connect_fn(self._uri)
        self._status = PresenceStatus.ONLINE
        self._last_activity = time.monotonic()
        self._recv_task = asyncio.create_task(self._recv_loop(), name="bluered-ws-recv")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="bluered-ws-heartbeat"
        )

    async def disconnect(self, *, clear_credentials: bool = True) -> None:
        """Gracefully close WebSocket and cancel background tasks."""
        await self._close_transport_only()
        self._status = PresenceStatus.OFFLINE
        if clear_credentials:
            self._password = None
            self._username = None
            self._user_id = None

    def _fail_request_futures(
        self,
        exc: BaseException,
        *,
        include_search: bool = True,
    ) -> None:
        if self._review_ack_future is not None and not self._review_ack_future.done():
            self._review_ack_future.set_exception(exc)
        if self._community_future is not None and not self._community_future.done():
            self._community_future.set_exception(exc)
        if self._debate_action_future is not None and not self._debate_action_future.done():
            self._debate_action_future.set_exception(exc)
        if self._room_future is not None and not self._room_future.done():
            self._room_future.set_exception(exc)
        if self._library_future is not None and not self._library_future.done():
            self._library_future.set_exception(exc)
        if include_search:
            for search_fut in self._search_futures.values():
                if not search_fut.done():
                    search_fut.set_exception(exc)
            self._search_futures.clear()
        for rev_fut in self._reviews_futures.values():
            if not rev_fut.done():
                rev_fut.set_exception(exc)
        self._reviews_futures.clear()
        for deb_fut in self._debate_futures.values():
            if not deb_fut.done():
                deb_fut.set_exception(exc)
        self._debate_futures.clear()

    def _fail_pending_futures(self, exc: BaseException) -> None:
        if self._auth_future is not None and not self._auth_future.done():
            self._auth_future.set_exception(exc)
        self._fail_request_futures(exc)

    async def _send_model(self, msg: ControlPlaneEnvelope) -> None:
        if self._ws is None:
            raise ControlPlaneError("WebSocket is not connected")
        await self._ws.send(serialize_control_message(msg))

    def _schedule_reauth(self) -> None:
        if self._closing or not self.has_credentials:
            return
        if self._reauth_task is not None and not self._reauth_task.done():
            return
        task = asyncio.create_task(self._reauthenticate(), name="bluered-ws-reauth")
        self._reauth_task = task

        def _clear(done_task: asyncio.Task[None]) -> None:
            if self._reauth_task is done_task:
                self._reauth_task = None

        task.add_done_callback(_clear)

    async def _reauthenticate(self) -> None:
        async with self._reauth_lock:
            if self._closing or self._ws is None or self._authenticated:
                return
            if not (self._username and self._password and self._tcp_port is not None):
                return
            try:
                user_id, _ = await self.login(self._username, self._password, self._tcp_port)
                if self._last_bloom_payload is not None:
                    size_bits, k, bitset_b64 = self._last_bloom_payload
                    await self.sync_bloom_filter(user_id, size_bits, k, bitset_b64)
                if self._on_reauthenticated is not None:
                    await self._on_reauthenticated(user_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._authenticated = False
                self._status = PresenceStatus.OFFLINE

    async def set_presence_status(self, status: PresenceStatus) -> PresenceStatus:
        """Update client presence status, disconnecting on OFFLINE and reconnecting on ONLINE."""
        if status == PresenceStatus.OFFLINE:
            await self.disconnect(clear_credentials=False)
            return PresenceStatus.OFFLINE
        if status == PresenceStatus.AWAY:
            self._status = PresenceStatus.AWAY
            return PresenceStatus.AWAY

        self.record_activity()
        if not self.is_connected and self._uri is not None:
            parsed = urlparse(self._uri)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or (443 if parsed.scheme == "wss" else 8000)
            await self.connect(host, port)
        if self.is_connected and not self.is_authenticated and self.has_credentials:
            await self._reauthenticate()
            if not self._authenticated:
                self._status = PresenceStatus.OFFLINE
                return self._status
        self._status = PresenceStatus.ONLINE
        if self.is_authenticated and self._user_id is not None and self._tcp_port is not None:
            with contextlib.suppress(Exception):
                await self.send_heartbeat(self._user_id, self._tcp_port)
        return self._status

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
                self._authenticated = False
                self._session_token = None
                self._fail_pending_futures(ControlPlaneError("Connection closed"))
                if self._closing or not self._auto_reconnect or self._uri is None:
                    self._status = PresenceStatus.OFFLINE
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 15.0)
                try:
                    self._ws = await self._connect_fn(self._uri)
                    self._status = PresenceStatus.ONLINE
                    if self.has_credentials:
                        self._schedule_reauth()
                    elif self._user_id is not None and self._tcp_port is not None:
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
            if self._authenticated and self._user_id is not None and self._tcp_port is not None:
                with contextlib.suppress(Exception):
                    await self.send_heartbeat(self._user_id, self._tcp_port)

    @staticmethod
    def _map_communities(msg: CommunityListMessage) -> list[dict[str, Any]]:
        return [
            {"id": c.id, "name": c.name, "description": c.description}
            for c in msg.communities
        ]

    @staticmethod
    def _map_debates(msg: DebateListMessage) -> list[dict[str, Any]]:
        return [
            {
                "post_id": p.post_id,
                "community_id": p.community_id,
                "author_id": p.author_id,
                "title": p.title,
                "body": p.body,
                "parent_post_id": p.parent_post_id,
                "created_at_iso": p.created_at_iso,
            }
            for p in msg.posts
        ]

    @staticmethod
    def _map_rooms(msg: RoomListMessage) -> list[dict[str, Any]]:
        return [
            {"id": r.id, "name": r.name, "member_count": r.member_count}
            for r in msg.rooms
        ]

    @staticmethod
    def _map_library(msg: LibraryListMessage) -> list[dict[str, Any]]:
        return [
            {
                "file_hash": item.file_hash,
                "title": item.title,
                "saved_at_iso": item.saved_at_iso,
            }
            for item in msg.items
        ]

    async def _dispatch_message(self, msg: ControlPlaneEnvelope) -> None:
        if isinstance(msg, AuthOkMessage):
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_result(msg)
        elif isinstance(msg, AuthErrorMessage):
            err = ControlPlaneError(f"{msg.code}: {msg.detail}")
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_exception(err)
                return
            if msg.code in {"SESSION_EXPIRED", "UNAUTHENTICATED", "INVALID_TOKEN"}:
                self._authenticated = False
                self._session_token = None
                self._fail_request_futures(err, include_search=True)
                self._schedule_reauth()
            else:
                self._fail_request_futures(err, include_search=False)
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
        elif isinstance(msg, CommunityListMessage):
            if self._community_future is not None and not self._community_future.done():
                self._community_future.set_result(msg)
            if self._on_communities is not None:
                await self._on_communities(self._map_communities(msg))
        elif isinstance(msg, DebateListMessage):
            deb_fut = self._debate_futures.pop(msg.community_id, None)
            if deb_fut is not None and not deb_fut.done():
                deb_fut.set_result(msg)
            if self._debate_action_future is not None and not self._debate_action_future.done():
                self._debate_action_future.set_result(msg)
            if self._on_debates is not None:
                await self._on_debates(msg.community_id, self._map_debates(msg))
        elif isinstance(msg, RoomListMessage):
            if self._room_future is not None and not self._room_future.done():
                self._room_future.set_result(msg)
            if self._on_rooms is not None:
                await self._on_rooms(self._map_rooms(msg))
        elif isinstance(msg, LibraryListMessage):
            if self._library_future is not None and not self._library_future.done():
                self._library_future.set_result(msg)
            if self._on_library is not None:
                await self._on_library(self._map_library(msg))

    # --- IServerCoordinator ---

    async def register(self, username: str, password: str) -> str:
        """Register a new account and return the assigned user_id."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()
        try:
            await self._send_model(AuthRegisterMessage(username=username, password=password))
            auth_ok = await asyncio.wait_for(self._auth_future, timeout=self._request_timeout_s)
        finally:
            self._auth_future = None
        self._user_id = auth_ok.user_id
        self._username = auth_ok.username
        self._session_token = auth_ok.session_token
        return auth_ok.user_id

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        """Authenticate and return (user_id, session_token)."""
        self.record_activity()
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()
        self._username = username
        self._password = password
        self._tcp_port = tcp_port
        try:
            await self._send_model(
                AuthLoginMessage(username=username, password=password, tcp_port=tcp_port)
            )
            auth_ok = await asyncio.wait_for(self._auth_future, timeout=self._request_timeout_s)
        finally:
            self._auth_future = None
        self._user_id = auth_ok.user_id
        self._username = auth_ok.username
        self._session_token = auth_ok.session_token
        self._authenticated = True
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
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            self._review_ack_future = loop.create_future()
            try:
                await self._send_model(
                    ReviewPublishMessage(file_hash=file_hash, rating=rating, comment=comment)
                )
                await asyncio.wait_for(
                    self._review_ack_future, timeout=self._request_timeout_s
                )
            finally:
                self._review_ack_future = None

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        """Fetch all reviews for file_hash and map to domain FileReview entities."""
        async with self._rpc_lock:
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

    # --- Communities, Rooms, Library & NAT Signaling (Cas_01-Cas_06, Cas_13, Cas_20) ---

    async def fetch_communities(self) -> list[dict[str, Any]]:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[CommunityListMessage] = loop.create_future()
            self._community_future = fut
            try:
                await self._send_model(CommunityFetchMessage())
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._community_future is fut:
                    self._community_future = None
            return self._map_communities(msg)

    async def create_community(self, name: str, description: str = "") -> dict[str, Any] | None:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[CommunityListMessage] = loop.create_future()
            self._community_future = fut
            try:
                await self._send_model(
                    CommunityCreateMessage(name=name, description=description)
                )
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._community_future is fut:
                    self._community_future = None
            mapped = self._map_communities(msg)
            matches = [c for c in mapped if c["name"] == name.strip()]
            return matches[-1] if matches else (mapped[-1] if mapped else None)

    async def fetch_debates(self, community_id: str) -> list[dict[str, Any]]:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[DebateListMessage] = loop.create_future()
            self._debate_futures[community_id] = fut
            try:
                await self._send_model(DebateFetchMessage(community_id=community_id))
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                self._debate_futures.pop(community_id, None)
            return self._map_debates(msg)

    async def post_debate(
        self, community_id: str, title: str, body: str
    ) -> dict[str, Any] | None:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[DebateListMessage] = loop.create_future()
            self._debate_action_future = fut
            try:
                await self._send_model(
                    DebatePostMessage(community_id=community_id, title=title, body=body)
                )
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._debate_action_future is fut:
                    self._debate_action_future = None
            mapped = self._map_debates(msg)
            matches = [
                p
                for p in mapped
                if p["parent_post_id"] is None
                and p["title"] == title.strip()
                and p["body"] == body.strip()
            ]
            if matches:
                return max(matches, key=lambda p: str(p["created_at_iso"]))
            return mapped[0] if mapped else None

    async def reply_debate(self, post_id: str, body: str) -> dict[str, Any] | None:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[DebateListMessage] = loop.create_future()
            self._debate_action_future = fut
            try:
                await self._send_model(DebateReplyMessage(post_id=post_id, body=body))
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._debate_action_future is fut:
                    self._debate_action_future = None
            mapped = self._map_debates(msg)
            matches = [
                p
                for p in mapped
                if p["parent_post_id"] == post_id and p["body"] == body.strip()
            ]
            if matches:
                return max(matches, key=lambda p: str(p["created_at_iso"]))
            return None

    async def fetch_library(self) -> list[dict[str, Any]]:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[LibraryListMessage] = loop.create_future()
            self._library_future = fut
            try:
                await self._send_model(LibraryFetchMessage())
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._library_future is fut:
                    self._library_future = None
            return self._map_library(msg)

    async def save_to_library(self, file_hash: str, title: str) -> dict[str, Any] | None:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[LibraryListMessage] = loop.create_future()
            self._library_future = fut
            try:
                await self._send_model(LibrarySaveMessage(file_hash=file_hash, title=title))
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._library_future is fut:
                    self._library_future = None
            mapped = self._map_library(msg)
            matches = [i for i in mapped if i["file_hash"] == file_hash]
            return matches[0] if matches else (mapped[0] if mapped else None)

    async def fetch_rooms(self) -> list[dict[str, Any]]:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[RoomListMessage] = loop.create_future()
            self._room_future = fut
            try:
                await self._send_model(RoomFetchMessage())
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._room_future is fut:
                    self._room_future = None
            return self._map_rooms(msg)

    async def join_room(self, room_id: str) -> list[dict[str, Any]]:
        async with self._rpc_lock:
            self.record_activity()
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[RoomListMessage] = loop.create_future()
            self._room_future = fut
            try:
                await self._send_model(RoomJoinMessage(room_id=room_id))
                msg = await asyncio.wait_for(fut, timeout=self._request_timeout_s)
            finally:
                if self._room_future is fut:
                    self._room_future = None
            return self._map_rooms(msg)

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
