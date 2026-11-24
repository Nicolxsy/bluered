"""Unit tests for WebSocketServerCoordinator with an in-memory fake WebSocket."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from bluered_client.domain import PresenceStatus
from bluered_client.infrastructure import WebSocketServerCoordinator
from bluered_contracts.control_plane import (
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    BloomSyncMessage,
    HeartbeatMessage,
    NatPunchCoordinateMessage,
    PresenceUpdateMessage,
    ReviewAckMessage,
    ReviewItem,
    ReviewPublishMessage,
    ReviewsFetchMessage,
    ReviewsListMessage,
    RoomMessage,
    SearchBroadcastMessage,
    SearchHitItem,
    SearchHitReportMessage,
    SearchQueryMessage,
    SearchResultsMessage,
    parse_control_message,
    serialize_control_message,
)


class FakeWebSocketTransport:
    """In-memory fake WebSocket transport for testing send/recv flows."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.closed: bool = False

    async def send(self, message: str | bytes) -> None:
        text = message.decode("utf-8") if isinstance(message, bytes) else message
        self.sent.append(text)

    async def recv(self) -> str | bytes:
        return await self.incoming.get()

    async def close(self) -> None:
        self.closed = True

    def push_server_message(self, msg: object) -> None:
        self.incoming.put_nowait(serialize_control_message(msg))  # type: ignore[arg-type]


async def test_ws_coordinator_auth_search_and_reviews() -> None:
    fake_ws = FakeWebSocketTransport()

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        return fake_ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=60.0,
        auto_reconnect=False,
        connect_fn=connect_fn,
    )
    await coord.connect("127.0.0.1", 8000)

    # Register
    reg_task = asyncio.create_task(coord.register("alice", "secret123"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), AuthRegisterMessage)
    fake_ws.push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-1")
    )
    user_id = await reg_task
    assert user_id == "u1"

    # Login
    login_task = asyncio.create_task(coord.login("alice", "secret123", 9001))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), AuthLoginMessage)
    fake_ws.push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-2")
    )
    uid, tok = await login_task
    assert (uid, tok) == ("u1", "tok-2")

    # Bloom sync & heartbeat
    await coord.sync_bloom_filter("u1", 1024, 4, "AAAA")
    assert isinstance(parse_control_message(fake_ws.sent[-1]), BloomSyncMessage)
    await coord.send_heartbeat("u1", 9001)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), HeartbeatMessage)

    # Search
    search_task = asyncio.create_task(coord.search("symphony", max_results=10))
    await asyncio.sleep(0)
    sent_search = parse_control_message(fake_ws.sent[-1])
    assert isinstance(sent_search, SearchQueryMessage)
    fake_ws.push_server_message(
        SearchResultsMessage(
            search_token=sent_search.search_token,
            results=[
                SearchHitItem(
                    file_hash="a" * 64,
                    filename="symphony.flac",
                    size_bytes=2048,
                    peer_id="p2",
                    peer_username="bob",
                    peer_ip="127.0.0.1",
                    peer_port=9002,
                    avg_rating=4.8,
                    review_count=5,
                )
            ],
        )
    )
    matches = await search_task
    assert len(matches) == 1
    assert matches[0].file.file_name == "symphony.flac"
    assert matches[0].peer.username == "bob"

    # Publish review
    pub_task = asyncio.create_task(coord.publish_review("a" * 64, 5, "Great"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), ReviewPublishMessage)
    fake_ws.push_server_message(
        ReviewAckMessage(file_hash="a" * 64, review_count=1, avg_rating=5.0)
    )
    await pub_task

    # Fetch reviews
    fetch_task = asyncio.create_task(coord.fetch_reviews("a" * 64))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), ReviewsFetchMessage)
    fake_ws.push_server_message(
        ReviewsListMessage(
            file_hash="a" * 64,
            review_count=1,
            avg_rating=5.0,
            reviews=[
                ReviewItem(
                    file_hash="a" * 64,
                    author_id="u2",
                    author_username="bob",
                    rating=5,
                    comment="Awesome",
                    updated_at_iso=datetime.now(UTC).isoformat(),
                )
            ],
        )
    )
    reviews = await fetch_task
    assert len(reviews) == 1
    assert reviews[0].rating == 5

    await coord.disconnect()
    assert fake_ws.closed is True


async def test_ws_coordinator_handles_incoming_broadcast_and_auto_away() -> None:
    fake_ws = FakeWebSocketTransport()

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        return fake_ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=0.02,
        inactivity_timeout_s=0.03,
        auto_reconnect=False,
        connect_fn=connect_fn,
    )

    room_events: list[RoomMessage] = []
    nat_events: list[object] = []
    presence_events: list[PresenceUpdateMessage] = []

    async def on_broadcast(msg: SearchBroadcastMessage) -> list[SearchHitItem]:
        return [
            SearchHitItem(
                file_hash="d" * 64,
                filename=f"{msg.query}.mp3",
                size_bytes=1234,
                peer_id="u1",
                peer_username="alice",
                peer_ip="127.0.0.1",
                peer_port=9001,
            )
        ]

    coord.set_search_broadcast_handler(on_broadcast)
    coord.set_room_message_handler(lambda m: _append_async(room_events, m))
    coord.set_nat_signal_handler(lambda m: _append_async(nat_events, m))
    coord.set_presence_handler(lambda m: _append_async(presence_events, m))

    await coord.connect("127.0.0.1", 8000)

    fake_ws.push_server_message(
        SearchBroadcastMessage(search_token="st-1", requester_id="u9", query="bach")
    )
    fake_ws.push_server_message(
        RoomMessage(room_id="r1", sender_id="u2", sender_username="bob", content="hi")
    )
    fake_ws.push_server_message(
        NatPunchCoordinateMessage(
            peer_id="u2",
            peer_ip="10.0.0.2",
            peer_port=9002,
            rendezvous_at_epoch_ms=1700000000000,
        )
    )
    fake_ws.push_server_message(
        PresenceUpdateMessage(
            user_id="u2", username="bob", online=True, ip="10.0.0.2", tcp_port=9002
        )
    )

    await asyncio.sleep(0.08)

    hit_reports = [
        parse_control_message(raw)
        for raw in fake_ws.sent
        if isinstance(parse_control_message(raw), SearchHitReportMessage)
    ]
    assert len(hit_reports) == 1
    assert len(room_events) == 1
    assert len(nat_events) == 1
    assert len(presence_events) == 1
    assert coord.presence_status == PresenceStatus.AWAY

    coord.record_activity()
    assert coord.presence_status == PresenceStatus.ONLINE
    await coord.disconnect()


async def _append_async(target: list[object], item: object) -> None:
    target.append(item)
