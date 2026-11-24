"""Unit tests for WebSocketServerCoordinator with an in-memory fake WebSocket."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from bluered_client.domain import PresenceStatus
from bluered_client.infrastructure import ControlPlaneError, WebSocketServerCoordinator
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
    DebateFetchMessage,
    DebateItem,
    DebateListMessage,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryFetchMessage,
    LibraryItem,
    LibraryListMessage,
    LibrarySaveMessage,
    NatPunchCoordinateMessage,
    PresenceUpdateMessage,
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
    SearchBroadcastMessage,
    SearchHitItem,
    SearchHitReportMessage,
    SearchQueryMessage,
    SearchResultsMessage,
    parse_control_message,
    serialize_control_message,
)
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close


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


async def test_ws_coordinator_auto_reauth_on_session_expired_and_reconnect() -> None:
    transports: list[FakeWebSocketTransport] = []

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        ws = FakeWebSocketTransport()
        transports.append(ws)
        return ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=60.0,
        auto_reconnect=True,
        connect_fn=connect_fn,
    )
    reauth_calls: list[str] = []

    async def on_reauth(user_id: str) -> None:
        reauth_calls.append(user_id)

    coord.set_reauthenticated_handler(on_reauth)
    await coord.connect("127.0.0.1", 8000)
    assert coord.is_authenticated is False

    # Initial login + bloom sync
    login_task = asyncio.create_task(coord.login("alice", "secret123", 9001))
    await asyncio.sleep(0)
    transports[0].push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-1")
    )
    await login_task
    assert coord.is_authenticated is True
    await coord.sync_bloom_filter("u1", 1024, 4, "AAAA")

    # 1. Server sends SESSION_EXPIRED asynchronously -> non-blocking reauth + bloom resync
    sent_before = len(transports[0].sent)
    transports[0].push_server_message(
        AuthErrorMessage(code="SESSION_EXPIRED", detail="Session expired in Redis")
    )
    await asyncio.sleep(0.02)
    assert coord.is_authenticated is False
    reauth_sent = [parse_control_message(m) for m in transports[0].sent[sent_before:]]
    assert any(isinstance(m, AuthLoginMessage) for m in reauth_sent)

    transports[0].push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-2")
    )
    await asyncio.sleep(0.02)
    assert coord.is_authenticated is True
    after_ok = [parse_control_message(m) for m in transports[0].sent[sent_before:]]
    assert any(isinstance(m, BloomSyncMessage) for m in after_ok)
    assert reauth_calls == ["u1"]

    # 2. Presence OFFLINE keeps credentials; ONLINE reconnects and re-authenticates
    assert await coord.set_presence_status(PresenceStatus.OFFLINE) == PresenceStatus.OFFLINE
    assert coord.is_connected is False
    assert coord.is_authenticated is False
    assert coord.has_credentials is True

    online_task = asyncio.create_task(coord.set_presence_status(PresenceStatus.ONLINE))
    await asyncio.sleep(0.02)
    assert len(transports) == 2
    assert any(isinstance(parse_control_message(m), AuthLoginMessage) for m in transports[1].sent)
    transports[1].push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-3")
    )
    resolved = await online_task
    assert resolved == PresenceStatus.ONLINE
    assert coord.is_authenticated is True
    assert reauth_calls == ["u1", "u1"]

    # 3. WS drop triggers auto-reconnect + re-auth without deadlocking _recv_loop
    transports[1].incoming.put_nowait(
        serialize_control_message(AuthOkMessage(user_id="u1", username="alice", session_token="x"))
    )
    # Replace recv with one that raises ConnectionClosedError once
    original_recv = transports[1].recv

    async def drop_once() -> str | bytes:
        transports[1].recv = original_recv  # type: ignore[method-assign]
        raise ConnectionClosedError(Close(1006, "drop"), None)

    transports[1].recv = drop_once  # type: ignore[method-assign]
    await asyncio.sleep(0.6)
    assert len(transports) == 3
    assert any(isinstance(parse_control_message(m), AuthLoginMessage) for m in transports[2].sent)
    transports[2].push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-4")
    )
    await asyncio.sleep(0.02)
    assert coord.is_authenticated is True

    # Explicit disconnect clears credentials
    await coord.disconnect()
    assert coord.has_credentials is False


async def test_ws_coordinator_community_debate_room_library_sync_and_errors() -> None:
    import pytest

    fake_ws = FakeWebSocketTransport()

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        return fake_ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=60.0,
        auto_reconnect=False,
        connect_fn=connect_fn,
    )
    await coord.connect("127.0.0.1", 8000)

    # create_community + fetch_communities
    cc_task = asyncio.create_task(coord.create_community("DistSys", "Papers"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), CommunityCreateMessage)
    fake_ws.push_server_message(
        CommunityListMessage(
            communities=[CommunitySummary(id="c-srv-1", name="DistSys", description="Papers")]
        )
    )
    created_comm = await cc_task
    assert created_comm is not None and created_comm["id"] == "c-srv-1"

    fc_task = asyncio.create_task(coord.fetch_communities())
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), CommunityFetchMessage)
    fake_ws.push_server_message(
        CommunityListMessage(
            communities=[CommunitySummary(id="c-srv-1", name="DistSys", description="Papers")]
        )
    )
    assert len(await fc_task) == 1

    # post_debate, reply_debate, fetch_debates
    dp_task = asyncio.create_task(coord.post_debate("c-srv-1", "Paxos", "Consensus"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), DebatePostMessage)
    now_iso = datetime.now(UTC).isoformat()
    fake_ws.push_server_message(
        DebateListMessage(
            community_id="c-srv-1",
            posts=[
                DebateItem(
                    post_id="p-srv-1",
                    community_id="c-srv-1",
                    author_id="u1",
                    title="Paxos",
                    body="Consensus",
                    parent_post_id=None,
                    created_at_iso=now_iso,
                )
            ],
        )
    )
    post = await dp_task
    assert post is not None and post["post_id"] == "p-srv-1"

    dr_task = asyncio.create_task(coord.reply_debate("p-srv-1", "Agreed"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), DebateReplyMessage)
    fake_ws.push_server_message(
        DebateListMessage(
            community_id="c-srv-1",
            posts=[
                DebateItem(
                    post_id="p-srv-2",
                    community_id="c-srv-1",
                    author_id="u2",
                    title="Re: Paxos",
                    body="Agreed",
                    parent_post_id="p-srv-1",
                    created_at_iso=now_iso,
                )
            ],
        )
    )
    reply = await dr_task
    assert reply is not None and reply["post_id"] == "p-srv-2"

    fd_task = asyncio.create_task(coord.fetch_debates("c-srv-1"))
    for _ in range(50):
        if isinstance(parse_control_message(fake_ws.sent[-1]), DebateFetchMessage):
            break
        await asyncio.sleep(0.01)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), DebateFetchMessage)
    fake_ws.push_server_message(
        AuthErrorMessage(code="NOT_FOUND", detail="Community not found")
    )
    with pytest.raises(ControlPlaneError, match="NOT_FOUND"):
        await fd_task

    # fetch_rooms, join_room, save_to_library, fetch_library
    fr_task = asyncio.create_task(coord.fetch_rooms())
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), RoomFetchMessage)
    fake_ws.push_server_message(
        RoomListMessage(rooms=[RoomSummary(id="general", name="general", member_count=2)])
    )
    assert (await fr_task)[0]["member_count"] == 2

    jr_task = asyncio.create_task(coord.join_room("papers-open"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), RoomJoinMessage)
    fake_ws.push_server_message(
        RoomListMessage(rooms=[RoomSummary(id="papers-open", name="papers-open", member_count=1)])
    )
    assert (await jr_task)[0]["id"] == "papers-open"

    sl_task = asyncio.create_task(coord.save_to_library("a" * 64, "Paper"))
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), LibrarySaveMessage)
    fake_ws.push_server_message(
        LibraryListMessage(
            items=[LibraryItem(file_hash="a" * 64, title="Paper", saved_at_iso=now_iso)]
        )
    )
    saved = await sl_task
    assert saved is not None and saved["file_hash"] == "a" * 64

    fl_task = asyncio.create_task(coord.fetch_library())
    await asyncio.sleep(0)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), LibraryFetchMessage)
    fake_ws.push_server_message(
        LibraryListMessage(
            items=[LibraryItem(file_hash="a" * 64, title="Paper", saved_at_iso=now_iso)]
        )
    )
    assert len(await fl_task) == 1

    await coord.disconnect()


async def test_ws_coordinator_concurrent_rpc_not_found_does_not_fail_unrelated_requests() -> None:
    fake_ws = FakeWebSocketTransport()

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        return fake_ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=60.0,
        auto_reconnect=False,
        connect_fn=connect_fn,
    )
    await coord.connect("127.0.0.1", 8000)

    # Start a concurrent long-running search outside _rpc_lock
    search_task = asyncio.create_task(coord.search("symphony"))
    await asyncio.sleep(0)
    sent_search = parse_control_message(fake_ws.sent[-1])
    assert isinstance(sent_search, SearchQueryMessage)

    # Launch concurrent metadata RPCs: one succeeds, one fails with NOT_FOUND
    async def _run_gather() -> list[object]:
        return await asyncio.gather(
            coord.fetch_communities(),
            coord.fetch_debates("missing"),
            return_exceptions=True,
        )

    gather_task = asyncio.create_task(_run_gather())
    for _ in range(50):
        if isinstance(parse_control_message(fake_ws.sent[-1]), CommunityFetchMessage):
            break
        await asyncio.sleep(0.01)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), CommunityFetchMessage)
    fake_ws.push_server_message(
        CommunityListMessage(
            communities=[CommunitySummary(id="c1", name="Main", description="")]
        )
    )
    for _ in range(50):
        if isinstance(parse_control_message(fake_ws.sent[-1]), DebateFetchMessage):
            break
        await asyncio.sleep(0.01)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), DebateFetchMessage)
    fake_ws.push_server_message(
        AuthErrorMessage(code="NOT_FOUND", detail="Community not found")
    )

    comm_res, deb_res = await gather_task
    assert isinstance(comm_res, list) and len(comm_res) == 1
    assert isinstance(deb_res, ControlPlaneError) and "NOT_FOUND" in str(deb_res)

    # Search future must remain intact and resolve normally
    fake_ws.push_server_message(
        SearchResultsMessage(search_token=sent_search.search_token, results=[])
    )
    assert await search_task == []

    await coord.disconnect()


async def test_ws_coordinator_failed_reauth_sets_presence_offline() -> None:
    fake_ws = FakeWebSocketTransport()

    async def connect_fn(_uri: str) -> FakeWebSocketTransport:
        return fake_ws

    coord = WebSocketServerCoordinator(
        heartbeat_interval_s=60.0,
        auto_reconnect=False,
        connect_fn=connect_fn,
    )
    await coord.connect("127.0.0.1", 8000)

    login_task = asyncio.create_task(coord.login("alice", "secret123", 9001))
    await asyncio.sleep(0)
    fake_ws.push_server_message(
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-1")
    )
    await login_task
    assert coord.is_authenticated is True
    assert coord.presence_status == PresenceStatus.ONLINE

    # Trigger SESSION_EXPIRED -> scheduled re-auth fails -> must flip presence to OFFLINE
    fake_ws.push_server_message(
        AuthErrorMessage(code="SESSION_EXPIRED", detail="Expired")
    )
    await asyncio.sleep(0.02)
    assert isinstance(parse_control_message(fake_ws.sent[-1]), AuthLoginMessage)
    fake_ws.push_server_message(
        AuthErrorMessage(code="INVALID_CREDENTIALS", detail="Bad password")
    )
    await asyncio.sleep(0.02)
    assert coord.is_authenticated is False
    assert coord.presence_status == PresenceStatus.OFFLINE

    # Explicit set_presence_status(ONLINE) with failing re-auth must stay OFFLINE
    online_task = asyncio.create_task(coord.set_presence_status(PresenceStatus.ONLINE))
    await asyncio.sleep(0.01)
    fake_ws.push_server_message(
        AuthErrorMessage(code="INVALID_CREDENTIALS", detail="Bad password")
    )
    resolved = await online_task
    assert resolved == PresenceStatus.OFFLINE
    assert coord.presence_status == PresenceStatus.OFFLINE

    await coord.disconnect()
