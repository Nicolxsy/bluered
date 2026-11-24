"""Unit tests for CommunityAndChatService (Cas_01..Cas_06, Cas_13) and WebSocket/REST gateway."""

from __future__ import annotations

import hashlib

from bluered_contracts.control_plane import (
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    HeartbeatMessage,
    RoomMessage,
    parse_control_message,
    serialize_control_message,
)
from bluered_server.application import (
    AuthService,
    CommunityAndChatService,
    FileReputationService,
    PresenceAndSessionService,
    SearchCoordinatorService,
)
from bluered_server.infrastructure import (
    Argon2PasswordHasher,
    FullBroadcastRoutingStrategy,
    JwtTokenProvider,
    ServerServices,
    WebSocketClientGateway,
    create_fastapi_app,
)
from fastapi.testclient import TestClient

from tests.unit.server.fakes import (
    InMemoryCommunityRepository,
    InMemoryFileReviewRepository,
    InMemoryPushGateway,
    InMemorySessionStore,
    InMemoryUserRepository,
)

SAMPLE_HASH = hashlib.sha256(b"community-lib-item").hexdigest()


async def test_community_debate_chat_and_library_cas01_to_06_13() -> None:
    repo = InMemoryCommunityRepository()
    gateway = InMemoryPushGateway()
    service = CommunityAndChatService(repo, gateway)

    comm = await service.create_community(
        name="Vinyl Collectors",
        description="Analog rips",
        creator_id="u1",
    )
    assert len(await service.list_communities()) == 1

    post = await service.create_debate_post(
        community_id=comm.community_id,
        author_id="u1",
        title="Best cartridge?",
        body="Share setups",
    )
    reply = await service.create_debate_post(
        community_id=comm.community_id,
        author_id="u2",
        title="Re: Best cartridge?",
        body="Ortofon 2M Blue",
        parent_post_id=post.post_id,
    )
    posts = await service.list_debate_posts(comm.community_id)
    assert len(posts) == 2
    assert posts[1].parent_post_id == reply.parent_post_id == post.post_id

    rooms = await service.list_chat_rooms()
    assert len(rooms) == 1
    await service.join_chat_room("general", "u1")
    room = await service.join_chat_room("general", "u2")
    assert room.member_count == 2

    sent_count = await service.send_room_message(
        room_id="general",
        sender_id="u1",
        sender_username="alice",
        content="Hello room!",
    )
    assert sent_count == 1
    assert gateway.sent[0][0] == "u2"
    msg = parse_control_message(gateway.sent[0][1])
    assert isinstance(msg, RoomMessage)

    await service.save_library_item(owner_id="u1", file_hash=SAMPLE_HASH, title="Saved Track")
    items = await service.list_library_items("u1")
    assert len(items) == 1
    assert items[0].file_hash == SAMPLE_HASH


def test_fastapi_rest_and_websocket_gateway() -> None:
    users = InMemoryUserRepository()
    sessions = InMemorySessionStore()
    reviews = InMemoryFileReviewRepository()
    communities = InMemoryCommunityRepository()
    gateway = WebSocketClientGateway()

    services = ServerServices(
        auth=AuthService(
            users,
            sessions,
            Argon2PasswordHasher(),
            JwtTokenProvider("gw-secret-key-at-least-32-bytes-long"),
        ),
        presence=PresenceAndSessionService(sessions, gateway),
        search=SearchCoordinatorService(
            sessions, reviews, FullBroadcastRoutingStrategy(), gateway
        ),
        reputation=FileReputationService(reviews),
        community=CommunityAndChatService(communities, gateway),
        gateway=gateway,
    )
    app = create_fastapi_app(services)

    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}

        reg_res = client.post(
            "/auth/register",
            json={"username": "alice", "password": "password123"},
        )
        assert reg_res.status_code == 201

        login_res = client.post(
            "/auth/login",
            json={"username": "alice", "password": "password123", "tcp_port": 9001},
        )
        assert login_res.status_code == 200
        token = login_res.json()["session_token"]

        comm_res = client.post(
            "/communities",
            json={"name": "Lossless", "description": "FLAC"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert comm_res.status_code == 201
        assert len(client.get("/communities").json()) == 1

        rev_res = client.post(
            f"/reviews/{SAMPLE_HASH}",
            json={"rating": 5, "comment": "Superb"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert rev_res.status_code == 201
        assert client.get(f"/reviews/{SAMPLE_HASH}").json()["review_count"] == 1

        # WebSocket register + login + heartbeat + disconnect cleanup
        with client.websocket_connect("/ws") as ws:
            ws.send_text(
                serialize_control_message(
                    AuthRegisterMessage(username="bob", password="password123")
                )
            )
            reg_ack = parse_control_message(ws.receive_text())
            assert isinstance(reg_ack, AuthOkMessage)

            ws.send_text(
                serialize_control_message(
                    AuthLoginMessage(username="bob", password="password123", tcp_port=9002)
                )
            )
            login_ack = parse_control_message(ws.receive_text())
            assert isinstance(login_ack, AuthOkMessage)
            assert login_ack.session_token

            ws.send_text(
                serialize_control_message(
                    HeartbeatMessage(user_id=login_ack.user_id, tcp_port=9002)
                )
            )

        # After WS closes, bob's session is cleaned up by PresenceAndSessionService.disconnect
        assert login_ack.user_id not in sessions.sessions
