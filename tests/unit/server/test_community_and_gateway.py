"""Unit tests for CommunityAndChatService (Cas_01..Cas_06, Cas_13) and WebSocket/REST gateway."""

from __future__ import annotations

import hashlib

import pytest
from bluered_contracts.control_plane import (
    AuthErrorMessage,
    AuthLoginMessage,
    AuthOkMessage,
    AuthRegisterMessage,
    CommunityFetchMessage,
    CommunityListMessage,
    DebateFetchMessage,
    DebateListMessage,
    DebatePostMessage,
    DebateReplyMessage,
    HeartbeatMessage,
    LibraryFetchMessage,
    LibraryListMessage,
    LibrarySaveMessage,
    ReviewPublishMessage,
    RoomFetchMessage,
    RoomJoinMessage,
    RoomListMessage,
    RoomMessage,
    parse_control_message,
    serialize_control_message,
)
from bluered_server.application import (
    AuthService,
    CommunityAndChatService,
    FileReputationService,
    NotFoundError,
    PresenceAndSessionService,
    SearchCoordinatorService,
    ValidationError,
)
from bluered_server.infrastructure import (
    Argon2PasswordHasher,
    FullBroadcastRoutingStrategy,
    JwtTokenProvider,
    WebSocketClientGateway,
)
from bluered_server.presentation import ServerServices, create_fastapi_app
from fastapi.testclient import TestClient

from tests.unit.server.fakes import (
    InMemoryCommunityRepository,
    InMemoryFileReviewRepository,
    InMemoryPushGateway,
    InMemorySessionStore,
    InMemoryUserRepository,
)

SAMPLE_HASH = hashlib.sha256(b"community-lib-item").hexdigest()


def _make_gateway_deps() -> tuple[
    ServerServices,
    InMemorySessionStore,
    InMemoryCommunityRepository,
]:
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
    return services, sessions, communities


async def test_community_debate_chat_and_library_cas01_to_06_13() -> None:
    repo = InMemoryCommunityRepository()
    gateway = InMemoryPushGateway()
    service = CommunityAndChatService(repo, gateway)

    comm = await service.create_community(
        name="Vinyl Collectors",
        description="Analog rips",
        creator_id="u1",
    )
    comm_other = await service.create_community(
        name="Other",
        description="Separate",
        creator_id="u1",
    )
    assert len(await service.list_communities()) == 2

    post = await service.create_debate_post(
        community_id=comm.community_id,
        author_id="u1",
        title="Best cartridge?",
        body="Share setups",
    )
    reply = await service.reply_debate(
        post_id=post.post_id,
        author_id="u2",
        body="Ortofon 2M Blue",
    )
    assert reply.community_id == comm.community_id
    assert reply.title == "Re: Best cartridge?"
    assert reply.parent_post_id == post.post_id

    with pytest.raises(NotFoundError):
        await service.reply_debate(post_id="missing-post", author_id="u2", body="Nope")

    with pytest.raises(ValidationError):
        await service.create_debate_post(
            community_id=comm_other.community_id,
            author_id="u2",
            title="Cross-community",
            body="Invalid parent",
            parent_post_id=post.post_id,
        )

    posts = await service.list_debate_posts(comm.community_id)
    assert len(posts) == 2
    assert posts[1].parent_post_id == post.post_id

    rooms = await service.list_chat_rooms()
    assert len(rooms) == 3
    assert {r.room_id for r in rooms} == {"general", "music-lossless", "papers-open"}
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
    services, sessions, _ = _make_gateway_deps()
    app = create_fastapi_app(services)

    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}

        reg_res = client.post(
            "/auth/register",
            json={"username": "alice", "password": "password123"},
        )
        assert reg_res.status_code == 201
        alice_id = reg_res.json()["user_id"]

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
        community_id = comm_res.json()["id"]
        assert len(client.get("/communities").json()) == 1

        # New REST endpoints: /rooms, /communities/{id}/debates, /library/{user_id}
        rooms_res = client.get("/rooms")
        assert rooms_res.status_code == 200
        assert len(rooms_res.json()) == 3

        assert client.get(f"/communities/{community_id}/debates").status_code == 200
        assert client.get("/communities/missing-comm/debates").status_code == 404

        assert client.get(f"/library/{alice_id}").status_code == 401
        assert (
            client.get(
                "/library/other-user",
                headers={"Authorization": f"Bearer {token}"},
            ).status_code
            == 403
        )
        assert (
            client.get(
                f"/library/{alice_id}",
                headers={"Authorization": f"Bearer {token}"},
            ).json()
            == []
        )

        rev_res = client.post(
            f"/reviews/{SAMPLE_HASH}",
            json={"rating": 5, "comment": "Superb"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert rev_res.status_code == 201
        assert client.get(f"/reviews/{SAMPLE_HASH}").json()["review_count"] == 1

        # WebSocket register + login + debates + fetch + heartbeat + disconnect cleanup
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

            ws.send_text(serialize_control_message(CommunityFetchMessage()))
            comm_list = parse_control_message(ws.receive_text())
            assert isinstance(comm_list, CommunityListMessage)
            assert len(comm_list.communities) == 1

            ws.send_text(
                serialize_control_message(
                    DebatePostMessage(
                        community_id=community_id,
                        title="FLAC vs ALAC",
                        body="Discuss lossless codecs",
                    )
                )
            )
            debate_list = parse_control_message(ws.receive_text())
            assert isinstance(debate_list, DebateListMessage)
            assert len(debate_list.posts) == 1
            root_post_id = debate_list.posts[0].post_id

            ws.send_text(
                serialize_control_message(
                    DebateReplyMessage(post_id=root_post_id, body="FLAC has wider tooling")
                )
            )
            reply_list = parse_control_message(ws.receive_text())
            assert isinstance(reply_list, DebateListMessage)
            assert len(reply_list.posts) == 2
            assert reply_list.posts[1].parent_post_id == root_post_id
            assert reply_list.posts[1].title == "Re: FLAC vs ALAC"

            ws.send_text(serialize_control_message(DebateFetchMessage(community_id=community_id)))
            fetched_debates = parse_control_message(ws.receive_text())
            assert isinstance(fetched_debates, DebateListMessage)
            assert len(fetched_debates.posts) == 2

            # Verify REST endpoint reflects WS-persisted debates
            rest_debates = client.get(f"/communities/{community_id}/debates").json()
            assert len(rest_debates) == 2

            ws.send_text(serialize_control_message(RoomFetchMessage()))
            room_list = parse_control_message(ws.receive_text())
            assert isinstance(room_list, RoomListMessage)
            assert len(room_list.rooms) == 3

            ws.send_text(
                serialize_control_message(LibrarySaveMessage(file_hash=SAMPLE_HASH, title="Saved"))
            )
            lib_saved = parse_control_message(ws.receive_text())
            assert isinstance(lib_saved, LibraryListMessage)
            assert len(lib_saved.items) == 1

            ws.send_text(serialize_control_message(LibraryFetchMessage()))
            lib_fetched = parse_control_message(ws.receive_text())
            assert isinstance(lib_fetched, LibraryListMessage)
            assert len(lib_fetched.items) == 1

            ws.send_text(
                serialize_control_message(
                    HeartbeatMessage(user_id=login_ack.user_id, tcp_port=9002)
                )
            )

        # After WS closes, bob's session is cleaned up by PresenceAndSessionService.disconnect
        assert login_ack.user_id not in sessions.sessions


def test_websocket_error_resilience_keeps_connection_alive() -> None:
    services, sessions, _ = _make_gateway_deps()
    app = create_fastapi_app(services)

    with TestClient(app) as client, client.websocket_connect("/ws") as ws:
        # 1. Invalid JSON / unknown envelope returns INVALID_MESSAGE without closing WS
        ws.send_text("{not valid json")
        err1 = parse_control_message(ws.receive_text())
        assert isinstance(err1, AuthErrorMessage)
        assert err1.code == "INVALID_MESSAGE"

        # 2. Register and login normally on the same open WS
        ws.send_text(
            serialize_control_message(
                AuthRegisterMessage(username="carol", password="password123")
            )
        )
        reg_ok = parse_control_message(ws.receive_text())
        assert isinstance(reg_ok, AuthOkMessage)

        ws.send_text(
            serialize_control_message(
                AuthLoginMessage(username="carol", password="password123", tcp_port=9003)
            )
        )
        login_ok = parse_control_message(ws.receive_text())
        assert isinstance(login_ok, AuthOkMessage)

        # 3. Domain/Application validation error (banned word in review) returns VALIDATION_ERROR
        ws.send_text(
            serialize_control_message(
                ReviewPublishMessage(file_hash=SAMPLE_HASH, rating=1, comment="This is malware")
            )
        )
        err2 = parse_control_message(ws.receive_text())
        assert isinstance(err2, AuthErrorMessage)
        assert err2.code == "VALIDATION_ERROR"

        # 4. Missing entity (unknown room / debate) returns NOT_FOUND
        ws.send_text(serialize_control_message(RoomJoinMessage(room_id="nonexistent-room")))
        err3 = parse_control_message(ws.receive_text())
        assert isinstance(err3, AuthErrorMessage)
        assert err3.code == "NOT_FOUND"

        # 5. Expired Redis session on Heartbeat returns SESSION_EXPIRED & clears WS auth state
        sessions.sessions.pop(login_ok.user_id, None)
        ws.send_text(
            serialize_control_message(
                HeartbeatMessage(user_id=login_ok.user_id, tcp_port=9003)
            )
        )
        err4 = parse_control_message(ws.receive_text())
        assert isinstance(err4, AuthErrorMessage)
        assert err4.code == "SESSION_EXPIRED"

        # 6. Socket remains open and accepts re-authentication
        ws.send_text(
            serialize_control_message(
                AuthLoginMessage(username="carol", password="password123", tcp_port=9003)
            )
        )
        reauth_ok = parse_control_message(ws.receive_text())
        assert isinstance(reauth_ok, AuthOkMessage)
        assert reauth_ok.user_id == login_ok.user_id
