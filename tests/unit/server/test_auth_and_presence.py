"""Unit tests for AuthService (Cas_07) and PresenceAndSessionService (Cas_08, Cas_20)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from bluered_contracts.bloom import BloomFilter
from bluered_contracts.control_plane import (
    NatPunchCoordinateMessage,
    NatRelayRequestMessage,
    PresenceUpdateMessage,
    parse_control_message,
)
from bluered_server.application import (
    AccountLockedError,
    AuthenticationError,
    AuthService,
    DuplicateSessionError,
    DuplicateUserError,
    NotFoundError,
    PresenceAndSessionService,
    ValidationError,
)
from bluered_server.infrastructure import Argon2PasswordHasher, JwtTokenProvider

from tests.unit.server.fakes import (
    InMemoryPushGateway,
    InMemorySessionStore,
    InMemoryUserRepository,
)


def _make_auth() -> tuple[AuthService, InMemoryUserRepository, InMemorySessionStore]:
    users = InMemoryUserRepository()
    sessions = InMemorySessionStore()
    auth = AuthService(
        user_repo=users,
        session_store=sessions,
        password_hasher=Argon2PasswordHasher(),
        token_provider=JwtTokenProvider("test-secret-key-at-least-32-bytes-long"),
        max_failed_attempts=5,
        lock_duration_seconds=300,
    )
    return auth, users, sessions


async def test_auth_register_login_and_jwt_cas07() -> None:
    auth, _, sessions = _make_auth()
    account = await auth.register("alice", "supersecret")
    assert account.username == "alice"
    assert account.password_hash.startswith("$argon2")

    with pytest.raises(DuplicateUserError):
        await auth.register("alice", "anotherpass")

    logged_in, token = await auth.login("alice", "supersecret", "127.0.0.1", 9001)
    assert logged_in.user_id == account.user_id
    assert auth.verify_token(token) == (account.user_id, "alice")
    assert await sessions.get_session(account.user_id) is not None

    with pytest.raises(DuplicateSessionError):
        await auth.login("alice", "supersecret", "127.0.0.1", 9002)


async def test_auth_lockout_after_failed_attempts_cas07() -> None:
    auth, _, _ = _make_auth()
    await auth.register("bob", "correct-password")
    t0 = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)

    for _ in range(4):
        with pytest.raises(AuthenticationError):
            await auth.login("bob", "wrong", "127.0.0.1", 9001, now=t0)

    with pytest.raises(AccountLockedError):
        await auth.login("bob", "wrong", "127.0.0.1", 9001, now=t0)

    with pytest.raises(AccountLockedError):
        await auth.login(
            "bob",
            "correct-password",
            "127.0.0.1",
            9001,
            now=t0 + timedelta(seconds=60),
        )

    # After lockout expires, valid login succeeds and resets counter.
    account, _ = await auth.login(
        "bob", "correct-password", "127.0.0.1", 9001, now=t0 + timedelta(seconds=301)
    )
    assert account.failed_login_attempts == 0


async def test_presence_heartbeat_status_bloom_and_disconnect_cas08() -> None:
    sessions = InMemorySessionStore()
    gateway = InMemoryPushGateway()
    presence = PresenceAndSessionService(
        sessions, gateway, session_ttl_seconds=45, away_threshold_seconds=30
    )

    t0 = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)
    await presence.register_session("u1", "alice", "10.0.0.1", 9001, now=t0)
    await presence.register_session("u2", "bob", "10.0.0.2", 9002, now=t0)

    assert await presence.get_presence_status("u1", now=t0 + timedelta(seconds=10)) == "ONLINE"
    assert await presence.get_presence_status("u1", now=t0 + timedelta(seconds=30)) == "AWAY"
    assert await presence.get_presence_status("u1", now=t0 + timedelta(seconds=45)) == "OFFLINE"

    await presence.heartbeat("u1", tcp_port=9011, now=t0 + timedelta(seconds=35))
    assert await presence.get_presence_status("u1", now=t0 + timedelta(seconds=50)) == "ONLINE"

    bloom = BloomFilter.optimal(100)
    bloom.add_filename("pink_floyd.flac")
    await presence.sync_bloom_filter(
        "u1", bloom.to_bytes(), bloom.size_bits, bloom.hash_functions_k
    )
    stored_bloom = await sessions.get_bloom_filter("u1")
    assert stored_bloom is not None
    assert stored_bloom[0] == bloom.to_bytes()

    with pytest.raises(ValidationError):
        await presence.sync_bloom_filter("u1", b"\x00", bloom.size_bits, bloom.hash_functions_k)

    await presence.disconnect("u1")
    assert await sessions.get_session("u1") is None
    last_recipient, last_payload = gateway.sent[-1]
    assert last_recipient == "u2"
    parsed = parse_control_message(last_payload)
    assert isinstance(parsed, PresenceUpdateMessage)
    assert parsed.online is False

    with pytest.raises(NotFoundError):
        await presence.heartbeat("missing")


async def test_nat_punch_and_relay_signaling_cas20() -> None:
    sessions = InMemorySessionStore()
    gateway = InMemoryPushGateway()
    presence = PresenceAndSessionService(sessions, gateway)

    await presence.register_session("u1", "alice", "203.0.113.10", 9001)
    await presence.register_session("u2", "bob", "203.0.113.20", 9002)
    gateway.sent.clear()

    ok = await presence.coordinate_nat_punch("u1", "u2", "203.0.113.10", 9001)
    assert ok is True
    assert len(gateway.sent) == 2
    msg_u2 = parse_control_message(gateway.sent[0][1])
    msg_u1 = parse_control_message(gateway.sent[1][1])
    assert isinstance(msg_u2, NatPunchCoordinateMessage)
    assert isinstance(msg_u1, NatPunchCoordinateMessage)
    assert msg_u2.peer_id == "u1"
    assert msg_u1.peer_id == "u2"

    relay_ok = await presence.request_nat_relay("u1", "u2", "relay-sess-1")
    assert relay_ok is True
    relay_msg = parse_control_message(gateway.sent[-1][1])
    assert isinstance(relay_msg, NatRelayRequestMessage)
    assert relay_msg.session_id == "relay-sess-1"
