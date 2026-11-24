"""Unit tests for server domain entities (Fig. 54 and Cas_01..Cas_08, Cas_14..Cas_24)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from bluered_server.domain import (
    ChatRoom,
    Community,
    DebatePost,
    DomainValidationError,
    FileReputationSummary,
    LibraryItem,
    PeerSession,
    ReviewEntry,
    SearchRequestPacket,
    UserAccount,
)

SAMPLE_HASH = hashlib.sha256(b"bluered-server-domain").hexdigest()


# --- ReviewEntry & FileReputationSummary ---


@pytest.mark.parametrize("rating", [1, 2, 3, 4, 5])
def test_review_entry_valid_ratings(rating: int) -> None:
    entry = ReviewEntry(
        review_id="rev-1",
        file_hash=SAMPLE_HASH,
        author_id="u1",
        author_username="alice",
        rating=rating,
        comment="Clean audio",
    )
    assert entry.rating == rating


@pytest.mark.parametrize("bad_rating", [0, 6, -1, True, False])
def test_review_entry_rejects_invalid_ratings(bad_rating: int | bool) -> None:
    with pytest.raises(DomainValidationError):
        ReviewEntry(
            review_id="rev-1",
            file_hash=SAMPLE_HASH,
            author_id="u1",
            author_username="alice",
            rating=bad_rating,
            comment="Clean audio",
        )


def test_review_entry_rejects_empty_comment_or_bad_hash() -> None:
    with pytest.raises(DomainValidationError):
        ReviewEntry(
            review_id="rev-1",
            file_hash=SAMPLE_HASH,
            author_id="u1",
            author_username="alice",
            rating=4,
            comment="   ",
        )
    with pytest.raises(DomainValidationError):
        ReviewEntry(
            review_id="rev-1",
            file_hash="bad",
            author_id="u1",
            author_username="alice",
            rating=4,
            comment="ok",
        )


def test_file_reputation_summary_incremental_math() -> None:
    summary = FileReputationSummary(file_hash=SAMPLE_HASH)
    assert summary.review_count == 0
    assert summary.avg_rating == 0.0

    summary.apply_new_review(5)
    summary.apply_new_review(3)
    assert summary.review_count == 2
    assert summary.rating_sum == 8
    assert summary.avg_rating == pytest.approx(4.0)

    summary.apply_updated_review(old_rating=3, new_rating=4)
    assert summary.review_count == 2
    assert summary.rating_sum == 9
    assert summary.avg_rating == pytest.approx(4.5)

    summary.apply_removed_review(5)
    summary.apply_removed_review(4)
    assert summary.review_count == 0
    assert summary.rating_sum == 0
    assert summary.avg_rating == 0.0

    with pytest.raises(DomainValidationError):
        summary.apply_removed_review(1)


# --- PeerSession & SearchRequestPacket ---


def test_peer_session_ttl_and_heartbeat() -> None:
    t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
    session = PeerSession(
        user_id="u1",
        username="alice",
        ip_address="192.168.1.10",
        tcp_port=9001,
        last_heartbeat_at=t0,
        ttl_seconds=45,
    )
    assert session.is_expired(now=t0 + timedelta(seconds=44)) is False
    assert session.is_expired(now=t0 + timedelta(seconds=45)) is True

    t1 = t0 + timedelta(seconds=30)
    session.refresh_heartbeat(now=t1)
    assert session.is_expired(now=t0 + timedelta(seconds=60)) is False


def test_peer_session_and_search_packet_validation() -> None:
    with pytest.raises(DomainValidationError):
        PeerSession(user_id="u1", username="alice", ip_address="127.0.0.1", tcp_port=0)

    packet = SearchRequestPacket(
        search_token="tok-1",
        requester_id="u1",
        query="pink floyd",
        max_results=25,
    )
    assert packet.ttl_seconds == 60

    with pytest.raises(DomainValidationError):
        SearchRequestPacket(search_token="tok-1", requester_id="u1", query="   ")
    with pytest.raises(DomainValidationError):
        SearchRequestPacket(search_token="tok-1", requester_id="u1", query="flac", max_results=0)


# --- UserAccount & Community Entities ---


def test_user_account_lockout_and_reset() -> None:
    t0 = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
    account = UserAccount(
        user_id="u1",
        username="alice",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$salt$hash",
    )
    assert account.is_locked(now=t0) is False

    for _ in range(4):
        account.record_failed_login(max_attempts=5, lock_duration_seconds=300, now=t0)
        assert account.is_locked(now=t0) is False

    account.record_failed_login(max_attempts=5, lock_duration_seconds=300, now=t0)
    assert account.is_locked(now=t0 + timedelta(seconds=100)) is True
    assert account.is_locked(now=t0 + timedelta(seconds=300)) is False

    account.record_successful_login()
    assert account.failed_login_attempts == 0
    assert account.locked_until is None

    with pytest.raises(DomainValidationError):
        UserAccount(user_id="u2", username="ab", password_hash="hash")


def test_community_chat_debate_and_library_entities() -> None:
    community = Community(
        community_id="c1",
        name="Lossless Audio",
        description="FLAC & vinyl rips",
        creator_id="u1",
        member_ids={"u1", "u2"},
    )
    assert "u1" in community.member_ids

    room = ChatRoom(
        room_id="r1",
        name="General",
        community_id="c1",
        member_ids={"u1", "u2", "u3"},
    )
    assert room.member_count == 3

    post = DebatePost(
        post_id="p1",
        community_id="c1",
        author_id="u1",
        title="Best mastering",
        body="Discuss dynamic range",
    )
    reply = DebatePost(
        post_id="p2",
        community_id="c1",
        author_id="u2",
        title="Re: Best mastering",
        body="1985 pressing",
        parent_post_id=post.post_id,
    )
    assert reply.parent_post_id == "p1"

    item = LibraryItem(
        item_id="lib-1",
        owner_id="u1",
        file_hash=SAMPLE_HASH,
        title="Favorite Track",
    )
    assert item.file_hash == SAMPLE_HASH

    with pytest.raises(DomainValidationError):
        LibraryItem(item_id="lib-2", owner_id="u1", file_hash="bad", title="Bad")
