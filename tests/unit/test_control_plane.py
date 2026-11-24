"""Unit tests for shared/bluered_contracts/control_plane.py."""

from __future__ import annotations

import pytest
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
    LibraryItem,
    LibraryListMessage,
    LibrarySaveMessage,
    NatPunchCoordinateMessage,
    NatPunchRequestMessage,
    NatRelayRequestMessage,
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
from pydantic import ValidationError

SAMPLE_HASH = "a" * 64


@pytest.mark.parametrize(
    "msg",
    [
        AuthRegisterMessage(username="alice", password="secret_password"),
        AuthLoginMessage(username="alice", password="secret_password", tcp_port=7001),
        AuthOkMessage(user_id="u1", username="alice", session_token="tok-1"),
        AuthErrorMessage(code="INVALID_CREDENTIALS", detail="Bad password"),
        HeartbeatMessage(user_id="u1", tcp_port=7001),
        PresenceUpdateMessage(
            user_id="u1", username="alice", online=True, ip="127.0.0.1", tcp_port=7001
        ),
        BloomSyncMessage(
            user_id="u1", size_bits=64, hash_functions_k=3, bitset_base64="AAAAAAAAAAA="
        ),
        SearchQueryMessage(search_token="st-1", query="clean architecture", max_results=20),
        SearchBroadcastMessage(search_token="st-1", requester_id="u1", query="clean architecture"),
        SearchHitReportMessage(
            search_token="st-1",
            hits=[
                SearchHitItem(
                    file_hash=SAMPLE_HASH,
                    filename="clean.pdf",
                    size_bytes=1024,
                    peer_id="u2",
                    peer_username="bob",
                    peer_ip="127.0.0.1",
                    peer_port=7002,
                    avg_rating=4.5,
                    review_count=2,
                )
            ],
        ),
        SearchResultsMessage(search_token="st-1", results=[]),
        ReviewPublishMessage(file_hash=SAMPLE_HASH, rating=5, comment="Excelente"),
        ReviewAckMessage(file_hash=SAMPLE_HASH, review_count=1, avg_rating=5.0),
        ReviewsFetchMessage(file_hash=SAMPLE_HASH),
        ReviewsListMessage(
            file_hash=SAMPLE_HASH,
            review_count=1,
            avg_rating=5.0,
            reviews=[
                ReviewItem(
                    file_hash=SAMPLE_HASH,
                    author_id="u1",
                    author_username="alice",
                    rating=5,
                    comment="Excelente",
                    updated_at_iso="2026-11-23T20:00:00Z",
                )
            ],
        ),
        CommunityCreateMessage(name="Redes P2P", description="Debates de protocolos"),
        CommunityFetchMessage(),
        CommunityListMessage(communities=[CommunitySummary(id="c1", name="Redes P2P")]),
        DebatePostMessage(community_id="c1", title="TCP vs UDP", body="Comparativa"),
        DebateReplyMessage(post_id="p1", body="TCP framing !II funciona bien"),
        DebateFetchMessage(community_id="c1"),
        DebateListMessage(
            community_id="c1",
            posts=[
                DebateItem(
                    post_id="p1",
                    community_id="c1",
                    author_id="u1",
                    title="TCP vs UDP",
                    body="Comparativa",
                    parent_post_id=None,
                    created_at_iso="2026-11-23T20:00:00Z",
                )
            ],
        ),
        LibrarySaveMessage(file_hash=SAMPLE_HASH, title="Paper P2P"),
        LibraryFetchMessage(),
        LibraryListMessage(
            items=[
                LibraryItem(
                    file_hash=SAMPLE_HASH,
                    title="Paper P2P",
                    saved_at_iso="2026-11-23T20:00:00Z",
                )
            ]
        ),
        RoomFetchMessage(),
        RoomListMessage(rooms=[RoomSummary(id="r1", name="General", member_count=3)]),
        RoomJoinMessage(room_id="r1"),
        RoomMessage(room_id="r1", sender_id="u1", sender_username="alice", content="Hola"),
        NatPunchRequestMessage(
            target_peer_id="u2", requester_ip="203.0.113.10", requester_port=9000
        ),
        NatPunchCoordinateMessage(
            peer_id="u2",
            peer_ip="203.0.113.20",
            peer_port=9001,
            rendezvous_at_epoch_ms=1700000000000,
        ),
        NatRelayRequestMessage(target_peer_id="u2", session_id="rel-1"),
        NatRelayRequestMessage(target_peer_id="u2", session_id="rel-2", requester_id="u1"),
    ],
)
def test_control_plane_roundtrip_all_messages(msg: ControlPlaneEnvelope) -> None:
    raw_json = serialize_control_message(msg)
    parsed = parse_control_message(raw_json)
    assert parsed == msg
    assert parsed.protocol_version == "1.0"


def test_control_plane_rejects_unknown_type_and_extra_fields() -> None:
    with pytest.raises(ValidationError):
        parse_control_message('{"type": "UNKNOWN_OP", "protocol_version": "1.0"}')

    with pytest.raises(ValidationError):
        parse_control_message(
            '{"type": "HEARTBEAT", "user_id": "u1", "tcp_port": 7000, "extra": 1}'
        )


def test_review_publish_validation_constraints() -> None:
    with pytest.raises(ValidationError):
        ReviewPublishMessage(file_hash=SAMPLE_HASH, rating=0)
    with pytest.raises(ValidationError):
        ReviewPublishMessage(file_hash=SAMPLE_HASH, rating=6)
    with pytest.raises(ValidationError):
        ReviewPublishMessage(file_hash="not-a-sha256", rating=4)
