"""Unit tests for SearchCoordinatorService (Cas_11, Cas_12) & FileReputationService."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from bluered_contracts.bloom import BloomFilter
from bluered_contracts.control_plane import (
    SearchBroadcastMessage,
    SearchResultsMessage,
    parse_control_message,
)
from bluered_server.application import (
    FileReputationService,
    SearchCoordinatorService,
    ValidationError,
)
from bluered_server.domain import PeerSession, SearchRequestPacket
from bluered_server.infrastructure import (
    BloomFilterRoutingStrategy,
    FullBroadcastRoutingStrategy,
    ShardedRoomRoutingStrategy,
)

from tests.unit.server.fakes import (
    InMemoryFileReviewRepository,
    InMemoryPushGateway,
    InMemorySessionStore,
)

HASH_A = hashlib.sha256(b"track-a").hexdigest()
HASH_B = hashlib.sha256(b"track-b").hexdigest()


async def test_search_routing_strategies_and_bloom_filtering_cas11() -> None:
    bloom_a = BloomFilter.optimal(50)
    bloom_a.add_filename("dark_side_of_the_moon.flac")
    bloom_b = BloomFilter.optimal(50)
    bloom_b.add_filename("kind_of_blue.flac")

    s_req = PeerSession(user_id="req", username="req", ip_address="127.0.0.1", tcp_port=9000)
    s_a = PeerSession(
        user_id="u1",
        username="alice",
        ip_address="127.0.0.1",
        tcp_port=9001,
        bloom_filter_bytes=bloom_a.to_bytes(),
        bloom_size_bits=bloom_a.size_bits,
        bloom_hash_functions_k=bloom_a.hash_functions_k,
    )
    s_b = PeerSession(
        user_id="u2",
        username="bob",
        ip_address="127.0.0.1",
        tcp_port=9002,
        bloom_filter_bytes=bloom_b.to_bytes(),
        bloom_size_bits=bloom_b.size_bits,
        bloom_hash_functions_k=bloom_b.hash_functions_k,
    )
    packet = SearchRequestPacket(search_token="tok-1", requester_id="req", query="moon")

    bloom_targets = BloomFilterRoutingStrategy().select_target_peers(packet, [s_req, s_a, s_b])
    assert [t.user_id for t in bloom_targets] == ["u1"]

    full_targets = FullBroadcastRoutingStrategy().select_target_peers(packet, [s_req, s_a, s_b])
    assert {t.user_id for t in full_targets} == {"u1", "u2"}

    shard_targets = ShardedRoomRoutingStrategy(shard_count=2).select_target_peers(
        packet, [s_req, s_a, s_b]
    )
    assert len(shard_targets) >= 1


async def test_search_coordinator_timeout_consolidation_and_batch_enrichment_cas12() -> None:
    sessions = InMemorySessionStore()
    reviews = InMemoryFileReviewRepository()
    gateway = InMemoryPushGateway()
    reputation = FileReputationService(reviews)

    await reputation.publish_review(
        file_hash=HASH_A,
        author_id="u9",
        author_username="reviewer",
        rating=5,
        comment="Crystal clear mastering",
    )

    await sessions.upsert_session(
        PeerSession(user_id="req", username="req", ip_address="127.0.0.1", tcp_port=9000)
    )
    await sessions.upsert_session(
        PeerSession(user_id="u1", username="alice", ip_address="127.0.0.1", tcp_port=9001)
    )
    await sessions.upsert_session(
        PeerSession(user_id="u2", username="bob", ip_address="127.0.0.1", tcp_port=9002)
    )

    coordinator = SearchCoordinatorService(
        session_store=sessions,
        review_repo=reviews,
        routing_strategy=FullBroadcastRoutingStrategy(),
        push_gateway=gateway,
        route_ttl_seconds=60,
        default_timeout_seconds=0.15,
    )

    async def _simulate_one_peer_reply() -> None:
        await asyncio.sleep(0.03)
        await coordinator.handle_hit_report(
            "search-tok-1",
            [
                {
                    "file_hash": HASH_A,
                    "filename": "track_a.flac",
                    "size_bytes": 1024,
                    "peer_id": "u1",
                    "peer_username": "alice",
                    "peer_ip": "127.0.0.1",
                    "peer_port": 9001,
                },
                {
                    "file_hash": HASH_B,
                    "filename": "track_b.flac",
                    "size_bytes": 2048,
                    "peer_id": "u1",
                    "peer_username": "alice",
                    "peer_ip": "127.0.0.1",
                    "peer_port": 9001,
                },
            ],
        )

    task = asyncio.create_task(_simulate_one_peer_reply())
    token, results = await coordinator.search_and_wait(
        requester_id="req",
        query="track",
        search_token="search-tok-1",
        timeout_seconds=0.15,
    )
    await task

    assert token == "search-tok-1"
    assert sessions.last_route_ttl == 60
    assert len(results) == 2
    assert results[0]["avg_rating"] == pytest.approx(5.0)
    assert results[0]["review_count"] == 1
    assert results[1]["avg_rating"] is None
    assert results[1]["review_count"] == 0
    assert reviews.batch_calls == 1

    # Verify broadcast and final SEARCH_RESULTS payloads conform to Pydantic contracts
    broadcasts = [
        parse_control_message(p) for uid, p in gateway.sent if uid in {"u1", "u2"}
    ]
    assert all(isinstance(b, SearchBroadcastMessage) for b in broadcasts)
    final_msgs = [parse_control_message(p) for uid, p in gateway.sent if uid == "req"]
    assert isinstance(final_msgs[-1], SearchResultsMessage)
    assert len(final_msgs[-1].results) == 2


async def test_file_reputation_validation_upsert_and_pagination_cas14_15_24() -> None:
    repo = InMemoryFileReviewRepository()
    service = FileReputationService(repo)

    with pytest.raises(ValidationError):
        await service.publish_review(
            file_hash=HASH_A,
            author_id="u1",
            author_username="alice",
            rating=5,
            comment="   ",
        )

    with pytest.raises(ValidationError):
        await service.publish_review(
            file_hash=HASH_A,
            author_id="u1",
            author_username="alice",
            rating=1,
            comment="This file is malware!",
        )

    s1 = await service.publish_review(
        file_hash=HASH_A,
        author_id="u1",
        author_username="alice",
        rating=5,
        comment="Great quality",
    )
    assert s1.review_count == 1
    assert s1.avg_rating == pytest.approx(5.0)

    # Update existing author's review (Cas_24)
    s2 = await service.publish_review(
        file_hash=HASH_A,
        author_id="u1",
        author_username="alice",
        rating=3,
        comment="Actually bit noisy",
    )
    assert s2.review_count == 1
    assert s2.avg_rating == pytest.approx(3.0)

    await service.publish_review(
        file_hash=HASH_A,
        author_id="u2",
        author_username="bob",
        rating=5,
        comment="Clean rip",
    )

    summary, page1, total = await service.get_reviews(HASH_A, page=1, page_size=1)
    _, page2, _ = await service.get_reviews(HASH_A, page=2, page_size=1)
    assert total == 2
    assert summary.avg_rating == pytest.approx(4.0)
    assert len(page1) == 1
    assert len(page2) == 1
    assert page1[0].author_id != page2[0].author_id


async def test_handle_hit_report_direct_route_emits_valid_search_results() -> None:
    sessions = InMemorySessionStore()
    reviews = InMemoryFileReviewRepository()
    gateway = InMemoryPushGateway()
    reputation = FileReputationService(reviews)
    await reputation.publish_review(
        file_hash=HASH_A,
        author_id="u9",
        author_username="reviewer",
        rating=4,
        comment="Solid lossless rip",
    )
    await sessions.register_search_route("direct-tok-1", "req-user", ttl_seconds=60)

    coordinator = SearchCoordinatorService(
        session_store=sessions,
        review_repo=reviews,
        routing_strategy=FullBroadcastRoutingStrategy(),
        push_gateway=gateway,
    )
    enriched = await coordinator.handle_hit_report(
        "direct-tok-1",
        [
            {
                "file_hash": HASH_A,
                "filename": "track_a.flac",
                "size_bytes": 4096,
                "peer_id": "u1",
                "peer_username": "alice",
                "peer_ip": "127.0.0.1",
                "peer_port": 9001,
            }
        ],
    )
    assert len(enriched) == 1
    assert enriched[0]["avg_rating"] == pytest.approx(4.0)
    assert enriched[0]["review_count"] == 1
    assert len(gateway.sent) == 1
    recipient, payload = gateway.sent[0]
    assert recipient == "req-user"
    parsed = parse_control_message(payload)
    assert isinstance(parsed, SearchResultsMessage)
    assert parsed.search_token == "direct-tok-1"
    assert len(parsed.results) == 1
    assert parsed.results[0].avg_rating == pytest.approx(4.0)
