"""Concrete ISearchRoutingStrategy implementations (OCP)."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from bluered_contracts.bloom import BloomFilter

from bluered_server.contracts import ISearchRoutingStrategy
from bluered_server.domain import PeerSession, SearchRequestPacket


class FullBroadcastRoutingStrategy(ISearchRoutingStrategy):
    """Broadcasts search requests to all active candidate peers."""

    def select_target_peers(
        self,
        packet: SearchRequestPacket,
        active_sessions: Sequence[PeerSession],
    ) -> list[PeerSession]:
        return [s for s in active_sessions if s.user_id != packet.requester_id]


class BloomFilterRoutingStrategy(ISearchRoutingStrategy):
    """Routes search requests only to peers whose Bloom filter may match query tokens."""

    def __init__(self, *, include_without_bloom: bool = True) -> None:
        self._include_without_bloom = include_without_bloom

    def select_target_peers(
        self,
        packet: SearchRequestPacket,
        active_sessions: Sequence[PeerSession],
    ) -> list[PeerSession]:
        selected: list[PeerSession] = []
        for session in active_sessions:
            if session.user_id == packet.requester_id:
                continue
            if (
                session.bloom_filter_bytes is None
                or session.bloom_size_bits is None
                or session.bloom_hash_functions_k is None
            ):
                if self._include_without_bloom:
                    selected.append(session)
                continue
            bloom = BloomFilter.from_bytes(
                session.bloom_filter_bytes,
                size_bits=session.bloom_size_bits,
                hash_functions_k=session.bloom_hash_functions_k,
            )
            if bloom.might_match_query(packet.query):
                selected.append(session)
        return selected


class ShardedRoomRoutingStrategy(ISearchRoutingStrategy):
    """Deterministically routes search requests to a shard subset of active peers."""

    def __init__(self, *, shard_count: int = 2) -> None:
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        self._shard_count = shard_count

    def select_target_peers(
        self,
        packet: SearchRequestPacket,
        active_sessions: Sequence[PeerSession],
    ) -> list[PeerSession]:
        candidates = [s for s in active_sessions if s.user_id != packet.requester_id]
        if len(candidates) <= 1 or self._shard_count == 1:
            return candidates

        target_shard = (
            int.from_bytes(hashlib.sha256(packet.search_token.encode("utf-8")).digest()[:4], "big")
            % self._shard_count
        )
        selected = [
            session
            for session in candidates
            if (
                int.from_bytes(hashlib.sha256(session.user_id.encode("utf-8")).digest()[:4], "big")
                % self._shard_count
            )
            == target_shard
        ]
        return selected or candidates
