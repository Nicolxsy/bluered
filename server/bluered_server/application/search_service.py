"""Federated search coordinator service (Cas_11, Cas_12)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from bluered_server.contracts import (
    IClientPushGateway,
    IFileReviewRepository,
    ISearchRoutingStrategy,
    ISessionStore,
)
from bluered_server.domain import PeerSession, SearchRequestPacket

_PROTOCOL_VERSION = "1.0"


@dataclass(slots=True)
class _PendingSearch:
    expected_peers: int
    max_results: int
    done_event: asyncio.Event = field(default_factory=asyncio.Event)
    hits: list[dict[str, Any]] = field(default_factory=list)
    reported_peers: int = 0


class SearchCoordinatorService:
    """Coordinates federated search, 60s routes, timeout consolidation, and O(1) enrichment."""

    def __init__(
        self,
        session_store: ISessionStore,
        review_repo: IFileReviewRepository,
        routing_strategy: ISearchRoutingStrategy,
        push_gateway: IClientPushGateway,
        *,
        route_ttl_seconds: int = 60,
        default_timeout_seconds: float = 2.0,
    ) -> None:
        self._session_store = session_store
        self._review_repo = review_repo
        self._routing_strategy = routing_strategy
        self._push_gateway = push_gateway
        self._route_ttl_seconds = route_ttl_seconds
        self._default_timeout_seconds = default_timeout_seconds
        self._pending: dict[str, _PendingSearch] = {}

    async def initiate_search(
        self,
        requester_id: str,
        query: str,
        *,
        search_token: str | None = None,
        max_results: int = 50,
    ) -> tuple[str, list[PeerSession]]:
        """Register search route (TTL 60s) and broadcast SEARCH_BROADCAST to routed peers."""
        token = search_token or uuid4().hex
        packet = SearchRequestPacket(
            search_token=token,
            requester_id=requester_id,
            query=query,
            max_results=max_results,
            ttl_seconds=self._route_ttl_seconds,
        )
        await self._session_store.register_search_route(
            packet.search_token,
            requester_id,
            ttl_seconds=self._route_ttl_seconds,
        )

        active_sessions = await self._session_store.list_active_sessions()
        candidate_sessions = [s for s in active_sessions if s.user_id != requester_id]
        targets = self._routing_strategy.select_target_peers(packet, candidate_sessions)

        if targets:
            payload = json.dumps(
                {
                    "protocol_version": _PROTOCOL_VERSION,
                    "type": "SEARCH_BROADCAST",
                    "search_token": packet.search_token,
                    "requester_id": requester_id,
                    "query": packet.query,
                }
            )
            await self._push_gateway.broadcast([t.user_id for t in targets], payload)

        return packet.search_token, targets

    async def search_and_wait(
        self,
        requester_id: str,
        query: str,
        *,
        search_token: str | None = None,
        max_results: int = 50,
        timeout_seconds: float | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Initiate search, consolidate partial hits on timeout, enrich, and push results."""
        token = search_token or uuid4().hex
        pending = _PendingSearch(expected_peers=0, max_results=max_results)
        self._pending[token] = pending
        try:
            _, targets = await self.initiate_search(
                requester_id=requester_id,
                query=query,
                search_token=token,
                max_results=max_results,
            )
            pending.expected_peers = len(targets)

            if targets:
                wait_for = (
                    self._default_timeout_seconds
                    if timeout_seconds is None
                    else timeout_seconds
                )
                with suppress(TimeoutError):
                    await asyncio.wait_for(pending.done_event.wait(), timeout=wait_for)

            enriched = await self.enrich_hits(pending.hits[:max_results])
            results_payload = json.dumps(
                {
                    "protocol_version": _PROTOCOL_VERSION,
                    "type": "SEARCH_RESULTS",
                    "search_token": token,
                    "results": enriched,
                }
            )
            await self._push_gateway.send_to_peer(requester_id, results_payload)
            return token, enriched
        finally:
            self._pending.pop(token, None)

    async def handle_hit_report(
        self,
        search_token: str,
        hits: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Accept SEARCH_HIT_REPORT from a peer, enrich in batch, and route to requester."""
        requester_id = await self._session_store.get_search_route(search_token)
        if requester_id is None:
            return []

        pending = self._pending.get(search_token)
        if pending is not None:
            pending.hits.extend(dict(h) for h in hits)
            pending.reported_peers += 1
            if pending.expected_peers > 0 and pending.reported_peers >= pending.expected_peers:
                pending.done_event.set()
            return []

        enriched = await self.enrich_hits(hits)
        results_payload = json.dumps(
            {
                "protocol_version": _PROTOCOL_VERSION,
                "type": "SEARCH_RESULTS",
                "search_token": search_token,
                "results": enriched,
            }
        )
        await self._push_gateway.send_to_peer(requester_id, results_payload)
        return enriched

    async def enrich_hits(self, hits: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        """Enrich search hits with O(1) batch reputation summaries from file_catalog."""
        if not hits:
            return []

        unique_hashes = list(
            dict.fromkeys(
                str(hit["file_hash"]) for hit in hits if hit.get("file_hash") is not None
            )
        )
        summaries = (
            await self._review_repo.get_reputation_summaries(unique_hashes)
            if unique_hashes
            else {}
        )

        enriched: list[dict[str, Any]] = []
        for hit in hits:
            item = dict(hit)
            file_hash = str(item.get("file_hash", ""))
            summary = summaries.get(file_hash)
            if summary is not None:
                item["avg_rating"] = round(summary.avg_rating, 4)
                item["review_count"] = summary.review_count
            else:
                item["avg_rating"] = None
                item["review_count"] = 0
            enriched.append(item)
        return enriched
