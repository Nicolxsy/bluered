"""PeerSession and SearchRequestPacket domain models (Figure 54, Cas_08, Cas_11, Cas_12)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .errors import DomainValidationError


def _require_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainValidationError(f"{field_name} must be a non-empty string")
    return value


@dataclass(slots=True)
class PeerSession:
    """Ephemeral active peer session stored in Redis with strict 45s TTL."""

    user_id: str
    username: str
    ip_address: str
    tcp_port: int
    bloom_filter_bytes: bytes | None = None
    bloom_size_bits: int | None = None
    bloom_hash_functions_k: int | None = None
    last_heartbeat_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 45

    def __post_init__(self) -> None:
        _require_non_empty(self.user_id, "user_id")
        _require_non_empty(self.username, "username")
        _require_non_empty(self.ip_address, "ip_address")
        if isinstance(self.tcp_port, bool) or not (1 <= self.tcp_port <= 65535):
            raise DomainValidationError(
                f"tcp_port must be between 1 and 65535, got {self.tcp_port}"
            )
        if isinstance(self.ttl_seconds, bool) or self.ttl_seconds <= 0:
            raise DomainValidationError("ttl_seconds must be positive")
        if self.bloom_size_bits is not None and (
            isinstance(self.bloom_size_bits, bool) or self.bloom_size_bits <= 0
        ):
            raise DomainValidationError("bloom_size_bits must be positive")
        if self.bloom_hash_functions_k is not None and (
            isinstance(self.bloom_hash_functions_k, bool) or self.bloom_hash_functions_k < 1
        ):
            raise DomainValidationError("bloom_hash_functions_k must be >= 1")

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return current >= self.last_heartbeat_at + timedelta(seconds=self.ttl_seconds)

    def refresh_heartbeat(self, now: datetime | None = None) -> None:
        self.last_heartbeat_at = now or datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SearchRequestPacket:
    """Federated search routing packet tracked with a 60s TTL."""

    search_token: str
    requester_id: str
    query: str
    max_results: int = 50
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60

    def __post_init__(self) -> None:
        _require_non_empty(self.search_token, "search_token")
        _require_non_empty(self.requester_id, "requester_id")
        _require_non_empty(self.query, "query")
        if isinstance(self.max_results, bool) or not (1 <= self.max_results <= 200):
            raise DomainValidationError("max_results must be between 1 and 200")
        if isinstance(self.ttl_seconds, bool) or self.ttl_seconds <= 0:
            raise DomainValidationError("ttl_seconds must be positive")
