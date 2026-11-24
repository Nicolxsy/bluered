"""Peer node hierarchy (PeerNode <- LocalPeer, RemotePeer) obeying LSP."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from .enums import PresenceStatus
from .errors import DomainValidationError


def _require_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainValidationError(f"{field_name} must be a non-empty string")
    return value


@dataclass(slots=True)
class PeerNode(ABC):
    """Abstract base class for local and remote P2P nodes (Figure 53)."""

    peer_id: str
    username: str
    ip_address: str
    tcp_port: int

    def _validate_base(self) -> None:
        _require_non_empty(self.peer_id, "peer_id")
        _require_non_empty(self.username, "username")
        _require_non_empty(self.ip_address, "ip_address")
        if isinstance(self.tcp_port, bool) or not (1 <= self.tcp_port <= 65535):
            raise DomainValidationError(
                f"tcp_port must be between 1 and 65535, got {self.tcp_port}"
            )

    def endpoint(self) -> tuple[str, int]:
        """Return the (ip_address, tcp_port) socket endpoint."""
        return (self.ip_address, self.tcp_port)

    @property
    @abstractmethod
    def is_available_for_transfer(self) -> bool:
        """Whether this node can currently participate in a P2P transfer."""

    @abstractmethod
    def describe(self) -> str:
        """Human-readable node description."""


@dataclass(slots=True)
class LocalPeer(PeerNode):
    """Local peer configuration and upload slot / blocklist state."""

    shared_directories: list[Path] = field(default_factory=list)
    max_upload_slots: int = 3
    blocked_peer_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._validate_base()
        if isinstance(self.max_upload_slots, bool) or self.max_upload_slots < 0:
            raise DomainValidationError("max_upload_slots must be a non-negative integer")

    @property
    def is_available_for_transfer(self) -> bool:
        return self.max_upload_slots > 0

    def describe(self) -> str:
        return (
            f"LocalPeer({self.username}@{self.ip_address}:{self.tcp_port}, "
            f"slots={self.max_upload_slots})"
        )

    def block_peer(self, peer_id: str) -> None:
        _require_non_empty(peer_id, "peer_id")
        self.blocked_peer_ids.add(peer_id)

    def unblock_peer(self, peer_id: str) -> None:
        self.blocked_peer_ids.discard(peer_id)

    def is_peer_blocked(self, peer_id: str) -> bool:
        return peer_id in self.blocked_peer_ids

    def add_shared_directory(self, path: Path) -> None:
        if path not in self.shared_directories:
            self.shared_directories.append(path)


@dataclass(slots=True)
class RemotePeer(PeerNode):
    """Remote peer discovered via search or directory browse."""

    status: PresenceStatus = PresenceStatus.OFFLINE
    average_upload_speed: float = 0.0

    def __post_init__(self) -> None:
        self._validate_base()
        if isinstance(self.average_upload_speed, bool) or self.average_upload_speed < 0.0:
            raise DomainValidationError("average_upload_speed must be non-negative")

    @property
    def is_available_for_transfer(self) -> bool:
        return self.status == PresenceStatus.ONLINE

    def describe(self) -> str:
        return (
            f"RemotePeer({self.username}@{self.ip_address}:{self.tcp_port}, "
            f"status={self.status.value})"
        )
