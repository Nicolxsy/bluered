"""Domain enumerations for BlueRed client (Figures 53 & 56)."""

from __future__ import annotations

from enum import StrEnum


class PresenceStatus(StrEnum):
    """Presence status of a remote peer in the hybrid P2P network."""

    ONLINE = "ONLINE"
    AWAY = "AWAY"
    OFFLINE = "OFFLINE"


class TransferState(StrEnum):
    """Formal state machine states for TransferTask (Figure 56)."""

    QUEUED = "QUEUED"
    HANDSHAKING = "HANDSHAKING"
    TRANSFERRING = "TRANSFERRING"
    PAUSED = "PAUSED"
    RESUMING = "RESUMING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in {TransferState.COMPLETED, TransferState.CANCELLED}
