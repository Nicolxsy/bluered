"""Formal TransferTask state machine (Figure 56)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final

from .enums import TransferState
from .errors import DomainValidationError, InvalidStateTransitionError

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

VALID_TRANSITIONS: Final[dict[TransferState, frozenset[TransferState]]] = {
    TransferState.QUEUED: frozenset(
        {
            TransferState.HANDSHAKING,
            TransferState.PAUSED,
            TransferState.FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.HANDSHAKING: frozenset(
        {
            TransferState.TRANSFERRING,
            TransferState.QUEUED,
            TransferState.FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.TRANSFERRING: frozenset(
        {
            TransferState.PAUSED,
            TransferState.COMPLETED,
            TransferState.FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.PAUSED: frozenset(
        {
            TransferState.RESUMING,
            TransferState.QUEUED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.RESUMING: frozenset(
        {
            TransferState.TRANSFERRING,
            TransferState.QUEUED,
            TransferState.PAUSED,
            TransferState.FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.FAILED: frozenset(
        {
            TransferState.RESUMING,
            TransferState.QUEUED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.COMPLETED: frozenset(),
    TransferState.CANCELLED: frozenset(),
}


@dataclass(slots=True)
class TransferTask:
    """P2P file transfer entity enforcing the Figure 56 finite state machine."""

    task_id: str
    file_hash: str
    file_name: str
    total_bytes: int
    remote_peer_id: str
    state: TransferState = TransferState.QUEUED
    bytes_transferred: int = 0
    retry_count: int = 0
    max_retries: int = 3
    queue_position: int | None = None
    error_message: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.task_id.strip():
            raise DomainValidationError("task_id must be non-empty")
        if not _SHA256_RE.fullmatch(self.file_hash):
            raise DomainValidationError("file_hash must be a valid 64-char SHA-256 hex string")
        if not self.file_name.strip():
            raise DomainValidationError("file_name must be non-empty")
        if not self.remote_peer_id.strip():
            raise DomainValidationError("remote_peer_id must be non-empty")
        if isinstance(self.total_bytes, bool) or self.total_bytes < 0:
            raise DomainValidationError("total_bytes must be non-negative")
        if (
            isinstance(self.bytes_transferred, bool)
            or self.bytes_transferred < 0
            or self.bytes_transferred > self.total_bytes
        ):
            raise DomainValidationError("bytes_transferred must be within 0..total_bytes")
        if isinstance(self.retry_count, bool) or self.retry_count < 0:
            raise DomainValidationError("retry_count must be non-negative")
        if isinstance(self.max_retries, bool) or self.max_retries < 0:
            raise DomainValidationError("max_retries must be non-negative")
        if self.queue_position is not None and (
            isinstance(self.queue_position, bool) or self.queue_position < 0
        ):
            raise DomainValidationError("queue_position must be non-negative when set")

    @property
    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries

    @property
    def progress_ratio(self) -> float:
        if self.total_bytes == 0:
            return 1.0
        return self.bytes_transferred / self.total_bytes

    def can_transition_to(self, target: TransferState) -> bool:
        if target not in VALID_TRANSITIONS[self.state]:
            return False
        return not (
            self.state == TransferState.FAILED
            and target in {TransferState.RESUMING, TransferState.QUEUED}
            and not self.can_retry
        )

    def transition_to(self, target: TransferState, *, error_message: str | None = None) -> None:
        if not self.can_transition_to(target):
            reason = (
                "retry budget exhausted"
                if self.state == TransferState.FAILED
                and target in {TransferState.RESUMING, TransferState.QUEUED}
                and not self.can_retry
                else None
            )
            raise InvalidStateTransitionError(self.state, target, reason=reason)

        if self.state == TransferState.FAILED and target in {
            TransferState.RESUMING,
            TransferState.QUEUED,
        }:
            self.retry_count += 1
            self.error_message = None

        if target == TransferState.FAILED:
            self.error_message = error_message

        if target in {
            TransferState.TRANSFERRING,
            TransferState.COMPLETED,
            TransferState.CANCELLED,
        }:
            self.queue_position = None

        self.state = target
        self.updated_at = datetime.now(UTC)

    def start_handshake(self) -> None:
        self.transition_to(TransferState.HANDSHAKING)

    def begin_transfer(self) -> None:
        self.transition_to(TransferState.TRANSFERRING)

    def pause(self) -> None:
        self.transition_to(TransferState.PAUSED)

    def resume(self) -> None:
        self.transition_to(TransferState.RESUMING)

    def fail(self, reason: str) -> None:
        self.transition_to(TransferState.FAILED, error_message=reason)

    def complete(self) -> None:
        self.transition_to(TransferState.COMPLETED)

    def cancel(self) -> None:
        self.transition_to(TransferState.CANCELLED)

    def requeue(self, position: int | None = None) -> None:
        if position is not None and (isinstance(position, bool) or position < 0):
            raise DomainValidationError("queue_position must be non-negative")
        self.transition_to(TransferState.QUEUED)
        self.queue_position = position

    def advance_progress(self, chunk_bytes: int) -> None:
        if self.state != TransferState.TRANSFERRING:
            raise DomainValidationError(
                f"Cannot advance progress while task is in state {self.state.value}"
            )
        if isinstance(chunk_bytes, bool) or chunk_bytes < 0:
            raise DomainValidationError("chunk_bytes must be non-negative")
        new_total = self.bytes_transferred + chunk_bytes
        if new_total > self.total_bytes:
            raise DomainValidationError(
                f"bytes_transferred ({new_total}) cannot exceed total_bytes ({self.total_bytes})"
            )
        self.bytes_transferred = new_total
        self.updated_at = datetime.now(UTC)
