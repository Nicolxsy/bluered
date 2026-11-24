"""Domain exceptions for BlueRed client."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .enums import TransferState


class DomainError(Exception):
    """Base exception for client domain invariant violations."""


class DomainValidationError(DomainError, ValueError):
    """Raised when a domain entity or value object fails invariant validation."""


class InvalidStateTransitionError(DomainError):
    """Raised when TransferTask is asked to perform an illegal state transition."""

    def __init__(
        self,
        current_state: TransferState,
        target_state: TransferState,
        reason: str | None = None,
    ) -> None:
        self.current_state = current_state
        self.target_state = target_state
        detail = f" ({reason})" if reason else ""
        super().__init__(
            f"Invalid TransferTask transition: {current_state.value} -> "
            f"{target_state.value}{detail}"
        )
