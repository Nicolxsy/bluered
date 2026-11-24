"""Direct peer-to-peer PrivateMessage entity (Figure 53 & Cas_23)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from .errors import DomainValidationError


@dataclass(slots=True)
class PrivateMessage:
    """Direct P2P chat message exchanged between two peers."""

    message_id: str
    sender_id: str
    recipient_id: str
    content: str
    delivered: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.message_id.strip():
            raise DomainValidationError("message_id must be non-empty")
        if not self.sender_id.strip():
            raise DomainValidationError("sender_id must be non-empty")
        if not self.recipient_id.strip():
            raise DomainValidationError("recipient_id must be non-empty")
        if not self.content.strip():
            raise DomainValidationError("content must be non-empty")

    def mark_delivered(self) -> None:
        self.delivered = True
