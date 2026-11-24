"""UserAccount domain entity for authentication & lockout (Cas_07)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .errors import DomainValidationError


@dataclass(slots=True)
class UserAccount:
    """Persistent user account with Argon2 password hash and lockout state."""

    user_id: str
    username: str
    password_hash: str
    failed_login_attempts: int = 0
    locked_until: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.user_id.strip():
            raise DomainValidationError("user_id must be non-empty")
        stripped_user = self.username.strip()
        if not (3 <= len(stripped_user) <= 64):
            raise DomainValidationError("username length must be between 3 and 64 characters")
        if not self.password_hash.strip():
            raise DomainValidationError("password_hash must be non-empty")
        if isinstance(self.failed_login_attempts, bool) or self.failed_login_attempts < 0:
            raise DomainValidationError("failed_login_attempts must be non-negative")

    def is_locked(self, now: datetime | None = None) -> bool:
        if self.locked_until is None:
            return False
        current = now or datetime.now(UTC)
        return current < self.locked_until

    def record_failed_login(
        self,
        *,
        max_attempts: int = 5,
        lock_duration_seconds: int = 300,
        now: datetime | None = None,
    ) -> None:
        if max_attempts < 1:
            raise DomainValidationError("max_attempts must be >= 1")
        if lock_duration_seconds <= 0:
            raise DomainValidationError("lock_duration_seconds must be positive")
        current = now or datetime.now(UTC)
        self.failed_login_attempts += 1
        if self.failed_login_attempts >= max_attempts:
            self.locked_until = current + timedelta(seconds=lock_duration_seconds)

    def record_successful_login(self) -> None:
        self.failed_login_attempts = 0
        self.locked_until = None
