"""ReviewEntry and FileReputationSummary domain models (Figure 54, Cas_14, Cas_15, Cas_24)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .errors import DomainValidationError

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_sha256(value: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise DomainValidationError("file_hash must be a 64-char lowercase SHA-256 hex string")
    return value


def _validate_rating(rating: int) -> int:
    if isinstance(rating, bool) or not isinstance(rating, int) or not (1 <= rating <= 5):
        raise DomainValidationError(f"rating must be an integer between 1 and 5, got {rating}")
    return rating


def _require_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainValidationError(f"{field_name} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class ReviewEntry:
    """Centralized file review submitted by an authenticated peer."""

    review_id: str
    file_hash: str
    author_id: str
    author_username: str
    rating: int
    comment: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.review_id, "review_id")
        _validate_sha256(self.file_hash)
        _require_non_empty(self.author_id, "author_id")
        _require_non_empty(self.author_username, "author_username")
        _validate_rating(self.rating)
        _require_non_empty(self.comment, "comment")


@dataclass(slots=True)
class FileReputationSummary:
    """Denormalized O(1) reputation aggregate for a file_hash in file_catalog."""

    file_hash: str
    review_count: int = 0
    rating_sum: int = 0
    avg_rating: float = 0.0

    def __post_init__(self) -> None:
        _validate_sha256(self.file_hash)
        if isinstance(self.review_count, bool) or self.review_count < 0:
            raise DomainValidationError("review_count must be non-negative")
        if isinstance(self.rating_sum, bool) or self.rating_sum < 0:
            raise DomainValidationError("rating_sum must be non-negative")
        if self.avg_rating < 0.0 or self.avg_rating > 5.0:
            raise DomainValidationError("avg_rating must be between 0.0 and 5.0")

    def _recalculate_avg(self) -> None:
        self.avg_rating = (self.rating_sum / self.review_count) if self.review_count > 0 else 0.0

    def apply_new_review(self, rating: int) -> None:
        _validate_rating(rating)
        self.review_count += 1
        self.rating_sum += rating
        self._recalculate_avg()

    def apply_updated_review(self, old_rating: int, new_rating: int) -> None:
        _validate_rating(old_rating)
        _validate_rating(new_rating)
        if self.review_count == 0:
            raise DomainValidationError("Cannot update review when review_count is 0")
        updated_sum = self.rating_sum - old_rating + new_rating
        if updated_sum < 0:
            raise DomainValidationError("rating_sum cannot become negative")
        self.rating_sum = updated_sum
        self._recalculate_avg()

    def apply_removed_review(self, old_rating: int) -> None:
        _validate_rating(old_rating)
        if self.review_count == 0:
            raise DomainValidationError("Cannot remove review when review_count is 0")
        if self.rating_sum < old_rating:
            raise DomainValidationError("rating_sum cannot become negative")
        self.review_count -= 1
        self.rating_sum -= old_rating
        self._recalculate_avg()
