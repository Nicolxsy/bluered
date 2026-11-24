"""SharedFile, FileReview, and SearchMatch domain models (Figure 53)."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from .errors import DomainValidationError
from .peers import RemotePeer

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_READ_CHUNK_SIZE = 64 * 1024


def _validate_sha256(value: str, field_name: str = "file_hash") -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise DomainValidationError(f"{field_name} must be a 64-char lowercase SHA-256 hex string")
    return value


def _validate_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainValidationError(f"{field_name} must be a non-empty string")
    return value


@dataclass(slots=True)
class SharedFile:
    """Metadata and integrity verification for a shared or discovered file."""

    file_hash: str
    file_name: str
    size_bytes: int
    bitrate_kbps: int | None = None
    local_path: Path | None = None
    mime_type: str = "application/octet-stream"
    category: str = "other"

    def __post_init__(self) -> None:
        _validate_sha256(self.file_hash, "file_hash")
        _validate_non_empty(self.file_name, "file_name")
        _validate_non_empty(self.mime_type, "mime_type")
        _validate_non_empty(self.category, "category")
        if isinstance(self.size_bytes, bool) or self.size_bytes < 0:
            raise DomainValidationError("size_bytes must be a non-negative integer")
        if self.bitrate_kbps is not None and (
            isinstance(self.bitrate_kbps, bool) or self.bitrate_kbps < 0
        ):
            raise DomainValidationError("bitrate_kbps must be a non-negative integer")

    def verify_integrity(self, actual_hash: str | None = None) -> bool:
        """Verify SHA-256 integrity against a supplied digest or the local file on disk."""
        if actual_hash is not None:
            return actual_hash.strip().lower() == self.file_hash
        if self.local_path is None or not self.local_path.is_file():
            return False
        digest = hashlib.sha256()
        with self.local_path.open("rb") as handle:
            while chunk := handle.read(_READ_CHUNK_SIZE):
                digest.update(chunk)
        return digest.hexdigest() == self.file_hash


@dataclass(frozen=True, slots=True)
class FileReview:
    """Immutable user review for a shared file (rating 1..5, non-empty comment)."""

    review_id: str
    file_hash: str
    author_id: str
    rating: int
    comment: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _validate_non_empty(self.review_id, "review_id")
        _validate_sha256(self.file_hash, "file_hash")
        _validate_non_empty(self.author_id, "author_id")
        if (
            isinstance(self.rating, bool)
            or not isinstance(self.rating, int)
            or not (1 <= self.rating <= 5)
        ):
            raise DomainValidationError(
                f"rating must be an integer between 1 and 5, got {self.rating}"
            )
        _validate_non_empty(self.comment, "comment")


@dataclass(frozen=True, slots=True)
class SearchMatch:
    """Pure domain search result pairing a discovered file with its hosting peer."""

    file: SharedFile
    peer: RemotePeer
    avg_rating: float | None = None
    review_count: int = 0
