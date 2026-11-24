"""Community, ChatRoom, DebatePost, and LibraryItem domain entities (Cas_01..Cas_06, Cas_13)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .errors import DomainValidationError

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _require_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainValidationError(f"{field_name} must be a non-empty string")
    return value


@dataclass(slots=True)
class Community:
    """User-created thematic community (Cas_01, Cas_02)."""

    community_id: str
    name: str
    description: str = ""
    creator_id: str = ""
    member_ids: set[str] = field(default_factory=set)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.community_id, "community_id")
        _require_non_empty(self.name, "name")
        if len(self.name.strip()) > 120:
            raise DomainValidationError("community name must be at most 120 characters")
        _require_non_empty(self.creator_id, "creator_id")


@dataclass(slots=True)
class ChatRoom:
    """Public or community chat room (Cas_05, Cas_06)."""

    room_id: str
    name: str
    community_id: str | None = None
    member_ids: set[str] = field(default_factory=set)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.room_id, "room_id")
        _require_non_empty(self.name, "name")

    @property
    def member_count(self) -> int:
        return len(self.member_ids)


@dataclass(frozen=True, slots=True)
class DebatePost:
    """Discussion thread post or reply inside a community (Cas_03, Cas_04)."""

    post_id: str
    community_id: str
    author_id: str
    title: str
    body: str
    parent_post_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.post_id, "post_id")
        _require_non_empty(self.community_id, "community_id")
        _require_non_empty(self.author_id, "author_id")
        _require_non_empty(self.title, "title")
        _require_non_empty(self.body, "body")


@dataclass(frozen=True, slots=True)
class LibraryItem:
    """Personal saved reference to a file hash in a user's library (Cas_13)."""

    item_id: str
    owner_id: str
    file_hash: str
    title: str
    saved_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.item_id, "item_id")
        _require_non_empty(self.owner_id, "owner_id")
        if not isinstance(self.file_hash, str) or not _SHA256_RE.fullmatch(self.file_hash):
            raise DomainValidationError("file_hash must be a 64-char lowercase SHA-256 hex string")
        _require_non_empty(self.title, "title")
