"""Centralized file reputation & review service (Cas_14, Cas_15, Cas_24)."""

from __future__ import annotations

import re
from collections.abc import Sequence
from uuid import uuid4

from bluered_server.contracts import IFileReviewRepository
from bluered_server.domain import FileReputationSummary, ReviewEntry

from .errors import ValidationError

_DEFAULT_BANNED_TERMS: tuple[str, ...] = (
    "malware",
    "ransomware",
    "trojan",
    "phishing",
    "spam",
)


class FileReputationService:
    """Validates, filters, persists (ACID), and queries centralized file reviews."""

    def __init__(
        self,
        review_repo: IFileReviewRepository,
        *,
        banned_terms: Sequence[str] | None = None,
    ) -> None:
        self._review_repo = review_repo
        terms = _DEFAULT_BANNED_TERMS if banned_terms is None else tuple(banned_terms)
        self._banned_patterns = [
            re.compile(rf"\b{re.escape(term.strip().lower())}\b")
            for term in terms
            if term.strip()
        ]

    async def publish_review(
        self,
        *,
        file_hash: str,
        author_id: str,
        author_username: str,
        rating: int,
        comment: str,
        review_id: str | None = None,
    ) -> FileReputationSummary:
        """Validate non-empty & inappropriate content, then UPSERT review in ACID transaction."""
        if not isinstance(comment, str) or not comment.strip():
            raise ValidationError("Review comment must be non-empty")

        lowered = comment.lower()
        for pattern in self._banned_patterns:
            if pattern.search(lowered):
                raise ValidationError("Review comment contains inappropriate content")

        entry = ReviewEntry(
            review_id=review_id or uuid4().hex,
            file_hash=file_hash,
            author_id=author_id,
            author_username=author_username,
            rating=rating,
            comment=comment.strip(),
        )
        return await self._review_repo.upsert_review(entry)

    async def get_reviews(
        self,
        file_hash: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[FileReputationSummary, list[ReviewEntry], int]:
        """Return denormalized summary, paginated reviews, and total review count (Cas_15)."""
        if page < 1:
            raise ValidationError("page must be >= 1")
        if not (1 <= page_size <= 100):
            raise ValidationError("page_size must be between 1 and 100")

        summary = await self._review_repo.get_reputation_summary(file_hash)
        if summary is None:
            summary = FileReputationSummary(file_hash=file_hash)

        all_reviews = await self._review_repo.list_reviews_for_file(file_hash)
        total = len(all_reviews)
        start = (page - 1) * page_size
        end = start + page_size
        return summary, all_reviews[start:end], total
