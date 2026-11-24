"""Application service for local indexing, federated search, and file reviews."""

from __future__ import annotations

import hashlib
import mimetypes
import re
from pathlib import Path

from bluered_client.contracts import (
    IBloomSyncSource,
    IFileIndexRepository,
    IIncrementalIndexRepository,
    IIntegrityVerifier,
    IServerCoordinator,
)
from bluered_client.domain import (
    DomainValidationError,
    FileReview,
    SearchMatch,
    SharedFile,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CHUNK_SIZE = 64 * 1024


class DiscoveryAndReviewService:
    """Use-case orchestrator for Cas_09, Cas_10, Cas_11, Cas_12, Cas_14, Cas_15, Cas_24."""

    def __init__(
        self,
        server_coordinator: IServerCoordinator,
        file_index: IFileIndexRepository,
        integrity_verifier: IIntegrityVerifier | None = None,
    ) -> None:
        self._server = server_coordinator
        self._file_index = file_index
        self._integrity = integrity_verifier

    @staticmethod
    def _infer_category(mime_type: str) -> str:
        if mime_type.startswith("audio/"):
            return "audio"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("text/") or mime_type == "application/pdf":
            return "document"
        return "other"

    async def _hash_file(self, path: Path) -> str:
        if self._integrity is not None:
            return await self._integrity.compute_file_hash(path)
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(_CHUNK_SIZE):
                digest.update(chunk)
        return digest.hexdigest()

    async def configure_and_index(
        self,
        directories: list[Path],
        user_id: str | None = None,
    ) -> int:
        """Index shared directories (Cas_09) skipping unchanged files (Cas_10) & sync Bloom."""
        indexed_count = 0
        incremental = isinstance(self._file_index, IIncrementalIndexRepository)

        for directory in directories:
            if not directory.is_dir():
                continue
            dir_id: int | None = None
            if incremental:
                assert isinstance(self._file_index, IIncrementalIndexRepository)
                dir_id = await self._file_index.add_shared_directory(directory)

            for file_path in sorted(directory.rglob("*")):
                if not file_path.is_file():
                    continue
                stat = file_path.stat()
                mtime = float(stat.st_mtime)
                size_bytes = int(stat.st_size)

                if incremental:
                    assert isinstance(self._file_index, IIncrementalIndexRepository)
                    cached = await self._file_index.get_file_stat_by_path(file_path)
                    if cached is not None and cached[1] == mtime and cached[2] == size_bytes:
                        # Unchanged file: skip re-hashing (Cas_10)
                        continue

                file_hash = await self._hash_file(file_path)
                guessed_mime, _ = mimetypes.guess_type(file_path.name)
                mime_type = guessed_mime or "application/octet-stream"
                shared_file = SharedFile(
                    file_hash=file_hash,
                    file_name=file_path.name,
                    size_bytes=size_bytes,
                    local_path=file_path,
                    mime_type=mime_type,
                    category=self._infer_category(mime_type),
                )
                if incremental:
                    assert isinstance(self._file_index, IIncrementalIndexRepository)
                    await self._file_index.upsert_indexed_file(
                        shared_file,
                        mtime=mtime,
                        directory_id=dir_id,
                    )
                else:
                    await self._file_index.upsert(shared_file)
                indexed_count += 1

        if user_id is not None and isinstance(self._file_index, IBloomSyncSource):
            size_bits, hash_k, bitset_b64 = self._file_index.export_bloom_sync_payload()
            await self._server.sync_bloom_filter(user_id, size_bits, hash_k, bitset_b64)

        return indexed_count

    async def execute_global_search(
        self,
        query: str,
        max_results: int = 50,
    ) -> list[SearchMatch]:
        """Validate query and dispatch federated search via IServerCoordinator (Cas_11)."""
        cleaned = query.strip()
        if not cleaned:
            raise DomainValidationError("Search query must be non-empty")
        return await self._server.search(cleaned, max_results=max_results)

    async def handle_incoming_search_broadcast(self, query: str) -> list[SharedFile]:
        """Query local index (Bloom-decorated) for incoming SEARCH_BROADCAST (Cas_12)."""
        if not query.strip():
            return []
        return await self._file_index.search_local(query)

    async def submit_file_review(
        self,
        file_hash: str,
        rating: int,
        comment: str,
    ) -> None:
        """Validate review via Domain Core and publish to central server (Cas_14, Cas_24)."""
        # Enforce domain invariants (64-char sha256, rating 1..5, non-empty comment)
        validated = FileReview(
            review_id="pending",
            file_hash=file_hash,
            author_id="local",
            rating=rating,
            comment=comment,
        )
        await self._server.publish_review(
            file_hash=validated.file_hash,
            rating=validated.rating,
            comment=validated.comment.strip(),
        )

    async def get_file_reputation(
        self,
        file_hash: str,
    ) -> tuple[list[FileReview], float | None, int]:
        """Fetch file reviews and compute (reviews, avg_rating, review_count) (Cas_15)."""
        if not _SHA256_RE.fullmatch(file_hash):
            raise DomainValidationError(
                "file_hash must be a 64-char lowercase SHA-256 hex string"
            )
        reviews = await self._server.fetch_reviews(file_hash)
        count = len(reviews)
        avg = (sum(r.rating for r in reviews) / count) if count > 0 else None
        return (reviews, avg, count)
