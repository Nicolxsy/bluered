"""SHA-256 streaming integrity verifier implementing IIntegrityVerifier."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

_CHUNK_SIZE = 64 * 1024  # 64 KB streaming chunks


class Sha256IntegrityVerifier:
    """Computes and verifies SHA-256 digests for chunks and local files."""

    def __init__(self, chunk_size: int = _CHUNK_SIZE) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._chunk_size = chunk_size

    def compute_chunk_hash(self, data: bytes) -> str:
        """Return lowercase 64-char SHA-256 hex digest for a raw chunk."""
        return hashlib.sha256(data).hexdigest()

    def verify_chunk(self, data: bytes, expected_hash: str) -> bool:
        """Verify that data matches expected_hash (case-insensitive)."""
        return self.compute_chunk_hash(data) == expected_hash.strip().lower()

    def _compute_file_hash_sync(self, file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as handle:
            while chunk := handle.read(self._chunk_size):
                digest.update(chunk)
        return digest.hexdigest()

    async def compute_file_hash(self, file_path: Path) -> str:
        """Compute lowercase SHA-256 hex digest of a file using 64 KB streaming chunks."""
        return await asyncio.to_thread(self._compute_file_hash_sync, file_path)

    async def verify_file(self, file_path: Path, expected_hash: str) -> bool:
        """Verify that file_path exists and its SHA-256 digest matches expected_hash."""
        if not file_path.is_file():
            return False
        actual = await self.compute_file_hash(file_path)
        return actual == expected_hash.strip().lower()
