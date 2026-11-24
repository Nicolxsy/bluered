"""GoF Decorator wrapping IFileIndexRepository with O(1) Bloom-filter rejection."""

from __future__ import annotations

from pathlib import Path

from bluered_contracts.bloom import BloomFilter

from bluered_client.contracts.ports import IFileIndexRepository, IIncrementalIndexRepository
from bluered_client.domain import SharedFile


class BloomFilterRepositoryDecorator:
    """Decorator implementing IFileIndexRepository with O(1) negative search filtering."""

    def __init__(
        self,
        inner: IFileIndexRepository,
        bloom_filter: BloomFilter | None = None,
    ) -> None:
        self._inner = inner
        self._bloom = bloom_filter if bloom_filter is not None else BloomFilter()
        self.bloom_hits: int = 0
        self.bloom_rejects: int = 0
        self.sqlite_queries: int = 0

    @property
    def bloom_filter(self) -> BloomFilter:
        return self._bloom

    def reset_metrics(self) -> None:
        """Reset diagnostic counters to zero."""
        self.bloom_hits = 0
        self.bloom_rejects = 0
        self.sqlite_queries = 0

    def export_bloom_sync_payload(self) -> tuple[int, int, str]:
        """Return (size_bits, hash_functions_k, bitset_base64) for BLOOM_SYNC."""
        return (
            self._bloom.size_bits,
            self._bloom.hash_functions_k,
            self._bloom.to_base64(),
        )

    async def rebuild_from_repository(self) -> int:
        """Populate the Bloom filter from all files currently in the wrapped repository."""
        self.sqlite_queries += 1
        files = await self._inner.list_all()
        fresh = BloomFilter(
            size_bits=self._bloom.size_bits,
            hash_functions_k=self._bloom.hash_functions_k,
        )
        for sf in files:
            fresh.add_filename(sf.file_name)
        self._bloom = fresh
        return len(files)

    async def upsert(self, shared_file: SharedFile) -> None:
        await self._inner.upsert(shared_file)
        self._bloom.add_filename(shared_file.file_name)

    async def remove_by_hash(self, file_hash: str) -> bool:
        return await self._inner.remove_by_hash(file_hash)

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        self.sqlite_queries += 1
        return await self._inner.get_by_hash(file_hash)

    async def search_local(self, query: str) -> list[SharedFile]:
        """Check Bloom filter first; skip SQLite completely on negative match."""
        if not self._bloom.might_match_query(query):
            self.bloom_rejects += 1
            return []
        self.bloom_hits += 1
        self.sqlite_queries += 1
        return await self._inner.search_local(query)

    async def list_all(self) -> list[SharedFile]:
        self.sqlite_queries += 1
        return await self._inner.list_all()

    # --- Optional forwarding for IIncrementalIndexRepository (Cas_09, Cas_10) ---

    async def add_shared_directory(self, dir_path: Path) -> int:
        if isinstance(self._inner, IIncrementalIndexRepository):
            return await self._inner.add_shared_directory(dir_path)
        raise NotImplementedError("Inner repository does not support add_shared_directory")

    async def get_file_stat_by_path(self, local_path: Path) -> tuple[str, float, int] | None:
        if isinstance(self._inner, IIncrementalIndexRepository):
            return await self._inner.get_file_stat_by_path(local_path)
        return None

    async def upsert_indexed_file(
        self,
        shared_file: SharedFile,
        *,
        mtime: float = 0.0,
        directory_id: int | None = None,
    ) -> None:
        if isinstance(self._inner, IIncrementalIndexRepository):
            await self._inner.upsert_indexed_file(
                shared_file,
                mtime=mtime,
                directory_id=directory_id,
            )
        else:
            await self._inner.upsert(shared_file)
        self._bloom.add_filename(shared_file.file_name)
