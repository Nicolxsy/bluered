"""Unit tests for BloomFilterRepositoryDecorator using an in-memory fake repository."""

from __future__ import annotations

from bluered_client.contracts import IFileIndexRepository
from bluered_client.domain import SharedFile
from bluered_client.infrastructure import BloomFilterRepositoryDecorator
from bluered_contracts.bloom import BloomFilter, tokenize_filename


class InMemoryFileIndexFake:
    """In-memory fake implementing IFileIndexRepository."""

    def __init__(self) -> None:
        self.files: dict[str, SharedFile] = {}

    async def upsert(self, shared_file: SharedFile) -> None:
        self.files[shared_file.file_hash] = shared_file

    async def remove_by_hash(self, file_hash: str) -> bool:
        return self.files.pop(file_hash, None) is not None

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        return self.files.get(file_hash)

    async def search_local(self, query: str) -> list[SharedFile]:
        q_tokens = tokenize_filename(query)
        if not q_tokens:
            return []
        return [
            sf
            for sf in self.files.values()
            if all(tok in set(tokenize_filename(sf.file_name)) for tok in q_tokens)
        ]

    async def list_all(self) -> list[SharedFile]:
        return list(self.files.values())


def _sample_file(name: str, hex_char: str = "a") -> SharedFile:
    return SharedFile(
        file_hash=hex_char * 64,
        file_name=name,
        size_bytes=1024,
    )


async def test_bloom_decorator_implements_protocol() -> None:
    decorator = BloomFilterRepositoryDecorator(InMemoryFileIndexFake())
    assert isinstance(decorator, IFileIndexRepository)


async def test_bloom_decorator_rejects_negative_search_in_o1_without_sqlite() -> None:
    inner = InMemoryFileIndexFake()
    decorator = BloomFilterRepositoryDecorator(inner)
    await decorator.upsert(_sample_file("beethoven_symphony_9.flac", "a"))

    results = await decorator.search_local("nonexistent_jazz_track")

    assert results == []
    assert decorator.bloom_rejects == 1
    assert decorator.bloom_hits == 0
    assert decorator.sqlite_queries == 0


async def test_bloom_decorator_hits_inner_on_positive_search() -> None:
    inner = InMemoryFileIndexFake()
    decorator = BloomFilterRepositoryDecorator(inner)
    sf = _sample_file("beethoven_symphony_9.flac", "b")
    await decorator.upsert(sf)

    results = await decorator.search_local("symphony beethoven")

    assert results == [sf]
    assert decorator.bloom_hits == 1
    assert decorator.bloom_rejects == 0
    assert decorator.sqlite_queries == 1


async def test_bloom_decorator_rebuild_and_export_payload() -> None:
    inner = InMemoryFileIndexFake()
    sf = _sample_file("clean_architecture_notes.pdf", "c")
    await inner.upsert(sf)

    decorator = BloomFilterRepositoryDecorator(
        inner, BloomFilter(size_bits=1024, hash_functions_k=4)
    )
    rebuilt_count = await decorator.rebuild_from_repository()

    assert rebuilt_count == 1
    assert decorator.sqlite_queries == 1
    size_bits, k, b64 = decorator.export_bloom_sync_payload()
    assert size_bits == 1024
    assert k == 4
    assert len(b64) > 0

    decorator.reset_metrics()
    assert decorator.bloom_hits == 0
    assert decorator.bloom_rejects == 0
    assert decorator.sqlite_queries == 0

    found = await decorator.search_local("architecture")
    assert found == [sf]
    assert decorator.bloom_hits == 1
