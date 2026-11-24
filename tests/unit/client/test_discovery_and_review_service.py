"""Unit tests for DiscoveryAndReviewService with in-memory fakes."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from bluered_client.application import DiscoveryAndReviewService
from bluered_client.domain import (
    DomainValidationError,
    FileReview,
    PresenceStatus,
    RemotePeer,
    SearchMatch,
    SharedFile,
)


class FakeServerCoordinator:
    def __init__(self) -> None:
        self.synced_blooms: list[tuple[str, int, int, str]] = []
        self.search_queries: list[tuple[str, int]] = []
        self.published_reviews: list[tuple[str, int, str]] = []
        self.reviews_by_hash: dict[str, list[FileReview]] = {}
        self.search_results: list[SearchMatch] = []

    async def connect(self, host: str, port: int) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def register(self, username: str, password: str) -> str:
        return "u1"

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]:
        return ("u1", "tok")

    async def send_heartbeat(self, user_id: str, tcp_port: int) -> None:
        pass

    async def sync_bloom_filter(
        self,
        user_id: str,
        size_bits: int,
        hash_functions_k: int,
        bitset_base64: str,
    ) -> None:
        self.synced_blooms.append((user_id, size_bits, hash_functions_k, bitset_base64))

    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]:
        self.search_queries.append((query, max_results))
        return list(self.search_results)

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None:
        self.published_reviews.append((file_hash, rating, comment))

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        return list(self.reviews_by_hash.get(file_hash, []))


class FakeIncrementalRepo:
    def __init__(self) -> None:
        self.dirs: dict[Path, int] = {}
        self.files: dict[str, SharedFile] = {}
        self.stats_by_path: dict[Path, tuple[str, float, int]] = {}

    async def add_shared_directory(self, dir_path: Path) -> int:
        if dir_path not in self.dirs:
            self.dirs[dir_path] = len(self.dirs) + 1
        return self.dirs[dir_path]

    async def get_file_stat_by_path(self, local_path: Path) -> tuple[str, float, int] | None:
        return self.stats_by_path.get(local_path)

    async def upsert_indexed_file(
        self,
        shared_file: SharedFile,
        *,
        mtime: float = 0.0,
        directory_id: int | None = None,
    ) -> None:
        self.files[shared_file.file_hash] = shared_file
        if shared_file.local_path is not None:
            self.stats_by_path[shared_file.local_path] = (
                shared_file.file_hash,
                mtime,
                shared_file.size_bytes,
            )

    def export_bloom_sync_payload(self) -> tuple[int, int, str]:
        return (8192, 5, "AAAA")

    async def upsert(self, shared_file: SharedFile) -> None:
        await self.upsert_indexed_file(shared_file)

    async def remove_by_hash(self, file_hash: str) -> bool:
        return self.files.pop(file_hash, None) is not None

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        return self.files.get(file_hash)

    async def search_local(self, query: str) -> list[SharedFile]:
        q = query.lower()
        return [sf for sf in self.files.values() if q in sf.file_name.lower()]

    async def list_all(self) -> list[SharedFile]:
        return list(self.files.values())


class CountingIntegrityVerifier:
    def __init__(self) -> None:
        self.hash_calls: int = 0

    def compute_chunk_hash(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def verify_chunk(self, data: bytes, expected_hash: str) -> bool:
        return self.compute_chunk_hash(data) == expected_hash

    async def compute_file_hash(self, file_path: Path) -> str:
        self.hash_calls += 1
        return hashlib.sha256(file_path.read_bytes()).hexdigest()

    async def verify_file(self, file_path: Path, expected_hash: str) -> bool:
        return await self.compute_file_hash(file_path) == expected_hash


async def test_configure_and_index_skips_unchanged_and_syncs_bloom(tmp_path: Path) -> None:
    f1 = tmp_path / "track.mp3"
    f1.write_bytes(b"audio-bytes")
    server = FakeServerCoordinator()
    repo = FakeIncrementalRepo()
    verifier = CountingIntegrityVerifier()
    service = DiscoveryAndReviewService(server, repo, verifier)

    first_count = await service.configure_and_index([tmp_path], user_id="user-1")
    second_count = await service.configure_and_index([tmp_path], user_id="user-1")

    assert first_count == 1
    assert second_count == 0
    assert verifier.hash_calls == 1
    assert len(server.synced_blooms) == 2


async def test_execute_global_search_and_incoming_broadcast() -> None:
    server = FakeServerCoordinator()
    repo = FakeIncrementalRepo()
    sf = SharedFile(file_hash="a" * 64, file_name="python_guide.pdf", size_bytes=500)
    await repo.upsert(sf)
    peer = RemotePeer("p2", "bob", "127.0.0.1", 9001, status=PresenceStatus.ONLINE)
    server.search_results = [SearchMatch(file=sf, peer=peer, avg_rating=4.5, review_count=2)]

    service = DiscoveryAndReviewService(server, repo)

    matches = await service.execute_global_search("  python  ", max_results=20)
    assert len(matches) == 1
    assert server.search_queries == [("python", 20)]

    local_hits = await service.handle_incoming_search_broadcast("python")
    assert local_hits == [sf]

    with pytest.raises(DomainValidationError):
        await service.execute_global_search("   ")


async def test_submit_and_get_file_reputation() -> None:
    server = FakeServerCoordinator()
    repo = FakeIncrementalRepo()
    service = DiscoveryAndReviewService(server, repo)
    fhash = "b" * 64

    await service.submit_file_review(fhash, 5, "  Excellent quality  ")
    assert server.published_reviews == [(fhash, 5, "Excellent quality")]

    with pytest.raises(DomainValidationError):
        await service.submit_file_review(fhash, 6, "Invalid rating")
    with pytest.raises(DomainValidationError):
        await service.submit_file_review(fhash, 4, "   ")

    server.reviews_by_hash[fhash] = [
        FileReview("r1", fhash, "u1", 5, "Great"),
        FileReview("r2", fhash, "u2", 3, "Okay"),
    ]
    reviews, avg, count = await service.get_file_reputation(fhash)
    assert count == 2
    assert avg == 4.0
    assert len(reviews) == 2

    empty_reviews, empty_avg, empty_count = await service.get_file_reputation("c" * 64)
    assert empty_reviews == []
    assert empty_avg is None
    assert empty_count == 0
