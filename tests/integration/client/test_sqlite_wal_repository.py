"""Real SQLite WAL integration tests in tmp_path (zero SQLite mocks)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import aiosqlite
from bluered_client.application import DiscoveryAndReviewService
from bluered_client.domain import (
    FileReview,
    PrivateMessage,
    SearchMatch,
    SharedFile,
)
from bluered_client.infrastructure import (
    BloomFilterRepositoryDecorator,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
)


class _DummyServerCoordinator:
    def __init__(self) -> None:
        self.bloom_syncs: list[tuple[str, int, int, str]] = []

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
        self.bloom_syncs.append((user_id, size_bits, hash_functions_k, bitset_base64))

    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]:
        return []

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None:
        pass

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]:
        return []


class _SpyVerifier(Sha256IntegrityVerifier):
    def __init__(self) -> None:
        super().__init__()
        self.computed_paths: list[Path] = []

    async def compute_file_hash(self, file_path: Path) -> str:
        self.computed_paths.append(file_path)
        return await super().compute_file_hash(file_path)


async def test_sqlite_wal_mode_and_foreign_keys_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "bluered_client.db"
    async with SqliteFileRepository(db_path) as repo:
        assert repo._conn is not None
        async with repo._conn.execute("PRAGMA journal_mode;") as cur:
            row = await cur.fetchone()
            assert row is not None
            assert str(row[0]).lower() == "wal"
        async with repo._conn.execute("PRAGMA foreign_keys;") as cur:
            row = await cur.fetchone()
            assert row is not None
            assert int(row[0]) == 1

    # Verify all 4 tables exist
    async with (
        aiosqlite.connect(db_path) as raw_conn,
        raw_conn.execute("SELECT name FROM sqlite_master WHERE type='table';") as cur,
    ):
        tables = {str(r[0]) for r in await cur.fetchall()}
    assert {
        "shared_directories",
        "indexed_files",
        "download_checkpoints",
        "pending_private_messages",
    }.issubset(tables)


async def test_cas09_cas10_incremental_indexing_and_bloom_o1_rejection(tmp_path: Path) -> None:
    db_path = tmp_path / "index.db"
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    file_a = shared_dir / "distributed_systems_notes.pdf"
    file_b = shared_dir / "jazz_improvisation.flac"
    file_a.write_bytes(b"A" * (128 * 1024))  # 128 KB (> 64 KB chunk size)
    file_b.write_bytes(b"B" * 4096)

    verifier = _SpyVerifier()
    server = _DummyServerCoordinator()

    async with SqliteFileRepository(db_path) as sqlite_repo:
        decorated = BloomFilterRepositoryDecorator(sqlite_repo)
        service = DiscoveryAndReviewService(server, decorated, verifier)

        # Cas_09: Initial index
        count_1 = await service.configure_and_index([shared_dir], user_id="peer-1")
        assert count_1 == 2
        assert len(verifier.computed_paths) == 2
        assert len(server.bloom_syncs) == 1

        # Cas_10: Re-index unchanged files -> 0 hashes recomputed
        verifier.computed_paths.clear()
        count_2 = await service.configure_and_index([shared_dir], user_id="peer-1")
        assert count_2 == 0
        assert verifier.computed_paths == []

        # Modify file_b -> only file_b is re-hashed
        file_b.write_bytes(b"B_MODIFIED_" * 500)
        count_3 = await service.configure_and_index([shared_dir], user_id="peer-1")
        assert count_3 == 1
        assert verifier.computed_paths == [file_b]

        # Bloom O(1) negative rejection: sqlite_queries stays 0!
        decorated.reset_metrics()
        neg_results = await decorated.search_local("quantum_physics_video")
        assert neg_results == []
        assert decorated.bloom_rejects == 1
        assert decorated.sqlite_queries == 0

        # Positive search hits real SQLite
        pos_results = await decorated.search_local("distributed systems")
        assert len(pos_results) == 1
        assert pos_results[0].file_name == "distributed_systems_notes.pdf"
        assert pos_results[0].file_hash == hashlib.sha256(b"A" * (128 * 1024)).hexdigest()
        assert decorated.bloom_hits == 1
        assert decorated.sqlite_queries == 1


async def test_cas22_download_checkpoints_and_cas23_pending_messages(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    async with SqliteFileRepository(db_path) as repo:
        # Cas_22: Download checkpoints
        await repo.save_download_checkpoint(
            task_id="task-1",
            file_hash="a" * 64,
            file_name="movie.mkv",
            target_path=tmp_path / "movie.mkv",
            total_bytes=10_000,
            bytes_transferred=4_096,
            state="PAUSED",
            source_peer_id="peer-2",
        )
        cp = await repo.get_download_checkpoint("task-1")
        assert cp is not None
        assert cp.bytes_transferred == 4_096
        assert cp.state == "PAUSED"

        incomplete = await repo.list_incomplete_checkpoints()
        assert len(incomplete) == 1

        await repo.save_download_checkpoint(
            task_id="task-1",
            file_hash="a" * 64,
            file_name="movie.mkv",
            target_path=tmp_path / "movie.mkv",
            total_bytes=10_000,
            bytes_transferred=10_000,
            state="COMPLETED",
            source_peer_id="peer-2",
        )
        assert await repo.list_incomplete_checkpoints() == []
        assert await repo.delete_download_checkpoint("task-1") is True

        # Cas_23: Pending private messages
        msg = PrivateMessage(
            message_id="msg-1",
            sender_id="peer-1",
            recipient_id="peer-2",
            content="Hola peer-2!",
        )
        await repo.enqueue_pending_message(msg)
        pending = await repo.list_pending_messages("peer-2")
        assert len(pending) == 1
        assert pending[0].content == "Hola peer-2!"

        assert await repo.mark_message_delivered("msg-1") is True
        assert await repo.list_pending_messages("peer-2") == []


async def test_shared_directory_cascade_delete(tmp_path: Path) -> None:
    db_path = tmp_path / "cascade.db"
    shared_dir = tmp_path / "music"
    shared_dir.mkdir()
    sf = SharedFile(file_hash="e" * 64, file_name="song.mp3", size_bytes=2048)

    async with SqliteFileRepository(db_path) as repo:
        dir_id = await repo.add_shared_directory(shared_dir)
        await repo.upsert_indexed_file(sf, mtime=100.0, directory_id=dir_id)
        assert len(await repo.list_all()) == 1

        assert await repo.remove_shared_directory(shared_dir) is True
        assert await repo.list_all() == []
