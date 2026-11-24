"""SQLite WAL-mode local persistence for index, checkpoints, and private messages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

import aiosqlite
from bluered_contracts.bloom import tokenize_filename

from bluered_client.domain import PrivateMessage, SharedFile

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS shared_directories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_path TEXT NOT NULL UNIQUE,
    added_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS indexed_files (
    file_hash TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    bitrate_kbps INTEGER,
    local_path TEXT UNIQUE,
    mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    category TEXT NOT NULL DEFAULT 'other',
    mtime REAL NOT NULL DEFAULT 0.0,
    directory_id INTEGER REFERENCES shared_directories(id) ON DELETE CASCADE,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_indexed_files_name ON indexed_files(file_name);
CREATE INDEX IF NOT EXISTS idx_indexed_files_local_path ON indexed_files(local_path);

CREATE TABLE IF NOT EXISTS download_checkpoints (
    task_id TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    file_name TEXT NOT NULL,
    target_path TEXT NOT NULL,
    total_bytes INTEGER NOT NULL,
    bytes_transferred INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    source_peer_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_private_messages (
    message_id TEXT PRIMARY KEY,
    sender_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    content TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
"""


@dataclass(frozen=True, slots=True)
class DownloadCheckpoint:
    """Persisted download checkpoint record for Cas_22 resume."""

    task_id: str
    file_hash: str
    file_name: str
    target_path: Path
    total_bytes: int
    bytes_transferred: int
    state: str
    source_peer_id: str
    updated_at: datetime


class SqliteFileRepository:
    """Async SQLite repository (WAL mode) implementing IFileIndexRepository."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> Self:
        await self.initialize()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def initialize(self) -> None:
        """Open connection, configure WAL + foreign keys, and create schema if needed."""
        if self._conn is not None:
            return
        if str(self._db_path) != ":memory:":
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self._db_path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA foreign_keys=ON;")
        await conn.execute("PRAGMA synchronous=NORMAL;")
        await conn.executescript(_SCHEMA_SQL)
        await conn.commit()
        self._conn = conn

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.initialize()
        assert self._conn is not None
        return self._conn

    @staticmethod
    def _row_to_shared_file(row: aiosqlite.Row) -> SharedFile:
        local_path_raw = row["local_path"]
        return SharedFile(
            file_hash=str(row["file_hash"]),
            file_name=str(row["file_name"]),
            size_bytes=int(row["size_bytes"]),
            bitrate_kbps=int(row["bitrate_kbps"]) if row["bitrate_kbps"] is not None else None,
            local_path=Path(str(local_path_raw)) if local_path_raw is not None else None,
            mime_type=str(row["mime_type"]),
            category=str(row["category"]),
        )

    # --- Shared Directories (Cas_09, Cas_10) ---

    async def add_shared_directory(self, dir_path: Path) -> int:
        """Register a shared directory and return its row id."""
        conn = await self._get_conn()
        resolved = str(dir_path.resolve())
        now_iso = datetime.now(UTC).isoformat()
        await conn.execute(
            "INSERT OR IGNORE INTO shared_directories (dir_path, added_at) VALUES (?, ?);",
            (resolved, now_iso),
        )
        await conn.commit()
        async with conn.execute(
            "SELECT id FROM shared_directories WHERE dir_path = ?;",
            (resolved,),
        ) as cursor:
            row = await cursor.fetchone()
        assert row is not None
        return int(row["id"])

    async def list_shared_directories(self) -> list[Path]:
        """Return all registered shared directory paths."""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT dir_path FROM shared_directories ORDER BY id ASC;"
        ) as cursor:
            rows = await cursor.fetchall()
        return [Path(str(row["dir_path"])) for row in rows]

    async def remove_shared_directory(self, dir_path: Path) -> bool:
        """Remove a shared directory (cascading to its indexed files)."""
        conn = await self._get_conn()
        resolved = str(dir_path.resolve())
        cursor = await conn.execute(
            "DELETE FROM shared_directories WHERE dir_path = ?;",
            (resolved,),
        )
        await conn.commit()
        return cursor.rowcount > 0

    # --- File Index (IFileIndexRepository + Cas_10 incremental) ---

    async def get_file_stat_by_path(self, local_path: Path) -> tuple[str, float, int] | None:
        """Return (file_hash, mtime, size_bytes) for an already-indexed path."""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT file_hash, mtime, size_bytes FROM indexed_files WHERE local_path = ?;",
            (str(local_path),),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return (str(row["file_hash"]), float(row["mtime"]), int(row["size_bytes"]))

    async def upsert_indexed_file(
        self,
        shared_file: SharedFile,
        *,
        mtime: float = 0.0,
        directory_id: int | None = None,
    ) -> None:
        """Insert or update an indexed file with explicit mtime and directory_id."""
        conn = await self._get_conn()
        now_iso = datetime.now(UTC).isoformat()
        local_path_str = str(shared_file.local_path) if shared_file.local_path is not None else None
        if local_path_str is not None:
            await conn.execute(
                "DELETE FROM indexed_files WHERE local_path = ? AND file_hash != ?;",
                (local_path_str, shared_file.file_hash),
            )
        await conn.execute(
            """
            INSERT INTO indexed_files (
                file_hash, file_name, size_bytes, bitrate_kbps,
                local_path, mime_type, category, mtime, directory_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_hash) DO UPDATE SET
                file_name = excluded.file_name,
                size_bytes = excluded.size_bytes,
                bitrate_kbps = excluded.bitrate_kbps,
                local_path = excluded.local_path,
                mime_type = excluded.mime_type,
                category = excluded.category,
                mtime = excluded.mtime,
                directory_id = COALESCE(excluded.directory_id, indexed_files.directory_id),
                updated_at = excluded.updated_at;
            """,
            (
                shared_file.file_hash,
                shared_file.file_name,
                shared_file.size_bytes,
                shared_file.bitrate_kbps,
                local_path_str,
                shared_file.mime_type,
                shared_file.category,
                mtime,
                directory_id,
                now_iso,
            ),
        )
        await conn.commit()

    async def upsert(self, shared_file: SharedFile) -> None:
        """Insert or update a SharedFile (deriving mtime from local_path if present)."""
        mtime = 0.0
        if shared_file.local_path is not None and shared_file.local_path.is_file():
            mtime = shared_file.local_path.stat().st_mtime
        await self.upsert_indexed_file(shared_file, mtime=mtime)

    async def remove_by_hash(self, file_hash: str) -> bool:
        """Delete an indexed file by its SHA-256 hash."""
        conn = await self._get_conn()
        cursor = await conn.execute(
            "DELETE FROM indexed_files WHERE file_hash = ?;",
            (file_hash,),
        )
        await conn.commit()
        return cursor.rowcount > 0

    async def get_by_hash(self, file_hash: str) -> SharedFile | None:
        """Retrieve a SharedFile by its SHA-256 hash."""
        conn = await self._get_conn()
        async with conn.execute(
            """
            SELECT file_hash, file_name, size_bytes, bitrate_kbps,
                   local_path, mime_type, category
            FROM indexed_files
            WHERE file_hash = ?;
            """,
            (file_hash,),
        ) as cursor:
            row = await cursor.fetchone()
        return self._row_to_shared_file(row) if row is not None else None

    async def search_local(self, query: str) -> list[SharedFile]:
        """Search indexed files matching all normalized tokens in query."""
        tokens = tokenize_filename(query)
        if not tokens:
            return []
        conn = await self._get_conn()
        async with conn.execute(
            """
            SELECT file_hash, file_name, size_bytes, bitrate_kbps,
                   local_path, mime_type, category
            FROM indexed_files
            ORDER BY file_name ASC;
            """
        ) as cursor:
            rows = await cursor.fetchall()

        matches: list[SharedFile] = []
        for row in rows:
            file_tokens = set(tokenize_filename(str(row["file_name"])))
            if all(token in file_tokens for token in tokens):
                matches.append(self._row_to_shared_file(row))
        return matches

    async def list_all(self) -> list[SharedFile]:
        """Return all indexed files ordered by file_name."""
        conn = await self._get_conn()
        async with conn.execute(
            """
            SELECT file_hash, file_name, size_bytes, bitrate_kbps,
                   local_path, mime_type, category
            FROM indexed_files
            ORDER BY file_name ASC;
            """
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._row_to_shared_file(row) for row in rows]

    # --- Download Checkpoints (Cas_22) ---

    async def save_download_checkpoint(
        self,
        task_id: str,
        file_hash: str,
        file_name: str,
        target_path: Path,
        total_bytes: int,
        bytes_transferred: int,
        state: str,
        source_peer_id: str,
    ) -> None:
        """Persist or update a download checkpoint for crash/disconnect recovery."""
        conn = await self._get_conn()
        now_iso = datetime.now(UTC).isoformat()
        await conn.execute(
            """
            INSERT INTO download_checkpoints (
                task_id, file_hash, file_name, target_path,
                total_bytes, bytes_transferred, state, source_peer_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                bytes_transferred = excluded.bytes_transferred,
                state = excluded.state,
                updated_at = excluded.updated_at;
            """,
            (
                task_id,
                file_hash,
                file_name,
                str(target_path),
                total_bytes,
                bytes_transferred,
                state,
                source_peer_id,
                now_iso,
            ),
        )
        await conn.commit()

    async def get_download_checkpoint(self, task_id: str) -> DownloadCheckpoint | None:
        """Fetch a download checkpoint by task_id."""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT * FROM download_checkpoints WHERE task_id = ?;",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return DownloadCheckpoint(
            task_id=str(row["task_id"]),
            file_hash=str(row["file_hash"]),
            file_name=str(row["file_name"]),
            target_path=Path(str(row["target_path"])),
            total_bytes=int(row["total_bytes"]),
            bytes_transferred=int(row["bytes_transferred"]),
            state=str(row["state"]),
            source_peer_id=str(row["source_peer_id"]),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
        )

    async def list_incomplete_checkpoints(self) -> list[DownloadCheckpoint]:
        """List all checkpoints that are not COMPLETED or CANCELLED."""
        conn = await self._get_conn()
        async with conn.execute(
            """
            SELECT * FROM download_checkpoints
            WHERE state NOT IN ('COMPLETED', 'CANCELLED')
            ORDER BY updated_at DESC;
            """
        ) as cursor:
            rows = await cursor.fetchall()
        return [
            DownloadCheckpoint(
                task_id=str(row["task_id"]),
                file_hash=str(row["file_hash"]),
                file_name=str(row["file_name"]),
                target_path=Path(str(row["target_path"])),
                total_bytes=int(row["total_bytes"]),
                bytes_transferred=int(row["bytes_transferred"]),
                state=str(row["state"]),
                source_peer_id=str(row["source_peer_id"]),
                updated_at=datetime.fromisoformat(str(row["updated_at"])),
            )
            for row in rows
        ]

    async def delete_download_checkpoint(self, task_id: str) -> bool:
        """Delete a download checkpoint record."""
        conn = await self._get_conn()
        cursor = await conn.execute(
            "DELETE FROM download_checkpoints WHERE task_id = ?;",
            (task_id,),
        )
        await conn.commit()
        return cursor.rowcount > 0

    # --- Pending Private Messages (Cas_23) ---

    async def enqueue_pending_message(self, message: PrivateMessage) -> None:
        """Store an undelivered private message in SQLite."""
        conn = await self._get_conn()
        await conn.execute(
            """
            INSERT OR REPLACE INTO pending_private_messages (
                message_id, sender_id, recipient_id, content, delivered, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                message.message_id,
                message.sender_id,
                message.recipient_id,
                message.content,
                1 if message.delivered else 0,
                message.created_at.isoformat(),
            ),
        )
        await conn.commit()

    async def list_pending_messages(
        self, recipient_id: str | None = None
    ) -> list[PrivateMessage]:
        """List undelivered private messages, optionally filtered by recipient_id."""
        conn = await self._get_conn()
        if recipient_id is None:
            query = (
                "SELECT * FROM pending_private_messages "
                "WHERE delivered = 0 ORDER BY created_at ASC;"
            )
            params: tuple[str, ...] = ()
        else:
            query = (
                "SELECT * FROM pending_private_messages "
                "WHERE delivered = 0 AND recipient_id = ? ORDER BY created_at ASC;"
            )
            params = (recipient_id,)
        async with conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [
            PrivateMessage(
                message_id=str(row["message_id"]),
                sender_id=str(row["sender_id"]),
                recipient_id=str(row["recipient_id"]),
                content=str(row["content"]),
                delivered=bool(row["delivered"]),
                created_at=datetime.fromisoformat(str(row["created_at"])),
            )
            for row in rows
        ]

    async def mark_message_delivered(self, message_id: str) -> bool:
        """Mark a pending private message as delivered."""
        conn = await self._get_conn()
        cursor = await conn.execute(
            "UPDATE pending_private_messages SET delivered = 1 WHERE message_id = ?;",
            (message_id,),
        )
        await conn.commit()
        return cursor.rowcount > 0
