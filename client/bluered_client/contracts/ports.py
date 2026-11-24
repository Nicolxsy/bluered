"""Abstract port contracts for BlueRed client (Figure 53)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from bluered_client.domain import (
    DownloadCheckpoint,
    FileReview,
    LocalPeer,
    PrivateMessage,
    RemotePeer,
    SearchMatch,
    SharedFile,
    TransferState,
    TransferTask,
)


@runtime_checkable
class IServerCoordinator(Protocol):
    """Control-plane gateway between the client peer and the central server."""

    async def connect(self, host: str, port: int) -> None: ...

    async def disconnect(self) -> None: ...

    async def register(self, username: str, password: str) -> str: ...

    async def login(self, username: str, password: str, tcp_port: int) -> tuple[str, str]: ...

    async def send_heartbeat(self, user_id: str, tcp_port: int) -> None: ...

    async def sync_bloom_filter(
        self,
        user_id: str,
        size_bits: int,
        hash_functions_k: int,
        bitset_base64: str,
    ) -> None: ...

    async def search(self, query: str, max_results: int = 50) -> list[SearchMatch]: ...

    async def publish_review(self, file_hash: str, rating: int, comment: str) -> None: ...

    async def fetch_reviews(self, file_hash: str) -> list[FileReview]: ...


@runtime_checkable
class IFileIndexRepository(Protocol):
    """Local shared-file index repository (implemented by SQLite + Bloom decorator)."""

    async def upsert(self, shared_file: SharedFile) -> None: ...

    async def remove_by_hash(self, file_hash: str) -> bool: ...

    async def get_by_hash(self, file_hash: str) -> SharedFile | None: ...

    async def search_local(self, query: str) -> list[SharedFile]: ...

    async def list_all(self) -> list[SharedFile]: ...


@runtime_checkable
class IPeerTransport(Protocol):
    """Data-plane P2P TCP framing transport for downloads, browsing, and private chat."""

    def request_file_chunks(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
    ) -> AsyncIterator[bytes]: ...

    async def browse_peer_files(self, peer: RemotePeer) -> list[SharedFile]: ...

    async def send_private_message(self, peer: RemotePeer, message: PrivateMessage) -> bool: ...

    async def close(self) -> None: ...


@runtime_checkable
class INatTraversalStrategy(Protocol):
    """OCP strategy for establishing TCP streams across NAT boundaries (Cas_20)."""

    @property
    def strategy_name(self) -> str: ...

    async def establish_connection(
        self,
        local_peer: LocalPeer,
        remote_peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...


@runtime_checkable
class ITransferQueuePolicy(Protocol):
    """Upload/transfer scheduling policy (e.g. FIFO, FairShareQueuePolicy)."""

    def enqueue(self, task: TransferTask) -> int: ...

    def dequeue_next(self) -> TransferTask | None: ...

    def remove(self, task_id: str) -> bool: ...

    def position_of(self, task_id: str) -> int | None: ...

    def pending_count(self) -> int: ...


@runtime_checkable
class ITransferEventListener(Protocol):
    """Observer notified by TransferOrchestrator on state, progress, and error events."""

    async def on_state_changed(
        self,
        task: TransferTask,
        previous_state: TransferState,
    ) -> None: ...

    async def on_progress(self, task: TransferTask) -> None: ...

    async def on_error(self, task: TransferTask, error_message: str) -> None: ...


@runtime_checkable
class IIntegrityVerifier(Protocol):
    """Chunk and full-file SHA-256 integrity verification port."""

    def compute_chunk_hash(self, data: bytes) -> str: ...

    def verify_chunk(self, data: bytes, expected_hash: str) -> bool: ...

    async def compute_file_hash(self, file_path: Path) -> str: ...

    async def verify_file(self, file_path: Path, expected_hash: str) -> bool: ...


@runtime_checkable
class IIncrementalIndexRepository(Protocol):
    """Optional extension for directory registration and mtime/size_bytes skip (Cas_09/Cas_10)."""

    async def add_shared_directory(self, dir_path: Path) -> int: ...

    async def get_file_stat_by_path(self, local_path: Path) -> tuple[str, float, int] | None: ...

    async def upsert_indexed_file(
        self,
        shared_file: SharedFile,
        *,
        mtime: float = 0.0,
        directory_id: int | None = None,
    ) -> None: ...


@runtime_checkable
class IBloomSyncSource(Protocol):
    """Optional extension for exporting Bloom filter state for server sync."""

    def export_bloom_sync_payload(self) -> tuple[int, int, str]: ...


@runtime_checkable
class IDownloadCheckpointStore(Protocol):
    """Segregated persistence port for TransferTask checkpoints (Cas_22)."""

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
    ) -> None: ...

    async def get_download_checkpoint(self, task_id: str) -> DownloadCheckpoint | None: ...

    async def list_incomplete_checkpoints(self) -> list[DownloadCheckpoint]: ...

    async def delete_download_checkpoint(self, task_id: str) -> bool: ...


@runtime_checkable
class IPendingMessageStore(Protocol):
    """Segregated persistence port for offline private chat messages (Cas_23)."""

    async def enqueue_pending_message(self, message: PrivateMessage) -> None: ...

    async def list_pending_messages(
        self, recipient_id: str | None = None
    ) -> list[PrivateMessage]: ...

    async def mark_message_delivered(self, message_id: str) -> bool: ...


@runtime_checkable
class INatSignalingCoordinator(Protocol):
    """Optional NAT signaling extension on IServerCoordinator (Cas_20)."""

    async def request_nat_punch(
        self, target_peer_id: str, requester_ip: str, requester_port: int
    ) -> None: ...

    async def request_nat_relay(self, target_peer_id: str, session_id: str) -> None: ...

    def set_nat_signal_handler(
        self, handler: Callable[[Any], Awaitable[None]]
    ) -> None: ...


@runtime_checkable
class IStreamingPeerTransport(IPeerTransport, Protocol):
    """Optional streaming extension of IPeerTransport exposing queue/slot/hash metadata."""

    def stream_file_download(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
        *,
        on_queue_status: Callable[[int], Awaitable[None]] | None = None,
        on_slot_available: Callable[[], Awaitable[None]] | None = None,
    ) -> AsyncIterator[tuple[int, bytes, str]]: ...
