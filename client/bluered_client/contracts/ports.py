"""Abstract port contracts for BlueRed client (Figure 53)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Protocol, runtime_checkable

from bluered_client.domain import (
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
