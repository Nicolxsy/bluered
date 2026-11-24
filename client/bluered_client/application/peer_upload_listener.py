"""Symmetric P2P upload & request handler (Cas_16, Cas_18, Cas_21, Cas_22, Cas_23)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from pathlib import Path

from bluered_client.contracts import (
    IFileIndexRepository,
    IIntegrityVerifier,
    IPeerUploadHandler,
    IPeerUploadServer,
    IPeerUploadSession,
    ITransferQueuePolicy,
)
from bluered_client.domain import (
    LocalPeer,
    PeerTransportError,
    PrivateMessage,
    QueueFullError,
    TransferTask,
)


class PeerUploadListener(IPeerUploadHandler):
    """Application use-case handler for incoming P2P browse, download, and private chat."""

    def __init__(
        self,
        local_peer: LocalPeer,
        file_index: IFileIndexRepository,
        queue_policy: ITransferQueuePolicy,
        integrity_verifier: IIntegrityVerifier,
        *,
        upload_server: IPeerUploadServer | None = None,
        chunk_size: int = 64 * 1024,
        io_timeout_s: float = 15.0,
        on_private_message: Callable[[PrivateMessage], Awaitable[None]] | None = None,
        chunk_delay_s: float = 0.0,
        bind_port: int | None = None,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._local_peer = local_peer
        self._file_index = file_index
        self._queue_policy = queue_policy
        self._integrity = integrity_verifier
        self._upload_server = upload_server
        self._chunk_size = chunk_size
        self._io_timeout_s = io_timeout_s
        self._on_private_message = on_private_message
        self._chunk_delay_s = chunk_delay_s
        self._bind_port = bind_port

        self._bound_port: int | None = None
        self._slot_lock = asyncio.Lock()
        self._active_uploads = 0
        self._waiters: dict[str, asyncio.Event] = {}
        self.received_download_offsets: list[int] = []

    @property
    def bound_port(self) -> int | None:
        if self._upload_server is not None and self._upload_server.bound_port is not None:
            return self._upload_server.bound_port
        return self._bound_port

    @property
    def active_uploads(self) -> int:
        return self._active_uploads

    async def start(self) -> int:
        """Start the injected IPeerUploadServer and return the bound port."""
        if self._upload_server is None:
            raise PeerTransportError("upload_server is required to start PeerUploadListener")
        if self._upload_server.bound_port is not None:
            self._bound_port = self._upload_server.bound_port
            return self._bound_port
        port = self._local_peer.tcp_port if self._bind_port is None else self._bind_port
        bound = await self._upload_server.start(self._local_peer.ip_address, port, self)
        self._bound_port = bound
        if port == 0:
            self._local_peer.tcp_port = bound
        return bound

    async def stop(self) -> None:
        """Gracefully stop the injected IPeerUploadServer."""
        if self._upload_server is not None:
            await self._upload_server.stop()
        self._bound_port = None

    def is_peer_blocked(self, peer_id: str) -> bool:
        return self._local_peer.is_peer_blocked(peer_id)

    async def handle_browse(self, session: IPeerUploadSession) -> None:
        if self._local_peer.is_peer_blocked(session.remote_peer_id):
            await session.send_transfer_error("403", "Peer is blocked")
            return
        files = await self._file_index.list_all()
        await session.send_browse_response(files)

    async def handle_private_chat(
        self,
        session: IPeerUploadSession,
        *,
        sender_id: str,
        recipient_id: str,
        message_id: str,
        content: str,
    ) -> None:
        if self._local_peer.is_peer_blocked(sender_id):
            await session.send_private_chat_ack("403", "Sender is blocked")
            return

        if not content.strip():
            await session.send_private_chat_ack("400", "Message content cannot be empty")
            return

        msg = PrivateMessage(
            message_id=message_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            content=content,
            delivered=True,
        )
        if self._on_private_message is not None:
            await self._on_private_message(msg)

        await session.send_private_chat_ack("200")

    async def _acquire_upload_slot(
        self,
        session: IPeerUploadSession,
        request_id: str,
        file_hash: str,
        file_name: str,
        file_size: int,
        remote_peer_id: str,
    ) -> bool:
        """Arbitrate upload slots & queue policy (Cas_18, Fig. 57). Returns True if slot held."""
        wait_event: asyncio.Event | None = None
        async with self._slot_lock:
            if (
                self._active_uploads < self._local_peer.max_upload_slots
                and self._queue_policy.pending_count() == 0
            ):
                self._active_uploads += 1
                return True

            queued_task = TransferTask(
                task_id=request_id,
                file_hash=file_hash,
                file_name=file_name,
                total_bytes=file_size,
                remote_peer_id=remote_peer_id,
            )
            try:
                pos = self._queue_policy.enqueue(queued_task)
            except QueueFullError as exc:
                await session.send_transfer_error("503", str(exc), file_hash=file_hash)
                return False

            wait_event = asyncio.Event()
            self._waiters[request_id] = wait_event

        await session.send_queue_status(pos, file_hash)

        wait_task = asyncio.create_task(wait_event.wait())
        eof_task = asyncio.create_task(session.wait_disconnected())
        try:
            done, pending = await asyncio.wait(
                {wait_task, eof_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            if wait_task in done and wait_event.is_set():
                return True
        except asyncio.CancelledError:
            for t in (wait_task, eof_task):
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t
            raise

        async with self._slot_lock:
            self._queue_policy.remove(request_id)
            self._waiters.pop(request_id, None)
        return False

    async def _release_upload_slot(self) -> None:
        """Always release slot or transfer ownership to next queued peer (Fig. 57)."""
        async with self._slot_lock:
            while True:
                next_task = self._queue_policy.dequeue_next()
                if next_task is None:
                    self._active_uploads = max(0, self._active_uploads - 1)
                    return
                waiter = self._waiters.pop(next_task.task_id, None)
                if waiter is not None:
                    # Keep _active_uploads unchanged: slot transfers directly to promoted waiter.
                    waiter.set()
                    return

    @staticmethod
    def _read_chunk_at(path: Path, offset: int, size: int) -> bytes:
        with path.open("rb") as handle:
            handle.seek(offset)
            return handle.read(size)

    async def handle_download(
        self,
        session: IPeerUploadSession,
        file_hash: str | None,
        offset: int,
    ) -> None:
        remote_peer_id = session.remote_peer_id
        if self._local_peer.is_peer_blocked(remote_peer_id):
            await session.send_transfer_error("403", "Peer is blocked")
            return
        if not file_hash:
            await session.send_transfer_error("400", "file_hash is required")
            return

        shared_file = await self._file_index.get_by_hash(file_hash)
        if (
            shared_file is None
            or shared_file.local_path is None
            or not shared_file.local_path.is_file()
        ):
            await session.send_transfer_error(
                "404", "Requested file not found", file_hash=file_hash
            )
            return

        local_path = shared_file.local_path
        if not await self._integrity.verify_file(local_path, shared_file.file_hash):
            await session.send_transfer_error(
                "409", "Remote file content changed on disk", file_hash=file_hash
            )
            return

        file_size = local_path.stat().st_size
        if offset < 0 or offset > file_size:
            await session.send_transfer_error(
                "416",
                f"Requested offset {offset} out of range for size {file_size}",
                file_hash=file_hash,
            )
            return

        self.received_download_offsets.append(offset)
        request_id = f"{remote_peer_id}:{file_hash}:{id(session)}"
        acquired = await self._acquire_upload_slot(
            session,
            request_id,
            file_hash,
            shared_file.file_name,
            file_size,
            remote_peer_id,
        )
        if not acquired:
            return

        try:
            await session.send_slot_available(file_hash)

            cursor_offset = offset
            chunk_index = offset // self._chunk_size
            while cursor_offset < file_size:
                chunk = await asyncio.to_thread(
                    self._read_chunk_at,
                    local_path,
                    cursor_offset,
                    self._chunk_size,
                )
                if not chunk:
                    break
                chunk_hash = self._integrity.compute_chunk_hash(chunk)
                await session.send_file_chunk(
                    file_hash,
                    cursor_offset,
                    chunk_index,
                    chunk_hash,
                    chunk,
                )
                cursor_offset += len(chunk)
                chunk_index += 1
                if self._chunk_delay_s > 0:
                    await asyncio.sleep(self._chunk_delay_s)

            await session.send_transfer_complete(file_hash, cursor_offset)
        finally:
            await self._release_upload_slot()
