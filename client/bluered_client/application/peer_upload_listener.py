"""Symmetric P2P TCP upload & request listener (Cas_16, Cas_18, Cas_21, Cas_22, Cas_23)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from bluered_contracts.peer_framing import (
    FramingError,
    PeerFrame,
    PeerFrameHeader,
    PeerOp,
    read_frame,
    write_frame,
)

from bluered_client.contracts import (
    IFileIndexRepository,
    IIntegrityVerifier,
    ITransferQueuePolicy,
)
from bluered_client.domain import (
    LocalPeer,
    PrivateMessage,
    QueueFullError,
    TransferTask,
)


class PeerUploadListener:
    """Asyncio TCP server handling incoming P2P browse, download, and private chat frames."""

    def __init__(
        self,
        local_peer: LocalPeer,
        file_index: IFileIndexRepository,
        queue_policy: ITransferQueuePolicy,
        integrity_verifier: IIntegrityVerifier,
        *,
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
        self._chunk_size = chunk_size
        self._io_timeout_s = io_timeout_s
        self._on_private_message = on_private_message
        self._chunk_delay_s = chunk_delay_s
        self._bind_port = bind_port

        self._server: asyncio.Server | None = None
        self._bound_port: int | None = None
        self._client_tasks: set[asyncio.Task[None]] = set()
        self._active_writers: set[asyncio.StreamWriter] = set()
        self._slot_lock = asyncio.Lock()
        self._active_uploads = 0
        self._waiters: dict[str, asyncio.Event] = {}
        self.received_download_offsets: list[int] = []

    @property
    def bound_port(self) -> int | None:
        return self._bound_port

    @property
    def active_uploads(self) -> int:
        return self._active_uploads

    async def start(self) -> int:
        """Start the TCP listener and return the bound port."""
        if self._server is not None and self._bound_port is not None:
            return self._bound_port
        port = self._local_peer.tcp_port if self._bind_port is None else self._bind_port
        self._server = await asyncio.start_server(
            self._on_client_connected,
            host=self._local_peer.ip_address,
            port=port,
        )
        sockets = self._server.sockets
        if sockets:
            self._bound_port = int(sockets[0].getsockname()[1])
            if port == 0:
                self._local_peer.tcp_port = self._bound_port
        else:
            self._bound_port = port
        return self._bound_port

    async def stop(self) -> None:
        """Gracefully stop the TCP server and close all active client streams."""
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(OSError, ConnectionError):
                await self._server.wait_closed()
            self._server = None
        for task in list(self._client_tasks):
            task.cancel()
        if self._client_tasks:
            await asyncio.gather(*self._client_tasks, return_exceptions=True)
        for writer in list(self._active_writers):
            await self._close_writer(writer)

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(OSError, ConnectionError):
            await writer.wait_closed()

    def _on_client_connected(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.create_task(self._handle_client(reader, writer))
        self._client_tasks.add(task)
        task.add_done_callback(self._client_tasks.discard)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._active_writers.add(writer)
        try:
            async with asyncio.timeout(self._io_timeout_s):
                first_frame = await read_frame(reader)

            remote_peer_id = str(first_frame.header.metadata.get("peer_id", "unknown-peer"))
            if first_frame.header.op == PeerOp.HANDSHAKE_REQ:
                if self._local_peer.is_peer_blocked(remote_peer_id):
                    await write_frame(
                        writer,
                        PeerFrame(
                            header=PeerFrameHeader(
                                op=PeerOp.HANDSHAKE_ACK,
                                request_id=first_frame.header.request_id,
                                status="403",
                                error_detail="Peer is blocked",
                            )
                        ),
                    )
                    return
                await write_frame(
                    writer,
                    PeerFrame(
                        header=PeerFrameHeader(
                            op=PeerOp.HANDSHAKE_ACK,
                            request_id=first_frame.header.request_id,
                            status="200",
                            metadata={
                                "peer_id": self._local_peer.peer_id,
                                "username": self._local_peer.username,
                            },
                        )
                    ),
                )
                async with asyncio.timeout(self._io_timeout_s):
                    op_frame = await read_frame(reader)
            else:
                op_frame = first_frame

            if op_frame.header.op == PeerOp.BROWSE_REQ:
                await self._handle_browse(writer, op_frame, remote_peer_id)
            elif op_frame.header.op == PeerOp.DOWNLOAD_REQ:
                await self._handle_download(reader, writer, op_frame, remote_peer_id)
            elif op_frame.header.op == PeerOp.PRIVATE_CHAT_MSG:
                await self._handle_private_chat(writer, op_frame)
            else:
                await self._send_transfer_error(
                    writer,
                    op_frame.header.request_id,
                    "400",
                    f"Unsupported operation: {op_frame.header.op}",
                )
        except (FramingError, TimeoutError, OSError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._active_writers.discard(writer)
            await self._close_writer(writer)

    async def _send_transfer_error(
        self,
        writer: asyncio.StreamWriter,
        request_id: str,
        status: str,
        detail: str,
        *,
        file_hash: str | None = None,
    ) -> None:
        await write_frame(
            writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.TRANSFER_ERROR,
                    request_id=request_id,
                    file_hash=file_hash,
                    status=status,
                    error_detail=detail,
                )
            ),
        )

    async def _handle_browse(
        self,
        writer: asyncio.StreamWriter,
        frame: PeerFrame,
        remote_peer_id: str,
    ) -> None:
        if self._local_peer.is_peer_blocked(remote_peer_id):
            await self._send_transfer_error(
                writer, frame.header.request_id, "403", "Peer is blocked"
            )
            return
        files = await self._file_index.list_all()
        serialized: list[dict[str, Any]] = [
            {
                "file_hash": f.file_hash,
                "file_name": f.file_name,
                "size_bytes": f.size_bytes,
                "bitrate_kbps": f.bitrate_kbps,
                "mime_type": f.mime_type,
                "category": f.category,
            }
            for f in files
        ]
        await write_frame(
            writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.BROWSE_RESP,
                    request_id=frame.header.request_id,
                    status="200",
                    metadata={"files": serialized},
                )
            ),
        )

    async def _handle_private_chat(
        self,
        writer: asyncio.StreamWriter,
        frame: PeerFrame,
    ) -> None:
        meta = frame.header.metadata
        sender_id = str(meta.get("sender_id") or meta.get("peer_id") or "")
        recipient_id = str(meta.get("recipient_id") or self._local_peer.peer_id)
        message_id = str(meta.get("message_id") or frame.header.request_id)
        content = str(meta.get("content") or "")

        if self._local_peer.is_peer_blocked(sender_id):
            await write_frame(
                writer,
                PeerFrame(
                    header=PeerFrameHeader(
                        op=PeerOp.PRIVATE_CHAT_ACK,
                        request_id=frame.header.request_id,
                        status="403",
                        error_detail="Sender is blocked",
                    )
                ),
            )
            return

        if not content.strip():
            await write_frame(
                writer,
                PeerFrame(
                    header=PeerFrameHeader(
                        op=PeerOp.PRIVATE_CHAT_ACK,
                        request_id=frame.header.request_id,
                        status="400",
                        error_detail="Message content cannot be empty",
                    )
                ),
            )
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

        await write_frame(
            writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.PRIVATE_CHAT_ACK,
                    request_id=frame.header.request_id,
                    status="200",
                )
            ),
        )

    async def _acquire_upload_slot(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
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
                await self._send_transfer_error(
                    writer,
                    request_id,
                    "503",
                    str(exc),
                    file_hash=file_hash,
                )
                return False

            wait_event = asyncio.Event()
            self._waiters[request_id] = wait_event

        await write_frame(
            writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.QUEUE_STATUS,
                    request_id=request_id,
                    file_hash=file_hash,
                    status="QUEUED",
                    queue_position=pos,
                )
            ),
        )

        wait_task = asyncio.create_task(wait_event.wait())
        eof_task = asyncio.create_task(reader.read(1))
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

    async def _handle_download(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        frame: PeerFrame,
        remote_peer_id: str,
    ) -> None:
        request_id = frame.header.request_id
        file_hash = frame.header.file_hash
        if self._local_peer.is_peer_blocked(remote_peer_id):
            await self._send_transfer_error(writer, request_id, "403", "Peer is blocked")
            return
        if not file_hash:
            await self._send_transfer_error(writer, request_id, "400", "file_hash is required")
            return

        shared_file = await self._file_index.get_by_hash(file_hash)
        if (
            shared_file is None
            or shared_file.local_path is None
            or not shared_file.local_path.is_file()
        ):
            await self._send_transfer_error(
                writer, request_id, "404", "Requested file not found", file_hash=file_hash
            )
            return

        local_path = shared_file.local_path
        if not await self._integrity.verify_file(local_path, shared_file.file_hash):
            await self._send_transfer_error(
                writer,
                request_id,
                "409",
                "Remote file content changed on disk",
                file_hash=file_hash,
            )
            return

        file_size = local_path.stat().st_size
        offset = frame.header.offset or 0
        if offset < 0 or offset > file_size:
            await self._send_transfer_error(
                writer,
                request_id,
                "416",
                f"Requested offset {offset} out of range for size {file_size}",
                file_hash=file_hash,
            )
            return

        self.received_download_offsets.append(offset)
        acquired = await self._acquire_upload_slot(
            reader,
            writer,
            request_id,
            file_hash,
            shared_file.file_name,
            file_size,
            remote_peer_id,
        )
        if not acquired:
            return

        try:
            await write_frame(
                writer,
                PeerFrame(
                    header=PeerFrameHeader(
                        op=PeerOp.SLOT_AVAILABLE,
                        request_id=request_id,
                        file_hash=file_hash,
                        status="READY",
                        queue_position=0,
                    )
                ),
            )

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
                async with asyncio.timeout(self._io_timeout_s):
                    await write_frame(
                        writer,
                        PeerFrame(
                            header=PeerFrameHeader(
                                op=PeerOp.FILE_CHUNK,
                                request_id=request_id,
                                file_hash=file_hash,
                                offset=cursor_offset,
                                chunk_index=chunk_index,
                                chunk_sha256=chunk_hash,
                            ),
                            payload=chunk,
                        ),
                    )
                cursor_offset += len(chunk)
                chunk_index += 1
                if self._chunk_delay_s > 0:
                    await asyncio.sleep(self._chunk_delay_s)

            await write_frame(
                writer,
                PeerFrame(
                    header=PeerFrameHeader(
                        op=PeerOp.TRANSFER_COMPLETE,
                        request_id=request_id,
                        file_hash=file_hash,
                        offset=cursor_offset,
                        status="200",
                    )
                ),
            )
        finally:
            await self._release_upload_slot()
