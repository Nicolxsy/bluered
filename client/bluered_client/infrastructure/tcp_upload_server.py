"""Raw TCP binary-framed upload server implementing IPeerUploadServer and IPeerUploadSession."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from bluered_contracts.peer_framing import (
    FramingError,
    PeerFrame,
    PeerFrameHeader,
    PeerOp,
    read_frame,
    write_frame,
)

from bluered_client.contracts import IPeerUploadHandler, IPeerUploadSession
from bluered_client.domain import LocalPeer, SharedFile


class _TcpPeerUploadSession(IPeerUploadSession):
    """Per-connection session adapter translating application actions into PeerFrame writes."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        request_id: str,
        remote_peer_id: str,
        io_timeout_s: float,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._request_id = request_id
        self._remote_peer_id = remote_peer_id
        self._io_timeout_s = io_timeout_s

    @property
    def remote_peer_id(self) -> str:
        return self._remote_peer_id

    async def wait_disconnected(self) -> None:
        with contextlib.suppress(OSError, ConnectionError):
            while await self._reader.read(1):
                pass

    async def send_browse_response(self, files: list[SharedFile]) -> None:
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
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.BROWSE_RESP,
                    request_id=self._request_id,
                    status="200",
                    metadata={"files": serialized},
                )
            ),
        )

    async def send_private_chat_ack(
        self, status: str, error_detail: str | None = None
    ) -> None:
        await write_frame(
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.PRIVATE_CHAT_ACK,
                    request_id=self._request_id,
                    status=status,
                    error_detail=error_detail,
                )
            ),
        )

    async def send_queue_status(self, queue_position: int, file_hash: str) -> None:
        await write_frame(
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.QUEUE_STATUS,
                    request_id=self._request_id,
                    file_hash=file_hash,
                    status="QUEUED",
                    queue_position=queue_position,
                )
            ),
        )

    async def send_slot_available(self, file_hash: str) -> None:
        await write_frame(
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.SLOT_AVAILABLE,
                    request_id=self._request_id,
                    file_hash=file_hash,
                    status="READY",
                    queue_position=0,
                )
            ),
        )

    async def send_file_chunk(
        self,
        file_hash: str,
        offset: int,
        chunk_index: int,
        chunk_sha256: str,
        payload: bytes,
    ) -> None:
        async with asyncio.timeout(self._io_timeout_s):
            await write_frame(
                self._writer,
                PeerFrame(
                    header=PeerFrameHeader(
                        op=PeerOp.FILE_CHUNK,
                        request_id=self._request_id,
                        file_hash=file_hash,
                        offset=offset,
                        chunk_index=chunk_index,
                        chunk_sha256=chunk_sha256,
                    ),
                    payload=payload,
                ),
            )

    async def send_transfer_complete(self, file_hash: str, final_offset: int) -> None:
        await write_frame(
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.TRANSFER_COMPLETE,
                    request_id=self._request_id,
                    file_hash=file_hash,
                    offset=final_offset,
                    status="200",
                )
            ),
        )

    async def send_transfer_error(
        self, status: str, detail: str, *, file_hash: str | None = None
    ) -> None:
        await write_frame(
            self._writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.TRANSFER_ERROR,
                    request_id=self._request_id,
                    file_hash=file_hash,
                    status=status,
                    error_detail=detail,
                )
            ),
        )


class RawTcpUploadServer:
    """Asyncio TCP server implementing IPeerUploadServer over peer_framing.py (!II)."""

    def __init__(self, local_peer: LocalPeer, *, io_timeout_s: float = 15.0) -> None:
        self._local_peer = local_peer
        self._io_timeout_s = io_timeout_s
        self._server: asyncio.Server | None = None
        self._bound_port: int | None = None
        self._handler: IPeerUploadHandler | None = None
        self._client_tasks: set[asyncio.Task[None]] = set()
        self._active_writers: set[asyncio.StreamWriter] = set()

    @property
    def bound_port(self) -> int | None:
        return self._bound_port

    async def start(self, host: str, port: int, handler: IPeerUploadHandler) -> int:
        if self._server is not None and self._bound_port is not None:
            return self._bound_port
        self._handler = handler
        self._server = await asyncio.start_server(
            self._on_client_connected,
            host=host,
            port=port,
        )
        sockets = self._server.sockets
        if sockets:
            self._bound_port = int(sockets[0].getsockname()[1])
        else:
            self._bound_port = port
        return self._bound_port

    async def stop(self) -> None:
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
        self._bound_port = None

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
        handler = self._handler
        if handler is None:
            await self._close_writer(writer)
            return
        self._active_writers.add(writer)
        try:
            async with asyncio.timeout(self._io_timeout_s):
                first_frame = await read_frame(reader)

            remote_peer_id = str(first_frame.header.metadata.get("peer_id", "unknown-peer"))
            if first_frame.header.op == PeerOp.HANDSHAKE_REQ:
                if handler.is_peer_blocked(remote_peer_id):
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

            session = _TcpPeerUploadSession(
                reader=reader,
                writer=writer,
                request_id=op_frame.header.request_id,
                remote_peer_id=remote_peer_id,
                io_timeout_s=self._io_timeout_s,
            )

            if op_frame.header.op == PeerOp.BROWSE_REQ:
                await handler.handle_browse(session)
            elif op_frame.header.op == PeerOp.DOWNLOAD_REQ:
                await handler.handle_download(
                    session,
                    op_frame.header.file_hash,
                    op_frame.header.offset or 0,
                )
            elif op_frame.header.op == PeerOp.PRIVATE_CHAT_MSG:
                meta = op_frame.header.metadata
                sender_id = str(meta.get("sender_id") or meta.get("peer_id") or "")
                recipient_id = str(meta.get("recipient_id") or self._local_peer.peer_id)
                message_id = str(meta.get("message_id") or op_frame.header.request_id)
                content = str(meta.get("content") or "")
                await handler.handle_private_chat(
                    session,
                    sender_id=sender_id,
                    recipient_id=recipient_id,
                    message_id=message_id,
                    content=content,
                )
            else:
                await session.send_transfer_error(
                    "400", f"Unsupported operation: {op_frame.header.op}"
                )
        except (FramingError, TimeoutError, OSError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._active_writers.discard(writer)
            await self._close_writer(writer)
