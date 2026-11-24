"""Raw TCP binary-framed peer transport implementing IPeerTransport and IStreamingPeerTransport."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from bluered_contracts.peer_framing import (
    FramingError,
    PeerFrame,
    PeerFrameHeader,
    PeerOp,
    read_frame,
    write_frame,
)

from bluered_client.contracts import INatTraversalStrategy
from bluered_client.domain import (
    LocalPeer,
    PeerBlockedError,
    PeerTransportError,
    PrivateMessage,
    QueueFullError,
    RemoteFileChangedError,
    RemotePeer,
    SharedFile,
)


class RawTcpPeerTransport:
    """Data-plane P2P TCP transport over peer_framing.py (!II + JSON + binary payload)."""

    def __init__(
        self,
        local_peer: LocalPeer,
        nat_strategy: INatTraversalStrategy,
        *,
        io_timeout_s: float = 15.0,
        queue_wait_timeout_s: float = 60.0,
    ) -> None:
        self._local_peer = local_peer
        self._nat_strategy = nat_strategy
        self._io_timeout_s = io_timeout_s
        self._queue_wait_timeout_s = queue_wait_timeout_s
        self._active_writers: set[asyncio.StreamWriter] = set()

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(OSError, ConnectionError):
            await writer.wait_closed()

    def abort_active_streams(self) -> None:
        """Forcibly abort all active TCP transport sockets (simulates abrupt link drop)."""
        for writer in list(self._active_writers):
            writer.transport.abort()

    async def _open_and_handshake(
        self,
        peer: RemotePeer,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await self._nat_strategy.establish_connection(self._local_peer, peer)
        self._active_writers.add(writer)
        try:
            req_id = uuid.uuid4().hex
            try:
                async with asyncio.timeout(self._io_timeout_s):
                    await write_frame(
                        writer,
                        PeerFrame(
                            header=PeerFrameHeader(
                                op=PeerOp.HANDSHAKE_REQ,
                                request_id=req_id,
                                metadata={
                                    "peer_id": self._local_peer.peer_id,
                                    "username": self._local_peer.username,
                                },
                            )
                        ),
                    )
                    ack = await read_frame(reader)
            except FramingError as exc:
                raise PeerTransportError(str(exc)) from exc
            if ack.header.op != PeerOp.HANDSHAKE_ACK:
                raise ConnectionError(f"Expected HANDSHAKE_ACK, got {ack.header.op}")
            if ack.header.status == "403":
                raise PeerBlockedError(
                    ack.header.error_detail or f"Blocked by peer '{peer.peer_id}'"
                )
            if ack.header.status not in (None, "200"):
                raise ConnectionError(
                    ack.header.error_detail or f"Handshake rejected: {ack.header.status}"
                )
            return reader, writer
        except Exception:
            self._active_writers.discard(writer)
            await self._close_writer(writer)
            raise

    async def browse_peer_files(self, peer: RemotePeer) -> list[SharedFile]:
        """Request the remote peer's shared file catalog via BROWSE_REQ (Cas_16)."""
        reader, writer = await self._open_and_handshake(peer)
        try:
            req_id = uuid.uuid4().hex
            try:
                async with asyncio.timeout(self._io_timeout_s):
                    await write_frame(
                        writer,
                        PeerFrame(
                            header=PeerFrameHeader(
                                op=PeerOp.BROWSE_REQ,
                                request_id=req_id,
                                metadata={"peer_id": self._local_peer.peer_id},
                            )
                        ),
                    )
                    resp = await read_frame(reader)
            except FramingError as exc:
                raise PeerTransportError(str(exc)) from exc
            if resp.header.op == PeerOp.TRANSFER_ERROR:
                if resp.header.status == "403":
                    raise PeerBlockedError(resp.header.error_detail or "Blocked by remote peer")
                raise RuntimeError(resp.header.error_detail or "Browse request failed")
            if resp.header.op != PeerOp.BROWSE_RESP:
                raise ConnectionError(f"Expected BROWSE_RESP, got {resp.header.op}")

            raw_files = resp.header.metadata.get("files", [])
            result: list[SharedFile] = []
            for item in raw_files:
                if not isinstance(item, dict):
                    continue
                result.append(
                    SharedFile(
                        file_hash=str(item["file_hash"]),
                        file_name=str(item["file_name"]),
                        size_bytes=int(item["size_bytes"]),
                        bitrate_kbps=(
                            int(item["bitrate_kbps"])
                            if item.get("bitrate_kbps") is not None
                            else None
                        ),
                        mime_type=str(item.get("mime_type", "application/octet-stream")),
                        category=str(item.get("category", "other")),
                    )
                )
            return result
        finally:
            self._active_writers.discard(writer)
            await self._close_writer(writer)

    async def stream_file_download(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
        *,
        on_queue_status: Callable[[int], Awaitable[None]] | None = None,
        on_slot_available: Callable[[], Awaitable[None]] | None = None,
    ) -> AsyncIterator[tuple[int, bytes, str]]:
        """Stream (chunk_index, payload, chunk_sha256) frames from peer starting at offset."""
        reader, writer = await self._open_and_handshake(peer)
        queued = False
        try:
            req_id = uuid.uuid4().hex
            try:
                async with asyncio.timeout(self._io_timeout_s):
                    await write_frame(
                        writer,
                        PeerFrame(
                            header=PeerFrameHeader(
                                op=PeerOp.DOWNLOAD_REQ,
                                request_id=req_id,
                                file_hash=file_hash,
                                offset=offset,
                                metadata={"peer_id": self._local_peer.peer_id},
                            )
                        ),
                    )

                while True:
                    wait_s = self._queue_wait_timeout_s if queued else self._io_timeout_s
                    async with asyncio.timeout(wait_s):
                        frame = await read_frame(reader)

                    op = frame.header.op
                    if op == PeerOp.QUEUE_STATUS:
                        queued = True
                        if on_queue_status is not None:
                            await on_queue_status(frame.header.queue_position or 1)
                    elif op == PeerOp.SLOT_AVAILABLE:
                        queued = False
                        if on_slot_available is not None:
                            await on_slot_available()
                    elif op == PeerOp.FILE_CHUNK:
                        chunk_index = frame.header.chunk_index or 0
                        chunk_sha256 = frame.header.chunk_sha256 or ""
                        yield (chunk_index, frame.payload, chunk_sha256)
                    elif op == PeerOp.TRANSFER_COMPLETE:
                        break
                    elif op == PeerOp.TRANSFER_ERROR:
                        status = frame.header.status or "500"
                        detail = frame.header.error_detail or f"Remote transfer error ({status})"
                        if status == "403":
                            raise PeerBlockedError(detail)
                        if status == "409":
                            raise RemoteFileChangedError(detail)
                        if status == "503":
                            raise QueueFullError(detail)
                        raise RuntimeError(detail)
                    else:
                        raise ConnectionError(f"Unexpected frame op during download: {op}")
            except FramingError as exc:
                raise PeerTransportError(str(exc)) from exc
        finally:
            self._active_writers.discard(writer)
            await self._close_writer(writer)

    async def request_file_chunks(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
    ) -> AsyncIterator[bytes]:
        """Yield raw binary chunks implementing IPeerTransport.request_file_chunks."""
        async for _, payload, _ in self.stream_file_download(peer, file_hash, offset=offset):
            yield payload

    async def send_private_message(self, peer: RemotePeer, message: PrivateMessage) -> bool:
        """Send a direct P2P PRIVATE_CHAT_MSG frame and inspect PRIVATE_CHAT_ACK (Cas_23)."""
        try:
            reader, writer = await self._open_and_handshake(peer)
        except PeerBlockedError:
            raise
        except Exception:
            return False

        try:
            async with asyncio.timeout(self._io_timeout_s):
                await write_frame(
                    writer,
                    PeerFrame(
                        header=PeerFrameHeader(
                            op=PeerOp.PRIVATE_CHAT_MSG,
                            request_id=message.message_id,
                            metadata={
                                "message_id": message.message_id,
                                "sender_id": message.sender_id,
                                "recipient_id": message.recipient_id,
                                "content": message.content,
                            },
                        )
                    ),
                )
                ack = await read_frame(reader)
            if ack.header.op != PeerOp.PRIVATE_CHAT_ACK:
                return False
            if ack.header.status == "403":
                raise PeerBlockedError(
                    ack.header.error_detail or f"Peer '{peer.peer_id}' blocked sender"
                )
            return ack.header.status in (None, "200")
        except PeerBlockedError:
            raise
        except Exception:
            return False
        finally:
            self._active_writers.discard(writer)
            await self._close_writer(writer)

    async def close(self) -> None:
        """Close all currently open client-side peer TCP streams."""
        for writer in list(self._active_writers):
            self._active_writers.discard(writer)
            await self._close_writer(writer)
