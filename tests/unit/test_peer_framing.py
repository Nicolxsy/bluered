"""Unit tests for shared/bluered_contracts/peer_framing.py."""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from bluered_contracts.peer_framing import (
    MAX_HEADER_BYTES,
    MAX_PAYLOAD_BYTES,
    PREFIX_STRUCT,
    FrameIntegrityError,
    FrameTooLargeError,
    FramingError,
    IncompleteFrameError,
    PeerFrame,
    PeerFrameHeader,
    PeerOp,
    encode_frame,
    read_frame,
    write_frame,
)

SAMPLE_HASH = "b" * 64


@pytest.mark.parametrize("op", list(PeerOp))
async def test_roundtrip_all_peer_ops(op: PeerOp) -> None:
    payload = b"\x00\x01\x02chunk-binary-data" if op == PeerOp.FILE_CHUNK else b""
    chunk_sha = hashlib.sha256(payload).hexdigest() if op == PeerOp.FILE_CHUNK else None
    frame = PeerFrame(
        header=PeerFrameHeader(
            op=op,
            request_id="req-123",
            file_hash=SAMPLE_HASH if op in {PeerOp.DOWNLOAD_REQ, PeerOp.FILE_CHUNK} else None,
            offset=0 if op == PeerOp.FILE_CHUNK else None,
            chunk_index=0 if op == PeerOp.FILE_CHUNK else None,
            chunk_sha256=chunk_sha,
            queue_position=2 if op == PeerOp.QUEUE_STATUS else None,
            status="ok",
        ),
        payload=payload,
    )

    encoded = encode_frame(frame)
    reader = asyncio.StreamReader()
    reader.feed_data(encoded)
    reader.feed_eof()

    decoded = await read_frame(reader)
    assert decoded == frame


async def test_byte_by_byte_fragmented_stream_reader() -> None:
    payload = bytes(range(256)) * 4
    frame = PeerFrame(
        header=PeerFrameHeader(
            op=PeerOp.FILE_CHUNK,
            request_id="frag-1",
            file_hash=SAMPLE_HASH,
            offset=0,
            chunk_index=0,
            chunk_sha256=hashlib.sha256(payload).hexdigest(),
        ),
        payload=payload,
    )
    encoded = encode_frame(frame)

    reader = asyncio.StreamReader()

    async def feeder() -> None:
        for idx in range(len(encoded)):
            reader.feed_data(encoded[idx : idx + 1])
            await asyncio.sleep(0)
        reader.feed_eof()

    feed_task = asyncio.create_task(feeder())
    decoded = await read_frame(reader)
    await feed_task

    assert decoded == frame


async def test_real_tcp_loopback_fragmented_streaming() -> None:
    payload = b"BlueRed-TCP-Fragmented-Payload-" * 64
    expected_frame = PeerFrame(
        header=PeerFrameHeader(
            op=PeerOp.FILE_CHUNK,
            request_id="tcp-1",
            file_hash=SAMPLE_HASH,
            offset=512,
            chunk_index=1,
            chunk_sha256=hashlib.sha256(payload).hexdigest(),
        ),
        payload=payload,
    )

    async def handle_client(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        req = await read_frame(reader)
        assert req.header.op == PeerOp.DOWNLOAD_REQ
        raw = encode_frame(expected_frame)
        # Send in tiny fragmented TCP chunks
        for i in range(0, len(raw), 7):
            writer.write(raw[i : i + 7])
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        await write_frame(
            writer,
            PeerFrame(
                header=PeerFrameHeader(
                    op=PeerOp.DOWNLOAD_REQ,
                    request_id="tcp-1",
                    file_hash=SAMPLE_HASH,
                    offset=512,
                )
            ),
        )
        received = await read_frame(reader)
        writer.close()
        await writer.wait_closed()
        assert received == expected_frame
    finally:
        server.close()
        await server.wait_closed()


async def test_anti_dos_bounds_rejected_before_body_read() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(PREFIX_STRUCT.pack(MAX_HEADER_BYTES + 1, 0))
    with pytest.raises(FrameTooLargeError):
        await read_frame(reader)

    reader2 = asyncio.StreamReader()
    reader2.feed_data(PREFIX_STRUCT.pack(32, MAX_PAYLOAD_BYTES + 1))
    with pytest.raises(FrameTooLargeError):
        await read_frame(reader2)

    reader3 = asyncio.StreamReader()
    reader3.feed_data(PREFIX_STRUCT.pack(0, 0))
    with pytest.raises(FramingError):
        await read_frame(reader3)


async def test_integrity_and_incomplete_frame_errors() -> None:
    payload = b"real-data"
    bad_frame = PeerFrame(
        header=PeerFrameHeader(
            op=PeerOp.FILE_CHUNK,
            request_id="bad-hash",
            file_hash=SAMPLE_HASH,
            chunk_index=0,
            chunk_sha256="0" * 64,
        ),
        payload=payload,
    )
    with pytest.raises(FrameIntegrityError):
        encode_frame(bad_frame)

    # Missing chunk_sha256 on received FILE_CHUNK
    no_hash_frame = PeerFrame(
        header=PeerFrameHeader(
            op=PeerOp.FILE_CHUNK,
            request_id="no-hash",
            file_hash=SAMPLE_HASH,
            chunk_index=0,
        ),
        payload=payload,
    )
    reader = asyncio.StreamReader()
    reader.feed_data(encode_frame(no_hash_frame))
    reader.feed_eof()
    with pytest.raises(FrameIntegrityError):
        await read_frame(reader)

    # Truncated stream
    trunc_reader = asyncio.StreamReader()
    trunc_reader.feed_data(b"\x00\x00\x00\x10")
    trunc_reader.feed_eof()
    with pytest.raises(IncompleteFrameError):
        await read_frame(trunc_reader)
