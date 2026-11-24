"""Deterministic binary TCP framing (!II + JSON header + binary payload) for IPeerTransport."""

from __future__ import annotations

import asyncio
import hashlib
import struct
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

PREFIX_STRUCT = struct.Struct("!II")
MAX_HEADER_BYTES = 64 * 1024
MAX_PAYLOAD_BYTES = 4 * 1024 * 1024
PROTOCOL_VERSION = "1.0"
_SHA256_HEX_PATTERN = r"^[0-9a-f]{64}$"


class FramingError(Exception):
    """Base error for binary peer framing failures."""


class FrameTooLargeError(FramingError):
    """Raised when header_len or payload_len exceeds anti-DoS bounds."""


class FrameIntegrityError(FramingError):
    """Raised when FILE_CHUNK SHA-256 verification fails or is missing."""


class IncompleteFrameError(FramingError):
    """Raised when the TCP stream closes before a complete frame is received."""


class PeerOp(StrEnum):
    HANDSHAKE_REQ = "HANDSHAKE_REQ"
    HANDSHAKE_ACK = "HANDSHAKE_ACK"
    BROWSE_REQ = "BROWSE_REQ"
    BROWSE_RESP = "BROWSE_RESP"
    DOWNLOAD_REQ = "DOWNLOAD_REQ"
    FILE_CHUNK = "FILE_CHUNK"
    QUEUE_STATUS = "QUEUE_STATUS"
    SLOT_AVAILABLE = "SLOT_AVAILABLE"
    TRANSFER_COMPLETE = "TRANSFER_COMPLETE"
    TRANSFER_ERROR = "TRANSFER_ERROR"
    PRIVATE_CHAT_MSG = "PRIVATE_CHAT_MSG"
    PRIVATE_CHAT_ACK = "PRIVATE_CHAT_ACK"


class PeerFrameHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: PeerOp
    request_id: str = Field(min_length=1)
    protocol_version: str = PROTOCOL_VERSION
    file_hash: str | None = Field(default=None, pattern=_SHA256_HEX_PATTERN)
    offset: int | None = Field(default=None, ge=0)
    chunk_index: int | None = Field(default=None, ge=0)
    chunk_sha256: str | None = Field(default=None, pattern=_SHA256_HEX_PATTERN)
    status: str | None = None
    queue_position: int | None = Field(default=None, ge=0)
    error_detail: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PeerFrame:
    header: PeerFrameHeader
    payload: bytes = b""


def encode_frame(
    frame: PeerFrame,
    *,
    max_header_bytes: int = MAX_HEADER_BYTES,
    max_payload_bytes: int = MAX_PAYLOAD_BYTES,
) -> bytes:
    """Encode a PeerFrame into !II prefix + UTF-8 JSON header + binary payload."""
    header_bytes = frame.header.model_dump_json(exclude_none=True).encode("utf-8")
    if len(header_bytes) > max_header_bytes:
        raise FrameTooLargeError(
            f"Header length {len(header_bytes)} exceeds limit {max_header_bytes}"
        )
    if len(frame.payload) > max_payload_bytes:
        raise FrameTooLargeError(
            f"Payload length {len(frame.payload)} exceeds limit {max_payload_bytes}"
        )
    if frame.header.op == PeerOp.FILE_CHUNK and frame.header.chunk_sha256 is not None:
        actual = hashlib.sha256(frame.payload).hexdigest()
        if actual != frame.header.chunk_sha256:
            raise FrameIntegrityError(
                f"Chunk SHA-256 mismatch on encode: expected {frame.header.chunk_sha256}, "
                f"got {actual}"
            )
    return PREFIX_STRUCT.pack(len(header_bytes), len(frame.payload)) + header_bytes + frame.payload


async def write_frame(
    writer: asyncio.StreamWriter,
    frame: PeerFrame,
    *,
    max_header_bytes: int = MAX_HEADER_BYTES,
    max_payload_bytes: int = MAX_PAYLOAD_BYTES,
) -> None:
    """Serialize and flush a PeerFrame to an asyncio StreamWriter."""
    writer.write(
        encode_frame(
            frame,
            max_header_bytes=max_header_bytes,
            max_payload_bytes=max_payload_bytes,
        )
    )
    await writer.drain()


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    max_header_bytes: int = MAX_HEADER_BYTES,
    max_payload_bytes: int = MAX_PAYLOAD_BYTES,
    verify_chunk_hash: bool = True,
) -> PeerFrame:
    """Read one PeerFrame using readexactly so fragmented TCP reads assemble cleanly."""
    try:
        prefix = await reader.readexactly(PREFIX_STRUCT.size)
    except asyncio.IncompleteReadError as exc:
        raise IncompleteFrameError("Stream closed while reading frame prefix") from exc

    header_len, payload_len = PREFIX_STRUCT.unpack(prefix)
    if header_len == 0:
        raise FramingError("Frame header length cannot be zero")
    if header_len > max_header_bytes:
        raise FrameTooLargeError(f"Header length {header_len} exceeds limit {max_header_bytes}")
    if payload_len > max_payload_bytes:
        raise FrameTooLargeError(f"Payload length {payload_len} exceeds limit {max_payload_bytes}")

    try:
        header_bytes = await reader.readexactly(header_len)
        payload = await reader.readexactly(payload_len) if payload_len > 0 else b""
    except asyncio.IncompleteReadError as exc:
        raise IncompleteFrameError("Stream closed while reading frame body") from exc

    try:
        header = PeerFrameHeader.model_validate_json(header_bytes)
    except ValidationError as exc:
        raise FramingError(f"Invalid frame header JSON: {exc}") from exc

    if verify_chunk_hash and header.op == PeerOp.FILE_CHUNK:
        if header.chunk_sha256 is None:
            raise FrameIntegrityError("FILE_CHUNK frame requires chunk_sha256")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != header.chunk_sha256:
            raise FrameIntegrityError(
                f"Chunk SHA-256 mismatch: expected {header.chunk_sha256}, got {actual}"
            )

    return PeerFrame(header=header, payload=payload)
