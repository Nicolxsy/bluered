"""Pydantic v2 request schemas for the BlueRed local client REST API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class AuthCredentialsRequest(BaseModel):
    """Payload for /api/session/register and /api/session/login."""

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class PresenceUpdateRequest(BaseModel):
    """Payload for updating local presence status (Cas_08)."""

    status: Literal["ONLINE", "AWAY", "OFFLINE"]


class FolderPathRequest(BaseModel):
    """Payload for adding or removing a shared directory (Cas_09)."""

    path: str = Field(min_length=1, max_length=1024)


class BrowsePeerRequest(BaseModel):
    """Payload for exploring a remote peer's shared directory over P2P TCP (Cas_16)."""

    peer_id: str = Field(min_length=1)
    username: str = Field(min_length=1)
    ip_address: str = Field(min_length=1)
    tcp_port: int = Field(ge=1, le=65535)


class PublishReviewRequest(BaseModel):
    """Payload for publishing or updating a file review (Cas_14, Cas_24)."""

    rating: int = Field(ge=1, le=5)
    comment: str = Field(min_length=1, max_length=2000)


class DownloadRequest(BaseModel):
    """Payload for initiating a P2P file download (Cas_17)."""

    peer_id: str = Field(min_length=1)
    peer_username: str = Field(min_length=1)
    peer_ip: str = Field(min_length=1)
    peer_port: int = Field(ge=1, le=65535)
    file_hash: str = Field(min_length=64, max_length=64)
    file_name: str = Field(min_length=1, max_length=512)
    size_bytes: int = Field(ge=0)
    conflict_policy: Literal["keep_both", "replace"] = "keep_both"
    wait: bool = False


class CreateCommunityRequest(BaseModel):
    """Payload for creating a community or subcommunity (Cas_01)."""

    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)
    parent_id: str | None = None


class CreateDebateRequest(BaseModel):
    """Payload for posting a debate thread inside a community (Cas_02)."""

    title: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=5000)


class CreateDebateReplyRequest(BaseModel):
    """Payload for replying to a debate thread (Cas_03)."""

    body: str = Field(min_length=1, max_length=5000)


class RoomMessageRequest(BaseModel):
    """Payload for sending a message to a chat room (Cas_13)."""

    content: str = Field(min_length=1, max_length=2000)


class PrivateChatRequest(BaseModel):
    """Payload for sending a direct P2P private message (Cas_23)."""

    peer_id: str = Field(min_length=1)
    peer_username: str = Field(min_length=1)
    peer_ip: str = Field(min_length=1)
    peer_port: int = Field(ge=1, le=65535)
    status: Literal["ONLINE", "AWAY", "OFFLINE"] = "ONLINE"
    content: str = Field(min_length=1, max_length=2000)


class LibrarySaveRequest(BaseModel):
    """Payload for preserving a file reference in personal library (Cas_04)."""

    file_hash: str = Field(min_length=64, max_length=64)
    title: str = Field(min_length=1, max_length=256)
    category: str = Field(default="other", max_length=64)


class ResumeTransferRequest(BaseModel):
    """Optional peer hint payload when resuming a checkpoint after restart (Cas_22)."""

    peer_id: str | None = Field(default=None, min_length=1)
    peer_username: str | None = Field(default=None, min_length=1)
    peer_ip: str | None = Field(default=None, min_length=1)
    peer_port: int | None = Field(default=None, ge=1, le=65535)
