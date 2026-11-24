"""Pydantic v2 discriminated-union contracts for IControlPlane (JSON/WebSockets)."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

PROTOCOL_VERSION = "1.0"
SHA256_HEX_PATTERN = r"^[0-9a-f]{64}$"


class ControlPlaneMessage(BaseModel):
    """Base schema for all control-plane JSON messages."""

    model_config = ConfigDict(extra="forbid")
    protocol_version: str = PROTOCOL_VERSION


class SearchHitItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    filename: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    peer_id: str = Field(min_length=1)
    peer_username: str = Field(min_length=1)
    peer_ip: str = Field(min_length=1)
    peer_port: int = Field(ge=1, le=65535)
    avg_rating: float | None = Field(default=None, ge=0.0, le=5.0)
    review_count: int = Field(default=0, ge=0)


class ReviewItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    author_id: str = Field(min_length=1)
    author_username: str = Field(min_length=1)
    rating: int = Field(ge=1, le=5)
    comment: str = Field(default="", max_length=2000)
    updated_at_iso: str = Field(min_length=1)


class CommunitySummary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = ""


class LibraryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    title: str = Field(min_length=1)
    saved_at_iso: str = Field(min_length=1)


class RoomSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    member_count: int = Field(default=0, ge=0)


# --- Auth & Presence (Cas_07, Cas_08) ---


class AuthRegisterMessage(ControlPlaneMessage):
    type: Literal["AUTH_REGISTER"] = "AUTH_REGISTER"
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=6, max_length=256)


class AuthLoginMessage(ControlPlaneMessage):
    type: Literal["AUTH_LOGIN"] = "AUTH_LOGIN"
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=256)
    tcp_port: int = Field(ge=1, le=65535)


class AuthOkMessage(ControlPlaneMessage):
    type: Literal["AUTH_OK"] = "AUTH_OK"
    user_id: str = Field(min_length=1)
    username: str = Field(min_length=1)
    session_token: str = Field(min_length=1)


class AuthErrorMessage(ControlPlaneMessage):
    type: Literal["AUTH_ERROR"] = "AUTH_ERROR"
    code: str = Field(min_length=1)
    detail: str = Field(min_length=1)


class HeartbeatMessage(ControlPlaneMessage):
    type: Literal["HEARTBEAT"] = "HEARTBEAT"
    user_id: str = Field(min_length=1)
    tcp_port: int = Field(ge=1, le=65535)


class PresenceUpdateMessage(ControlPlaneMessage):
    type: Literal["PRESENCE_UPDATE"] = "PRESENCE_UPDATE"
    user_id: str = Field(min_length=1)
    username: str = Field(min_length=1)
    online: bool
    ip: str | None = None
    tcp_port: int | None = Field(default=None, ge=1, le=65535)


class BloomSyncMessage(ControlPlaneMessage):
    type: Literal["BLOOM_SYNC"] = "BLOOM_SYNC"
    user_id: str = Field(min_length=1)
    size_bits: int = Field(gt=0)
    hash_functions_k: int = Field(ge=1)
    bitset_base64: str = Field(min_length=1)


# --- Federated Search (Cas_11, Cas_12) ---


class SearchQueryMessage(ControlPlaneMessage):
    type: Literal["SEARCH_QUERY"] = "SEARCH_QUERY"
    search_token: str = Field(min_length=1)
    query: str = Field(min_length=1, max_length=256)
    max_results: int = Field(default=50, ge=1, le=200)


class SearchBroadcastMessage(ControlPlaneMessage):
    type: Literal["SEARCH_BROADCAST"] = "SEARCH_BROADCAST"
    search_token: str = Field(min_length=1)
    requester_id: str = Field(min_length=1)
    query: str = Field(min_length=1, max_length=256)


class SearchHitReportMessage(ControlPlaneMessage):
    type: Literal["SEARCH_HIT_REPORT"] = "SEARCH_HIT_REPORT"
    search_token: str = Field(min_length=1)
    hits: list[SearchHitItem] = Field(default_factory=list)


class SearchResultsMessage(ControlPlaneMessage):
    type: Literal["SEARCH_RESULTS"] = "SEARCH_RESULTS"
    search_token: str = Field(min_length=1)
    results: list[SearchHitItem] = Field(default_factory=list)


# --- Centralized Reviews (Cas_14, Cas_15, Cas_24) ---


class ReviewPublishMessage(ControlPlaneMessage):
    type: Literal["REVIEW_PUBLISH"] = "REVIEW_PUBLISH"
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    rating: int = Field(ge=1, le=5)
    comment: str = Field(default="", max_length=2000)


class ReviewAckMessage(ControlPlaneMessage):
    type: Literal["REVIEW_ACK"] = "REVIEW_ACK"
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    review_count: int = Field(ge=0)
    avg_rating: float = Field(ge=0.0, le=5.0)


class ReviewsFetchMessage(ControlPlaneMessage):
    type: Literal["REVIEWS_FETCH"] = "REVIEWS_FETCH"
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)


class ReviewsListMessage(ControlPlaneMessage):
    type: Literal["REVIEWS_LIST"] = "REVIEWS_LIST"
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    review_count: int = Field(ge=0)
    avg_rating: float = Field(ge=0.0, le=5.0)
    reviews: list[ReviewItem] = Field(default_factory=list)


# --- Community & Chat (Cas_01-Cas_06, Cas_13) ---


class CommunityCreateMessage(ControlPlaneMessage):
    type: Literal["COMMUNITY_CREATE"] = "COMMUNITY_CREATE"
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)


class CommunityListMessage(ControlPlaneMessage):
    type: Literal["COMMUNITY_LIST"] = "COMMUNITY_LIST"
    communities: list[CommunitySummary] = Field(default_factory=list)


class DebatePostMessage(ControlPlaneMessage):
    type: Literal["DEBATE_POST"] = "DEBATE_POST"
    community_id: str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=5000)


class DebateReplyMessage(ControlPlaneMessage):
    type: Literal["DEBATE_REPLY"] = "DEBATE_REPLY"
    post_id: str = Field(min_length=1)
    body: str = Field(min_length=1, max_length=5000)


class LibrarySaveMessage(ControlPlaneMessage):
    type: Literal["LIBRARY_SAVE"] = "LIBRARY_SAVE"
    file_hash: str = Field(pattern=SHA256_HEX_PATTERN)
    title: str = Field(min_length=1, max_length=256)


class LibraryListMessage(ControlPlaneMessage):
    type: Literal["LIBRARY_LIST"] = "LIBRARY_LIST"
    items: list[LibraryItem] = Field(default_factory=list)


class RoomListMessage(ControlPlaneMessage):
    type: Literal["ROOM_LIST"] = "ROOM_LIST"
    rooms: list[RoomSummary] = Field(default_factory=list)


class RoomJoinMessage(ControlPlaneMessage):
    type: Literal["ROOM_JOIN"] = "ROOM_JOIN"
    room_id: str = Field(min_length=1)


class RoomMessage(ControlPlaneMessage):
    type: Literal["ROOM_MESSAGE"] = "ROOM_MESSAGE"
    room_id: str = Field(min_length=1)
    sender_id: str = Field(min_length=1)
    sender_username: str = Field(min_length=1)
    content: str = Field(min_length=1, max_length=2000)


# --- NAT Signaling (Cas_20) ---


class NatPunchRequestMessage(ControlPlaneMessage):
    type: Literal["NAT_PUNCH_REQUEST"] = "NAT_PUNCH_REQUEST"
    target_peer_id: str = Field(min_length=1)
    requester_ip: str = Field(min_length=1)
    requester_port: int = Field(ge=1, le=65535)


class NatPunchCoordinateMessage(ControlPlaneMessage):
    type: Literal["NAT_PUNCH_COORDINATE"] = "NAT_PUNCH_COORDINATE"
    peer_id: str = Field(min_length=1)
    peer_ip: str = Field(min_length=1)
    peer_port: int = Field(ge=1, le=65535)
    rendezvous_at_epoch_ms: int = Field(ge=0)


class NatRelayRequestMessage(ControlPlaneMessage):
    type: Literal["NAT_RELAY_REQUEST"] = "NAT_RELAY_REQUEST"
    target_peer_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)


ControlPlaneEnvelope = Annotated[
    AuthRegisterMessage
    | AuthLoginMessage
    | AuthOkMessage
    | AuthErrorMessage
    | HeartbeatMessage
    | PresenceUpdateMessage
    | BloomSyncMessage
    | SearchQueryMessage
    | SearchBroadcastMessage
    | SearchHitReportMessage
    | SearchResultsMessage
    | ReviewPublishMessage
    | ReviewAckMessage
    | ReviewsFetchMessage
    | ReviewsListMessage
    | CommunityCreateMessage
    | CommunityListMessage
    | DebatePostMessage
    | DebateReplyMessage
    | LibrarySaveMessage
    | LibraryListMessage
    | RoomListMessage
    | RoomJoinMessage
    | RoomMessage
    | NatPunchRequestMessage
    | NatPunchCoordinateMessage
    | NatRelayRequestMessage,
    Field(discriminator="type"),
]

CONTROL_PLANE_ADAPTER: TypeAdapter[ControlPlaneEnvelope] = TypeAdapter(ControlPlaneEnvelope)


def parse_control_message(raw: str | bytes) -> ControlPlaneEnvelope:
    """Validate and deserialize a control-plane JSON payload into its concrete message model."""
    return CONTROL_PLANE_ADAPTER.validate_json(raw)


def serialize_control_message(msg: ControlPlaneEnvelope) -> str:
    """Serialize a control-plane message model to JSON."""
    return msg.model_dump_json()
