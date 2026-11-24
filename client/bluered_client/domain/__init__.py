"""Pure stdlib Domain Core for BlueRed client."""

from __future__ import annotations

from .enums import PresenceStatus, TransferState
from .errors import (
    DomainError,
    DomainValidationError,
    InsufficientDiskSpaceError,
    InvalidStateTransitionError,
    NatTraversalError,
    PeerBlockedError,
    PeerTransportError,
    QueueFullError,
    RemoteFileChangedError,
    TransferIntegrityError,
)
from .files import FileReview, SearchMatch, SharedFile
from .messaging import PrivateMessage
from .peers import LocalPeer, PeerNode, RemotePeer
from .transfer import VALID_TRANSITIONS, DownloadCheckpoint, TransferTask

__all__ = [
    "VALID_TRANSITIONS",
    "DomainError",
    "DomainValidationError",
    "DownloadCheckpoint",
    "FileReview",
    "InsufficientDiskSpaceError",
    "InvalidStateTransitionError",
    "LocalPeer",
    "NatTraversalError",
    "PeerBlockedError",
    "PeerNode",
    "PeerTransportError",
    "PresenceStatus",
    "PrivateMessage",
    "QueueFullError",
    "RemoteFileChangedError",
    "RemotePeer",
    "SearchMatch",
    "SharedFile",
    "TransferIntegrityError",
    "TransferState",
    "TransferTask",
]
