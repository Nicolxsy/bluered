"""Pure stdlib Domain Core for BlueRed client."""

from __future__ import annotations

from .enums import PresenceStatus, TransferState
from .errors import DomainError, DomainValidationError, InvalidStateTransitionError
from .files import FileReview, SearchMatch, SharedFile
from .messaging import PrivateMessage
from .peers import LocalPeer, PeerNode, RemotePeer
from .transfer import VALID_TRANSITIONS, TransferTask

__all__ = [
    "VALID_TRANSITIONS",
    "DomainError",
    "DomainValidationError",
    "FileReview",
    "InvalidStateTransitionError",
    "LocalPeer",
    "PeerNode",
    "PresenceStatus",
    "PrivateMessage",
    "RemotePeer",
    "SearchMatch",
    "SharedFile",
    "TransferState",
    "TransferTask",
]
