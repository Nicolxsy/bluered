"""Local Web UI presentation layer for BlueRed P2P client."""

from __future__ import annotations

from .app import ClientAppContext, create_client_app
from .event_bus import WebSocketTransferEventListener, serialize_transfer_task

__all__ = [
    "ClientAppContext",
    "WebSocketTransferEventListener",
    "create_client_app",
    "serialize_transfer_task",
]
