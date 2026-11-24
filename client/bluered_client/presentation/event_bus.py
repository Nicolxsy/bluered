"""Real-time WebSocket event bus implementing ITransferEventListener (GoF Observer)."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any

from fastapi import WebSocket

from bluered_client.domain import PrivateMessage, TransferState, TransferTask


def serialize_transfer_task(
    task: TransferTask,
    *,
    target_path: str | None = None,
) -> dict[str, Any]:
    """Convert a TransferTask domain entity into a JSON-serializable dict."""
    progress_percent = round(task.progress_ratio * 100.0, 2)
    return {
        "task_id": task.task_id,
        "file_hash": task.file_hash,
        "file_name": task.file_name,
        "total_bytes": task.total_bytes,
        "bytes_transferred": task.bytes_transferred,
        "progress_ratio": task.progress_ratio,
        "progress_percent": progress_percent,
        "state": task.state.value,
        "retry_count": task.retry_count,
        "max_retries": task.max_retries,
        "queue_position": task.queue_position,
        "remote_peer_id": task.remote_peer_id,
        "error_message": task.error_message,
        "target_path": target_path,
        "updated_at": task.updated_at.isoformat(),
    }


class WebSocketTransferEventListener:
    """Broadcasts transfer, chat, and presence events to connected browser UI sockets."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._task_snapshots: dict[str, dict[str, Any]] = {}
        self._event_history: list[dict[str, Any]] = []

    @property
    def active_connections_count(self) -> int:
        return len(self._connections)

    @property
    def event_history(self) -> list[dict[str, Any]]:
        return list(self._event_history)

    async def connect(self, websocket: WebSocket) -> None:
        """Accept and register a browser WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        """Unregister a disconnected browser WebSocket."""
        async with self._lock:
            self._connections.discard(websocket)

    async def close_all(self) -> None:
        """Close all connected browser WebSocket sessions on shutdown."""
        async with self._lock:
            sockets = list(self._connections)
            self._connections.clear()
        for ws in sockets:
            with contextlib.suppress(Exception):
                await ws.close(code=1001, reason="Client UI shutting down")

    def record_task_snapshot(
        self,
        task: TransferTask,
        *,
        target_path: str | None = None,
    ) -> dict[str, Any]:
        """Store or update the latest serialized snapshot of a TransferTask."""
        existing = self._task_snapshots.get(task.task_id)
        resolved_path = target_path or (
            str(existing["target_path"])
            if existing is not None and existing.get("target_path") is not None
            else None
        )
        snapshot = serialize_transfer_task(task, target_path=resolved_path)
        self._task_snapshots[task.task_id] = snapshot
        return snapshot

    def list_task_snapshots(self) -> list[dict[str, Any]]:
        """Return all known transfer task snapshots ordered by most recently updated."""
        return sorted(
            self._task_snapshots.values(),
            key=lambda item: str(item.get("updated_at", "")),
            reverse=True,
        )

    def get_task_snapshot(self, task_id: str) -> dict[str, Any] | None:
        """Return the latest snapshot for a single transfer task."""
        return self._task_snapshots.get(task_id)

    async def broadcast_event(self, event_type: str, payload: dict[str, Any]) -> int:
        """Broadcast a structured JSON event to all connected UI WebSocket clients."""
        envelope: dict[str, Any] = {
            "event": event_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "data": payload,
        }
        self._event_history.append(envelope)
        if len(self._event_history) > 200:
            self._event_history = self._event_history[-200:]

        async with self._lock:
            targets = list(self._connections)

        delivered = 0
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(envelope)
                delivered += 1
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections.discard(ws)
        return delivered

    # --- ITransferEventListener implementation ---

    async def on_state_changed(
        self,
        task: TransferTask,
        previous_state: TransferState,
    ) -> None:
        """Emit transfer_state_changed when TransferTask transitions in the FSM."""
        snapshot = self.record_task_snapshot(task)
        await self.broadcast_event(
            "transfer_state_changed",
            {
                **snapshot,
                "previous_state": previous_state.value,
            },
        )

    async def on_progress(self, task: TransferTask) -> None:
        """Emit transfer_progress as verified chunks are written to disk."""
        snapshot = self.record_task_snapshot(task)
        await self.broadcast_event("transfer_progress", snapshot)

    async def on_error(self, task: TransferTask, error_message: str) -> None:
        """Emit transfer_error when a transfer fails."""
        snapshot = self.record_task_snapshot(task)
        await self.broadcast_event(
            "transfer_error",
            {
                **snapshot,
                "error_message": error_message,
            },
        )

    # --- Auxiliary UI real-time events ---

    async def publish_private_message(self, message: PrivateMessage) -> int:
        """Notify browser tabs of an incoming or sent P2P private message."""
        return await self.broadcast_event(
            "private_message",
            {
                "message_id": message.message_id,
                "sender_id": message.sender_id,
                "recipient_id": message.recipient_id,
                "content": message.content,
                "delivered": message.delivered,
                "created_at": message.created_at.isoformat(),
            },
        )

    async def publish_room_message(self, payload: dict[str, Any]) -> int:
        """Notify browser tabs of an incoming chat room message."""
        return await self.broadcast_event("room_message", payload)

    async def publish_presence_update(self, payload: dict[str, Any]) -> int:
        """Notify browser tabs of a peer presence update."""
        return await self.broadcast_event("presence_update", payload)
