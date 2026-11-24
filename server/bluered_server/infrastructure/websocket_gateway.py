"""WebSocket client push gateway for BlueRed central server."""

from __future__ import annotations

from collections.abc import Sequence

from fastapi import WebSocket

from bluered_server.contracts import IWebSocketGateway


class WebSocketClientGateway(IWebSocketGateway):
    """Tracks active WebSocket connections per user_id and pushes control-plane payloads."""

    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}

    def register_connection(self, user_id: str, websocket: WebSocket) -> None:
        self._connections[user_id] = websocket

    def unregister_connection(self, user_id: str, websocket: WebSocket | None = None) -> None:
        current = self._connections.get(user_id)
        if current is None:
            return
        if websocket is None or current is websocket:
            self._connections.pop(user_id, None)

    async def send_to_peer(self, user_id: str, payload_json: str) -> bool:
        ws = self._connections.get(user_id)
        if ws is None:
            return False
        try:
            await ws.send_text(payload_json)
            return True
        except Exception:
            self.unregister_connection(user_id, ws)
            return False

    async def broadcast(self, user_ids: Sequence[str], payload_json: str) -> int:
        sent = 0
        for user_id in user_ids:
            if await self.send_to_peer(user_id, payload_json):
                sent += 1
        return sent

    async def is_connected(self, user_id: str) -> bool:
        return user_id in self._connections

    async def close_all(self) -> None:
        for user_id, ws in list(self._connections.items()):
            try:
                await ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
            finally:
                self._connections.pop(user_id, None)
