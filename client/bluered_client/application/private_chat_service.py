"""PrivateChatService for direct P2P messaging and SQLite offline queue (Cas_23)."""

from __future__ import annotations

import uuid

from bluered_client.contracts import IPeerTransport, IPendingMessageStore
from bluered_client.domain import (
    LocalPeer,
    PeerBlockedError,
    PresenceStatus,
    PrivateMessage,
    RemotePeer,
)


class PrivateChatService:
    """Use-case service for P2P private chat with blocklist enforcement & offline queue."""

    def __init__(
        self,
        local_peer: LocalPeer,
        peer_transport: IPeerTransport,
        pending_store: IPendingMessageStore,
    ) -> None:
        self._local_peer = local_peer
        self._transport = peer_transport
        self._pending_store = pending_store

    async def send_message(
        self,
        peer: RemotePeer,
        content: str,
        *,
        message_id: str | None = None,
    ) -> PrivateMessage:
        """Send a P2P private message or queue it in SQLite if peer is OFFLINE/unreachable."""
        msg = PrivateMessage(
            message_id=message_id or uuid.uuid4().hex,
            sender_id=self._local_peer.peer_id,
            recipient_id=peer.peer_id,
            content=content,
            delivered=False,
        )
        if peer.status != PresenceStatus.ONLINE:
            await self._pending_store.enqueue_pending_message(msg)
            return msg

        try:
            delivered = await self._transport.send_private_message(peer, msg)
        except PeerBlockedError:
            raise

        if delivered:
            msg.mark_delivered()
        else:
            await self._pending_store.enqueue_pending_message(msg)
        return msg

    async def deliver_pending_messages(self, peer: RemotePeer) -> list[PrivateMessage]:
        """Flush queued SQLite messages to a peer that has come back ONLINE (Cas_23)."""
        if peer.status != PresenceStatus.ONLINE:
            return []

        pending = await self._pending_store.list_pending_messages(recipient_id=peer.peer_id)
        delivered_messages: list[PrivateMessage] = []
        for msg in pending:
            delivered = await self._transport.send_private_message(peer, msg)
            if not delivered:
                break
            msg.mark_delivered()
            await self._pending_store.mark_message_delivered(msg.message_id)
            delivered_messages.append(msg)
        return delivered_messages

    async def list_pending_messages(self, recipient_id: str | None = None) -> list[PrivateMessage]:
        """List undelivered private messages stored in the local pending store."""
        return await self._pending_store.list_pending_messages(recipient_id=recipient_id)
