"""Community, debate forum, chat room, and personal library service (Cas_01..Cas_06, Cas_13)."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import uuid4

from bluered_contracts.control_plane import RoomMessage, serialize_control_message

from bluered_server.contracts import IClientPushGateway, ICommunityRepository
from bluered_server.domain import ChatRoom, Community, DebatePost, LibraryItem

from .errors import NotFoundError, ValidationError


class CommunityAndChatService:
    """Orchestrates thematic communities, debate threads, chat rooms, and personal library."""

    def __init__(
        self,
        community_repo: ICommunityRepository,
        push_gateway: IClientPushGateway | None = None,
    ) -> None:
        self._community_repo = community_repo
        self._push_gateway = push_gateway

    async def create_community(
        self,
        *,
        name: str,
        description: str,
        creator_id: str,
    ) -> Community:
        """Create a thematic community with the creator as initial member (Cas_01)."""
        community = Community(
            community_id=uuid4().hex,
            name=name.strip(),
            description=description.strip(),
            creator_id=creator_id,
            member_ids={creator_id},
        )
        await self._community_repo.create_community(community)
        return community

    async def list_communities(self) -> list[Community]:
        """List all communities (Cas_02)."""
        return await self._community_repo.list_communities()

    async def get_community(self, community_id: str) -> Community:
        """Fetch a single community by ID."""
        community = await self._community_repo.get_community(community_id)
        if community is None:
            raise NotFoundError(f"Community '{community_id}' not found")
        return community

    async def create_debate_post(
        self,
        *,
        community_id: str,
        author_id: str,
        title: str,
        body: str,
        parent_post_id: str | None = None,
    ) -> DebatePost:
        """Create a debate thread post or reply inside a community (Cas_03, Cas_04)."""
        community = await self._community_repo.get_community(community_id)
        if community is None:
            raise NotFoundError(f"Community '{community_id}' not found")
        if parent_post_id is not None:
            parent = await self._community_repo.get_debate_post(parent_post_id)
            if parent is None:
                raise NotFoundError(f"Parent debate post '{parent_post_id}' not found")
            if parent.community_id != community_id:
                raise ValidationError(
                    f"Parent post '{parent_post_id}' belongs to a different community"
                )

        post = DebatePost(
            post_id=uuid4().hex,
            community_id=community_id,
            author_id=author_id,
            title=title.strip(),
            body=body.strip(),
            parent_post_id=parent_post_id,
        )
        await self._community_repo.create_debate_post(post)
        return post

    async def reply_debate(
        self,
        *,
        post_id: str,
        author_id: str,
        body: str,
    ) -> DebatePost:
        """Create a reply to an existing debate post, inheriting its community (Cas_04)."""
        parent = await self._community_repo.get_debate_post(post_id)
        if parent is None:
            raise NotFoundError(f"Debate post '{post_id}' not found")
        reply_title = (
            parent.title[:200] if parent.title.startswith("Re: ") else f"Re: {parent.title}"[:200]
        )
        return await self.create_debate_post(
            community_id=parent.community_id,
            author_id=author_id,
            title=reply_title,
            body=body,
            parent_post_id=post_id,
        )

    async def list_debate_posts(self, community_id: str) -> list[DebatePost]:
        """List debate posts for a community."""
        return await self._community_repo.list_debate_posts(community_id)

    async def list_chat_rooms(self) -> list[ChatRoom]:
        """List available chat rooms (Cas_05, Cas_06)."""
        return await self._community_repo.list_chat_rooms()

    async def join_chat_room(self, room_id: str, user_id: str) -> ChatRoom:
        """Join a chat room and return updated ChatRoom (Cas_05, Cas_13)."""
        room = await self._community_repo.join_chat_room(room_id, user_id)
        if room is None:
            raise NotFoundError(f"Chat room '{room_id}' not found")
        return room

    async def send_room_message(
        self,
        *,
        room_id: str,
        sender_id: str,
        sender_username: str,
        content: str,
        recipient_ids: Sequence[str] | None = None,
    ) -> int:
        """Broadcast a ROOM_MESSAGE to members of a chat room."""
        if not content.strip():
            raise ValidationError("Room message content must be non-empty")
        if self._push_gateway is None:
            return 0

        targets = list(recipient_ids) if recipient_ids is not None else []
        if not targets:
            rooms = await self._community_repo.list_chat_rooms()
            for room in rooms:
                if room.room_id == room_id:
                    targets = [uid for uid in room.member_ids if uid != sender_id]
                    break

        if not targets:
            return 0

        payload = serialize_control_message(
            RoomMessage(
                room_id=room_id,
                sender_id=sender_id,
                sender_username=sender_username,
                content=content.strip(),
            )
        )
        return await self._push_gateway.broadcast(targets, payload)

    async def save_library_item(
        self,
        *,
        owner_id: str,
        file_hash: str,
        title: str,
    ) -> LibraryItem:
        """Save a file hash reference to a user's personal library (Cas_04 / Cas_13)."""
        item = LibraryItem(
            item_id=uuid4().hex,
            owner_id=owner_id,
            file_hash=file_hash,
            title=title.strip(),
        )
        await self._community_repo.save_library_item(item)
        return item

    async def list_library_items(self, owner_id: str) -> list[LibraryItem]:
        """List saved personal library items for a user."""
        return await self._community_repo.list_library_items(owner_id)
