"""Application service for communities, debates, chat rooms, library, and peer blocks."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from bluered_client.contracts import ICommunityActionPort, ICommunitySyncPort
from bluered_client.domain import LocalPeer, SharedFile

_DEFAULT_ROOMS: list[dict[str, Any]] = [
    {
        "id": "general",
        "name": "General",
        "visibility": "public",
        "member_count": 1,
    },
    {
        "id": "music-lossless",
        "name": "Audio & Vinilos",
        "visibility": "public",
        "member_count": 1,
    },
    {
        "id": "papers-open",
        "name": "Documentos Abiertos",
        "visibility": "public",
        "member_count": 1,
    },
]


class CommunityClientService:
    """Orchestrates community/debate/room/library state and server sync (Cas_01-Cas_06, Cas_13)."""

    def __init__(
        self,
        local_peer: LocalPeer,
        community_port: ICommunityActionPort | None = None,
        *,
        known_peers: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._local_peer = local_peer
        self._port = community_port
        self._known_peers = known_peers if known_peers is not None else {}

        self._communities: list[dict[str, Any]] = []
        self._debates_by_community: dict[str, list[dict[str, Any]]] = {}
        self._active_community_id: str | None = None
        self._rooms: list[dict[str, Any]] = [dict(r) for r in _DEFAULT_ROOMS]
        self._room_messages: list[dict[str, Any]] = []
        self._library: list[dict[str, Any]] = []

    @property
    def communities(self) -> list[dict[str, Any]]:
        return list(self._communities)

    @property
    def rooms(self) -> list[dict[str, Any]]:
        return list(self._rooms)

    @property
    def room_messages(self) -> list[dict[str, Any]]:
        return list(self._room_messages)

    @property
    def library(self) -> list[dict[str, Any]]:
        return list(self._library)

    def _resolve_username(self, author_id: str) -> str:
        if author_id == self._local_peer.peer_id:
            return self._local_peer.username
        known = self._known_peers.get(author_id)
        if known and known.get("username"):
            return str(known["username"])
        return author_id[:8]

    def _build_debate_threads(self, raw_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Reconstruct multi-level Cas_04 debate threads via parent_post_id links."""
        if not raw_posts:
            return []
        by_id: dict[str, dict[str, Any]] = {}
        parent_map: dict[str, str | None] = {}
        for item in raw_posts:
            post_id = str(item.get("post_id") or item.get("id") or "")
            if not post_id:
                continue
            parent_id_raw = item.get("parent_post_id")
            parent_id = str(parent_id_raw) if parent_id_raw else None
            author_id = str(item.get("author_id") or "")
            author_username = str(
                item.get("author_username")
                or (self._resolve_username(author_id) if author_id else self._local_peer.username)
            )
            created_at = str(
                item.get("created_at_iso")
                or item.get("created_at")
                or datetime.now(UTC).isoformat()
            )
            by_id[post_id] = {
                "id": post_id,
                "post_id": post_id,
                "community_id": str(item.get("community_id") or ""),
                "author_id": author_id,
                "author_username": author_username,
                "title": str(item.get("title") or ""),
                "body": str(item.get("body") or ""),
                "parent_post_id": parent_id,
                "created_at": created_at,
                "replies": [],
            }
            parent_map[post_id] = parent_id

        roots: list[dict[str, Any]] = []
        replies_to_attach: list[tuple[str, dict[str, Any]]] = []

        for post_id, node in by_id.items():
            parent_id = parent_map.get(post_id)
            if parent_id is None or parent_id not in by_id:
                roots.append(node)
                continue
            # Walk up parent links with cycle protection to find the root debate thread.
            visited: set[str] = {post_id}
            cursor = parent_id
            while parent_map.get(cursor) is not None and parent_map[cursor] in by_id:
                nxt = parent_map[cursor]
                if nxt is None or nxt in visited:
                    break
                visited.add(nxt)
                cursor = nxt
            replies_to_attach.append((cursor, node))

        replies_to_attach.sort(key=lambda pair: str(pair[1].get("created_at", "")))
        for root_id, reply_node in replies_to_attach:
            root = by_id.get(root_id)
            if root is not None:
                reply_entry = {
                    "id": reply_node["id"],
                    "post_id": reply_node["parent_post_id"] or root_id,
                    "root_post_id": root_id,
                    "parent_post_id": reply_node["parent_post_id"],
                    "author_id": reply_node["author_id"],
                    "author_username": reply_node["author_username"],
                    "title": reply_node["title"],
                    "body": reply_node["body"],
                    "created_at": reply_node["created_at"],
                }
                root["replies"].append(reply_entry)

        roots.sort(key=lambda r: str(r.get("created_at", "")), reverse=True)
        return roots

    def apply_server_communities(self, communities: list[dict[str, Any]]) -> None:
        existing_by_id = {c["id"]: c for c in self._communities}
        merged: list[dict[str, Any]] = []
        for item in communities:
            cid = str(item["id"])
            prev = existing_by_id.get(cid, {})
            merged.append(
                {
                    "id": cid,
                    "name": str(item["name"]),
                    "description": str(item.get("description", "")),
                    "parent_id": prev.get("parent_id"),
                    "created_at": prev.get("created_at") or datetime.now(UTC).isoformat(),
                }
            )
        self._communities = merged

    def apply_server_debates(self, community_id: str, posts: list[dict[str, Any]]) -> None:
        self._debates_by_community[community_id] = self._build_debate_threads(posts)

    def apply_server_rooms(self, rooms: list[dict[str, Any]]) -> None:
        existing_vis = {r["id"]: r.get("visibility", "public") for r in self._rooms}
        default_ids = {r["id"] for r in _DEFAULT_ROOMS}
        merged: list[dict[str, Any]] = []
        for item in rooms:
            rid = str(item["id"])
            vis = existing_vis.get(rid, "public" if rid in default_ids else "private")
            merged.append(
                {
                    "id": rid,
                    "name": str(item["name"]),
                    "visibility": vis,
                    "member_count": int(item.get("member_count", 0)),
                }
            )
        self._rooms = merged

    def apply_server_library(self, items: list[dict[str, Any]]) -> None:
        preserved: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()
        for item in items:
            fhash = str(item["file_hash"])
            seen_hashes.add(fhash)
            preserved.append(
                {
                    "file_hash": fhash,
                    "title": str(item["title"]),
                    "category": str(item.get("category", "other")),
                    "saved_at_iso": str(item.get("saved_at_iso") or datetime.now(UTC).isoformat()),
                    "source": "preserved",
                }
            )
        downloaded = [
            dict(entry)
            for entry in self._library
            if entry.get("source") == "downloaded" and entry["file_hash"] not in seen_hashes
        ]
        self._library = preserved + downloaded

    async def sync_all(self, active_community_id: str | None = None) -> None:
        """Pull communities, rooms, library, and active community debates from server."""
        if not isinstance(self._port, ICommunitySyncPort):
            return
        try:
            communities = await self._port.fetch_communities()
            self.apply_server_communities(communities)
        except Exception:
            pass
        try:
            rooms = await self._port.fetch_rooms()
            self.apply_server_rooms(rooms)
        except Exception:
            pass
        try:
            lib_items = await self._port.fetch_library()
            self.apply_server_library(lib_items)
        except Exception:
            pass

        target_cid = active_community_id or self._active_community_id
        if target_cid is None and self._communities:
            target_cid = str(self._communities[0]["id"])
        if target_cid is not None:
            try:
                posts = await self._port.fetch_debates(target_cid)
                self.apply_server_debates(target_cid, posts)
                self._active_community_id = target_cid
            except Exception:
                pass

    async def list_communities(
        self,
        community_id: str | None = None,
        *,
        refresh: bool = True,
    ) -> dict[str, Any]:
        if refresh and isinstance(self._port, ICommunitySyncPort):
            try:
                communities = await self._port.fetch_communities()
                self.apply_server_communities(communities)
            except Exception:
                pass

        target_cid = community_id or self._active_community_id
        if target_cid is None and self._communities:
            target_cid = str(self._communities[0]["id"])

        if (
            refresh
            and target_cid is not None
            and isinstance(self._port, ICommunitySyncPort)
        ):
            try:
                posts = await self._port.fetch_debates(target_cid)
                self.apply_server_debates(target_cid, posts)
            except Exception:
                pass

        if community_id is not None:
            self._active_community_id = community_id
            debates = list(self._debates_by_community.get(community_id, []))
        elif target_cid is not None and target_cid in self._debates_by_community:
            self._active_community_id = target_cid
            debates = list(self._debates_by_community[target_cid])
        else:
            debates = [
                d
                for threads in self._debates_by_community.values()
                for d in threads
            ]

        return {
            "communities": list(self._communities),
            "debates": debates,
        }

    async def create_community(
        self,
        name: str,
        description: str = "",
        parent_id: str | None = None,
    ) -> dict[str, Any]:
        created: dict[str, Any] | None = None
        if self._port is not None:
            created = await self._port.create_community(name, description)

        comm_id = str(created["id"]) if created and created.get("id") else uuid.uuid4().hex[:12]
        item = {
            "id": comm_id,
            "name": str(created["name"]) if created and created.get("name") else name.strip(),
            "description": (
                str(created.get("description", ""))
                if created is not None
                else description.strip()
            ),
            "parent_id": parent_id,
            "created_at": datetime.now(UTC).isoformat(),
        }
        self._communities = [c for c in self._communities if c["id"] != comm_id]
        self._communities.append(item)
        if self._active_community_id is None:
            self._active_community_id = comm_id
        return item

    async def post_debate(
        self,
        community_id: str,
        title: str,
        body: str,
    ) -> dict[str, Any]:
        created: dict[str, Any] | None = None
        if self._port is not None:
            created = await self._port.post_debate(community_id, title, body)

        post_id = (
            str(created.get("post_id") or created.get("id"))
            if created and (created.get("post_id") or created.get("id"))
            else uuid.uuid4().hex[:12]
        )
        post: dict[str, Any] = {
            "id": post_id,
            "post_id": post_id,
            "community_id": community_id,
            "author_id": (
                str(created["author_id"])
                if created and created.get("author_id")
                else self._local_peer.peer_id
            ),
            "author_username": self._local_peer.username,
            "title": str(created["title"]) if created and created.get("title") else title.strip(),
            "body": str(created["body"]) if created and created.get("body") else body.strip(),
            "parent_post_id": None,
            "replies": [],
            "created_at": (
                str(created["created_at_iso"])
                if created and created.get("created_at_iso")
                else datetime.now(UTC).isoformat()
            ),
        }
        bucket = self._debates_by_community.setdefault(community_id, [])
        bucket[:] = [d for d in bucket if d["id"] != post_id]
        bucket.insert(0, post)
        self._active_community_id = community_id
        return post

    async def reply_debate(self, post_id: str, body: str) -> dict[str, Any]:
        created: dict[str, Any] | None = None
        if self._port is not None:
            created = await self._port.reply_debate(post_id, body)

        reply_id = (
            str(created.get("post_id") or created.get("id"))
            if created and (created.get("post_id") or created.get("id"))
            else uuid.uuid4().hex[:12]
        )
        reply = {
            "id": reply_id,
            "post_id": post_id,
            "parent_post_id": post_id,
            "author_id": (
                str(created["author_id"])
                if created and created.get("author_id")
                else self._local_peer.peer_id
            ),
            "author_username": self._local_peer.username,
            "title": str(created.get("title", "")) if created else "",
            "body": str(created["body"]) if created and created.get("body") else body.strip(),
            "created_at": (
                str(created["created_at_iso"])
                if created and created.get("created_at_iso")
                else datetime.now(UTC).isoformat()
            ),
        }
        attached = False
        for threads in self._debates_by_community.values():
            for debate in threads:
                if debate["id"] == post_id or any(
                    r["id"] == post_id for r in debate.get("replies", [])
                ):
                    replies = debate.setdefault("replies", [])
                    if not any(r["id"] == reply_id for r in replies):
                        replies.append(reply)
                    attached = True
                    break
            if attached:
                break
        return reply

    async def list_rooms(self, *, refresh: bool = True) -> dict[str, Any]:
        if refresh and isinstance(self._port, ICommunitySyncPort):
            try:
                rooms = await self._port.fetch_rooms()
                self.apply_server_rooms(rooms)
            except Exception:
                pass
        return {
            "rooms": list(self._rooms),
            "messages": list(self._room_messages[-100:]),
        }

    async def join_room(self, room_id: str) -> dict[str, Any]:
        if self._port is not None:
            rooms = await self._port.join_room(room_id)
            if rooms is not None:
                self.apply_server_rooms(rooms)
        if not any(r["id"] == room_id for r in self._rooms):
            self._rooms.append(
                {
                    "id": room_id,
                    "name": room_id,
                    "visibility": "private",
                    "member_count": 1,
                }
            )
        return {"room_id": room_id, "joined": True}

    async def send_room_message(self, room_id: str, content: str) -> dict[str, Any]:
        if self._port is not None:
            await self._port.send_room_message(
                room_id=room_id,
                sender_id=self._local_peer.peer_id,
                sender_username=self._local_peer.username,
                content=content,
            )
        payload = {
            "room_id": room_id,
            "sender_id": self._local_peer.peer_id,
            "sender_username": self._local_peer.username,
            "content": content.strip(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        self._room_messages.append(payload)
        return payload

    def record_incoming_room_message(self, payload: dict[str, Any]) -> None:
        self._room_messages.append(payload)

    async def list_library(self, *, refresh: bool = True) -> list[dict[str, Any]]:
        if refresh and isinstance(self._port, ICommunitySyncPort):
            try:
                items = await self._port.fetch_library()
                self.apply_server_library(items)
            except Exception:
                pass
        return list(self._library)

    async def save_to_library(
        self,
        file_hash: str,
        title: str,
        category: str = "other",
    ) -> dict[str, Any]:
        SharedFile(file_hash=file_hash, file_name=title, size_bytes=0)
        saved: dict[str, Any] | None = None
        if self._port is not None:
            saved = await self._port.save_to_library(file_hash, title)
        entry = {
            "file_hash": file_hash,
            "title": str(saved["title"]) if saved and saved.get("title") else title.strip(),
            "category": category,
            "saved_at_iso": (
                str(saved["saved_at_iso"])
                if saved and saved.get("saved_at_iso")
                else datetime.now(UTC).isoformat()
            ),
            "source": "preserved",
        }
        self._library = [i for i in self._library if i["file_hash"] != file_hash]
        self._library.insert(0, entry)
        return entry

    def remember_downloaded_file(self, shared_file: SharedFile) -> None:
        if any(i["file_hash"] == shared_file.file_hash for i in self._library):
            return
        self._library.insert(
            0,
            {
                "file_hash": shared_file.file_hash,
                "title": shared_file.file_name,
                "category": shared_file.category,
                "saved_at_iso": datetime.now(UTC).isoformat(),
                "source": "downloaded",
            },
        )

    def block_peer(self, peer_id: str) -> list[str]:
        self._local_peer.block_peer(peer_id)
        return sorted(self._local_peer.blocked_peer_ids)

    def unblock_peer(self, peer_id: str) -> list[str]:
        self._local_peer.unblock_peer(peer_id)
        return sorted(self._local_peer.blocked_peer_ids)

    def list_blocked_peers(self) -> list[str]:
        return sorted(self._local_peer.blocked_peer_ids)
