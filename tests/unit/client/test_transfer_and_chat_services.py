"""Unit tests for TransferOrchestrator and PrivateChatService (Fig. 56, Fig. 57, Cas_17-23)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import NamedTuple

import pytest
from bluered_client.application import PrivateChatService, TransferOrchestrator
from bluered_client.domain import (
    DomainValidationError,
    InsufficientDiskSpaceError,
    LocalPeer,
    PeerBlockedError,
    PeerTransportError,
    PresenceStatus,
    PrivateMessage,
    RemoteFileChangedError,
    RemotePeer,
    SharedFile,
    TransferState,
    TransferTask,
)
from bluered_client.infrastructure import (
    CompositeNatTraversalStrategy,
    DirectSocketNatStrategy,
    RawTcpPeerTransport,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
)


class _DiskUsage(NamedTuple):
    total: int
    used: int
    free: int


class _FakeTransport:
    def __init__(self, chunks: list[bytes], *, fail_with: Exception | None = None) -> None:
        self._chunks = chunks
        self._fail_with = fail_with
        self._integrity = Sha256IntegrityVerifier()
        self.requested_offsets: list[int] = []
        self.blocked = False
        self.online = True

    async def stream_file_download(
        self,
        peer: RemotePeer,
        file_hash: str,
        offset: int = 0,
        *,
        on_queue_status: Callable[[int], Awaitable[None]] | None = None,
        on_slot_available: Callable[[], Awaitable[None]] | None = None,
    ) -> AsyncIterator[tuple[int, bytes, str]]:
        del peer, file_hash, on_queue_status
        self.requested_offsets.append(offset)
        if on_slot_available is not None:
            await on_slot_available()
        if self._fail_with is not None:
            raise self._fail_with
        pos = 0
        for idx, chunk in enumerate(self._chunks):
            end = pos + len(chunk)
            if end <= offset:
                pos = end
                continue
            slice_data = chunk[max(0, offset - pos) :]
            yield (idx, slice_data, self._integrity.compute_chunk_hash(slice_data))
            pos = end

    async def request_file_chunks(
        self, peer: RemotePeer, file_hash: str, offset: int = 0
    ) -> AsyncIterator[bytes]:
        async for _, payload, _ in self.stream_file_download(peer, file_hash, offset=offset):
            yield payload

    async def browse_peer_files(self, peer: RemotePeer) -> list[SharedFile]:
        del peer
        return []

    async def send_private_message(self, peer: RemotePeer, message: PrivateMessage) -> bool:
        del peer, message
        if self.blocked:
            raise PeerBlockedError("403 blocked")
        return self.online

    async def close(self) -> None:
        pass


class _RecordingListener:
    def __init__(self) -> None:
        self.states: list[tuple[TransferState, TransferState]] = []
        self.progress: list[int] = []
        self.errors: list[str] = []

    async def on_state_changed(
        self, task: TransferTask, previous_state: TransferState
    ) -> None:
        self.states.append((previous_state, task.state))

    async def on_progress(self, task: TransferTask) -> None:
        self.progress.append(task.bytes_transferred)

    async def on_error(self, task: TransferTask, error_message: str) -> None:
        del task
        self.errors.append(error_message)


def _local() -> LocalPeer:
    return LocalPeer(peer_id="local-1", username="alice", ip_address="127.0.0.1", tcp_port=9100)


def _remote() -> RemotePeer:
    return RemotePeer(
        peer_id="remote-1",
        username="bob",
        ip_address="127.0.0.1",
        tcp_port=9101,
        status=PresenceStatus.ONLINE,
    )


async def test_orchestrator_checks_disk_space_and_collision_policies(tmp_path: Path) -> None:
    payload = b"hello-bluered-chunk-1" + b"hello-bluered-chunk-2"
    file_hash = hashlib.sha256(payload).hexdigest()
    sf = SharedFile(file_hash=file_hash, file_name="notes.txt", size_bytes=len(payload))
    transport = _FakeTransport([b"hello-bluered-chunk-1", b"hello-bluered-chunk-2"])
    listener = _RecordingListener()

    # Insufficient disk space
    low_disk_orch = TransferOrchestrator(
        _local(),
        transport,
        Sha256IntegrityVerifier(),
        tmp_path,
        listeners=[listener],
        disk_usage_fn=lambda _: _DiskUsage(1000, 995, 5),
    )
    with pytest.raises(InsufficientDiskSpaceError):
        await low_disk_orch.start_download(_remote(), sf)
    assert len(listener.errors) == 1

    # Collision keep_both vs replace
    orch = TransferOrchestrator(
        _local(),
        transport,
        Sha256IntegrityVerifier(),
        tmp_path,
        disk_usage_fn=lambda _: _DiskUsage(10_000_000, 0, 10_000_000),
    )
    (tmp_path / "notes.txt").write_bytes(b"existing")
    t1 = await orch.start_download(_remote(), sf, conflict_policy="keep_both")
    assert t1.state == TransferState.COMPLETED
    target1 = orch.get_target_path(t1.task_id)
    assert target1 is not None and target1.name == "notes (1).txt"
    assert target1.read_bytes() == payload

    t2 = await orch.start_download(_remote(), sf, conflict_policy="replace")
    assert t2.state == TransferState.COMPLETED
    target2 = orch.get_target_path(t2.task_id)
    assert target2 is not None and target2.name == "notes.txt"
    assert target2.read_bytes() == payload


async def test_orchestrator_cancel_deletes_part_and_409_stops_resume(tmp_path: Path) -> None:
    payload = b"A" * 4096
    file_hash = hashlib.sha256(payload).hexdigest()
    sf = SharedFile(file_hash=file_hash, file_name="data.bin", size_bytes=len(payload))

    transport = _FakeTransport([payload[:2048]], fail_with=OSError("drop"))
    async with SqliteFileRepository(tmp_path / "client.db") as repo:
        orch = TransferOrchestrator(
            _local(),
            transport,
            Sha256IntegrityVerifier(),
            tmp_path / "downloads",
            checkpoint_store=repo,
            disk_usage_fn=lambda _: _DiskUsage(10_000_000, 0, 10_000_000),
        )
        t = await orch.start_download(_remote(), sf, task_id="task-cancel")
        assert t.state == TransferState.FAILED
        part_path = (tmp_path / "downloads") / "data.bin.part"
        part_path.write_bytes(b"partial-bytes")

        # Cancel deletes .part
        await orch.cancel_download("task-cancel")
        assert t.state == TransferState.CANCELLED
        assert not part_path.exists()

        # 409 RemoteFileChangedError stops resume immediately
        t2 = await orch.start_download(_remote(), sf, task_id="task-409", max_retries=3)
        assert t2.state == TransferState.FAILED
        transport._fail_with = RemoteFileChangedError("Remote file hash changed")
        resumed = await orch.resume_download("task-409", _remote())
        assert resumed.state == TransferState.FAILED
        assert resumed.retry_count == 1


async def test_private_chat_service_validation_offline_and_blocked(tmp_path: Path) -> None:
    transport = _FakeTransport([])
    async with SqliteFileRepository(tmp_path / "chat.db") as repo:
        svc = PrivateChatService(_local(), transport, repo)

        with pytest.raises(DomainValidationError):
            await svc.send_message(_remote(), "   ")

        offline_peer = RemotePeer(
            peer_id="peer-offline",
            username="carol",
            ip_address="127.0.0.1",
            tcp_port=9102,
            status=PresenceStatus.OFFLINE,
        )
        queued = await svc.send_message(offline_peer, "See you online!")
        assert queued.delivered is False
        assert len(await repo.list_pending_messages("peer-offline")) == 1

        offline_peer.status = PresenceStatus.ONLINE
        flushed = await svc.deliver_pending_messages(offline_peer)
        assert len(flushed) == 1
        assert flushed[0].delivered is True
        assert await repo.list_pending_messages("peer-offline") == []

        transport.blocked = True
        with pytest.raises(PeerBlockedError):
            await svc.send_message(_remote(), "Blocked msg")


async def test_orchestrator_resumes_crash_interrupted_checkpoint_and_handles_transport_error(
    tmp_path: Path,
) -> None:
    payload = b"0123456789ABCDEF" * 64
    file_hash = hashlib.sha256(payload).hexdigest()
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    target_path = downloads / "crashed.bin"
    part_path = downloads / "crashed.bin.part"
    part_path.write_bytes(payload[:512])

    async with SqliteFileRepository(tmp_path / "resume.db") as repo:
        await repo.save_download_checkpoint(
            task_id="task-crash-1",
            file_hash=file_hash,
            file_name="crashed.bin",
            target_path=target_path,
            total_bytes=len(payload),
            bytes_transferred=512,
            state="TRANSFERRING",
            source_peer_id="remote-1",
        )
        transport = _FakeTransport([payload[:512], payload[512:]])
        orch = TransferOrchestrator(
            _local(),
            transport,
            Sha256IntegrityVerifier(),
            downloads,
            checkpoint_store=repo,
        )
        resumed = await orch.resume_download("task-crash-1", _remote())
        assert resumed.state == TransferState.COMPLETED
        assert transport.requested_offsets == [512]
        assert target_path.read_bytes() == payload

        # Orchestrator catches PeerTransportError cleanly
        bad_transport = _FakeTransport([], fail_with=PeerTransportError("bad frame"))
        bad_orch = TransferOrchestrator(
            _local(),
            bad_transport,
            Sha256IntegrityVerifier(),
            downloads,
            checkpoint_store=repo,
            disk_usage_fn=lambda _: _DiskUsage(10_000_000, 0, 10_000_000),
        )
        failed = await bad_orch.start_download(
            _remote(),
            SharedFile(file_hash=file_hash, file_name="bad.bin", size_bytes=len(payload)),
        )
        assert failed.state == TransferState.FAILED
        assert failed.error_message == "bad frame"


async def test_raw_tcp_peer_transport_maps_framing_error_to_peer_transport_error() -> None:
    import asyncio
    import struct

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        del reader
        # Send absurd header_len (100 MB) to trigger FramingError on client read_frame
        writer.write(struct.pack("!II", 100 * 1024 * 1024, 0))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    try:
        local = _local()
        remote = RemotePeer("remote-1", "bob", "127.0.0.1", port, status=PresenceStatus.ONLINE)
        nat = CompositeNatTraversalStrategy([DirectSocketNatStrategy(connect_timeout_s=2.0)])
        transport = RawTcpPeerTransport(local, nat, io_timeout_s=2.0)
        with pytest.raises(PeerTransportError):
            await transport.browse_peer_files(remote)
        await transport.close()
    finally:
        server.close()
        await server.wait_closed()
