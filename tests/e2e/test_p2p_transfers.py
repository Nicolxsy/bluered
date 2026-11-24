"""Real 3-peer TCP E2E tests on 127.0.0.1 with SQLite WAL (ZERO network mocks)."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from bluered_client.application import (
    PeerUploadListener,
    PrivateChatService,
    TransferOrchestrator,
)
from bluered_client.domain import (
    LocalPeer,
    PeerBlockedError,
    PresenceStatus,
    PrivateMessage,
    QueueFullError,
    RemotePeer,
    SharedFile,
    TransferState,
    TransferTask,
)
from bluered_client.infrastructure import (
    CompositeNatTraversalStrategy,
    DirectSocketNatStrategy,
    FairShareQueuePolicy,
    RawTcpPeerTransport,
    Sha256IntegrityVerifier,
    SqliteFileRepository,
)


class _EventRecorder:
    def __init__(
        self,
        *,
        abort_at_bytes: int | None = None,
        transport: RawTcpPeerTransport | None = None,
    ) -> None:
        self.states: list[TransferState] = []
        self.progress_bytes: list[int] = []
        self.errors: list[str] = []
        self._abort_at_bytes = abort_at_bytes
        self._transport = transport
        self._aborted = False

    async def on_state_changed(
        self, task: TransferTask, previous_state: TransferState
    ) -> None:
        del previous_state
        self.states.append(task.state)

    async def on_progress(self, task: TransferTask) -> None:
        self.progress_bytes.append(task.bytes_transferred)
        if (
            self._abort_at_bytes is not None
            and self._transport is not None
            and not self._aborted
            and task.bytes_transferred >= self._abort_at_bytes
        ):
            self._aborted = True
            self._transport.abort_active_streams()

    async def on_error(self, task: TransferTask, error_message: str) -> None:
        del task
        self.errors.append(error_message)


def _deterministic_1_5mb_payload() -> bytes:
    """Generate 1.5 MB (1,572,864 bytes) deterministic binary payload."""
    block = bytes(range(256)) * 4  # 1 KB
    return block * 1536  # 1,572,864 bytes = 1.5 MiB


async def _make_peer_node(
    tmp_path: Path,
    *,
    peer_id: str,
    username: str,
    max_upload_slots: int = 3,
    max_queue_size: int = 10,
    chunk_delay_s: float = 0.0,
    on_private_message: list[PrivateMessage] | None = None,
) -> tuple[
    LocalPeer,
    SqliteFileRepository,
    PeerUploadListener,
    RawTcpPeerTransport,
    TransferOrchestrator,
    PrivateChatService,
]:
    peer_dir = tmp_path / peer_id
    shared_dir = peer_dir / "shared"
    download_dir = peer_dir / "downloads"
    shared_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    local_peer = LocalPeer(
        peer_id=peer_id,
        username=username,
        ip_address="127.0.0.1",
        tcp_port=1,  # updated to ephemeral bound port by start(bind_port=0)
        shared_directories=[shared_dir],
        max_upload_slots=max_upload_slots,
    )
    repo = SqliteFileRepository(peer_dir / "client.db")
    await repo.initialize()
    verifier = Sha256IntegrityVerifier(chunk_size=64 * 1024)
    queue_policy = FairShareQueuePolicy(
        max_queue_size=max_queue_size,
        max_concurrent_slots=max(1, max_upload_slots),
    )

    async def _chat_cb(msg: PrivateMessage) -> None:
        if on_private_message is not None:
            on_private_message.append(msg)

    listener = PeerUploadListener(
        local_peer=local_peer,
        file_index=repo,
        queue_policy=queue_policy,
        integrity_verifier=verifier,
        chunk_size=64 * 1024,
        io_timeout_s=10.0,
        on_private_message=_chat_cb,
        chunk_delay_s=chunk_delay_s,
        bind_port=0,
    )
    await listener.start()

    nat = CompositeNatTraversalStrategy([DirectSocketNatStrategy(connect_timeout_s=5.0)])
    transport = RawTcpPeerTransport(local_peer, nat, io_timeout_s=10.0, queue_wait_timeout_s=20.0)
    orchestrator = TransferOrchestrator(
        local_peer=local_peer,
        peer_transport=transport,
        integrity_verifier=verifier,
        download_dir=download_dir,
        checkpoint_store=repo,
    )
    chat_service = PrivateChatService(local_peer, transport, repo)
    return local_peer, repo, listener, transport, orchestrator, chat_service


def _as_remote(local: LocalPeer, *, status: PresenceStatus = PresenceStatus.ONLINE) -> RemotePeer:
    return RemotePeer(
        peer_id=local.peer_id,
        username=local.username,
        ip_address=local.ip_address,
        tcp_port=local.tcp_port,
        status=status,
    )


async def test_e2e_cas16_cas17_cas21_browse_and_download_1_5mb(tmp_path: Path) -> None:
    """Cas_16 + Cas_17 + Cas_21: Peer A browses Peer B and downloads a 1.5 MB binary file."""
    peer_a, repo_a, srv_a, tr_a, orch_a, _ = await _make_peer_node(
        tmp_path, peer_id="peer-a", username="alice"
    )
    peer_b, repo_b, srv_b, tr_b, _, _ = await _make_peer_node(
        tmp_path, peer_id="peer-b", username="bob"
    )
    del peer_a
    try:
        payload = _deterministic_1_5mb_payload()
        expected_hash = hashlib.sha256(payload).hexdigest()
        src_path = peer_b.shared_directories[0] / "symphony_lossless.bin"
        src_path.write_bytes(payload)
        await repo_b.upsert(
            SharedFile(
                file_hash=expected_hash,
                file_name=src_path.name,
                size_bytes=len(payload),
                local_path=src_path,
                mime_type="application/octet-stream",
                category="other",
            )
        )

        remote_b = _as_remote(peer_b)
        browsed = await tr_a.browse_peer_files(remote_b)
        assert len(browsed) == 1
        assert browsed[0].file_hash == expected_hash
        assert browsed[0].file_name == "symphony_lossless.bin"
        assert browsed[0].size_bytes == len(payload)

        recorder = _EventRecorder()
        orch_a.add_listener(recorder)
        task = await orch_a.start_download(remote_b, browsed[0], task_id="dl-15mb")

        assert task.state == TransferState.COMPLETED
        assert task.bytes_transferred == len(payload)
        assert TransferState.HANDSHAKING in recorder.states
        assert TransferState.TRANSFERRING in recorder.states
        assert recorder.states[-1] == TransferState.COMPLETED

        downloaded_path = orch_a.get_target_path("dl-15mb")
        assert downloaded_path is not None and downloaded_path.is_file()
        assert hashlib.sha256(downloaded_path.read_bytes()).hexdigest() == expected_hash
    finally:
        await tr_a.close()
        await tr_b.close()
        await srv_a.stop()
        await srv_b.stop()
        await repo_a.close()
        await repo_b.close()


async def test_e2e_cas18_upload_slot_queueing_with_three_real_peers(tmp_path: Path) -> None:
    """Cas_18: With max_upload_slots=1 on Peer B, Peer A and Peer C queue and both complete."""
    _, repo_a, srv_a, tr_a, orch_a, _ = await _make_peer_node(
        tmp_path, peer_id="peer-a", username="alice"
    )
    peer_b, repo_b, srv_b, tr_b, _, _ = await _make_peer_node(
        tmp_path,
        peer_id="peer-b",
        username="bob",
        max_upload_slots=1,
        max_queue_size=4,
        chunk_delay_s=0.015,
    )
    _, repo_c, srv_c, tr_c, orch_c, _ = await _make_peer_node(
        tmp_path, peer_id="peer-c", username="carol"
    )
    try:
        payload = b"Z" * (512 * 1024)  # 512 KB = 8 chunks * 15ms = ~120ms active slot hold
        expected_hash = hashlib.sha256(payload).hexdigest()
        src_path = peer_b.shared_directories[0] / "queued_dataset.bin"
        src_path.write_bytes(payload)
        sf = SharedFile(
            file_hash=expected_hash,
            file_name=src_path.name,
            size_bytes=len(payload),
            local_path=src_path,
        )
        await repo_b.upsert(sf)

        rec_a = _EventRecorder()
        rec_c = _EventRecorder()
        orch_a.add_listener(rec_a)
        orch_c.add_listener(rec_c)

        remote_b = _as_remote(peer_b)
        task_a, task_c = await asyncio.gather(
            orch_a.start_download(remote_b, sf, task_id="dl-a"),
            orch_c.start_download(remote_b, sf, task_id="dl-c"),
        )

        assert task_a.state == TransferState.COMPLETED
        assert task_c.state == TransferState.COMPLETED
        assert TransferState.QUEUED in rec_a.states or TransferState.QUEUED in rec_c.states

        # Verify QueueFullError when max_queue_size=0 and slot is busy
        peer_b_full, repo_bf, srv_bf, tr_bf, _, _ = await _make_peer_node(
            tmp_path,
            peer_id="peer-b-full",
            username="bob_full",
            max_upload_slots=1,
            max_queue_size=0,
            chunk_delay_s=0.05,
        )
        try:
            src_full = peer_b_full.shared_directories[0] / "full.bin"
            src_full.write_bytes(payload)
            sf_full = SharedFile(
                file_hash=expected_hash,
                file_name=src_full.name,
                size_bytes=len(payload),
                local_path=src_full,
            )
            await repo_bf.upsert(sf_full)
            remote_bf = _as_remote(peer_b_full)

            t_first = asyncio.create_task(
                orch_a.start_download(remote_bf, sf_full, task_id="first")
            )
            while srv_bf.active_uploads == 0:
                await asyncio.sleep(0.01)

            with pytest.raises(QueueFullError):
                async for _ in tr_c.stream_file_download(remote_bf, expected_hash):
                    pass
            res_first = await t_first
            assert res_first.state == TransferState.COMPLETED
        finally:
            await tr_bf.close()
            await srv_bf.stop()
            await repo_bf.close()
    finally:
        await tr_a.close()
        await tr_b.close()
        await tr_c.close()
        await srv_a.stop()
        await srv_b.stop()
        await srv_c.stop()
        await repo_a.close()
        await repo_b.close()
        await repo_c.close()


async def test_e2e_cas22_tcp_socket_cut_and_resume_from_offset(tmp_path: Path) -> None:
    """Cas_22: Deliberate TCP socket cut followed by resume requesting only remaining bytes."""
    _, repo_a, srv_a, tr_a, orch_a, _ = await _make_peer_node(
        tmp_path, peer_id="peer-a", username="alice"
    )
    peer_b, repo_b, srv_b, tr_b, _, _ = await _make_peer_node(
        tmp_path, peer_id="peer-b", username="bob", chunk_delay_s=0.005
    )
    try:
        payload = _deterministic_1_5mb_payload()
        expected_hash = hashlib.sha256(payload).hexdigest()
        src_path = peer_b.shared_directories[0] / "resumable_1_5mb.bin"
        src_path.write_bytes(payload)
        sf = SharedFile(
            file_hash=expected_hash,
            file_name=src_path.name,
            size_bytes=len(payload),
            local_path=src_path,
        )
        await repo_b.upsert(sf)

        abort_recorder = _EventRecorder(abort_at_bytes=256 * 1024, transport=tr_a)
        orch_a.add_listener(abort_recorder)

        remote_b = _as_remote(peer_b)
        failed_task = await orch_a.start_download(remote_b, sf, task_id="dl-resume")
        assert failed_task.state == TransferState.FAILED

        target_path = orch_a.get_target_path("dl-resume")
        assert target_path is not None
        part_path = target_path.with_name(f"{target_path.name}.part")
        assert part_path.is_file()
        partial_size = part_path.stat().st_size
        assert 256 * 1024 <= partial_size < len(payload)

        cp = await repo_a.get_download_checkpoint("dl-resume")
        assert cp is not None and cp.bytes_transferred == partial_size

        resumed_task = await orch_a.resume_download("dl-resume", remote_b)
        assert resumed_task.state == TransferState.COMPLETED
        assert resumed_task.bytes_transferred == len(payload)
        assert srv_b.received_download_offsets == [0, partial_size]
        assert target_path.is_file()
        assert not part_path.exists()
        assert hashlib.sha256(target_path.read_bytes()).hexdigest() == expected_hash
    finally:
        await tr_a.close()
        await tr_b.close()
        await srv_a.stop()
        await srv_b.stop()
        await repo_a.close()
        await repo_b.close()


async def test_e2e_cas23_private_chat_blocklist_and_offline_delivery(tmp_path: Path) -> None:
    """Cas_23: Direct P2P chat, 403 block rejection, and deferred delivery when ONLINE."""
    received_b: list[PrivateMessage] = []
    received_c: list[PrivateMessage] = []

    peer_a, repo_a, srv_a, tr_a, _, chat_a = await _make_peer_node(
        tmp_path, peer_id="peer-a", username="alice"
    )
    peer_b, repo_b, srv_b, tr_b, _, _ = await _make_peer_node(
        tmp_path, peer_id="peer-b", username="bob", on_private_message=received_b
    )
    peer_c, repo_c, srv_c, tr_c, _, _ = await _make_peer_node(
        tmp_path, peer_id="peer-c", username="carol", on_private_message=received_c
    )
    try:
        # 1. Direct P2P message A -> B
        remote_b = _as_remote(peer_b, status=PresenceStatus.ONLINE)
        msg1 = await chat_a.send_message(remote_b, "Hola Bob directo por TCP!")
        assert msg1.delivered is True
        assert len(received_b) == 1
        assert received_b[0].content == "Hola Bob directo por TCP!"
        assert received_b[0].sender_id == peer_a.peer_id

        # 2. Peer B blocks Peer A -> 403 PeerBlockedError
        peer_b.block_peer(peer_a.peer_id)
        with pytest.raises(PeerBlockedError):
            await chat_a.send_message(remote_b, "Deberia ser rechazado con 403")
        assert len(received_b) == 1

        # 3. Peer C is OFFLINE -> stored in SQLite -> delivered when Peer C comes ONLINE
        remote_c = _as_remote(peer_c, status=PresenceStatus.OFFLINE)
        deferred = await chat_a.send_message(remote_c, "Mensaje diferido para Carol")
        assert deferred.delivered is False
        assert len(await repo_a.list_pending_messages(peer_c.peer_id)) == 1
        assert len(received_c) == 0

        remote_c.status = PresenceStatus.ONLINE
        flushed = await chat_a.deliver_pending_messages(remote_c)
        assert len(flushed) == 1
        assert flushed[0].delivered is True
        assert await repo_a.list_pending_messages(peer_c.peer_id) == []
        assert len(received_c) == 1
        assert received_c[0].content == "Mensaje diferido para Carol"
    finally:
        await tr_a.close()
        await tr_b.close()
        await tr_c.close()
        await srv_a.stop()
        await srv_b.stop()
        await srv_c.stop()
        await repo_a.close()
        await repo_b.close()
        await repo_c.close()
