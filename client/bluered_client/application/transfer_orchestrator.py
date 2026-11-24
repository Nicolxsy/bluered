"""TransferOrchestrator implementing Figure 56 FSM and Figure 57 swimlanes (Cas_17, Cas_22)."""

from __future__ import annotations

import os
import shutil
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from bluered_contracts.peer_framing import FramingError

from bluered_client.contracts import (
    IDownloadCheckpointStore,
    IIntegrityVerifier,
    IPeerTransport,
    IStreamingPeerTransport,
    ITransferEventListener,
)
from bluered_client.domain import (
    InsufficientDiskSpaceError,
    LocalPeer,
    PeerBlockedError,
    QueueFullError,
    RemoteFileChangedError,
    RemotePeer,
    SharedFile,
    TransferIntegrityError,
    TransferState,
    TransferTask,
)

ConflictPolicy = Literal["replace", "keep_both"]


class TransferOrchestrator:
    """Application orchestrator for P2P downloads, pause/cancel, resume, and Observer events."""

    def __init__(
        self,
        local_peer: LocalPeer,
        peer_transport: IPeerTransport,
        integrity_verifier: IIntegrityVerifier,
        download_dir: Path,
        *,
        checkpoint_store: IDownloadCheckpointStore | None = None,
        listeners: list[ITransferEventListener] | None = None,
        disk_usage_fn: Callable[[Path], Any] | None = None,
    ) -> None:
        self._local_peer = local_peer
        self._transport = peer_transport
        self._integrity = integrity_verifier
        self._download_dir = Path(download_dir)
        self._checkpoint_store = checkpoint_store
        self._listeners: list[ITransferEventListener] = list(listeners or [])
        self._disk_usage_fn = disk_usage_fn or shutil.disk_usage

        self._tasks: dict[str, TransferTask] = {}
        self._target_paths: dict[str, Path] = {}
        self._peers: dict[str, RemotePeer] = {}
        self._pause_flags: set[str] = set()
        self._cancel_flags: set[str] = set()

    def add_listener(self, listener: ITransferEventListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: ITransferEventListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def get_task(self, task_id: str) -> TransferTask | None:
        return self._tasks.get(task_id)

    def get_target_path(self, task_id: str) -> Path | None:
        return self._target_paths.get(task_id)

    @staticmethod
    def _part_path_for(target_path: Path) -> Path:
        return target_path.with_name(f"{target_path.name}.part")

    def _resolve_target_path(self, file_name: str, conflict_policy: ConflictPolicy) -> Path:
        candidate = self._download_dir / Path(file_name).name
        if conflict_policy == "replace" or not candidate.exists():
            return candidate
        stem = candidate.stem
        suffix = candidate.suffix
        counter = 1
        while True:
            numbered = self._download_dir / f"{stem} ({counter}){suffix}"
            if not numbered.exists():
                return numbered
            counter += 1

    async def _save_checkpoint(self, task: TransferTask, target_path: Path) -> None:
        if self._checkpoint_store is None:
            return
        await self._checkpoint_store.save_download_checkpoint(
            task_id=task.task_id,
            file_hash=task.file_hash,
            file_name=task.file_name,
            target_path=target_path,
            total_bytes=task.total_bytes,
            bytes_transferred=task.bytes_transferred,
            state=task.state.value,
            source_peer_id=task.remote_peer_id,
        )

    async def _notify_state_changed(
        self,
        task: TransferTask,
        previous_state: TransferState,
    ) -> None:
        for listener in list(self._listeners):
            await listener.on_state_changed(task, previous_state)

    async def _notify_progress(self, task: TransferTask) -> None:
        for listener in list(self._listeners):
            await listener.on_progress(task)

    async def _notify_error(self, task: TransferTask, message: str) -> None:
        for listener in list(self._listeners):
            await listener.on_error(task, message)

    async def _transition(
        self,
        task: TransferTask,
        target: TransferState,
        target_path: Path,
        *,
        error_message: str | None = None,
        queue_position: int | None = None,
    ) -> None:
        previous = task.state
        if previous == target:
            return
        if target == TransferState.QUEUED:
            task.requeue(position=queue_position)
        else:
            task.transition_to(target, error_message=error_message)
        await self._save_checkpoint(task, target_path)
        await self._notify_state_changed(task, previous)
        if target == TransferState.FAILED and error_message is not None:
            await self._notify_error(task, error_message)

    async def start_download(
        self,
        peer: RemotePeer,
        shared_file: SharedFile,
        *,
        conflict_policy: ConflictPolicy = "keep_both",
        task_id: str | None = None,
        max_retries: int = 3,
    ) -> TransferTask:
        """Execute Figure 57 download flow: disk check, collision policy, NAT, stream, SHA-256."""
        self._download_dir.mkdir(parents=True, exist_ok=True)
        resolved_id = task_id or uuid.uuid4().hex
        target_path = self._resolve_target_path(shared_file.file_name, conflict_policy)
        part_path = self._part_path_for(target_path)

        task = TransferTask(
            task_id=resolved_id,
            file_hash=shared_file.file_hash,
            file_name=shared_file.file_name,
            total_bytes=shared_file.size_bytes,
            remote_peer_id=peer.peer_id,
            state=TransferState.QUEUED,
            max_retries=max_retries,
        )
        self._tasks[resolved_id] = task
        self._target_paths[resolved_id] = target_path
        self._peers[resolved_id] = peer

        usage = self._disk_usage_fn(self._download_dir)
        free_bytes = int(usage.free)
        if free_bytes < shared_file.size_bytes:
            reason = (
                f"Insufficient disk space: need {shared_file.size_bytes} bytes, "
                f"available {free_bytes} bytes"
            )
            await self._transition(task, TransferState.FAILED, target_path, error_message=reason)
            raise InsufficientDiskSpaceError(reason)

        await self._save_checkpoint(task, target_path)
        await self._execute_transfer(
            task=task,
            peer=peer,
            target_path=target_path,
            part_path=part_path,
            offset=0,
            resuming=False,
        )
        return task

    async def _execute_transfer(
        self,
        *,
        task: TransferTask,
        peer: RemotePeer,
        target_path: Path,
        part_path: Path,
        offset: int,
        resuming: bool,
    ) -> None:
        if not resuming and task.state == TransferState.QUEUED:
            await self._transition(task, TransferState.HANDSHAKING, target_path)

        async def _on_queue_status(position: int) -> None:
            if task.state in {TransferState.HANDSHAKING, TransferState.RESUMING}:
                await self._transition(
                    task,
                    TransferState.QUEUED,
                    target_path,
                    queue_position=position,
                )

        async def _on_slot_available() -> None:
            if task.state == TransferState.QUEUED:
                await self._transition(task, TransferState.HANDSHAKING, target_path)
            if task.state in {TransferState.HANDSHAKING, TransferState.RESUMING}:
                await self._transition(task, TransferState.TRANSFERRING, target_path)

        mode = "r+b" if offset > 0 and part_path.is_file() else "wb"
        part_file = part_path.open(mode)
        if offset > 0 and mode == "r+b":
            part_file.seek(offset)
            part_file.truncate(offset)

        try:
            if isinstance(self._transport, IStreamingPeerTransport):
                stream = self._transport.stream_file_download(
                    peer,
                    task.file_hash,
                    offset=offset,
                    on_queue_status=_on_queue_status,
                    on_slot_available=_on_slot_available,
                )
                async for _, chunk_data, chunk_sha256 in stream:
                    if task.task_id in self._cancel_flags or task.task_id in self._pause_flags:
                        break
                    if task.state == TransferState.QUEUED:
                        await self._transition(task, TransferState.HANDSHAKING, target_path)
                    if task.state in {TransferState.HANDSHAKING, TransferState.RESUMING}:
                        await self._transition(task, TransferState.TRANSFERRING, target_path)
                    if chunk_sha256 and not self._integrity.verify_chunk(chunk_data, chunk_sha256):
                        raise TransferIntegrityError("Chunk SHA-256 verification failed")
                    part_file.write(chunk_data)
                    part_file.flush()
                    task.advance_progress(len(chunk_data))
                    await self._save_checkpoint(task, target_path)
                    await self._notify_progress(task)
            else:
                async for chunk_data in self._transport.request_file_chunks(
                    peer, task.file_hash, offset=offset
                ):
                    if task.task_id in self._cancel_flags or task.task_id in self._pause_flags:
                        break
                    if task.state in {TransferState.HANDSHAKING, TransferState.RESUMING}:
                        await self._transition(task, TransferState.TRANSFERRING, target_path)
                    part_file.write(chunk_data)
                    part_file.flush()
                    task.advance_progress(len(chunk_data))
                    await self._save_checkpoint(task, target_path)
                    await self._notify_progress(task)
        except (
            RemoteFileChangedError,
            PeerBlockedError,
            QueueFullError,
            TransferIntegrityError,
            FramingError,
            OSError,
            TimeoutError,
            RuntimeError,
        ) as exc:
            part_file.close()
            if task.task_id in self._cancel_flags:
                await self._finalize_cancel(task, target_path, part_path)
                return
            if task.task_id in self._pause_flags:
                await self._finalize_pause(task, target_path)
                return
            if not task.state.is_terminal and task.state != TransferState.FAILED:
                await self._transition(
                    task,
                    TransferState.FAILED,
                    target_path,
                    error_message=str(exc),
                )
            return
        finally:
            if not part_file.closed:
                part_file.close()

        if task.task_id in self._cancel_flags:
            await self._finalize_cancel(task, target_path, part_path)
            return
        if task.task_id in self._pause_flags:
            await self._finalize_pause(task, target_path)
            return

        if task.state == TransferState.HANDSHAKING or task.state == TransferState.RESUMING:
            await self._transition(task, TransferState.TRANSFERRING, target_path)

        if not await self._integrity.verify_file(part_path, task.file_hash):
            await self._transition(
                task,
                TransferState.FAILED,
                target_path,
                error_message="Final file SHA-256 verification failed",
            )
            return

        os.replace(part_path, target_path)
        await self._transition(task, TransferState.COMPLETED, target_path)

    async def _finalize_pause(self, task: TransferTask, target_path: Path) -> None:
        if task.state != TransferState.PAUSED and task.can_transition_to(TransferState.PAUSED):
            await self._transition(task, TransferState.PAUSED, target_path)

    async def _finalize_cancel(
        self,
        task: TransferTask,
        target_path: Path,
        part_path: Path,
    ) -> None:
        part_path.unlink(missing_ok=True)
        if task.state != TransferState.CANCELLED and task.can_transition_to(
            TransferState.CANCELLED
        ):
            await self._transition(task, TransferState.CANCELLED, target_path)

    async def pause_download(self, task_id: str) -> TransferTask:
        """Pause an active or queued download while preserving its .part file."""
        task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"Unknown transfer task '{task_id}'")
        self._pause_flags.add(task_id)
        target_path = self._target_paths[task_id]
        if task.can_transition_to(TransferState.PAUSED):
            await self._transition(task, TransferState.PAUSED, target_path)
        return task

    async def cancel_download(self, task_id: str) -> TransferTask:
        """Cancel a download and delete any partial .part file on disk."""
        task = self._tasks.get(task_id)
        if task is None:
            raise KeyError(f"Unknown transfer task '{task_id}'")
        self._cancel_flags.add(task_id)
        target_path = self._target_paths[task_id]
        part_path = self._part_path_for(target_path)
        await self._finalize_cancel(task, target_path, part_path)
        return task

    async def resume_download(
        self,
        task_id: str,
        peer: RemotePeer | None = None,
    ) -> TransferTask:
        """Resume a PAUSED or FAILED download from the persisted .part byte offset (Cas_22)."""
        task = self._tasks.get(task_id)
        target_path = self._target_paths.get(task_id)
        resolved_peer = peer or self._peers.get(task_id)

        if (task is None or target_path is None) and self._checkpoint_store is not None:
            cp = await self._checkpoint_store.get_download_checkpoint(task_id)
            if cp is not None:
                task = TransferTask(
                    task_id=cp.task_id,
                    file_hash=cp.file_hash,
                    file_name=cp.file_name,
                    total_bytes=cp.total_bytes,
                    remote_peer_id=cp.source_peer_id,
                    state=TransferState(cp.state),
                    bytes_transferred=cp.bytes_transferred,
                )
                target_path = cp.target_path
                self._tasks[task_id] = task
                self._target_paths[task_id] = target_path

        if task is None or target_path is None:
            raise KeyError(f"Transfer task '{task_id}' not found in memory or checkpoint store")
        if resolved_peer is None:
            raise ValueError(f"RemotePeer is required to resume task '{task_id}'")
        self._peers[task_id] = resolved_peer

        part_path = self._part_path_for(target_path)
        offset = part_path.stat().st_size if part_path.is_file() else 0
        if offset > task.total_bytes:
            reason = f"Corrupt .part file: size {offset} exceeds total_bytes {task.total_bytes}"
            await self._transition(task, TransferState.FAILED, target_path, error_message=reason)
            raise TransferIntegrityError(reason)
        task.bytes_transferred = offset

        while True:
            if task.state == TransferState.FAILED and not task.can_retry:
                break
            if not task.can_transition_to(TransferState.RESUMING):
                break
            self._pause_flags.discard(task_id)
            await self._transition(task, TransferState.RESUMING, target_path)
            await self._execute_transfer(
                task=task,
                peer=resolved_peer,
                target_path=target_path,
                part_path=part_path,
                offset=offset,
                resuming=True,
            )
            if task.state in {
                TransferState.COMPLETED,
                TransferState.PAUSED,
                TransferState.CANCELLED,
            }:
                break
            if task.error_message and (
                "changed" in task.error_message.lower()
                or "blocked" in task.error_message.lower()
            ):
                break
            offset = part_path.stat().st_size if part_path.is_file() else 0
            task.bytes_transferred = min(offset, task.total_bytes)

        return task
