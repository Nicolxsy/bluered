"""Transfer queue scheduling policies implementing ITransferQueuePolicy (LSP, Cas_18)."""

from __future__ import annotations

from collections import deque

from bluered_client.domain import DomainValidationError, QueueFullError, TransferTask


class FifoQueuePolicy:
    """Strict First-In-First-Out queue policy for pending P2P transfers."""

    def __init__(
        self,
        *,
        max_queue_size: int = 100,
        max_concurrent_slots: int = 3,
    ) -> None:
        if max_queue_size < 0:
            raise DomainValidationError("max_queue_size must be non-negative")
        if max_concurrent_slots < 1:
            raise DomainValidationError("max_concurrent_slots must be at least 1")
        self._max_queue_size = max_queue_size
        self._max_concurrent_slots = max_concurrent_slots
        self._queue: deque[TransferTask] = deque()
        self._task_ids: set[str] = set()

    @property
    def max_queue_size(self) -> int:
        return self._max_queue_size

    @property
    def max_concurrent_slots(self) -> int:
        return self._max_concurrent_slots

    def enqueue(self, task: TransferTask) -> int:
        """Append task to FIFO queue and return 1-based queue position."""
        if task.task_id in self._task_ids:
            raise DomainValidationError(f"Task '{task.task_id}' is already queued")
        if len(self._queue) >= self._max_queue_size:
            raise QueueFullError(f"Transfer queue is full (limit={self._max_queue_size})")
        self._queue.append(task)
        self._task_ids.add(task.task_id)
        return len(self._queue)

    def dequeue_next(self) -> TransferTask | None:
        """Pop and return the oldest pending task, or None if empty."""
        if not self._queue:
            return None
        task = self._queue.popleft()
        self._task_ids.discard(task.task_id)
        return task

    def remove(self, task_id: str) -> bool:
        """Remove a pending task by task_id."""
        if task_id not in self._task_ids:
            return False
        self._queue = deque(t for t in self._queue if t.task_id != task_id)
        self._task_ids.discard(task_id)
        return True

    def position_of(self, task_id: str) -> int | None:
        """Return 1-based queue position of task_id, or None if not queued."""
        for idx, task in enumerate(self._queue, start=1):
            if task.task_id == task_id:
                return idx
        return None

    def pending_count(self) -> int:
        """Return total number of queued tasks."""
        return len(self._queue)


class FairShareQueuePolicy:
    """Round-robin per-peer queue policy preventing single-peer starvation (Cas_18)."""

    def __init__(
        self,
        *,
        max_queue_size: int = 100,
        max_concurrent_slots: int = 3,
    ) -> None:
        if max_queue_size < 0:
            raise DomainValidationError("max_queue_size must be non-negative")
        if max_concurrent_slots < 1:
            raise DomainValidationError("max_concurrent_slots must be at least 1")
        self._max_queue_size = max_queue_size
        self._max_concurrent_slots = max_concurrent_slots
        self._peer_queues: dict[str, deque[TransferTask]] = {}
        self._peer_order: deque[str] = deque()
        self._task_to_peer: dict[str, str] = {}

    @property
    def max_queue_size(self) -> int:
        return self._max_queue_size

    @property
    def max_concurrent_slots(self) -> int:
        return self._max_concurrent_slots

    def enqueue(self, task: TransferTask) -> int:
        """Enqueue task into its peer's bucket and return 1-based fair-share position."""
        if task.task_id in self._task_to_peer:
            raise DomainValidationError(f"Task '{task.task_id}' is already queued")
        if len(self._task_to_peer) >= self._max_queue_size:
            raise QueueFullError(f"Transfer queue is full (limit={self._max_queue_size})")
        peer_id = task.remote_peer_id
        bucket = self._peer_queues.get(peer_id)
        if bucket is None:
            bucket = deque()
            self._peer_queues[peer_id] = bucket
            self._peer_order.append(peer_id)
        bucket.append(task)
        self._task_to_peer[task.task_id] = peer_id
        pos = self.position_of(task.task_id)
        assert pos is not None
        return pos

    def dequeue_next(self) -> TransferTask | None:
        """Dequeue one task from the next peer in round-robin order."""
        while self._peer_order:
            peer_id = self._peer_order.popleft()
            bucket = self._peer_queues.get(peer_id)
            if not bucket:
                self._peer_queues.pop(peer_id, None)
                continue
            task = bucket.popleft()
            self._task_to_peer.pop(task.task_id, None)
            if bucket:
                self._peer_order.append(peer_id)
            else:
                self._peer_queues.pop(peer_id, None)
            return task
        return None

    def remove(self, task_id: str) -> bool:
        """Remove a pending task by task_id and clean up empty peer buckets."""
        peer_id = self._task_to_peer.pop(task_id, None)
        if peer_id is None:
            return False
        bucket = self._peer_queues.get(peer_id)
        if bucket is None:
            return False
        self._peer_queues[peer_id] = deque(t for t in bucket if t.task_id != task_id)
        if not self._peer_queues[peer_id]:
            self._peer_queues.pop(peer_id, None)
            self._peer_order = deque(p for p in self._peer_order if p != peer_id)
        return True

    def position_of(self, task_id: str) -> int | None:
        """Compute exact 1-based dequeue order position under round-robin scheduling."""
        if task_id not in self._task_to_peer:
            return None
        sim_queues = {peer_id: deque(bucket) for peer_id, bucket in self._peer_queues.items()}
        sim_order = deque(self._peer_order)
        position = 0
        while sim_order:
            peer_id = sim_order.popleft()
            bucket = sim_queues.get(peer_id)
            if not bucket:
                continue
            position += 1
            candidate = bucket.popleft()
            if candidate.task_id == task_id:
                return position
            if bucket:
                sim_order.append(peer_id)
        return None

    def pending_count(self) -> int:
        """Return total number of queued tasks across all peers."""
        return len(self._task_to_peer)
