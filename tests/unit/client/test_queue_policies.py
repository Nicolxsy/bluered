"""Unit & LSP substitutability tests for FifoQueuePolicy and FairShareQueuePolicy (Cas_18)."""

from __future__ import annotations

import hashlib

import pytest
from bluered_client.contracts import ITransferQueuePolicy
from bluered_client.domain import DomainValidationError, QueueFullError, TransferTask
from bluered_client.infrastructure import FairShareQueuePolicy, FifoQueuePolicy

SAMPLE_HASH = hashlib.sha256(b"queue-sample").hexdigest()


def _task(task_id: str, peer_id: str = "peer-1") -> TransferTask:
    return TransferTask(
        task_id=task_id,
        file_hash=SAMPLE_HASH,
        file_name="archive.bin",
        total_bytes=1024,
        remote_peer_id=peer_id,
    )


@pytest.mark.parametrize("policy_cls", [FifoQueuePolicy, FairShareQueuePolicy])
def test_queue_policies_lsp_basic_lifecycle(
    policy_cls: type[FifoQueuePolicy] | type[FairShareQueuePolicy],
) -> None:
    policy: ITransferQueuePolicy = policy_cls(max_queue_size=3, max_concurrent_slots=1)
    assert isinstance(policy, ITransferQueuePolicy)
    assert policy.pending_count() == 0
    assert policy.dequeue_next() is None

    t1 = _task("t1", "peer-a")
    t2 = _task("t2", "peer-b")
    t3 = _task("t3", "peer-c")
    assert policy.enqueue(t1) == 1
    assert policy.enqueue(t2) == 2
    assert policy.enqueue(t3) == 3
    assert policy.pending_count() == 3
    assert policy.position_of("t1") == 1
    assert policy.position_of("t2") == 2
    assert policy.position_of("t3") == 3
    assert policy.position_of("missing") is None

    with pytest.raises(DomainValidationError):
        policy.enqueue(t1)

    with pytest.raises(QueueFullError):
        policy.enqueue(_task("t4", "peer-d"))

    assert policy.remove("t2") is True
    assert policy.remove("t2") is False
    assert policy.pending_count() == 2
    assert policy.position_of("t3") == 2

    first = policy.dequeue_next()
    second = policy.dequeue_next()
    assert first is not None and first.task_id == "t1"
    assert second is not None and second.task_id == "t3"
    assert policy.dequeue_next() is None
    assert policy.pending_count() == 0


def test_fifo_queue_preserves_strict_arrival_order() -> None:
    policy = FifoQueuePolicy(max_queue_size=10, max_concurrent_slots=1)
    policy.enqueue(_task("a1", "peer-a"))
    policy.enqueue(_task("a2", "peer-a"))
    policy.enqueue(_task("b1", "peer-b"))

    order = [policy.dequeue_next() for _ in range(3)]
    assert [t.task_id for t in order if t is not None] == ["a1", "a2", "b1"]


def test_fair_share_queue_prevents_single_peer_starvation() -> None:
    policy = FairShareQueuePolicy(max_queue_size=10, max_concurrent_slots=1)
    policy.enqueue(_task("a1", "peer-a"))
    policy.enqueue(_task("a2", "peer-a"))
    policy.enqueue(_task("a3", "peer-a"))
    policy.enqueue(_task("b1", "peer-b"))

    # Expected round-robin order: a1 (1), b1 (2), a2 (3), a3 (4)
    assert policy.position_of("a1") == 1
    assert policy.position_of("b1") == 2
    assert policy.position_of("a2") == 3
    assert policy.position_of("a3") == 4

    order = [policy.dequeue_next() for _ in range(4)]
    assert [t.task_id for t in order if t is not None] == ["a1", "b1", "a2", "a3"]
