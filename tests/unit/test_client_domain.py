"""Unit tests for client domain entities, value objects, LSP, and TransferTask FSM."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from bluered_client.domain import (
    VALID_TRANSITIONS,
    DomainError,
    DomainValidationError,
    DownloadCheckpoint,
    FileReview,
    InvalidStateTransitionError,
    LocalPeer,
    PeerNode,
    PeerTransportError,
    PresenceStatus,
    PrivateMessage,
    RemotePeer,
    SearchMatch,
    SharedFile,
    TransferState,
    TransferTask,
)

SAMPLE_HASH = hashlib.sha256(b"bluered-sample-file").hexdigest()
OTHER_HASH = hashlib.sha256(b"bluered-other-file").hexdigest()


def _make_task(
    state: TransferState = TransferState.QUEUED,
    *,
    total_bytes: int = 1024,
    bytes_transferred: int = 0,
    retry_count: int = 0,
    max_retries: int = 3,
    queue_position: int | None = None,
) -> TransferTask:
    return TransferTask(
        task_id="task-1",
        file_hash=SAMPLE_HASH,
        file_name="track.flac",
        total_bytes=total_bytes,
        remote_peer_id="peer-remote-1",
        state=state,
        bytes_transferred=bytes_transferred,
        retry_count=retry_count,
        max_retries=max_retries,
        queue_position=queue_position,
    )


# --- TransferTask Exhaustive State Machine (Figure 56) ---


@pytest.mark.parametrize(
    ("source", "target"),
    [(src, dst) for src in TransferState for dst in TransferState],
)
def test_transfer_task_exhaustive_transition_matrix(
    source: TransferState,
    target: TransferState,
) -> None:
    task = _make_task(state=source)
    expected_valid = target in VALID_TRANSITIONS[source]

    assert task.can_transition_to(target) is expected_valid

    if expected_valid:
        task.transition_to(target)
        assert task.state == target
    else:
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            task.transition_to(target)
        assert exc_info.value.current_state == source
        assert exc_info.value.target_state == target
        assert task.state == source


@pytest.mark.parametrize("target", [TransferState.RESUMING, TransferState.QUEUED])
def test_transfer_task_retry_budget_and_counter(target: TransferState) -> None:
    task = _make_task(state=TransferState.FAILED, retry_count=1, max_retries=2)
    task.error_message = "socket reset"
    assert task.can_retry is True

    task.transition_to(target)
    assert task.state == target
    assert task.retry_count == 2
    assert task.error_message is None

    # Exhausted budget blocks retry from FAILED
    task.fail("second failure")
    assert task.can_retry is False
    assert task.can_transition_to(target) is False
    with pytest.raises(InvalidStateTransitionError):
        task.transition_to(target)


def test_transfer_task_terminal_states_and_helpers() -> None:
    assert TransferState.COMPLETED.is_terminal is True
    assert TransferState.CANCELLED.is_terminal is True
    assert TransferState.TRANSFERRING.is_terminal is False

    task = _make_task(total_bytes=1000, queue_position=3)
    task.start_handshake()
    assert task.state == TransferState.HANDSHAKING

    task.requeue(position=1)
    assert task.state == TransferState.QUEUED
    assert task.queue_position == 1

    task.start_handshake()
    task.begin_transfer()
    assert task.state == TransferState.TRANSFERRING
    assert task.queue_position is None

    task.advance_progress(400)
    assert task.bytes_transferred == 400
    assert task.progress_ratio == pytest.approx(0.4)

    task.pause()
    assert task.state == TransferState.PAUSED

    task.resume()
    assert task.state == TransferState.RESUMING

    task.begin_transfer()
    task.advance_progress(600)
    task.complete()
    assert task.state == TransferState.COMPLETED
    assert task.progress_ratio == pytest.approx(1.0)


def test_transfer_task_progress_and_validation_invariants() -> None:
    task = _make_task(state=TransferState.QUEUED, total_bytes=100)
    with pytest.raises(DomainValidationError):
        task.advance_progress(10)

    task.start_handshake()
    task.begin_transfer()
    with pytest.raises(DomainValidationError):
        task.advance_progress(-1)
    with pytest.raises(DomainValidationError):
        task.advance_progress(101)

    empty_task = _make_task(total_bytes=0)
    assert empty_task.progress_ratio == pytest.approx(1.0)

    with pytest.raises(DomainValidationError):
        TransferTask(
            task_id="",
            file_hash=SAMPLE_HASH,
            file_name="x.bin",
            total_bytes=10,
            remote_peer_id="p2",
        )
    with pytest.raises(DomainValidationError):
        TransferTask(
            task_id="t1",
            file_hash="bad-hash",
            file_name="x.bin",
            total_bytes=10,
            remote_peer_id="p2",
        )


def test_transfer_task_cancel_clears_queue_position() -> None:
    task = _make_task(state=TransferState.QUEUED, queue_position=4)
    task.cancel()
    assert task.state == TransferState.CANCELLED
    assert task.queue_position is None


# --- PeerNode LSP & Subclasses ---


def _assert_peer_node_contract(node: PeerNode, expected_ip: str, expected_port: int) -> None:
    assert node.endpoint() == (expected_ip, expected_port)
    assert isinstance(node.is_available_for_transfer, bool)
    description = node.describe()
    assert isinstance(description, str) and len(description) > 0


@pytest.mark.parametrize(
    "node",
    [
        LocalPeer(
            peer_id="local-1",
            username="alice",
            ip_address="127.0.0.1",
            tcp_port=9001,
            max_upload_slots=2,
        ),
        RemotePeer(
            peer_id="remote-1",
            username="bob",
            ip_address="192.168.1.50",
            tcp_port=9002,
            status=PresenceStatus.ONLINE,
            average_upload_speed=1250.5,
        ),
    ],
)
def test_peer_node_lsp_substitutability(node: PeerNode) -> None:
    _assert_peer_node_contract(node, node.ip_address, node.tcp_port)
    assert node.is_available_for_transfer is True


@pytest.mark.parametrize("cls", [LocalPeer, RemotePeer])
@pytest.mark.parametrize(
    ("peer_id", "username", "ip", "port"),
    [
        ("", "alice", "127.0.0.1", 9000),
        ("p1", "   ", "127.0.0.1", 9000),
        ("p1", "alice", "", 9000),
        ("p1", "alice", "127.0.0.1", 0),
        ("p1", "alice", "127.0.0.1", 70000),
    ],
)
def test_peer_node_lsp_base_validation(
    cls: type[PeerNode],
    peer_id: str,
    username: str,
    ip: str,
    port: int,
) -> None:
    with pytest.raises(DomainValidationError):
        cls(peer_id=peer_id, username=username, ip_address=ip, tcp_port=port)


def test_local_peer_slots_and_blocking(tmp_path: Path) -> None:
    peer = LocalPeer(
        peer_id="local-1",
        username="alice",
        ip_address="127.0.0.1",
        tcp_port=9001,
        max_upload_slots=0,
    )
    assert peer.is_available_for_transfer is False

    peer.block_peer("bad-peer")
    assert peer.is_peer_blocked("bad-peer") is True
    peer.unblock_peer("bad-peer")
    assert peer.is_peer_blocked("bad-peer") is False

    peer.add_shared_directory(tmp_path)
    peer.add_shared_directory(tmp_path)
    assert peer.shared_directories == [tmp_path]


def test_remote_peer_presence_and_speed_validation() -> None:
    away = RemotePeer(
        peer_id="r1",
        username="bob",
        ip_address="10.0.0.2",
        tcp_port=8000,
        status=PresenceStatus.AWAY,
    )
    offline = RemotePeer(
        peer_id="r2",
        username="carol",
        ip_address="10.0.0.3",
        tcp_port=8000,
        status=PresenceStatus.OFFLINE,
    )
    assert away.is_available_for_transfer is False
    assert offline.is_available_for_transfer is False

    with pytest.raises(DomainValidationError):
        RemotePeer(
            peer_id="r3",
            username="dan",
            ip_address="10.0.0.4",
            tcp_port=8000,
            average_upload_speed=-1.0,
        )


# --- SharedFile & FileReview & SearchMatch & PrivateMessage ---


def test_shared_file_integrity_hash_and_disk(tmp_path: Path) -> None:
    content = b"blue-red-audio-payload"
    digest = hashlib.sha256(content).hexdigest()
    file_path = tmp_path / "song.mp3"
    file_path.write_bytes(content)

    shared = SharedFile(
        file_hash=digest,
        file_name="song.mp3",
        size_bytes=len(content),
        bitrate_kbps=320,
        local_path=file_path,
        mime_type="audio/mpeg",
        category="audio",
    )
    assert shared.verify_integrity(digest) is True
    assert shared.verify_integrity(OTHER_HASH) is False
    assert shared.verify_integrity() is True

    file_path.write_bytes(b"corrupted")
    assert shared.verify_integrity() is False

    remote_only = SharedFile(file_hash=digest, file_name="song.mp3", size_bytes=len(content))
    assert remote_only.verify_integrity() is False


def test_shared_file_validation() -> None:
    with pytest.raises(DomainValidationError):
        SharedFile(file_hash="short", file_name="a.txt", size_bytes=10)
    with pytest.raises(DomainValidationError):
        SharedFile(file_hash=SAMPLE_HASH, file_name="   ", size_bytes=10)
    with pytest.raises(DomainValidationError):
        SharedFile(file_hash=SAMPLE_HASH, file_name="a.txt", size_bytes=-1)
    with pytest.raises(DomainValidationError):
        SharedFile(file_hash=SAMPLE_HASH, file_name="a.txt", size_bytes=10, bitrate_kbps=-128)


@pytest.mark.parametrize("rating", [1, 2, 3, 4, 5])
def test_file_review_valid_ratings(rating: int) -> None:
    review = FileReview(
        review_id="rev-1",
        file_hash=SAMPLE_HASH,
        author_id="user-1",
        rating=rating,
        comment="Solid lossless rip",
        created_at=datetime.now(UTC),
    )
    assert review.rating == rating


@pytest.mark.parametrize("bad_rating", [0, 6, -2, True, False])
def test_file_review_rejects_invalid_ratings(bad_rating: int | bool) -> None:
    with pytest.raises(DomainValidationError):
        FileReview(
            review_id="rev-1",
            file_hash=SAMPLE_HASH,
            author_id="user-1",
            rating=bad_rating,
            comment="Valid comment",
        )


def test_file_review_rejects_empty_comment_and_bad_hash() -> None:
    with pytest.raises(DomainValidationError):
        FileReview(
            review_id="rev-1",
            file_hash=SAMPLE_HASH,
            author_id="user-1",
            rating=5,
            comment="   \t\n  ",
        )
    with pytest.raises(DomainValidationError):
        FileReview(
            review_id="rev-1",
            file_hash="not-sha256",
            author_id="user-1",
            rating=5,
            comment="Nice",
        )


def test_search_match_and_private_message() -> None:
    sf = SharedFile(file_hash=SAMPLE_HASH, file_name="song.mp3", size_bytes=100)
    rp = RemotePeer(
        peer_id="p2",
        username="bob",
        ip_address="127.0.0.1",
        tcp_port=9002,
        status=PresenceStatus.ONLINE,
    )
    match = SearchMatch(file=sf, peer=rp, avg_rating=4.5, review_count=2)
    assert match.avg_rating == 4.5

    msg = PrivateMessage(
        message_id="m1",
        sender_id="p1",
        recipient_id="p2",
        content="Hola!",
    )
    assert msg.delivered is False
    msg.mark_delivered()
    assert msg.delivered is True

    with pytest.raises(DomainValidationError):
        PrivateMessage(message_id="m2", sender_id="p1", recipient_id="p2", content="  ")


def test_download_checkpoint_and_peer_transport_error() -> None:
    cp = DownloadCheckpoint(
        task_id="task-cp-1",
        file_hash=SAMPLE_HASH,
        file_name="track.flac",
        target_path=Path("downloads/track.flac"),
        total_bytes=4096,
        bytes_transferred=1024,
        state="PAUSED",
        source_peer_id="peer-bob",
        updated_at=datetime.now(UTC),
    )
    assert cp.task_id == "task-cp-1"
    assert cp.bytes_transferred == 1024
    assert cp.source_peer_id == "peer-bob"

    err = PeerTransportError("corrupt frame")
    assert isinstance(err, DomainError)
    assert str(err) == "corrupt frame"
