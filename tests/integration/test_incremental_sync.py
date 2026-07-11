import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest
from helpers.sync_harness import (
    LocalReplicaClient,
    SyncPair,
    snapshot_matching_replicas,
)

from remote_sandbox.conflicts import resolve_conflict_transaction
from remote_sandbox.engine import SyncEngine
from remote_sandbox.journal import EventKind
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.state import WorkspacePhase, WorkspaceStore
from remote_sandbox.transport import LocalPairTransport, TransferDirection


@dataclass(slots=True)
class ProductionSyncPair:
    local: Path
    remote: Path
    store: WorkspaceStore
    remote_client: LocalReplicaClient
    transport: LocalPairTransport
    engine: SyncEngine

    def seed_current_base(self) -> None:
        entries = snapshot_matching_replicas(self.local, self.remote, with_hash=True)
        self.store.replace_base(entries)
        self.engine.audit_coordinator.refresh(entries)


@pytest.fixture
def production_sync_pair(tmp_path: Path) -> ProductionSyncPair:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    store = WorkspaceStore.open(tmp_path / "state.sqlite3")
    remote_client = LocalReplicaClient(remote, tmp_path / "remote-state.sqlite3")
    transport = LocalPairTransport(local, remote)
    engine = SyncEngine(
        store=store,
        local_root=local,
        remote=remote_client,
        transport=transport,
        policy=StaticPolicyEngine(),
    )
    pair = ProductionSyncPair(local, remote, store, remote_client, transport, engine)
    try:
        yield pair
    finally:
        store.close()
        remote_client.close()


def test_local_modify_and_remote_delete_are_reconciled_incrementally(
    sync_pair: SyncPair,
) -> None:
    (sync_pair.local / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.remote / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.local / "remote.txt").write_text("delete", encoding="utf-8")
    (sync_pair.remote / "remote.txt").write_text("delete", encoding="utf-8")
    sync_pair.seed_current_base()

    sync_pair.append_local_modify("local.txt", b"new")
    sync_pair.append_remote_delete("remote.txt")
    result = sync_pair.engine.run_once("integration")

    assert set(result.completed) == {"local.txt", "remote.txt"}
    assert (sync_pair.remote / "local.txt").read_bytes() == b"new"
    assert not (sync_pair.local / "remote.txt").exists()
    assert sync_pair.transport.transfer_calls == 1
    assert sync_pair.remote_client.snapshot_calls == 0


def test_move_coalesces_source_and_destination_without_full_snapshot(sync_pair: SyncPair) -> None:
    (sync_pair.local / "old.txt").write_bytes(b"content")
    (sync_pair.remote / "old.txt").write_bytes(b"content")
    sync_pair.seed_current_base()
    (sync_pair.local / "old.txt").rename(sync_pair.local / "new.txt")
    sync_pair.store.append_event("local", EventKind.MOVE, "old.txt", "new.txt")

    result = sync_pair.engine.run_once("move")

    assert result.completed == ("new.txt", "old.txt")
    assert not (sync_pair.remote / "old.txt").exists()
    assert (sync_pair.remote / "new.txt").read_bytes() == b"content"
    assert sync_pair.remote_client.metadata_calls[0] == ("new.txt", "old.txt")
    assert sync_pair.remote_client.snapshot_calls == 0


def test_local_non_empty_directory_move_reconciles_descendants(
    sync_pair: SyncPair,
) -> None:
    for root in (sync_pair.local, sync_pair.remote):
        child = root / "old/subdir/value.txt"
        child.parent.mkdir(parents=True)
        child.write_bytes(b"content")
    sync_pair.seed_current_base()
    (sync_pair.local / "old").rename(sync_pair.local / "new")
    sequence = sync_pair.store.append_event("local", EventKind.MOVE, "old", "new")

    result = sync_pair.engine.run_once("local-directory-move")

    assert not (sync_pair.remote / "old").exists()
    assert (sync_pair.remote / "new/subdir/value.txt").read_bytes() == b"content"
    assert "old/subdir/value.txt" in result.completed
    assert "new/subdir/value.txt" in result.completed
    assert result.requeued == ()
    assert sync_pair.store.acknowledged_sequence("local") == sequence.sequence
    assert sync_pair.store.list_requeued_paths() == ()


def test_remote_non_empty_directory_move_reconciles_descendants(
    sync_pair: SyncPair,
) -> None:
    for root in (sync_pair.local, sync_pair.remote):
        child = root / "old/subdir/value.txt"
        child.parent.mkdir(parents=True)
        child.write_bytes(b"content")
    sync_pair.seed_current_base()
    (sync_pair.remote / "old").rename(sync_pair.remote / "new")
    sync_pair.remote_client.append_event(EventKind.MOVE, "old", "new")

    result = sync_pair.engine.run_once("remote-directory-move")

    assert not (sync_pair.local / "old").exists()
    assert (sync_pair.local / "new/subdir/value.txt").read_bytes() == b"content"
    assert "old/subdir/value.txt" in result.completed
    assert "new/subdir/value.txt" in result.completed
    assert result.requeued == ()
    assert sync_pair.remote_client.acknowledged_sequence() == 1
    assert sync_pair.store.list_requeued_paths() == ()


def test_local_directory_delete_defers_verified_remote_ancestor_for_child_conflict(
    production_sync_pair: ProductionSyncPair,
) -> None:
    pair = production_sync_pair
    child = "tree/value.txt"
    for root in (pair.local, pair.remote):
        (root / child).parent.mkdir()
        (root / child).write_bytes(b"base")
    pair.seed_current_base()
    shutil.rmtree(pair.local / "tree")
    (pair.remote / child).write_bytes(b"remote changed")
    local_event = pair.store.append_event("local", EventKind.DELETE, "tree")
    pair.remote_client.append_event(EventKind.MODIFY, child)
    remote_sequence = pair.remote_client.latest_sequence()

    first = pair.engine.run_once("local-directory-delete-conflict")

    conflicts = pair.store.list_conflicts(unresolved_only=True)
    assert len(conflicts) == 1
    assert conflicts[0].path == child
    assert (pair.remote / child).read_bytes() == b"remote changed"
    assert {"tree", child} <= set(pair.store.list_base())
    assert first.requeued == ("tree",)
    assert pair.store.list_requeued_paths() == ("tree",)
    assert pair.store.acknowledged_sequence("local") == local_event.sequence
    assert pair.remote_client.acknowledged_sequence() == remote_sequence
    assert pair.store.get_status().phase is WorkspacePhase.DEGRADED
    assert pair.store.get_status().last_error is None

    repeated = pair.engine.run_once("unresolved-directory-delete-conflict")

    assert repeated.requeued == ("tree",)
    assert pair.store.list_requeued_paths() == ("tree",)
    assert pair.store.get_status().phase is WorkspacePhase.DEGRADED
    assert (pair.remote / child).read_bytes() == b"remote changed"

    resolve_conflict_transaction(
        store=pair.store,
        local_root=pair.local,
        remote=pair.remote_client,
        transport=pair.transport,
        path=child,
        use_local=True,
    )
    final = pair.engine.run_once("resolved-directory-delete-conflict")

    assert not (pair.local / "tree").exists()
    assert not (pair.remote / "tree").exists()
    assert "tree" in final.completed
    assert pair.store.list_requeued_paths() == ()
    assert "tree" not in pair.store.list_base()
    assert pair.store.list_conflicts(unresolved_only=True) == []


def test_remote_directory_delete_defers_verified_local_ancestor_for_child_conflict(
    production_sync_pair: ProductionSyncPair,
) -> None:
    pair = production_sync_pair
    child = "tree/value.txt"
    for root in (pair.local, pair.remote):
        (root / child).parent.mkdir()
        (root / child).write_bytes(b"base")
    pair.seed_current_base()
    shutil.rmtree(pair.remote / "tree")
    (pair.local / child).write_bytes(b"local changed")
    local_event = pair.store.append_event("local", EventKind.MODIFY, child)
    pair.remote_client.append_event(EventKind.DELETE, "tree")
    remote_sequence = pair.remote_client.latest_sequence()

    first = pair.engine.run_once("remote-directory-delete-conflict")

    conflicts = pair.store.list_conflicts(unresolved_only=True)
    assert len(conflicts) == 1
    assert conflicts[0].path == child
    assert (pair.local / child).read_bytes() == b"local changed"
    assert {"tree", child} <= set(pair.store.list_base())
    assert first.requeued == ("tree",)
    assert pair.store.list_requeued_paths() == ("tree",)
    assert pair.store.acknowledged_sequence("local") == local_event.sequence
    assert pair.remote_client.acknowledged_sequence() == remote_sequence
    assert pair.store.get_status().phase is WorkspacePhase.DEGRADED
    assert pair.store.get_status().last_error is None

    repeated = pair.engine.run_once("unresolved-directory-delete-conflict")

    assert repeated.requeued == ("tree",)
    assert pair.store.list_requeued_paths() == ("tree",)
    assert pair.store.get_status().phase is WorkspacePhase.DEGRADED
    assert (pair.local / child).read_bytes() == b"local changed"

    resolve_conflict_transaction(
        store=pair.store,
        local_root=pair.local,
        remote=pair.remote_client,
        transport=pair.transport,
        path=child,
        use_local=False,
    )
    final = pair.engine.run_once("resolved-directory-delete-conflict")

    assert not (pair.local / "tree").exists()
    assert not (pair.remote / "tree").exists()
    assert "tree" in final.completed
    assert pair.store.list_requeued_paths() == ()
    assert "tree" not in pair.store.list_base()
    assert pair.store.list_conflicts(unresolved_only=True) == []


def test_engine_uses_one_transfer_batch_per_direction(sync_pair: SyncPair) -> None:
    for path in ("push.txt", "pull.txt"):
        (sync_pair.local / path).write_bytes(b"base")
        shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    sync_pair.append_local_modify("push.txt", b"local")
    (sync_pair.remote / "pull.txt").write_bytes(b"remote")
    sync_pair.remote_client.append_event(EventKind.MODIFY, "pull.txt")

    result = sync_pair.engine.run_once("both-directions")

    assert result.completed == ("pull.txt", "push.txt")
    assert [batch.direction for batch in sync_pair.transport.batches] == [
        TransferDirection.PUSH,
        TransferDirection.PULL,
    ]
