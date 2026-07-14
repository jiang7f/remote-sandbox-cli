import hashlib
import os
import shutil

import pytest
from helpers.sync_harness import EngineHarness

from remote_sandbox.engine import SyncEngine
from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry, fingerprint_local
from remote_sandbox.placeholder import PlaceholderMetadata, encode_placeholder
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferPreflightError,
)


def test_engine_does_not_ack_event_when_transfer_changes_midflight(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.transport.change_source_before_commit("a.py")
    engine_fixture.append_local_modify("a.py", b"new")
    result = engine_fixture.engine.run_once("watcher")
    assert result.requeued == ("a.py",)
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert engine_fixture.store.get_expected_echo("remote", "a.py") is None


def test_expected_echo_is_committed_before_transfer_destination_changes(
    engine_fixture: EngineHarness,
) -> None:
    def observe(side: str, path: str) -> None:
        assert engine_fixture.store.get_expected_echo(side, path) is not None

    engine_fixture.transport.before_destination_change = observe
    engine_fixture.append_local_modify("a.py", b"new")

    result = engine_fixture.engine.run_once("watcher")

    assert result.completed == ("a.py",)


def test_expected_destination_event_is_acknowledged_as_echo(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")
    first = engine_fixture.engine.run_once("watcher")
    assert first.completed == ("a.py",)
    engine_fixture.append_remote_event_for_current_fingerprint("a.py")
    second = engine_fixture.engine.run_once("remote-watch")
    assert second.transferred == ()
    assert second.echoes == ("a.py",)


def test_echo_only_cycle_does_not_publish_transient_syncing(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")
    engine_fixture.engine.run_once("watcher")
    engine_fixture.append_remote_event_for_current_fingerprint("a.py")
    observed: list[WorkspaceStatus] = []
    original = WorkspaceStore.set_status

    def record_status(store: WorkspaceStore, status: WorkspaceStatus) -> None:
        if store is engine_fixture.store:
            observed.append(status)
        original(store, status)

    monkeypatch.setattr(WorkspaceStore, "set_status", record_status)

    result = engine_fixture.engine.run_once("remote-watch")

    assert result.echoes == ("a.py",)
    assert observed
    assert all(status.phase is not WorkspacePhase.SYNCING for status in observed)


def test_initial_replay_never_publishes_ready_before_coordinator_finishes(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")
    observed: list[WorkspaceStatus] = []
    original = WorkspaceStore.set_status

    def record_status(store: WorkspaceStore, status: WorkspaceStatus) -> None:
        if store is engine_fixture.store:
            observed.append(status)
        original(store, status)

    monkeypatch.setattr(WorkspaceStore, "set_status", record_status)

    engine_fixture.engine.run_once("initial-replay")

    assert observed
    assert {status.phase for status in observed} == {WorkspacePhase.INITIAL_SYNCING}
    assert {status.progress.stage for status in observed} == {"replaying"}


def test_differing_remote_event_is_not_suppressed_as_an_echo(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.append_local_modify("a.py", b"first")
    engine_fixture.engine.run_once("watcher")
    (engine_fixture.remote / "a.py").write_bytes(b"concurrent")
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "a.py")

    result = engine_fixture.engine.run_once("remote-watch")

    assert result.echoes == ()
    assert result.completed == ("a.py",)
    assert (engine_fixture.local / "a.py").read_bytes() == b"concurrent"


@pytest.mark.parametrize("kind", ["directory", "symlink", "deletion"])
def test_expected_echo_handles_non_regular_entry_kinds(
    engine_fixture: EngineHarness,
    kind: str,
) -> None:
    path = "entry"
    if kind == "directory":
        (engine_fixture.local / path).mkdir()
        engine_fixture.store.append_event("local", EventKind.CREATE, path)
    elif kind == "symlink":
        os.symlink("target", engine_fixture.local / path)
        engine_fixture.store.append_event("local", EventKind.CREATE, path)
    else:
        (engine_fixture.local / path).write_bytes(b"old")
        (engine_fixture.remote / path).write_bytes(b"old")
        engine_fixture.store.replace_base(
            engine_fixture.remote_client.hash_paths([path])  # type: ignore[arg-type]
        )
        (engine_fixture.local / path).unlink()
        engine_fixture.store.append_event("local", EventKind.DELETE, path)

    engine_fixture.engine.run_once("watcher")
    event_kind = EventKind.DELETE if kind == "deletion" else EventKind.CREATE
    engine_fixture.remote_client.append_event(event_kind, path)
    result = engine_fixture.engine.run_once("remote-watch")

    assert result.echoes == (path,)
    assert result.transferred == ()


def test_conflict_is_persisted_before_both_watermarks_advance(
    engine_fixture: EngineHarness,
) -> None:
    (engine_fixture.local / "a.py").write_bytes(b"base")
    (engine_fixture.remote / "a.py").write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths(["a.py"]))  # type: ignore[arg-type]
    (engine_fixture.local / "a.py").write_bytes(b"local")
    (engine_fixture.remote / "a.py").write_bytes(b"remote")
    local_event = engine_fixture.store.append_event("local", EventKind.MODIFY, "a.py")
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "a.py")

    result = engine_fixture.engine.run_once("both")

    conflict = engine_fixture.store.get_conflict(result.conflict_ids[0])
    assert conflict.local_blob == b"local"
    assert conflict.remote_blob == b"remote"
    assert engine_fixture.store.acknowledged_sequence("local") == local_event.sequence
    assert engine_fixture.store.acknowledged_sequence("remote") == 1
    assert engine_fixture.remote_client.acknowledge_calls == [1]


def test_commit_failure_rolls_back_base_echo_and_watermark_before_remote_ack(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")
    original = WorkspaceStore.upsert_base

    def fail_upsert(store: WorkspaceStore, entry: object) -> None:
        del store, entry
        raise RuntimeError("commit failed")

    monkeypatch.setattr(WorkspaceStore, "upsert_base", fail_upsert)
    with pytest.raises(RuntimeError, match="commit failed"):
        engine_fixture.engine.run_once("watcher")
    monkeypatch.setattr(WorkspaceStore, "upsert_base", original)

    assert engine_fixture.store.list_base() == {}
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert engine_fixture.store.get_expected_echo("remote", "a.py") is not None
    assert engine_fixture.remote_client.acknowledge_calls == []


def test_partial_batch_keeps_watermark_and_commits_successful_path(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.transport.change_source_before_commit("b.py")
    engine_fixture.append_local_modify("a.py", b"a")
    engine_fixture.append_local_modify("b.py", b"b")

    result = engine_fixture.engine.run_once("watcher")

    assert result.completed == ("a.py",)
    assert result.requeued == ("b.py",)
    assert engine_fixture.store.get_base("a.py").path == "a.py"
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert [event.path for event in engine_fixture.store.pending_events("local", 0)] == [
        "a.py",
        "b.py",
    ]
    assert engine_fixture.store.get_expected_echo("remote", "a.py") is not None
    assert engine_fixture.store.get_expected_echo("remote", "b.py") is None


def test_changed_action_clears_unused_echo_when_final_commit_fails(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.transport.change_source_before_commit("a.py")
    engine_fixture.append_local_modify("a.py", b"new")
    original = WorkspaceStore.set_status

    def fail_final_status(store: WorkspaceStore, status: object) -> None:
        if getattr(status, "phase", None).value != "syncing":
            raise RuntimeError("final commit failed")
        original(store, status)  # type: ignore[arg-type]

    monkeypatch.setattr(WorkspaceStore, "set_status", fail_final_status)

    with pytest.raises(RuntimeError, match="final commit failed"):
        engine_fixture.engine.run_once("watcher")

    assert engine_fixture.store.get_expected_echo("remote", "a.py") is None
    assert engine_fixture.store.acknowledged_sequence("local") == 0


def test_post_mutation_transport_error_retains_uncertain_echo_intent(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")

    def mutate_then_fail(batch: TransferBatch, on_progress: object) -> object:
        del batch, on_progress
        shutil.copy2(engine_fixture.local / "a.py", engine_fixture.remote / "a.py")
        raise RuntimeError("post-mutation failure")

    monkeypatch.setattr(engine_fixture.transport, "transfer", mutate_then_fail)

    with pytest.raises(RuntimeError, match="post-mutation failure"):
        engine_fixture.engine.run_once("watcher")

    assert (engine_fixture.remote / "a.py").read_bytes() == b"new"
    assert engine_fixture.store.get_expected_echo("remote", "a.py") is not None
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert isinstance(engine_fixture.store.get_base("a.py"), MissingEntry)
    database = engine_fixture.store.path
    engine_fixture.store.close()
    engine_fixture.store = WorkspaceStore.open(database)
    engine_fixture.engine = SyncEngine(
        store=engine_fixture.store,
        local_root=engine_fixture.local,
        remote=engine_fixture.remote_client,
        transport=engine_fixture.transport,
        policy=StaticPolicyEngine(),
    )
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "a.py")

    result = engine_fixture.engine.run_once("matching-echo-after-error")

    assert result.echoes == ("a.py",)
    assert result.conflict_ids == ()


def test_differing_event_after_uncertain_transport_error_is_not_suppressed(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"local")

    def mutate_then_fail(batch: TransferBatch, on_progress: object) -> object:
        del batch, on_progress
        shutil.copy2(engine_fixture.local / "a.py", engine_fixture.remote / "a.py")
        raise RuntimeError("post-mutation failure")

    monkeypatch.setattr(engine_fixture.transport, "transfer", mutate_then_fail)
    with pytest.raises(RuntimeError, match="post-mutation failure"):
        engine_fixture.engine.run_once("watcher")

    database = engine_fixture.store.path
    engine_fixture.store.close()
    engine_fixture.store = WorkspaceStore.open(database)
    engine_fixture.engine = SyncEngine(
        store=engine_fixture.store,
        local_root=engine_fixture.local,
        remote=engine_fixture.remote_client,
        transport=engine_fixture.transport,
        policy=StaticPolicyEngine(),
    )
    (engine_fixture.remote / "a.py").write_bytes(b"remote-later")
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "a.py")

    result = engine_fixture.engine.run_once("differing-event-after-error")

    assert result.echoes == ()
    assert len(result.conflict_ids) == 1


def test_first_direction_failure_clears_later_unattempted_direction_intent(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for path in ("push.txt", "pull.txt"):
        (engine_fixture.local / path).write_bytes(b"base")
        (engine_fixture.remote / path).write_bytes(b"base")
    base = engine_fixture.remote_client.hash_paths(("push.txt", "pull.txt"))
    engine_fixture.store.replace_base(base)  # type: ignore[arg-type]
    engine_fixture.append_local_modify("push.txt", b"local")
    (engine_fixture.remote / "pull.txt").write_bytes(b"remote")
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "pull.txt")

    def mutate_then_fail(batch: TransferBatch, on_progress: object) -> object:
        del on_progress
        assert batch.direction is TransferDirection.PUSH
        shutil.copy2(
            engine_fixture.local / "push.txt",
            engine_fixture.remote / "push.txt",
        )
        raise RuntimeError("first direction failed")

    monkeypatch.setattr(engine_fixture.transport, "transfer", mutate_then_fail)

    with pytest.raises(RuntimeError, match="first direction failed"):
        engine_fixture.engine.run_once("both-directions")

    assert engine_fixture.store.get_expected_echo("remote", "push.txt") is not None
    assert engine_fixture.store.get_expected_echo("local", "pull.txt") is None
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert engine_fixture.store.acknowledged_sequence("remote") == 0


def test_uncertain_delete_error_retains_attempted_intent_and_clears_no_watermark(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = "delete.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    (engine_fixture.remote / path).write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.remote / path).unlink()
    engine_fixture.remote_client.append_event(EventKind.DELETE, path)

    def delete_then_fail(expected: object) -> object:
        del expected
        (engine_fixture.local / path).unlink()
        raise RuntimeError("post-delete failure")

    monkeypatch.setattr(engine_fixture.transport, "delete_local", delete_then_fail)

    with pytest.raises(RuntimeError, match="post-delete failure"):
        engine_fixture.engine.run_once("remote-delete")

    assert not (engine_fixture.local / path).exists()
    assert engine_fixture.store.get_expected_echo("local", path) == MissingEntry(path)
    assert engine_fixture.store.acknowledged_sequence("remote") == 0
    assert engine_fixture.remote_client.acknowledge_calls == []


def test_preflight_transport_error_clears_unused_echo_intent(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")

    def fail_preflight(batch: TransferBatch, on_progress: object) -> object:
        del batch, on_progress
        raise TransferPreflightError("preflight failed")

    monkeypatch.setattr(engine_fixture.transport, "transfer", fail_preflight)

    with pytest.raises(TransferPreflightError, match="preflight failed"):
        engine_fixture.engine.run_once("watcher")

    assert not (engine_fixture.remote / "a.py").exists()
    assert engine_fixture.store.get_expected_echo("remote", "a.py") is None


def test_remote_delete_does_not_remove_concurrent_local_replacement(
    engine_fixture: EngineHarness,
) -> None:
    path = "delete.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    (engine_fixture.remote / path).write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.remote / path).unlink()
    engine_fixture.remote_client.append_event(EventKind.DELETE, path)
    engine_fixture.transport.change_destination_before_delete("local", path, b"concurrent")

    result = engine_fixture.engine.run_once("remote-delete")

    assert (engine_fixture.local / path).read_bytes() == b"concurrent"
    assert result.requeued == (path,)
    assert engine_fixture.store.acknowledged_sequence("remote") == 0
    assert engine_fixture.remote_client.acknowledge_calls == []
    assert engine_fixture.store.get_expected_echo("local", path) is None


def test_local_delete_does_not_remove_concurrent_remote_replacement(
    engine_fixture: EngineHarness,
) -> None:
    path = "delete.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    (engine_fixture.remote / path).write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.local / path).unlink()
    engine_fixture.store.append_event("local", EventKind.DELETE, path)
    engine_fixture.transport.change_destination_before_delete("remote", path, b"concurrent")

    result = engine_fixture.engine.run_once("local-delete")

    assert (engine_fixture.remote / path).read_bytes() == b"concurrent"
    assert result.requeued == (path,)
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert engine_fixture.store.get_expected_echo("remote", path) is None


@pytest.mark.parametrize("destination_side", ["local", "remote"])
def test_delete_requires_strong_identity_when_replacement_preserves_quick_metadata(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
    destination_side: str,
) -> None:
    path = "same-quick.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    shutil.copy2(engine_fixture.local / path, engine_fixture.remote / path)
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    if destination_side == "local":
        (engine_fixture.remote / path).unlink()
        engine_fixture.remote_client.append_event(EventKind.DELETE, path)
        method_name = "delete_local"
        destination = engine_fixture.local / path
    else:
        (engine_fixture.local / path).unlink()
        engine_fixture.store.append_event("local", EventKind.DELETE, path)
        method_name = "delete_remote"
        destination = engine_fixture.remote / path
    original = getattr(engine_fixture.transport, method_name)

    def replace_then_delete(expected: object) -> object:
        expected_entry = expected[path]  # type: ignore[index]
        assert isinstance(expected_entry, EntryFingerprint)
        assert expected_entry.content_hash is not None
        metadata = destination.stat(follow_symlinks=False)
        destination.write_bytes(b"evil")
        destination.chmod(metadata.st_mode)
        os.utime(destination, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        return original(expected)

    monkeypatch.setattr(engine_fixture.transport, method_name, replace_then_delete)

    result = engine_fixture.engine.run_once("same-quick-delete")

    assert destination.read_bytes() == b"evil"
    assert result.requeued == (path,)
    source_side = "remote" if destination_side == "local" else "local"
    assert engine_fixture.store.acknowledged_sequence(source_side) == 0
    assert engine_fixture.store.get_expected_echo(destination_side, path) is None


def test_unchanged_remote_regular_delete_completes_with_strong_expected_identity(
    engine_fixture: EngineHarness,
) -> None:
    path = "delete.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    shutil.copy2(engine_fixture.local / path, engine_fixture.remote / path)
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.local / path).unlink()
    engine_fixture.store.append_event("local", EventKind.DELETE, path)

    result = engine_fixture.engine.run_once("strong-delete")

    assert result.completed == (path,)
    assert not (engine_fixture.remote / path).exists()
    assert engine_fixture.store.list_requeued_paths() == ()


def test_signature_refresh_requeues_same_quick_change_after_base_commit(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = "refresh-race.txt"
    engine_fixture.append_local_modify(path, b"new")
    original_refresh = engine_fixture.engine.audit_coordinator.refresh

    def mutate_then_refresh(paths: object) -> None:
        candidate = engine_fixture.local / path
        metadata = candidate.stat(follow_symlinks=False)
        candidate.write_bytes(b"bad")
        candidate.chmod(metadata.st_mode)
        os.utime(candidate, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        original_refresh(paths)  # type: ignore[arg-type]

    monkeypatch.setattr(engine_fixture.engine.audit_coordinator, "refresh", mutate_then_refresh)

    first = engine_fixture.engine.run_once("watcher")

    assert first.completed == (path,)
    assert engine_fixture.store.list_requeued_paths() == (path,)
    monkeypatch.setattr(engine_fixture.engine.audit_coordinator, "refresh", original_refresh)

    recovered = engine_fixture.engine.audit()

    assert recovered.completed == (path,)
    assert (engine_fixture.remote / path).read_bytes() == b"bad"


def test_expected_echo_is_committed_before_delete_destination_changes(
    engine_fixture: EngineHarness,
) -> None:
    path = "delete.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    (engine_fixture.remote / path).write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.local / path).unlink()
    engine_fixture.store.append_event("local", EventKind.DELETE, path)

    def observe(side: str, observed_path: str) -> None:
        assert engine_fixture.store.get_expected_echo(side, observed_path) == MissingEntry(
            observed_path
        )

    engine_fixture.transport.before_destination_change = observe

    result = engine_fixture.engine.run_once("local-delete")

    assert result.completed == (path,)


@pytest.mark.parametrize("side", ["local", "remote"])
def test_conflict_content_change_before_capture_is_requeued_without_ack(
    engine_fixture: EngineHarness,
    monkeypatch: pytest.MonkeyPatch,
    side: str,
) -> None:
    path = "conflict.txt"
    (engine_fixture.local / path).write_bytes(b"base")
    (engine_fixture.remote / path).write_bytes(b"base")
    engine_fixture.store.replace_base(engine_fixture.remote_client.hash_paths([path]))  # type: ignore[arg-type]
    (engine_fixture.local / path).write_bytes(b"local")
    (engine_fixture.remote / path).write_bytes(b"remote")
    engine_fixture.store.append_event("local", EventKind.MODIFY, path)
    engine_fixture.remote_client.append_event(EventKind.MODIFY, path)
    original = engine_fixture.engine._prepare_conflict

    def mutate_then_capture(conflict: object) -> object:
        root = engine_fixture.local if side == "local" else engine_fixture.remote
        (root / path).write_bytes(f"{side}-later".encode())
        return original(conflict)  # type: ignore[arg-type]

    monkeypatch.setattr(engine_fixture.engine, "_prepare_conflict", mutate_then_capture)

    result = engine_fixture.engine.run_once("conflict-race")

    assert result.conflict_ids == ()
    assert result.requeued == (path,)
    assert engine_fixture.store.list_conflicts() == []
    assert engine_fixture.store.acknowledged_sequence("local") == 0
    assert engine_fixture.store.acknowledged_sequence("remote") == 0
    assert engine_fixture.remote_client.acknowledge_calls == []


def test_cycle_fetches_metadata_for_only_coalesced_dirty_paths(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.append_local_modify("dirty.py", b"new")
    (engine_fixture.local / "clean.py").write_bytes(b"untouched")
    (engine_fixture.remote / "clean.py").write_bytes(b"untouched")

    engine_fixture.engine.run_once("watcher")

    assert engine_fixture.remote_client.metadata_calls[0] == ("dirty.py",)
    assert engine_fixture.remote_client.snapshot_calls == 0


def test_restart_preserves_pending_event_and_expected_echo(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.append_local_modify("pending.py", b"pending")
    database = engine_fixture.store.path
    engine_fixture.store.close()
    engine_fixture.store = WorkspaceStore.open(database)
    engine_fixture.engine = SyncEngine(
        store=engine_fixture.store,
        local_root=engine_fixture.local,
        remote=engine_fixture.remote_client,
        transport=engine_fixture.transport,
        policy=StaticPolicyEngine(),
    )

    first = engine_fixture.engine.run_once("restart")
    engine_fixture.remote_client.append_event(EventKind.MODIFY, "pending.py")
    second = engine_fixture.engine.run_once("echo-after-restart")

    assert first.completed == ("pending.py",)
    assert second.echoes == ("pending.py",)


def test_seed_base_from_transfer_registers_destination_echo(
    engine_fixture: EngineHarness,
) -> None:
    (engine_fixture.local / "seed.py").write_bytes(b"seed")
    source = fingerprint_local(engine_fixture.local, "seed.py", with_hash=True)
    assert isinstance(source, EntryFingerprint)
    batch = TransferBatch(
        TransferDirection.PUSH,
        (TransferItem("seed.py", source, MissingEntry("seed.py")),),
    )
    result = engine_fixture.transport.transfer(batch, lambda _result: None)

    engine_fixture.engine.seed_base_from_transfer(batch, result.completed)

    expected = engine_fixture.remote_client.hash_paths(["seed.py"])["seed.py"]
    assert engine_fixture.store.get_base("seed.py") == expected
    assert engine_fixture.store.get_expected_echo("remote", "seed.py") == expected


def test_apply_initial_placeholders_updates_base_and_expected_local_echo(
    engine_fixture: EngineHarness,
) -> None:
    content = b"remote-real-content"
    content_hash = hashlib.sha256(content).hexdigest()
    (engine_fixture.remote / "large.bin").write_bytes(content)
    remote_stat = (engine_fixture.remote / "large.bin").stat(follow_symlinks=False)
    (engine_fixture.local / "large.bin").write_bytes(
        encode_placeholder(
            PlaceholderMetadata(
                "large.bin",
                len(content),
                remote_stat.st_mtime_ns,
                content_hash,
            )
        )
    )
    placeholder = EntryFingerprint(
        "large.bin",
        EntryKind.FILE,
        len(content),
        remote_stat.st_mtime_ns,
        remote_stat.st_mode,
        content_hash=content_hash,
        is_placeholder=True,
    )

    engine_fixture.engine.apply_initial_placeholders({"large.bin": placeholder})

    assert engine_fixture.store.get_base("large.bin") == placeholder
    assert engine_fixture.store.get_expected_echo("local", "large.bin") == placeholder
    assert engine_fixture.store.list_requeued_paths() == ()
    assert "large.bin" in engine_fixture.store.list_audit_signatures("local")
    assert "large.bin" in engine_fixture.store.list_audit_signatures("remote")


def test_apply_initial_placeholders_requeues_remote_content_mismatch(
    engine_fixture: EngineHarness,
) -> None:
    expected_content = b"expected-content"
    remote_content = b"mismatch-content"
    content_hash = hashlib.sha256(expected_content).hexdigest()
    (engine_fixture.remote / "large.bin").write_bytes(remote_content)
    remote_stat = (engine_fixture.remote / "large.bin").stat(follow_symlinks=False)
    (engine_fixture.local / "large.bin").write_bytes(
        encode_placeholder(
            PlaceholderMetadata(
                "large.bin",
                len(expected_content),
                remote_stat.st_mtime_ns,
                content_hash,
            )
        )
    )
    placeholder = EntryFingerprint(
        "large.bin",
        EntryKind.FILE,
        len(expected_content),
        remote_stat.st_mtime_ns,
        remote_stat.st_mode,
        content_hash=content_hash,
        is_placeholder=True,
    )

    engine_fixture.engine.apply_initial_placeholders({"large.bin": placeholder})

    assert engine_fixture.store.list_requeued_paths() == ("large.bin",)


def test_requeue_paths_public_helper_is_durable(engine_fixture: EngineHarness) -> None:
    engine_fixture.engine.requeue_paths(["b.py", "a.py"], "manual-recovery")

    assert engine_fixture.store.list_requeued_paths() == ("a.py", "b.py")


def test_matching_independent_creates_update_base_without_transfer(
    engine_fixture: EngineHarness,
) -> None:
    (engine_fixture.local / "same.py").write_bytes(b"same")
    (engine_fixture.remote / "same.py").write_bytes(b"same")
    engine_fixture.store.append_event("local", EventKind.CREATE, "same.py")
    engine_fixture.remote_client.append_event(EventKind.CREATE, "same.py")

    result = engine_fixture.engine.run_once("matching-create")

    assert result.completed == ("same.py",)
    assert result.transferred == ()
    assert engine_fixture.transport.transfer_calls == 0
    assert engine_fixture.store.get_base("same.py").path == "same.py"


def test_special_entry_returns_warning_and_does_not_block_ack(
    engine_fixture: EngineHarness,
) -> None:
    os.mkfifo(engine_fixture.local / "pipe")
    event = engine_fixture.store.append_event("local", EventKind.CREATE, "pipe")

    result = engine_fixture.engine.run_once("special")

    assert [(warning.path, warning.reason) for warning in result.warnings] == [
        ("pipe", "special-entry-not-transferred")
    ]
    assert engine_fixture.store.acknowledged_sequence("local") == event.sequence
