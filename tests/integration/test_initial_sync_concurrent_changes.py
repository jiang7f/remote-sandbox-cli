from __future__ import annotations

import pytest
from helpers.sync_harness import InitialPairHarness

from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import EntryFingerprint, fingerprint_local


def test_change_created_during_bulk_copy_is_replayed(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "seed.txt").write_text("seed", encoding="utf-8")
    initial_pair.transport.on_first_progress = lambda: (
        initial_pair.remote / "late.txt"
    ).write_text("late", encoding="utf-8")

    initial_pair.coordinator.run()

    assert (initial_pair.local / "late.txt").read_text(encoding="utf-8") == "late"


def test_divergent_both_side_change_during_bulk_becomes_conflict(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "same.txt").write_text("base", encoding="utf-8")

    def diverge() -> None:
        (initial_pair.local / "same.txt").write_text("local", encoding="utf-8")
        (initial_pair.remote / "same.txt").write_text("remote", encoding="utf-8")

    initial_pair.transport.on_first_progress = diverge

    initial_pair.coordinator.run()

    conflicts = initial_pair.store.list_conflicts(unresolved_only=True)
    assert [conflict.path for conflict in conflicts] == ["same.txt"]
    assert (initial_pair.local / "same.txt").read_text(encoding="utf-8") == "local"
    assert (initial_pair.remote / "same.txt").read_text(encoding="utf-8") == "remote"


def test_restart_mid_transfer_only_copies_unfinished_paths(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")
    (initial_pair.remote / "b.txt").write_text("b", encoding="utf-8")
    initial_pair.transport.fail_after_first_progress = True

    with pytest.raises(RuntimeError, match="interruption"):
        initial_pair.coordinator.run()

    assert initial_pair.store.initial_sync_completed() is False
    assert initial_pair.store.get_initial_sync_watermarks() == (0, 0)

    assert initial_pair.store.get_status().phase.value == "degraded"
    initial_pair.coordinator.run()

    assert [item.path for item in initial_pair.transport.batches[0].items] == ["a.txt", "b.txt"]
    assert [item.path for item in initial_pair.transport.batches[1].items] == ["b.txt"]
    assert (initial_pair.local / "a.txt").read_text(encoding="utf-8") == "a"
    assert (initial_pair.local / "b.txt").read_text(encoding="utf-8") == "b"


def test_mid_transfer_exception_persists_every_reported_fingerprint(
    initial_pair: InitialPairHarness,
) -> None:
    for name in ("a.txt", "b.txt", "c.txt"):
        (initial_pair.remote / name).write_text(name, encoding="utf-8")
    initial_pair.transport.fail_after_progress_count = 2

    with pytest.raises(RuntimeError, match="interruption"):
        initial_pair.coordinator.run()

    for name in ("a.txt", "b.txt"):
        observed = fingerprint_local(initial_pair.local, name, with_hash=True)
        assert isinstance(observed, EntryFingerprint)
        assert initial_pair.store.get_base(name) == observed
        assert initial_pair.store.get_expected_echo("local", name) == observed
    assert initial_pair.store.get_base("c.txt").path == "c.txt"

    initial_pair.coordinator.run()

    assert [item.path for item in initial_pair.transport.batches[1].items] == ["c.txt"]


def test_initial_batch_persists_verified_fingerprints_without_rehash_drift(
    initial_pair: InitialPairHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (initial_pair.remote / "pkg").mkdir()
    (initial_pair.remote / "value.txt").write_text("value", encoding="utf-8")
    (initial_pair.remote / "link").symlink_to("value.txt")
    monkeypatch.setattr(initial_pair.coordinator, "_replay_until_quiet", lambda: None)

    initial_pair.coordinator.run()

    for name in ("link", "pkg", "value.txt"):
        observed = fingerprint_local(initial_pair.local, name, with_hash=True)
        assert isinstance(observed, EntryFingerprint)
        assert initial_pair.store.get_base(name) == observed
        assert initial_pair.store.get_expected_echo("local", name) == observed


def test_restart_preserves_edit_to_completed_destination(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("source-a", encoding="utf-8")
    (initial_pair.remote / "b.txt").write_text("source-b", encoding="utf-8")
    initial_pair.transport.fail_after_first_progress = True

    with pytest.raises(RuntimeError, match="interruption"):
        initial_pair.coordinator.run()

    initial_pair.local_watcher.stop()
    (initial_pair.local / "a.txt").write_text("user-edit", encoding="utf-8")
    edited = initial_pair.store.append_event("local", EventKind.MODIFY, "a.txt")
    initial_pair.coordinator.start_local_watcher = lambda: edited.sequence

    initial_pair.coordinator.run()

    assert (initial_pair.local / "a.txt").read_text(encoding="utf-8") == "user-edit"
    assert (initial_pair.remote / "a.txt").read_text(encoding="utf-8") == "user-edit"
    assert initial_pair.store.acknowledged_sequence("local") >= edited.sequence
    assert initial_pair.store.get_initial_sync_watermarks() is None
    assert (initial_pair.local / "b.txt").read_text(encoding="utf-8") == "source-b"


def test_restart_during_replay_does_not_repeat_bulk_transfer(
    initial_pair: InitialPairHarness,
    monkeypatch,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")
    original = initial_pair.engine.run_once
    failed = False

    def interrupt(reason: str):
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("injected replay interruption")
        return original(reason)

    monkeypatch.setattr(initial_pair.engine, "run_once", interrupt)

    with pytest.raises(RuntimeError, match="replay interruption"):
        initial_pair.coordinator.run()

    assert initial_pair.store.initial_sync_completed() is False
    assert initial_pair.store.get_initial_sync_watermarks() == (0, 0)
    initial_pair.coordinator.run()

    assert initial_pair.transport.transfer_calls == 1
    assert (initial_pair.local / "a.txt").read_text(encoding="utf-8") == "a"


def test_delete_during_bulk_transfer_is_replayed(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")
    (initial_pair.remote / "z.txt").write_text("z", encoding="utf-8")
    initial_pair.transport.on_first_progress = lambda: (
        initial_pair.remote / "z.txt"
    ).unlink()

    initial_pair.coordinator.run()

    assert not (initial_pair.local / "z.txt").exists()


def test_move_during_bulk_transfer_is_replayed(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")
    (initial_pair.remote / "z.txt").write_text("z", encoding="utf-8")
    initial_pair.transport.on_first_progress = lambda: (
        initial_pair.remote / "z.txt"
    ).rename(initial_pair.remote / "moved.txt")

    initial_pair.coordinator.run()

    assert not (initial_pair.local / "z.txt").exists()
    assert (initial_pair.local / "moved.txt").read_text(encoding="utf-8") == "z"


def test_change_during_scan_is_excluded_from_bulk_and_replayed(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "stable.txt").write_text("stable", encoding="utf-8")
    initial_pair.remote_client.on_before_snapshot = lambda: (
        initial_pair.remote / "late.txt"
    ).write_text("late", encoding="utf-8")

    initial_pair.coordinator.run()

    assert [item.path for item in initial_pair.transport.batches[0].items] == ["stable.txt"]
    assert [item.path for item in initial_pair.transport.batches[1].items] == ["late.txt"]
    assert (initial_pair.local / "late.txt").read_text(encoding="utf-8") == "late"
