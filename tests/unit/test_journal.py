from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from remote_sandbox.journal import EventKind, JournalEvent, coalesce_events
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.state import AuditSignature, WorkspaceStore


def _fingerprint(path: str, digest: str = "abc") -> EntryFingerprint:
    return EntryFingerprint(
        path,
        EntryKind.FILE,
        3,
        123,
        0o100644,
        content_hash=digest,
    )


def test_events_are_ordered_and_acknowledged_transactionally(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(db) as store:
        first = store.append_event("local", EventKind.MODIFY, "a.py")
        second = store.append_event("local", EventKind.DELETE, "b.py")
        assert [event.sequence for event in store.pending_events("local", 0)] == [
            first.sequence,
            second.sequence,
        ]
        store.acknowledge("local", first.sequence)
        assert [event.path for event in store.pending_events("local", first.sequence)] == ["b.py"]

    with WorkspaceStore.open(db) as store:
        assert store.acknowledged_sequence("local") == first.sequence
        assert [event.path for event in store.pending_events("local", 0)] == ["b.py"]


def test_event_sequences_are_monotonic_per_side(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        local_first = store.append_event("local", EventKind.CREATE, "a.py")
        remote_first = store.append_event("remote", EventKind.CREATE, "a.py")
        local_second = store.append_event("local", EventKind.MODIFY, "a.py")

        assert (local_first.sequence, local_second.sequence) == (1, 2)
        assert remote_first.sequence == 1


def test_acknowledgement_never_moves_backwards(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        first = store.append_event("local", EventKind.MODIFY, "a.py")
        second = store.append_event("local", EventKind.MODIFY, "b.py")
        store.acknowledge("local", second.sequence)
        store.acknowledge("local", first.sequence)

        assert store.acknowledged_sequence("local") == second.sequence
        assert store.pending_events("local", 0) == []


def test_acknowledgement_cannot_exceed_the_last_sequence_for_its_side(
    tmp_path: Path,
) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        local = store.append_event("local", EventKind.MODIFY, "a.py")

        with pytest.raises(ValueError, match="unallocated journal sequence"):
            store.acknowledge("local", local.sequence + 1)
        with pytest.raises(ValueError, match="unallocated journal sequence"):
            store.acknowledge("remote", local.sequence)

        assert store.acknowledged_sequence("local") == 0
        assert store.acknowledged_sequence("remote") == 0


def test_sequence_allocation_survives_pruning_acknowledged_rows(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(db) as store:
        first = store.append_event("local", EventKind.MODIFY, "a.py")
        second = store.append_event("local", EventKind.MODIFY, "b.py")
        store.acknowledge("local", second.sequence)

    connection = sqlite3.connect(db)
    try:
        connection.execute(
            "DELETE FROM events WHERE side = ? AND sequence <= ?",
            ("local", second.sequence),
        )
        connection.commit()
    finally:
        connection.close()

    with WorkspaceStore.open(db) as store:
        local = store.append_event("local", EventKind.CREATE, "c.py")
        remote = store.append_event("remote", EventKind.CREATE, "c.py")

        assert local.sequence == second.sequence + 1
        assert remote.sequence == first.sequence


def test_move_event_round_trips_its_destination(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        event = store.append_event("remote", EventKind.MOVE, "old.py", "new.py")

    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        assert store.pending_events("remote", 0) == [event]


def test_coalescing_preserves_move_delete_and_overflow_meaning() -> None:
    events = [
        JournalEvent("local", 1, EventKind.MODIFY, "a.py"),
        JournalEvent("local", 2, EventKind.MODIFY, "a.py"),
        JournalEvent("local", 3, EventKind.MOVE, "old.py", "new.py"),
        JournalEvent("local", 4, EventKind.DELETE, "a.py"),
        JournalEvent("local", 5, EventKind.RESCAN_REQUIRED, "*"),
    ]

    coalesced = coalesce_events(events)

    assert [(event.kind, event.path, event.destination_path) for event in coalesced] == [
        (EventKind.MOVE, "old.py", "new.py"),
        (EventKind.DELETE, "a.py", None),
        (EventKind.RESCAN_REQUIRED, "*", None),
    ]


def test_coalescing_create_then_modify_keeps_create_with_latest_sequence() -> None:
    events = [
        JournalEvent("local", 2, EventKind.CREATE, "a.py"),
        JournalEvent("local", 4, EventKind.MODIFY, "a.py"),
    ]

    assert coalesce_events(events) == (
        JournalEvent("local", 4, EventKind.CREATE, "a.py"),
    )


def test_event_sequence_requires_an_integer() -> None:
    with pytest.raises(ValueError, match="integer"):
        JournalEvent(
            "local",
            1.5,  # type: ignore[arg-type]
            EventKind.MODIFY,
            "a.py",
        )


def test_expected_echo_is_durable_and_consumed_only_on_match(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    expected = _fingerprint("a.py")
    with WorkspaceStore.open(db) as store:
        store.set_expected_echo("remote", expected)

    with WorkspaceStore.open(db) as store:
        assert not store.consume_expected_echo("remote", _fingerprint("a.py", "different"))
        assert store.consume_expected_echo("remote", expected)
        assert not store.consume_expected_echo("remote", expected)


def test_expected_echo_can_represent_a_synchronized_deletion(tmp_path: Path) -> None:
    missing = MissingEntry("deleted.py")
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        store.set_expected_echo("remote", missing)

        assert store.consume_expected_echo("remote", missing)


def test_remote_events_are_imported_with_their_original_sequences(tmp_path: Path) -> None:
    events = [
        JournalEvent("remote", 4, EventKind.MODIFY, "a.py"),
        JournalEvent("remote", 7, EventKind.MOVE, "old.py", "new.py"),
    ]
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        store.record_events(events)
        store.record_events(events)

        assert store.pending_events("remote", 0) == events
        store.acknowledge("remote", 7)
        assert store.acknowledged_sequence("remote") == 7


def test_requeued_paths_are_durable_until_explicitly_cleared(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(database) as store:
        store.requeue_paths(["b.py", "a.py"], "changed-during-transfer")

    with WorkspaceStore.open(database) as store:
        assert store.list_requeued_paths() == ("a.py", "b.py")
        store.clear_requeued_paths(["a.py"])
        assert store.list_requeued_paths() == ("b.py",)


def test_expected_echo_can_be_inspected_without_consuming_it(tmp_path: Path) -> None:
    expected = _fingerprint("a.py")
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        store.set_expected_echo("remote", expected)

        assert store.get_expected_echo("remote", "a.py") == expected
        assert store.consume_expected_echo("remote", expected)


def test_reconciliation_transaction_rolls_back_all_state(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        event = store.append_event("local", EventKind.MODIFY, "a.py")
        try:
            with store.transaction():
                store.replace_base({"a.py": _fingerprint("a.py")})
                store.acknowledge("local", event.sequence)
                raise RuntimeError("abort")
        except RuntimeError:
            pass

        assert store.list_base() == {}
        assert store.acknowledged_sequence("local") == 0


def test_nested_transaction_uses_a_savepoint_when_inner_work_fails(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        with store.transaction():
            try:
                with store.transaction():
                    store.replace_base({"a.py": _fingerprint("a.py")})
                    raise RuntimeError("abort inner work")
            except RuntimeError:
                pass
            store.upsert_base(_fingerprint("b.py"))

        assert store.list_base() == {"b.py": _fingerprint("b.py")}


def test_other_thread_cannot_read_partial_reconciliation_state(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        reader_started = threading.Event()
        reader_finished = threading.Event()
        observed: dict[str, EntryFingerprint] = {}

        def read_base() -> None:
            reader_started.set()
            observed.update(store.list_base())
            reader_finished.set()

        reader = threading.Thread(target=read_base)
        with (
            pytest.raises(RuntimeError, match="abort reconciliation"),
            store.transaction(),
        ):
            store.replace_base({"a.py": _fingerprint("a.py")})
            reader.start()
            assert reader_started.wait(timeout=1)
            assert not reader_finished.wait(timeout=0.05)
            raise RuntimeError("abort reconciliation")

        reader.join(timeout=1)
        assert not reader.is_alive()
        assert observed == {}


def test_audit_signatures_are_durable_per_side(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    signature = AuditSignature("a.py", EntryKind.FILE, 10, 20, 30)
    with WorkspaceStore.open(database) as store:
        store.update_audit_signatures("local", {"a.py": signature})

    with WorkspaceStore.open(database) as reopened:
        assert reopened.list_audit_signatures("local") == {"a.py": signature}
        assert reopened.list_audit_signatures("remote") == {}
