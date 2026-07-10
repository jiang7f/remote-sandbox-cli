from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from remote_sandbox.remote_agent.store import RemoteStore


def test_remote_events_survive_store_reopen(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with RemoteStore(db) as store:
        event = store.append_event("modify", "train.py", None)

    with RemoteStore(db) as store:
        assert store.events_after(0)[0].sequence == event.sequence
        store.acknowledge(event.sequence)
        assert store.acknowledged_sequence() == event.sequence


def test_remote_store_uses_wal_and_creates_all_durable_tables(tmp_path: Path) -> None:
    db = tmp_path / "metadata" / "state.sqlite3"

    with RemoteStore(db), sqlite3.connect(db) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert journal_mode == "wal"
    assert {"workspace", "events", "watermark", "watcher", "remote_index"} <= tables
    assert db.parent.stat().st_mode & 0o777 == 0o700
    assert db.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("root", [Path("relative"), Path("/")])
def test_register_rejects_unsafe_workspace_roots(tmp_path: Path, root: Path) -> None:
    with (
        RemoteStore(tmp_path / "state.sqlite3") as store,
        pytest.raises(ValueError, match="root"),
    ):
        store.register_workspace("workspace-1", root, home=tmp_path / "home")


def test_register_rejects_home_and_noncanonical_roots(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    root = tmp_path / "root"
    root.mkdir()

    with RemoteStore(tmp_path / "state.sqlite3") as store:
        with pytest.raises(ValueError, match="home"):
            store.register_workspace("workspace-1", home, home=home)
        with pytest.raises(ValueError, match="canonical"):
            store.register_workspace("workspace-1", root / ".." / "root", home=home)


def test_remote_index_rejects_rebinding_a_root_or_workspace(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    db = tmp_path / "index.sqlite3"

    with RemoteStore(db) as store:
        entry = store.register_index("workspace-1", first, tmp_path / "one.sqlite3")
        assert store.workspace_for_root(first) == entry
        assert store.index_entry("workspace-1") == entry

        with pytest.raises(ValueError, match="already registered"):
            store.register_index("workspace-2", first, tmp_path / "two.sqlite3")
        with pytest.raises(ValueError, match="already registered"):
            store.register_index("workspace-1", second, tmp_path / "two.sqlite3")

    with RemoteStore(db) as store:
        assert store.index_entry("workspace-1") == entry


def test_acknowledgement_is_monotonic_and_cannot_skip_unwritten_events(tmp_path: Path) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        first = store.append_event("create", "a.py", None)
        second = store.append_event("delete", "b.py", None)

        store.acknowledge(second.sequence)
        store.acknowledge(first.sequence)
        assert store.acknowledged_sequence() == second.sequence
        with pytest.raises(ValueError, match="latest"):
            store.acknowledge(second.sequence + 1)


def test_move_and_rescan_events_enforce_their_payload_shapes(tmp_path: Path) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        move = store.append_event("move", "old.py", "new.py")
        rescan = store.append_event("rescan-required", "*", None)

        assert move.destination_path == "new.py"
        assert rescan.path == "*"
        with pytest.raises(ValueError, match="destination"):
            store.append_event("move", "old.py", None)
        with pytest.raises(ValueError, match="relative"):
            store.append_event("modify", "../outside", None)
        with pytest.raises(ValueError, match="relative"):
            store.append_event("modify", ".", None)


def test_watcher_state_is_visible_before_and_after_reopen(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with RemoteStore(db) as store:
        store.record_watcher(1234, "starting", backend=None, token="watcher-token")

    with RemoteStore(db) as store:
        state = store.watcher_state()
        assert state.pid == 1234
        assert state.status == "starting"
        assert state.token == "watcher-token"
        store.record_watcher(
            1234,
            "running",
            backend="polling",
            token="watcher-token",
        )
        assert store.watcher_state().backend == "polling"


def test_unstreamed_modify_burst_coalesces_without_allocating_a_new_sequence(
    tmp_path: Path,
) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        first = store.append_event("modify", "model.py", None)
        second = store.append_event("modify", "model.py", None)

        assert second.sequence == first.sequence
        assert [(event.sequence, event.kind) for event in store.events_after(0)] == [
            (first.sequence, "modify")
        ]


def test_change_after_streaming_allocates_a_new_sequence(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with RemoteStore(db) as store:
        first = store.append_event("modify", "model.py", None)
        assert store.events_after(0) == [first]

    with RemoteStore(db) as store:
        second = store.append_event("modify", "model.py", None)

        assert second.sequence > first.sequence
        assert [event.sequence for event in store.events_after(0)] == [
            first.sequence,
            second.sequence,
        ]


def test_delete_then_create_coalesces_to_modify_without_losing_dirty_path(
    tmp_path: Path,
) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        deleted = store.append_event("delete", "model.py", None)
        recreated = store.append_event("create", "model.py", None)

        assert recreated.sequence == deleted.sequence
        assert [(event.kind, event.path) for event in store.events_after(0)] == [
            ("modify", "model.py")
        ]


def test_move_identity_is_not_coalesced_with_later_endpoint_changes(tmp_path: Path) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        moved = store.append_event("move", "old.py", "new.py")
        modified = store.append_event("modify", "new.py", None)

        assert modified.sequence > moved.sequence
        assert [event.kind for event in store.events_after(0)] == ["move", "modify"]


def test_event_queries_support_bounded_batches(tmp_path: Path) -> None:
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        for index in range(3):
            store.append_event("create", f"{index}.txt", None)

        first_batch = store.events_after(0, limit=2)
        second_batch = store.events_after(first_batch[-1].sequence, limit=2)

        assert [event.path for event in first_batch] == ["0.txt", "1.txt"]
        assert [event.path for event in second_batch] == ["2.txt"]
