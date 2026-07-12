from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.watch import PollingLocalWatcher, create_local_watcher


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for filesystem event")


def test_real_watchdog_creation_appends_one_relative_path_to_journal(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        watcher = create_local_watcher(
            root,
            StaticPolicyEngine(),
            lambda kind, path, destination: store.append_event("local", kind, path, destination),
        )
        watcher.start()
        try:
            (root / "hello.py").write_text("print('hello')\n", encoding="utf-8")
            _wait_until(
                lambda: any(event.path == "hello.py" for event in store.pending_events("local", 0))
            )
        finally:
            watcher.stop()

        events = store.pending_events("local", 0)

    assert any(
        event.kind in {EventKind.CREATE, EventKind.MODIFY} and event.path == "hello.py"
        for event in events
    )
    assert all(not event.path.startswith("/") for event in events)


def test_real_watchdog_preserves_a_move_as_one_journal_event(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    recorded: list[JournalEvent] = []
    sequence = 0

    def record(kind: EventKind, path: str, destination: str | None) -> None:
        nonlocal sequence
        sequence += 1
        recorded.append(JournalEvent("local", sequence, kind, path, destination))

    watcher = create_local_watcher(root, StaticPolicyEngine(), record)
    watcher.start()
    try:
        source = root / "before.py"
        source.touch()
        _wait_until(
            lambda: all(
                any(event.kind is kind and event.path == "before.py" for event in recorded)
                for kind in (EventKind.CREATE, EventKind.MODIFY)
            )
        )
        recorded.clear()
        source.rename(root / "after.py")
        _wait_until(lambda: any(event.kind is EventKind.MOVE for event in recorded))
    finally:
        watcher.stop()

    moves = [
        event
        for event in recorded
        if event.kind is EventKind.MOVE
        and event.path == "before.py"
        and event.destination_path == "after.py"
    ]
    assert moves == [
        JournalEvent("local", moves[0].sequence, EventKind.MOVE, "before.py", "after.py")
    ]
    assert not any(
        event.kind in {EventKind.CREATE, EventKind.DELETE}
        and (event.path == "before.py" or event.path == "after.py")
        for event in recorded
    )


def test_real_watchdog_never_emits_hard_ignored_metadata(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    ignored_directories = [
        root / ".git",
        root / ".remote-sandbox",
    ]
    for directory in ignored_directories:
        directory.mkdir()
    events: list[tuple[EventKind, str, str | None]] = []
    watcher = create_local_watcher(
        root,
        StaticPolicyEngine(),
        lambda kind, path, destination: events.append((kind, path, destination)),
    )
    watcher.start()
    try:
        for directory in ignored_directories:
            (directory / "metadata").write_text("private\n", encoding="utf-8")
        (root / "event-barrier").write_text("visible\n", encoding="utf-8")
        _wait_until(lambda: any(path == "event-barrier" for _kind, path, _dest in events))
    finally:
        watcher.stop()

    assert events
    assert all(
        path == "event-barrier" and destination is None for _kind, path, destination in events
    )


def test_polling_fallback_emits_changed_paths_instead_of_a_global_poke(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    events: list[tuple[EventKind, str, str | None]] = []
    watcher = PollingLocalWatcher(
        root,
        StaticPolicyEngine(),
        lambda kind, path, destination: events.append((kind, path, destination)),
        interval=0.02,
    )
    watcher.start()
    try:
        (root / "changed.txt").write_text("changed\n", encoding="utf-8")
        _wait_until(lambda: any(path == "changed.txt" for _kind, path, _dest in events))
    finally:
        watcher.stop()

    assert any(
        kind is EventKind.CREATE and path == "changed.txt" and destination is None
        for kind, path, destination in events
    )
    assert all(path != "*" for _kind, path, _destination in events)


def test_polling_fallback_requests_one_rescan_when_its_root_is_replaced(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    events: list[tuple[EventKind, str, str | None]] = []
    watcher = PollingLocalWatcher(
        root,
        StaticPolicyEngine(),
        lambda kind, path, destination: events.append((kind, path, destination)),
    )
    root.rename(tmp_path / "old-workspace")
    root.mkdir()

    watcher._poll_once()
    watcher._poll_once()

    assert events == [(EventKind.RESCAN_REQUIRED, "*", None)]
