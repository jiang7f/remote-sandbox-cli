from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from remote_sandbox.journal import EventKind
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.watch import WatchdogLocalWatcher, map_watchdog_event


@dataclass(slots=True)
class Event:
    event_type: str
    src_path: str
    dest_path: str = ""
    is_directory: bool = False


def test_watchdog_move_becomes_one_relative_move_event() -> None:
    event = Event("moved", "/workspace/old.py", "/workspace/new.py")

    mapped = map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event)

    assert mapped == (EventKind.MOVE, "old.py", "new.py")


@pytest.mark.parametrize(
    ("event_type", "expected_kind"),
    [
        ("created", EventKind.CREATE),
        ("modified", EventKind.MODIFY),
        ("deleted", EventKind.DELETE),
    ],
)
def test_watchdog_events_use_relative_paths(
    event_type: str,
    expected_kind: EventKind,
) -> None:
    event = Event(event_type, "/workspace/pkg/module.py")

    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) == (
        expected_kind,
        "pkg/module.py",
        None,
    )


@pytest.mark.parametrize(
    "ignored_path",
    [
        ".git/index",
        "nested/.git/config",
        ".remote-sandbox/state.sqlite3",
        ".codex-remote-sandbox/daemon.log",
    ],
)
def test_hard_ignored_paths_do_not_emit_events(ignored_path: str) -> None:
    event = Event("modified", f"/workspace/{ignored_path}")

    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) is None


def test_move_into_an_ignored_directory_becomes_a_delete() -> None:
    event = Event("moved", "/workspace/visible.py", "/workspace/.git/visible.py")

    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) == (
        EventKind.DELETE,
        "visible.py",
        None,
    )


def test_move_out_of_an_ignored_directory_becomes_a_create() -> None:
    event = Event("moved", "/workspace/.git/restored.py", "/workspace/restored.py")

    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) == (
        EventKind.CREATE,
        "restored.py",
        None,
    )


def test_event_outside_the_workspace_is_not_emitted() -> None:
    event = Event("modified", "/other/location.py")

    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) is None


def test_watchdog_watcher_debounces_only_identical_events(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        debounce=1.0,
    )
    event = Event("modified", str(tmp_path / "module.py"))

    watcher._dispatch(event)
    watcher._dispatch(event)
    watcher._dispatch(Event("deleted", str(tmp_path / "module.py")))

    assert emitted == [
        (EventKind.MODIFY, "module.py", None),
        (EventKind.DELETE, "module.py", None),
    ]
