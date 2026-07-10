from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


class FakeScheduledCall:
    def __init__(self, callback: Callable[[], None]) -> None:
        self.callback = callback
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeScheduler:
    def __init__(self) -> None:
        self.now = 0.0
        self._calls: list[tuple[float, FakeScheduledCall]] = []

    def clock(self) -> float:
        return self.now

    def call_later(self, delay: float, callback: Callable[[], None]) -> FakeScheduledCall:
        call = FakeScheduledCall(callback)
        self._calls.append((self.now + delay, call))
        return call

    def advance(self, seconds: float) -> None:
        target = self.now + seconds
        while True:
            due = [item for item in self._calls if item[0] <= target]
            if not due:
                break
            deadline, call = min(due, key=lambda item: item[0])
            self._calls.remove((deadline, call))
            self.now = deadline
            if not call.cancelled:
                call.callback()
        self.now = target


class FakeHandler:
    def dispatch(self, event: object) -> None:
        self.on_any_event(event)

    def on_any_event(self, event: object) -> None:
        del event


class FakeObserver:
    def __init__(self) -> None:
        self.handler: Any | None = None
        self.alive = False
        self.stopped = False

    def schedule(self, handler: object, path: str, *, recursive: bool) -> None:
        del path, recursive
        self.handler = handler

    def start(self) -> None:
        self.alive = True

    def stop(self) -> None:
        self.stopped = True
        self.alive = False

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def is_alive(self) -> bool:
        return self.alive


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


def test_watchdog_watcher_emits_a_trailing_identical_event(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    scheduler = FakeScheduler()
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        debounce=1.0,
        clock=scheduler.clock,
        scheduler=scheduler,
    )
    event = Event("modified", str(tmp_path / "module.py"))

    watcher._dispatch(event)
    scheduler.advance(0.05)
    watcher._dispatch(event)

    assert emitted == [(EventKind.MODIFY, "module.py", None)]

    scheduler.advance(0.95)

    assert emitted == [
        (EventKind.MODIFY, "module.py", None),
        (EventKind.MODIFY, "module.py", None),
    ]


def test_watchdog_watcher_bounds_a_burst_of_identical_events(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    scheduler = FakeScheduler()
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        debounce=1.0,
        clock=scheduler.clock,
        scheduler=scheduler,
    )
    event = Event("modified", str(tmp_path / "module.py"))

    for _ in range(100):
        watcher._dispatch(event)
    scheduler.advance(1.0)

    assert emitted == [
        (EventKind.MODIFY, "module.py", None),
        (EventKind.MODIFY, "module.py", None),
    ]


def test_watchdog_watcher_keeps_move_identity_through_trailing_debounce(
    tmp_path: Path,
) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    scheduler = FakeScheduler()
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        debounce=1.0,
        clock=scheduler.clock,
        scheduler=scheduler,
    )
    event = Event("moved", str(tmp_path / "old.py"), str(tmp_path / "new.py"))

    watcher._dispatch(event)
    watcher._dispatch(event)
    scheduler.advance(1.0)

    assert emitted == [
        (EventKind.MOVE, "old.py", "new.py"),
        (EventKind.MOVE, "old.py", "new.py"),
    ]


def test_watchdog_watcher_flushes_one_trailing_event_when_stopped(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    scheduler = FakeScheduler()
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        debounce=1.0,
        clock=scheduler.clock,
        scheduler=scheduler,
    )
    event = Event("modified", str(tmp_path / "module.py"))
    watcher._dispatch(event)
    watcher._dispatch(event)

    watcher.stop()
    scheduler.advance(2.0)

    assert emitted == [
        (EventKind.MODIFY, "module.py", None),
        (EventKind.MODIFY, "module.py", None),
    ]


def test_explicit_lost_history_emits_one_rescan_per_unhealthy_episode(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
    )
    lost_history = Event("overflow", str(tmp_path / ".git" / "private"))

    watcher._dispatch(lost_history)
    watcher._dispatch(lost_history)

    assert emitted == [(EventKind.RESCAN_REQUIRED, "*", None)]

    watcher._dispatch(Event("modified", str(tmp_path / "visible.py")))
    watcher._dispatch(lost_history)

    assert emitted == [
        (EventKind.RESCAN_REQUIRED, "*", None),
        (EventKind.MODIFY, "visible.py", None),
        (EventKind.RESCAN_REQUIRED, "*", None),
    ]


@pytest.mark.parametrize("event_type", ["deleted", "moved"])
def test_watched_root_loss_emits_one_rescan_without_a_path(
    tmp_path: Path,
    event_type: str,
) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
    )
    event = Event(event_type, str(tmp_path), str(tmp_path.parent / "moved-workspace"))

    watcher._dispatch(event)
    watcher._dispatch(event)

    assert emitted == [(EventKind.RESCAN_REQUIRED, "*", None)]


def test_unexpected_observer_termination_emits_one_rescan(tmp_path: Path) -> None:
    emitted: list[tuple[EventKind, str, str | None]] = []
    scheduler = FakeScheduler()
    observer = FakeObserver()
    watcher = WatchdogLocalWatcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: emitted.append((kind, path, destination)),
        clock=scheduler.clock,
        scheduler=scheduler,
        observer_factory=lambda: observer,
        event_handler_base=FakeHandler,
        health_interval=1.0,
    )
    watcher.start()
    observer.alive = False

    scheduler.advance(1.0)
    scheduler.advance(10.0)
    watcher.stop()

    assert emitted == [(EventKind.RESCAN_REQUIRED, "*", None)]
