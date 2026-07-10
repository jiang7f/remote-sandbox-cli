from __future__ import annotations

import importlib.util
import os
import stat
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, overload

from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import normalize_relative_path
from remote_sandbox.policy import PolicyEngine

EventCallback = Callable[[EventKind, str, str | None], None]
PolicySource = PolicyEngine | Callable[[], PolicyEngine]
Clock = Callable[[], float]


class ScheduledCall(Protocol):
    def cancel(self) -> None: ...


class Scheduler(Protocol):
    def call_later(self, delay: float, callback: Callable[[], None]) -> ScheduledCall: ...


class _ThreadingScheduler:
    def call_later(self, delay: float, callback: Callable[[], None]) -> ScheduledCall:
        timer = threading.Timer(delay, callback)
        timer.daemon = True
        timer.start()
        return timer


@dataclass(frozen=True, slots=True)
class LocalSignature:
    kind: str
    size: int | None
    mtime_ns: int
    mode: int
    link_target: str | None = None


@dataclass(frozen=True, slots=True)
class _RootIdentity:
    device: int
    inode: int


@dataclass(slots=True)
class _DebounceState:
    last_emitted: float
    pending: bool = False
    generation: int = 0
    scheduled_call: ScheduledCall | None = None


class WatchRootInvalid(RuntimeError):
    pass


class LocalEventWatcher(Protocol):
    last_error: BaseException | None

    def start(self) -> None: ...

    def stop(self) -> None: ...


# Temporary name retained until the daemon migration replaces its old watcher annotations.
LocalWatcher = LocalEventWatcher


class LocalChangeDetector:
    """Compatibility wrapper for callers that still construct the old detector."""

    def __init__(self, root: Path, policy: PolicySource) -> None:
        self._root = root.expanduser().resolve()
        self._policy = policy
        self._snapshot = self._scan()

    @property
    def root(self) -> Path:
        return self._root

    def current_policy(self) -> PolicyEngine:
        return _current_policy(self._policy)

    def changed(self) -> bool:
        current = self._scan()
        if current == self._snapshot:
            return False
        self._snapshot = current
        return True

    def peek_changed(self) -> bool:
        return self._scan() != self._snapshot

    def commit(self) -> None:
        self._snapshot = self._scan()

    def _scan(self) -> dict[str, LocalSignature]:
        return _scan_snapshot(self._root, self.current_policy())


class PollingLocalWatcher:
    """Metadata polling fallback that reports each changed relative path."""

    def __init__(
        self,
        root: Path,
        policy: PolicySource,
        on_event: EventCallback,
        *,
        interval: float = 0.5,
    ) -> None:
        self._root = root.expanduser().resolve()
        self._policy = policy
        self._on_event = on_event
        self._interval = interval
        self._root_identity = _root_identity(self._root)
        self._snapshot = _scan_snapshot(
            self._root,
            _current_policy(self._policy),
            expected_root=self._root_identity,
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._health_lock = threading.Lock()
        self._unhealthy = False
        self.last_error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="remote-sandbox-local-poll")
        self._thread.daemon = True
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval * 4))
        self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self._poll_once()

    def _poll_once(self) -> None:
        try:
            current = _scan_snapshot(
                self._root,
                _current_policy(self._policy),
                expected_root=self._root_identity,
            )
        except WatchRootInvalid as exc:
            self.last_error = exc
            self._report_unhealthy()
            return
        except Exception as exc:  # pragma: no cover - exercised through thread behaviour
            self.last_error = exc
            return

        try:
            for kind, path, destination in _snapshot_events(self._snapshot, current):
                self._on_event(kind, path, destination)
        except Exception as exc:  # pragma: no cover - exercised through thread behaviour
            self.last_error = exc
            return

        self._snapshot = current
        with self._health_lock:
            self._unhealthy = False
        self.last_error = None

    def _report_unhealthy(self) -> None:
        with self._health_lock:
            if self._unhealthy:
                return
            self._unhealthy = True
        try:
            self._on_event(EventKind.RESCAN_REQUIRED, "*", None)
        except Exception as exc:  # pragma: no cover - exercised through thread behaviour
            with self._health_lock:
                self._unhealthy = False
            self.last_error = exc


class WatchdogLocalWatcher:
    """Native path watcher with immediate and periodic rescan recovery signals.

    Backend overflow, watched-root loss, and observer failure emit an immediate bounded rescan
    request. The low-frequency periodic request covers native backends that hide queue overflow.
    This watcher never performs the metadata audit itself.
    """

    def __init__(
        self,
        root: Path,
        policy: PolicySource,
        on_event: EventCallback,
        *,
        debounce: float = 0.1,
        clock: Clock = time.monotonic,
        scheduler: Scheduler | None = None,
        observer_factory: Callable[[], Any] | None = None,
        event_handler_base: type[Any] | None = None,
        health_interval: float = 0.5,
        audit_interval: float = 300.0,
    ) -> None:
        if debounce < 0:
            raise ValueError("debounce must be non-negative")
        if health_interval <= 0:
            raise ValueError("health_interval must be positive")
        if audit_interval <= 0:
            raise ValueError("audit_interval must be positive")
        self._root = root.expanduser().resolve()
        self._policy = policy
        self._on_event = on_event
        self._debounce = debounce
        self._clock = clock
        self._scheduler = scheduler or _ThreadingScheduler()
        self._observer_factory = observer_factory
        self._event_handler_base = event_handler_base
        self._health_interval = health_interval
        self._audit_interval = audit_interval
        self._debounce_state: dict[tuple[EventKind, str, str | None], _DebounceState] = {}
        self._state_lock = threading.RLock()
        self._delivery_lock = threading.RLock()
        self._generation = 0
        self._observer: Any | None = None
        self._health_call: ScheduledCall | None = None
        self._audit_call: ScheduledCall | None = None
        self._accepting_events = True
        self._stopping = False
        self._unhealthy = False
        self._terminal_health_failure = False
        self.last_error: BaseException | None = None

    def start(self) -> None:
        with self._state_lock:
            if self._observer is not None:
                return
            self._accepting_events = True
            self._stopping = False
            self._unhealthy = False
            self._terminal_health_failure = False
        if self._observer_factory is None or self._event_handler_base is None:
            events, observers = _watchdog_modules()
            observer_factory = observers.Observer
            event_handler_base = events.FileSystemEventHandler
        else:
            observer_factory = self._observer_factory
            event_handler_base = self._event_handler_base
        watcher = self

        class Handler(event_handler_base):  # type: ignore[misc, valid-type]
            def on_any_event(self, event: object) -> None:
                watcher._dispatch(event)

        observer = observer_factory()
        observer.schedule(Handler(), str(self._root), recursive=True)
        try:
            observer.start()
        except BaseException:
            observer.stop()
            raise
        with self._state_lock:
            self._observer = observer
            self._schedule_health_check_locked()
            self._schedule_audit_locked()

    def stop(self) -> None:
        with self._state_lock:
            self._stopping = True
            observer = self._observer
            self._observer = None
            health_call = self._health_call
            self._health_call = None
            audit_call = self._audit_call
            self._audit_call = None
        if health_call is not None:
            health_call.cancel()
        if audit_call is not None:
            audit_call.cancel()
        if observer is not None:
            observer.stop()
            observer.join(timeout=2.0)
        pending = self._close_debounce()
        with self._delivery_lock:
            for mapped in pending:
                self._emit(mapped)
        with self._state_lock:
            self._stopping = False

    def _dispatch(self, event: object) -> None:
        with self._delivery_lock:
            try:
                mapped = map_watchdog_event(self._root, _current_policy(self._policy), event)
                if mapped is None:
                    return
                if mapped[0] is EventKind.RESCAN_REQUIRED:
                    terminal = _event_invalidates_root(self._root, event)
                    reason = (
                        "watched root was deleted or renamed"
                        if terminal
                        else "filesystem event history was lost"
                    )
                    self._report_unhealthy(RuntimeError(reason), terminal=terminal)
                    return
                self._mark_healthy()
                self._debounce_event(mapped)
            except Exception as exc:  # pragma: no cover - observer thread behaviour
                self.last_error = exc

    def _debounce_event(self, mapped: tuple[EventKind, str, str | None]) -> None:
        now = self._clock()
        emit_immediately = False
        with self._state_lock:
            if not self._accepting_events:
                return
            self._cancel_superseded_pending_locked(mapped)
            existing = self._debounce_state.get(mapped)
            if existing is None or now - existing.last_emitted >= self._debounce:
                if existing is not None and existing.scheduled_call is not None:
                    existing.scheduled_call.cancel()
                state = _DebounceState(last_emitted=now)
                self._debounce_state[mapped] = state
                emit_immediately = True
            else:
                state = existing
                state.pending = True
                if state.scheduled_call is None:
                    self._generation += 1
                    generation = self._generation
                    state.generation = generation
                    delay = max(0.0, self._debounce - (now - state.last_emitted))
                    state.scheduled_call = self._scheduler.call_later(
                        delay,
                        lambda: self._emit_trailing(mapped, generation),
                    )
            self._prune_debounce_locked(now)
        if emit_immediately:
            self._emit(mapped)

    def _emit_trailing(
        self,
        mapped: tuple[EventKind, str, str | None],
        generation: int,
    ) -> None:
        with self._delivery_lock:
            with self._state_lock:
                state = self._debounce_state.get(mapped)
                if (
                    state is None
                    or state.generation != generation
                    or not state.pending
                    or not self._accepting_events
                ):
                    return
                state.pending = False
                state.scheduled_call = None
                state.last_emitted = self._clock()
            self._emit(mapped)

    def _emit(self, mapped: tuple[EventKind, str, str | None]) -> None:
        try:
            self._on_event(*mapped)
        except Exception as exc:
            self.last_error = exc
            return
        with self._state_lock:
            if not self._terminal_health_failure:
                self.last_error = None

    def _close_debounce(self) -> list[tuple[EventKind, str, str | None]]:
        with self._state_lock:
            self._accepting_events = False
            pending = [mapped for mapped, state in self._debounce_state.items() if state.pending]
            for state in self._debounce_state.values():
                if state.scheduled_call is not None:
                    state.scheduled_call.cancel()
            self._debounce_state.clear()
        return pending

    def _cancel_superseded_pending_locked(
        self,
        mapped: tuple[EventKind, str, str | None],
    ) -> None:
        superseded = [
            candidate
            for candidate in self._debounce_state
            if _newer_event_supersedes_pending(mapped, candidate)
        ]
        for candidate in superseded:
            state = self._debounce_state.pop(candidate)
            if state.scheduled_call is not None:
                state.scheduled_call.cancel()

    def _discard_all_debounce_locked(self) -> None:
        for state in self._debounce_state.values():
            if state.scheduled_call is not None:
                state.scheduled_call.cancel()
        self._debounce_state.clear()

    def _prune_debounce_locked(self, now: float) -> None:
        if len(self._debounce_state) <= 1024:
            return
        cutoff = now - self._debounce
        self._debounce_state = {
            mapped: state
            for mapped, state in self._debounce_state.items()
            if state.pending or state.last_emitted >= cutoff
        }

    def _report_unhealthy(self, error: BaseException, *, terminal: bool) -> None:
        with self._delivery_lock:
            with self._state_lock:
                self.last_error = error
                self._terminal_health_failure = self._terminal_health_failure or terminal
                if terminal and self._audit_call is not None:
                    self._audit_call.cancel()
                    self._audit_call = None
                if self._unhealthy:
                    return
                self._unhealthy = True
                self._discard_all_debounce_locked()
            try:
                self._on_event(EventKind.RESCAN_REQUIRED, "*", None)
            except Exception as exc:
                with self._state_lock:
                    self._unhealthy = False
                    self.last_error = exc

    def _mark_healthy(self) -> None:
        with self._state_lock:
            if not self._terminal_health_failure:
                self._unhealthy = False

    def _schedule_health_check_locked(self) -> None:
        if self._health_call is not None or self._observer is None or self._stopping:
            return
        self._health_call = self._scheduler.call_later(
            self._health_interval,
            self._check_observer_health,
        )

    def _schedule_audit_locked(self) -> None:
        if (
            self._audit_call is not None
            or self._observer is None
            or self._stopping
            or self._terminal_health_failure
        ):
            return
        self._audit_call = self._scheduler.call_later(
            self._audit_interval,
            self._run_periodic_audit,
        )

    def _check_observer_health(self) -> None:
        with self._state_lock:
            self._health_call = None
            observer = self._observer
            if observer is None or self._stopping:
                return
        try:
            healthy = bool(observer.is_alive()) and all(
                bool(emitter.is_alive()) for emitter in getattr(observer, "emitters", ())
            )
        except Exception:
            healthy = False
        if not healthy:
            self._report_unhealthy(
                RuntimeError("filesystem observer stopped unexpectedly"),
                terminal=True,
            )
            return
        with self._state_lock:
            self._schedule_health_check_locked()

    def _run_periodic_audit(self) -> None:
        with self._delivery_lock:
            with self._state_lock:
                self._audit_call = None
                if self._observer is None or self._stopping or self._terminal_health_failure:
                    return
                healthy = not self._unhealthy
                if healthy:
                    self._discard_all_debounce_locked()
            if healthy:
                self._emit((EventKind.RESCAN_REQUIRED, "*", None))
            with self._state_lock:
                self._schedule_audit_locked()


def _newer_event_supersedes_pending(
    newer: tuple[EventKind, str, str | None],
    pending: tuple[EventKind, str, str | None],
) -> bool:
    if newer == pending:
        return False
    newer_kind = newer[0]
    pending_kind = pending[0]
    newer_paths = _event_paths(newer)
    pending_paths = _event_paths(pending)
    if pending_kind in {EventKind.DELETE, EventKind.MOVE}:
        return any(
            _paths_overlap(newer_path, pending_path)
            for newer_path in newer_paths
            for pending_path in pending_paths
        )
    if newer_kind in {EventKind.DELETE, EventKind.MOVE}:
        return any(
            _is_same_or_descendant(pending_path, newer_path)
            for newer_path in newer_paths
            for pending_path in pending_paths
        )
    return False


def _event_paths(mapped: tuple[EventKind, str, str | None]) -> tuple[str, ...]:
    _kind, path, destination = mapped
    return (path,) if destination is None else (path, destination)


def _paths_overlap(left: str, right: str) -> bool:
    return _is_same_or_descendant(left, right) or _is_same_or_descendant(right, left)


def _is_same_or_descendant(path: str, ancestor: str) -> bool:
    return path == ancestor or path.startswith(f"{ancestor}/")


def map_watchdog_event(
    root: Path,
    policy: PolicyEngine,
    event: object,
) -> tuple[EventKind, str, str | None] | None:
    """Map one watchdog event to a policy-visible relative path event."""

    event_type = getattr(event, "event_type", None)
    if not isinstance(event_type, str):
        return None
    if _event_requires_rescan(root, event):
        return EventKind.RESCAN_REQUIRED, "*", None
    source = _relative_event_path(root, getattr(event, "src_path", None))
    if source is not None and policy.is_ignored(source):
        source = None

    if event_type == "moved":
        destination = _relative_event_path(root, getattr(event, "dest_path", None))
        if destination is not None and policy.is_ignored(destination):
            destination = None
        if source is not None and destination is not None:
            return EventKind.MOVE, source, destination
        if source is not None:
            return EventKind.DELETE, source, None
        if destination is not None:
            return EventKind.CREATE, destination, None
        return None

    if source is None:
        return None
    kind = {
        "created": EventKind.CREATE,
        "modified": EventKind.MODIFY,
        "deleted": EventKind.DELETE,
    }.get(event_type)
    return None if kind is None else (kind, source, None)


@overload
def create_local_watcher(
    root: Path,
    policy: PolicySource,
    on_event: EventCallback,
) -> LocalEventWatcher: ...


@overload
def create_local_watcher(
    *,
    detector: LocalChangeDetector,
    on_change: Callable[[], None],
) -> LocalEventWatcher: ...


def create_local_watcher(
    root: Path | None = None,
    policy: PolicySource | None = None,
    on_event: EventCallback | None = None,
    *,
    detector: LocalChangeDetector | None = None,
    on_change: Callable[[], None] | None = None,
) -> LocalEventWatcher:
    """Create a native path watcher, or a path-emitting polling fallback."""

    if detector is not None or on_change is not None:
        if detector is None or on_change is None:
            raise TypeError("detector and on_change must be provided together")
        if root is not None or policy is not None or on_event is not None:
            raise TypeError("legacy and path-event watcher arguments cannot be mixed")
        root = detector.root
        policy = detector.current_policy

        def notify_legacy_caller(
            kind: EventKind,
            path: str,
            destination: str | None,
        ) -> None:
            del kind, path, destination
            on_change()

        on_event = notify_legacy_caller
    if root is None or policy is None or on_event is None:
        raise TypeError("root, policy, and on_event are required")
    if _watchdog_available():
        return WatchdogLocalWatcher(root, policy, on_event)
    return PollingLocalWatcher(root, policy, on_event)


def _snapshot_events(
    previous: dict[str, LocalSignature],
    current: dict[str, LocalSignature],
) -> list[tuple[EventKind, str, str | None]]:
    deleted = sorted(
        previous.keys() - current.keys(),
        key=lambda path: (path.count("/"), path),
        reverse=True,
    )
    created = sorted(
        current.keys() - previous.keys(),
        key=lambda path: (path.count("/"), path),
    )
    modified = sorted(
        path for path in previous.keys() & current.keys() if previous[path] != current[path]
    )
    return [
        *((EventKind.DELETE, path, None) for path in deleted),
        *((EventKind.CREATE, path, None) for path in created),
        *((EventKind.MODIFY, path, None) for path in modified),
    ]


def _scan_snapshot(
    root: Path,
    policy: PolicyEngine,
    *,
    expected_root: _RootIdentity | None = None,
) -> dict[str, LocalSignature]:
    _validate_root_identity(root, expected_root)
    snapshot: dict[str, LocalSignature] = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current_dir = Path(dirpath)
        relative_dir = current_dir.relative_to(root).as_posix()
        if relative_dir == ".":
            relative_dir = ""
        kept_directories: list[str] = []
        for dirname in sorted(dirnames):
            path = current_dir / dirname
            relative_path = f"{relative_dir}/{dirname}" if relative_dir else dirname
            if policy.is_ignored(relative_path):
                continue
            signature = _read_signature(path)
            if signature is None:
                continue
            snapshot[relative_path] = signature
            if signature.kind == "dir":
                kept_directories.append(dirname)
        dirnames[:] = kept_directories
        for filename in sorted(filenames):
            path = current_dir / filename
            relative_path = f"{relative_dir}/{filename}" if relative_dir else filename
            if policy.is_ignored(relative_path):
                continue
            signature = _read_signature(path)
            if signature is not None:
                snapshot[relative_path] = signature
    _validate_root_identity(root, expected_root)
    return snapshot


def _read_signature(path: Path) -> LocalSignature | None:
    try:
        metadata = path.lstat()
        mode = metadata.st_mode
        if stat.S_ISDIR(mode):
            kind = "dir"
            size = None
            link_target = None
        elif stat.S_ISLNK(mode):
            kind = "symlink"
            size = None
            link_target = os.readlink(path)
        elif stat.S_ISREG(mode):
            kind = "file"
            size = metadata.st_size
            link_target = None
        else:
            kind = "special"
            size = metadata.st_size
            link_target = None
        return LocalSignature(kind, size, metadata.st_mtime_ns, mode, link_target)
    except (FileNotFoundError, NotADirectoryError):
        return None


def _relative_event_path(root: Path, value: object) -> str | None:
    if not isinstance(value, (str, bytes, os.PathLike)):
        return None
    try:
        relative = Path(os.fsdecode(value)).relative_to(root).as_posix()
        return normalize_relative_path(relative)
    except (TypeError, ValueError):
        return None


def _event_requires_rescan(root: Path, event: object) -> bool:
    event_type = getattr(event, "event_type", None)
    if event_type in {
        "overflow",
        "queue-overflow",
        "lost-history",
        "history-lost",
        EventKind.RESCAN_REQUIRED.value,
    }:
        return True
    if any(
        getattr(event, attribute, False) is True
        for attribute in (
            "is_overflow",
            "overflow",
            "lost_history",
            "history_lost",
            "rescan_required",
        )
    ):
        return True
    return _event_invalidates_root(root, event)


def _event_invalidates_root(root: Path, event: object) -> bool:
    if getattr(event, "event_type", None) not in {"deleted", "moved"}:
        return False
    value = getattr(event, "src_path", None)
    if not isinstance(value, (str, bytes, os.PathLike)):
        return False
    return os.path.normcase(os.path.abspath(os.fsdecode(value))) == os.path.normcase(str(root))


def _root_identity(root: Path) -> _RootIdentity:
    try:
        metadata = root.stat()
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise WatchRootInvalid(f"watched root is unavailable: {root}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise WatchRootInvalid(f"watched root is not a directory: {root}")
    return _RootIdentity(metadata.st_dev, metadata.st_ino)


def _validate_root_identity(root: Path, expected: _RootIdentity | None) -> None:
    current = _root_identity(root)
    if expected is not None and current != expected:
        raise WatchRootInvalid(f"watched root was replaced: {root}")


def _current_policy(policy: PolicySource) -> PolicyEngine:
    if callable(policy):
        return policy()
    return policy


def _watchdog_available() -> bool:
    return (
        importlib.util.find_spec("watchdog.events") is not None
        and importlib.util.find_spec("watchdog.observers") is not None
    )


def _watchdog_modules() -> tuple[Any, Any]:
    events = __import__("watchdog.events", fromlist=["FileSystemEventHandler"])
    observers = __import__("watchdog.observers", fromlist=["Observer"])
    return events, observers
