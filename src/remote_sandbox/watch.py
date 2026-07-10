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


@dataclass(frozen=True, slots=True)
class LocalSignature:
    kind: str
    size: int | None
    mtime_ns: int
    mode: int
    link_target: str | None = None


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
        self._snapshot = _scan_snapshot(self._root, _current_policy(self._policy))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
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
            try:
                current = _scan_snapshot(self._root, _current_policy(self._policy))
                for kind, path, destination in _snapshot_events(self._snapshot, current):
                    self._on_event(kind, path, destination)
                self._snapshot = current
                self.last_error = None
            except Exception as exc:  # pragma: no cover - exercised through thread behaviour
                self.last_error = exc


class WatchdogLocalWatcher:
    """Native filesystem watcher that emits filtered relative path events."""

    def __init__(
        self,
        root: Path,
        policy: PolicySource,
        on_event: EventCallback,
        *,
        debounce: float = 0.1,
    ) -> None:
        self._root = root.expanduser().resolve()
        self._policy = policy
        self._on_event = on_event
        self._debounce = debounce
        self._recent: dict[tuple[EventKind, str, str | None], float] = {}
        self._recent_lock = threading.Lock()
        self._observer: Any | None = None
        self.last_error: BaseException | None = None

    def start(self) -> None:
        if self._observer is not None:
            return
        events, observers = _watchdog_modules()
        watcher = self

        class Handler(events.FileSystemEventHandler):  # type: ignore[misc, name-defined]
            def on_any_event(self, event: object) -> None:
                watcher._dispatch(event)

        observer = observers.Observer()
        observer.schedule(Handler(), str(self._root), recursive=True)
        try:
            observer.start()
        except BaseException:
            observer.stop()
            raise
        self._observer = observer

    def stop(self) -> None:
        if self._observer is None:
            return
        observer = self._observer
        self._observer = None
        observer.stop()
        observer.join(timeout=2.0)

    def _dispatch(self, event: object) -> None:
        try:
            mapped = map_watchdog_event(self._root, _current_policy(self._policy), event)
            if mapped is None:
                return
            now = time.monotonic()
            with self._recent_lock:
                previous = self._recent.get(mapped)
                if previous is not None and now - previous < self._debounce:
                    return
                self._recent[mapped] = now
                if len(self._recent) > 1024:
                    cutoff = now - self._debounce
                    self._recent = {
                        key: emitted_at
                        for key, emitted_at in self._recent.items()
                        if emitted_at >= cutoff
                    }
            try:
                self._on_event(*mapped)
            except Exception:
                with self._recent_lock:
                    if self._recent.get(mapped) == now:
                        self._recent.pop(mapped, None)
                raise
            self.last_error = None
        except Exception as exc:  # pragma: no cover - exercised through observer thread behaviour
            self.last_error = exc


def map_watchdog_event(
    root: Path,
    policy: PolicyEngine,
    event: object,
) -> tuple[EventKind, str, str | None] | None:
    """Map one watchdog event to a policy-visible relative path event."""

    event_type = getattr(event, "event_type", None)
    if not isinstance(event_type, str):
        return None
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


def _scan_snapshot(root: Path, policy: PolicyEngine) -> dict[str, LocalSignature]:
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
