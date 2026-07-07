from __future__ import annotations

import importlib.util
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from remote_sandbox.policy import PolicyEngine


@dataclass(frozen=True, slots=True)
class LocalSignature:
    kind: str
    size: int | None
    mtime_ns: int


class LocalWatcher(Protocol):
    last_error: BaseException | None

    def start(self) -> None: ...

    def stop(self) -> None: ...


class LocalChangeDetector:
    def __init__(self, root: Path, policy: PolicyEngine | Callable[[], PolicyEngine]) -> None:
        self._root = root.expanduser().resolve()
        self._policy = policy
        self._snapshot = self._scan()

    @property
    def root(self) -> Path:
        return self._root

    def changed(self) -> bool:
        current = self._scan()
        if current == self._snapshot:
            return False
        self._snapshot = current
        return True

    def peek_changed(self) -> bool:
        current = self._scan()
        return current != self._snapshot

    def commit(self) -> None:
        self._snapshot = self._scan()

    def _scan(self) -> dict[str, LocalSignature]:
        policy = self._current_policy()
        snapshot: dict[str, LocalSignature] = {}
        for dirpath, dirnames, filenames in os.walk(self._root):
            current_dir = Path(dirpath)
            rel_dir = current_dir.relative_to(self._root).as_posix()
            if rel_dir == ".":
                rel_dir = ""
            kept_dirs: list[str] = []
            for dirname in sorted(dirnames):
                path = current_dir / dirname
                rel_path = f"{rel_dir}/{dirname}" if rel_dir else dirname
                if path.is_symlink() or policy.is_ignored(rel_path):
                    continue
                kept_dirs.append(dirname)
                stat = path.stat()
                snapshot[rel_path] = LocalSignature("dir", None, stat.st_mtime_ns)
            dirnames[:] = kept_dirs
            for filename in sorted(filenames):
                path = current_dir / filename
                rel_path = f"{rel_dir}/{filename}" if rel_dir else filename
                if path.is_symlink() or policy.is_ignored(rel_path):
                    continue
                stat = path.stat()
                snapshot[rel_path] = LocalSignature("file", stat.st_size, stat.st_mtime_ns)
        return snapshot

    def _current_policy(self) -> PolicyEngine:
        if callable(self._policy):
            return self._policy()
        return self._policy


class PollingLocalWatcher:
    def __init__(
        self,
        *,
        detector: LocalChangeDetector,
        on_change: Callable[[], None],
        interval: float = 0.5,
    ) -> None:
        self._detector = detector
        self._on_change = on_change
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="remote-sandbox-local-watch")
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
                if self._detector.peek_changed():
                    self._on_change()
                    self._detector.commit()
                    self.last_error = None
            except Exception as exc:  # pragma: no cover - exercised through thread behaviour
                self.last_error = exc


class WatchdogLocalWatcher:
    """Event-driven watcher using watchdog, with LocalChangeDetector as filter.

    watchdog gives us native filesystem events (FSEvents/inotify/etc.). We still
    ask LocalChangeDetector whether a meaningful policy-visible change happened,
    which filters metadata noise and keeps fallback semantics identical.
    """

    def __init__(
        self,
        *,
        detector: LocalChangeDetector,
        on_change: Callable[[], None],
        debounce: float = 0.1,
    ) -> None:
        self._detector = detector
        self._on_change = on_change
        self._debounce = debounce
        self._changed = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._observer: Any | None = None
        self.last_error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        events, observers = _watchdog_modules()

        changed = self._changed

        class Handler(events.FileSystemEventHandler):  # type: ignore[misc, name-defined]
            def on_any_event(self, event: object) -> None:
                del event
                changed.set()

        observer = observers.Observer()
        observer.schedule(Handler(), str(self._detector.root), recursive=True)
        observer.start()
        self._observer = observer
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="remote-sandbox-watchdog")
        self._thread.daemon = True
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop.set()
        self._changed.set()
        self._thread.join(timeout=max(1.0, self._debounce * 4))
        self._thread = None
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=2.0)
            self._observer = None

    def _run(self) -> None:
        while not self._stop.is_set():
            self._changed.wait(timeout=0.5)
            if self._stop.is_set():
                break
            if not self._changed.is_set():
                continue
            self._changed.clear()
            # Debounce bursts from editors that save via temp file + rename.
            self._stop.wait(self._debounce)
            if self._stop.is_set():
                break
            try:
                if self._detector.peek_changed():
                    self._on_change()
                    self._detector.commit()
                    self.last_error = None
            except Exception as exc:  # pragma: no cover - exercised through thread behaviour
                self.last_error = exc


def create_local_watcher(
    *,
    detector: LocalChangeDetector,
    on_change: Callable[[], None],
) -> LocalWatcher:
    if _watchdog_available():
        return WatchdogLocalWatcher(detector=detector, on_change=on_change)
    return PollingLocalWatcher(detector=detector, on_change=on_change)


def _watchdog_available() -> bool:
    return (
        importlib.util.find_spec("watchdog.events") is not None
        and importlib.util.find_spec("watchdog.observers") is not None
    )


def _watchdog_modules() -> tuple[Any, Any]:
    events = __import__("watchdog.events", fromlist=["FileSystemEventHandler"])
    observers = __import__("watchdog.observers", fromlist=["Observer"])
    return events, observers
