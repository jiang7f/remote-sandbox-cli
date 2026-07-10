from __future__ import annotations

import os
import platform
import stat
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .store import RemoteStore

_HARD_IGNORED_NAMES = {".git", ".remote-sandbox", ".codex-remote-sandbox"}


@dataclass(frozen=True, slots=True)
class RemoteSignature:
    kind: str
    size: int | None
    mtime_ns: int
    mode: int
    link_target: str | None = None

    def to_payload(self, path: str) -> dict[str, object]:
        return {
            "path": path,
            "kind": self.kind,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "mode": self.mode,
            "link_target": self.link_target,
        }


@dataclass(frozen=True, slots=True)
class _RootIdentity:
    device: int
    inode: int


class WatchBackend(Protocol):
    last_error: BaseException | None

    def run(self) -> None: ...

    def stop(self) -> None: ...


class PollingWatcher:
    """Low-frequency remote watcher based on non-following metadata snapshots."""

    def __init__(self, root: Path, store: RemoteStore, *, interval: float = 1.0) -> None:
        if interval <= 0:
            raise ValueError("poll interval must be positive")
        self._root = root.expanduser().resolve(strict=True)
        self._store = store
        self._interval = interval
        self._stop = threading.Event()
        self._root_identity = _root_identity(self._root)
        self._snapshot = scan_snapshot(self._root)
        self._root_unhealthy = False
        self.last_error: BaseException | None = None

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            self._poll_once()

    def stop(self) -> None:
        self._stop.set()

    def _poll_once(self) -> None:
        try:
            current_identity = _root_identity(self._root)
            current = scan_snapshot(self._root)
        except (FileNotFoundError, NotADirectoryError, OSError) as exc:
            self.last_error = exc
            if not self._root_unhealthy:
                self._store.append_event("rescan-required", "*", None)
                self._root_unhealthy = True
            return

        if current_identity != self._root_identity:
            if not self._root_unhealthy:
                self._store.append_event("rescan-required", "*", None)
            self._root_identity = current_identity
            self._snapshot = current
            self._root_unhealthy = False
            self.last_error = None
            return

        try:
            for kind, path in _snapshot_events(self._snapshot, current):
                self._store.append_event(kind, path, None)
        except (OSError, RuntimeError, ValueError) as exc:
            self.last_error = exc
            return

        self._snapshot = current
        self._root_unhealthy = False
        self.last_error = None
        self._store.heartbeat()


class WatcherService:
    """Select and supervise the best remote filesystem event backend."""

    def __init__(self, root: Path, store: RemoteStore, *, poll_interval: float = 1.0) -> None:
        self._root = root.expanduser().resolve(strict=True)
        self._store = store
        self._backend: WatchBackend
        if platform.system() == "Linux":
            try:
                from .inotify import InotifyBackend

                self._backend = InotifyBackend(self._root, self._store)
                self.backend_name = "inotify"
            except OSError:
                self._backend = PollingWatcher(
                    self._root,
                    self._store,
                    interval=poll_interval,
                )
                self.backend_name = "polling"
        else:
            self._backend = PollingWatcher(
                self._root,
                self._store,
                interval=poll_interval,
            )
            self.backend_name = "polling"

    def run(self) -> None:
        pid = os.getpid()
        self._store.record_watcher(pid, "running", backend=self.backend_name)
        try:
            self._backend.run()
        except BaseException as exc:
            self._store.record_watcher(
                None,
                "failed",
                backend=self.backend_name,
                error=str(exc),
            )
            raise
        else:
            self._store.record_watcher(None, "stopped", backend=self.backend_name)

    def stop(self) -> None:
        self._backend.stop()


def scan_snapshot(root: Path) -> dict[str, RemoteSignature]:
    canonical_root = root.expanduser().resolve(strict=True)
    if not canonical_root.is_dir():
        raise NotADirectoryError(str(canonical_root))
    snapshot: dict[str, RemoteSignature] = {}
    _scan_directory(canonical_root, canonical_root, snapshot)
    return snapshot


def snapshot_entries(root: Path) -> list[dict[str, object]]:
    snapshot = scan_snapshot(root)
    return [snapshot[path].to_payload(path) for path in sorted(snapshot)]


def path_is_hard_ignored(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    return any(part in _HARD_IGNORED_NAMES for part in relative.parts)


def _scan_directory(
    root: Path,
    directory: Path,
    snapshot: dict[str, RemoteSignature],
) -> None:
    with os.scandir(directory) as entries:
        ordered = sorted(entries, key=lambda entry: entry.name)
    for entry in ordered:
        path = Path(entry.path)
        if path_is_hard_ignored(path, root):
            continue
        try:
            metadata = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        relative = path.relative_to(root).as_posix()
        mode = metadata.st_mode
        if stat.S_ISLNK(mode):
            try:
                target = os.readlink(path)
            except FileNotFoundError:
                continue
            snapshot[relative] = RemoteSignature(
                "symlink",
                len(os.fsencode(target)),
                metadata.st_mtime_ns,
                mode,
                target,
            )
        elif stat.S_ISDIR(mode):
            snapshot[relative] = RemoteSignature("dir", None, metadata.st_mtime_ns, mode)
            _scan_directory(root, path, snapshot)
        elif stat.S_ISREG(mode):
            snapshot[relative] = RemoteSignature(
                "file",
                metadata.st_size,
                metadata.st_mtime_ns,
                mode,
            )
        else:
            snapshot[relative] = RemoteSignature("special", None, metadata.st_mtime_ns, mode)


def _snapshot_events(
    previous: dict[str, RemoteSignature],
    current: dict[str, RemoteSignature],
) -> list[tuple[str, str]]:
    deleted = sorted(
        previous.keys() - current.keys(),
        key=lambda path: (-len(Path(path).parts), path),
    )
    created = sorted(
        current.keys() - previous.keys(),
        key=lambda path: (len(Path(path).parts), path),
    )
    modified = sorted(
        path for path in previous.keys() & current.keys() if previous[path] != current[path]
    )
    return [
        *(("delete", path) for path in deleted),
        *(("create", path) for path in created),
        *(("modify", path) for path in modified),
    ]


def _root_identity(root: Path) -> _RootIdentity:
    metadata = root.stat(follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode):
        raise NotADirectoryError(str(root))
    return _RootIdentity(metadata.st_dev, metadata.st_ino)
