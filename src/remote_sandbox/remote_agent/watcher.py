from __future__ import annotations

import os
import platform
import stat
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .paths import name_is_hard_ignored, path_parts_are_hard_ignored
from .store import RemoteStore


@dataclass(frozen=True, slots=True)
class RemoteSignature:
    kind: str
    size: int | None
    mtime_ns: int
    mode: int
    link_target: str | None = None
    ctime_ns: int = 0
    device: int = 0
    inode: int = 0

    def to_payload(self, path: str) -> dict[str, object]:
        return {
            "path": path,
            "kind": self.kind,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "mode": self.mode,
            "link_target": self.link_target,
            "ctime_ns": self.ctime_ns,
            "device": self.device,
            "inode": self.inode,
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
        self._root = watch_root_path(root)
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

    def __init__(
        self,
        root: Path,
        store: RemoteStore,
        *,
        poll_interval: float = 1.0,
        token: str | None = None,
    ) -> None:
        self._root = watch_root_path(root)
        self._store = store
        self._token = token
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
        self._record_state(pid, "running")
        try:
            self._backend.run()
        except BaseException as exc:
            self._record_state(None, "failed", error=str(exc))
            raise
        else:
            self._record_state(None, "stopped")

    def stop(self) -> None:
        self._backend.stop()

    def _record_state(self, pid: int | None, status: str, *, error: str | None = None) -> None:
        if self._token is None:
            self._store.record_watcher(pid, status, backend=self.backend_name, error=error)
            return
        self._store.record_watcher_for_generation(
            pid,
            status,
            backend=self.backend_name,
            token=self._token,
            error=error,
        )


def scan_snapshot(root: Path) -> dict[str, RemoteSignature]:
    canonical_root = watch_root_path(root)
    snapshot: dict[str, RemoteSignature] = {}
    descriptor = os.open(
        canonical_root,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
    )
    try:
        _scan_directory_descriptor(descriptor, "", snapshot)
    finally:
        os.close(descriptor)
    return snapshot


def snapshot_entries(root: Path) -> list[dict[str, object]]:
    snapshot = scan_snapshot(root)
    return [snapshot[path].to_payload(path) for path in sorted(snapshot)]


def path_is_hard_ignored(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    return path_parts_are_hard_ignored(relative.parts)


def watch_root_path(root: Path) -> Path:
    absolute = Path(os.path.abspath(root.expanduser()))
    metadata = absolute.stat(follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode):
        raise NotADirectoryError(str(absolute))
    return absolute


def _scan_directory_descriptor(
    descriptor: int,
    prefix: str,
    snapshot: dict[str, RemoteSignature],
) -> None:
    with os.scandir(descriptor) as entries:
        ordered = sorted(entries, key=lambda entry: entry.name)
    for entry in ordered:
        if name_is_hard_ignored(entry.name):
            continue
        try:
            metadata = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        relative = entry.name if not prefix else f"{prefix}/{entry.name}"
        mode = metadata.st_mode
        if stat.S_ISLNK(mode):
            try:
                target = os.readlink(entry.name, dir_fd=descriptor)
            except FileNotFoundError:
                continue
            snapshot[relative] = RemoteSignature(
                "symlink",
                len(os.fsencode(target)),
                metadata.st_mtime_ns,
                mode,
                target,
                metadata.st_ctime_ns,
                metadata.st_dev,
                metadata.st_ino,
            )
        elif stat.S_ISDIR(mode):
            snapshot[relative] = RemoteSignature(
                "dir",
                None,
                metadata.st_mtime_ns,
                mode,
                None,
                metadata.st_ctime_ns,
                metadata.st_dev,
                metadata.st_ino,
            )
            try:
                child = os.open(
                    entry.name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=descriptor,
                )
            except (FileNotFoundError, NotADirectoryError, OSError):
                continue
            try:
                _scan_directory_descriptor(child, relative, snapshot)
            finally:
                os.close(child)
        elif stat.S_ISREG(mode):
            snapshot[relative] = RemoteSignature(
                "file",
                metadata.st_size,
                metadata.st_mtime_ns,
                mode,
                None,
                metadata.st_ctime_ns,
                metadata.st_dev,
                metadata.st_ino,
            )
        else:
            snapshot[relative] = RemoteSignature(
                "special",
                None,
                metadata.st_mtime_ns,
                mode,
                None,
                metadata.st_ctime_ns,
                metadata.st_dev,
                metadata.st_ino,
            )


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
