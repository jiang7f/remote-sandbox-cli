from __future__ import annotations

import ctypes
import errno
import os
import platform
import select
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from .store import RemoteStore
from .watcher import path_is_hard_ignored

IN_MODIFY = 0x00000002
IN_ATTRIB = 0x00000004
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
IN_IGNORED = 0x00008000
IN_ISDIR = 0x40000000

_WATCH_MASK = (
    IN_MODIFY
    | IN_ATTRIB
    | IN_CLOSE_WRITE
    | IN_MOVED_FROM
    | IN_MOVED_TO
    | IN_CREATE
    | IN_DELETE
    | IN_DELETE_SELF
    | IN_MOVE_SELF
)
_HEADER = struct.Struct("iIII")


@dataclass(frozen=True, slots=True)
class InotifyEvent:
    watch_descriptor: int
    mask: int
    cookie: int
    name: str
    overflow: bool


@dataclass(frozen=True, slots=True)
class _PendingMove:
    path: Path
    is_directory: bool
    deadline: float


def parse_inotify_buffer(data: bytes) -> list[InotifyEvent]:
    offset = 0
    parsed: list[InotifyEvent] = []
    while offset + _HEADER.size <= len(data):
        descriptor, mask, cookie, name_length = _HEADER.unpack_from(data, offset)
        offset += _HEADER.size
        if offset + name_length > len(data):
            raise ValueError("truncated inotify buffer")
        raw_name = data[offset : offset + name_length]
        offset += name_length
        name = raw_name.split(b"\0", 1)[0].decode("utf-8", errors="surrogateescape")
        parsed.append(
            InotifyEvent(
                descriptor,
                mask,
                cookie,
                name,
                bool(mask & IN_Q_OVERFLOW),
            )
        )
    if offset != len(data):
        raise ValueError("truncated inotify buffer")
    return parsed


class InotifyBackend:
    """Recursive Linux inotify backend implemented directly through libc."""

    def __init__(self, root: Path, store: RemoteStore, *, read_timeout: float = 0.2) -> None:
        if platform.system() != "Linux":
            raise OSError(errno.ENOSYS, "inotify is available only on Linux")
        self._root = root.expanduser().resolve(strict=True)
        self._store = store
        self._read_timeout = read_timeout
        self._stop = threading.Event()
        self._libc = ctypes.CDLL(None, use_errno=True)
        self._configure_libc()
        self._fd = int(self._libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC))
        if self._fd < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        self._watch_paths: dict[int, Path] = {}
        self._path_watches: dict[Path, int] = {}
        self._pending_moves: dict[int, _PendingMove] = {}
        self.last_error: BaseException | None = None
        try:
            self._add_tree(self._root)
        except BaseException:
            self._close()
            raise

    def run(self) -> None:
        try:
            while not self._stop.is_set():
                readable, _, _ = select.select([self._fd], [], [], self._read_timeout)
                if readable:
                    try:
                        raw = os.read(self._fd, 1024 * 1024)
                    except BlockingIOError:
                        raw = b""
                    if raw:
                        self._process_events(parse_inotify_buffer(raw))
                self._flush_expired_moves(time.monotonic())
                self._store.heartbeat()
        except BaseException as exc:
            self.last_error = exc
            raise
        finally:
            self._close()

    def stop(self) -> None:
        self._stop.set()

    def _configure_libc(self) -> None:
        self._libc.inotify_init1.argtypes = [ctypes.c_int]
        self._libc.inotify_init1.restype = ctypes.c_int
        self._libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self._libc.inotify_add_watch.restype = ctypes.c_int
        self._libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        self._libc.inotify_rm_watch.restype = ctypes.c_int

    def _add_tree(self, root: Path) -> None:
        if root.is_symlink() or path_is_hard_ignored(root, self._root):
            return
        for directory, names, _files in os.walk(root, followlinks=False):
            path = Path(directory)
            names[:] = [
                name
                for name in names
                if not (path / name).is_symlink()
                and not path_is_hard_ignored(path / name, self._root)
            ]
            self._add_watch(path)

    def _add_watch(self, path: Path) -> None:
        if path in self._path_watches:
            return
        descriptor = int(
            self._libc.inotify_add_watch(
                self._fd,
                os.fsencode(path),
                _WATCH_MASK,
            )
        )
        if descriptor < 0:
            error = ctypes.get_errno()
            if error in {errno.ENOENT, errno.ENOTDIR}:
                return
            raise OSError(error, os.strerror(error), str(path))
        previous = self._watch_paths.get(descriptor)
        if previous is not None:
            self._path_watches.pop(previous, None)
        self._watch_paths[descriptor] = path
        self._path_watches[path] = descriptor

    def _process_events(self, events: list[InotifyEvent]) -> None:
        for event in events:
            if event.overflow:
                self._pending_moves.clear()
                self._store.append_event("rescan-required", "*", None)
                continue
            base = self._watch_paths.get(event.watch_descriptor)
            if base is None:
                continue
            if event.mask & IN_IGNORED:
                self._forget_descriptor(event.watch_descriptor)
                continue
            if event.mask & (IN_DELETE_SELF | IN_MOVE_SELF) and base == self._root:
                self._store.append_event("rescan-required", "*", None)
                continue

            path = base / event.name if event.name else base
            if path_is_hard_ignored(path, self._root):
                continue
            if path == self._root and not event.name:
                continue
            is_directory = bool(event.mask & IN_ISDIR)

            if event.mask & IN_MOVED_FROM:
                self._pending_moves[event.cookie] = _PendingMove(
                    path,
                    is_directory,
                    time.monotonic() + 0.1,
                )
                continue

            if event.mask & IN_MOVED_TO:
                pending = self._pending_moves.pop(event.cookie, None)
                if is_directory:
                    if pending is not None:
                        self._move_watch_paths(pending.path, path)
                    self._add_tree(path)
                if pending is None:
                    self._store.append_event("create", self._relative(path), None)
                    if is_directory:
                        self._emit_created_descendants(path)
                else:
                    self._store.append_event(
                        "move",
                        self._relative(pending.path),
                        self._relative(path),
                    )
                continue

            if event.mask & IN_CREATE:
                if is_directory:
                    self._add_tree(path)
                self._store.append_event("create", self._relative(path), None)
                if is_directory:
                    self._emit_created_descendants(path)
                continue

            if event.mask & IN_DELETE:
                self._store.append_event("delete", self._relative(path), None)
                if is_directory:
                    self._forget_subtree(path)
                continue

            if event.mask & (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE):
                self._store.append_event("modify", self._relative(path), None)

    def _flush_expired_moves(self, now: float) -> None:
        expired = [
            cookie for cookie, pending in self._pending_moves.items() if pending.deadline <= now
        ]
        for cookie in expired:
            pending = self._pending_moves.pop(cookie)
            self._store.append_event("delete", self._relative(pending.path), None)
            if pending.is_directory:
                self._forget_subtree(pending.path)

    def _emit_created_descendants(self, directory: Path) -> None:
        if not directory.is_dir() or directory.is_symlink():
            return
        for current, names, files in os.walk(directory, followlinks=False):
            parent = Path(current)
            names[:] = [
                name
                for name in names
                if not (parent / name).is_symlink()
                and not path_is_hard_ignored(parent / name, self._root)
            ]
            for name in sorted([*names, *files]):
                path = parent / name
                if path_is_hard_ignored(path, self._root):
                    continue
                self._store.append_event("create", self._relative(path), None)

    def _move_watch_paths(self, source: Path, destination: Path) -> None:
        replacements = [
            (descriptor, path)
            for descriptor, path in self._watch_paths.items()
            if path == source or source in path.parents
        ]
        for descriptor, old_path in replacements:
            relative = old_path.relative_to(source)
            new_path = destination / relative
            self._watch_paths[descriptor] = new_path
            self._path_watches.pop(old_path, None)
            self._path_watches[new_path] = descriptor

    def _forget_subtree(self, root: Path) -> None:
        descriptors = [
            descriptor
            for descriptor, path in self._watch_paths.items()
            if path == root or root in path.parents
        ]
        for descriptor in descriptors:
            result = int(self._libc.inotify_rm_watch(self._fd, descriptor))
            if result < 0 and ctypes.get_errno() not in {errno.EINVAL, errno.EBADF}:
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error))
            self._forget_descriptor(descriptor)

    def _forget_descriptor(self, descriptor: int) -> None:
        path = self._watch_paths.pop(descriptor, None)
        if path is not None:
            self._path_watches.pop(path, None)

    def _relative(self, path: Path) -> str:
        return path.relative_to(self._root).as_posix()

    def _close(self) -> None:
        if self._fd < 0:
            return
        fd = self._fd
        self._fd = -1
        os.close(fd)
        self._watch_paths.clear()
        self._path_watches.clear()
