from __future__ import annotations

import errno
import struct
from pathlib import Path
from typing import Any

import pytest

import remote_sandbox.remote_agent.inotify as inotify_module
from remote_sandbox.remote_agent.inotify import (
    IN_ATTRIB,
    IN_CREATE,
    IN_DELETE,
    IN_ISDIR,
    IN_MOVED_FROM,
    IN_MOVED_TO,
    IN_Q_OVERFLOW,
    InotifyBackend,
    InotifyEvent,
    parse_inotify_buffer,
)
from remote_sandbox.remote_agent.store import RemoteStore


def _record(descriptor: int, mask: int, cookie: int, name: bytes) -> bytes:
    padded_length = (len(name) + 1 + 3) & ~3
    field = name + b"\0" + b"\0" * (padded_length - len(name) - 1)
    return struct.pack("iIII", descriptor, mask, cookie, len(field)) + field


def test_inotify_overflow_becomes_rescan_required() -> None:
    raw = struct.pack("iIII", -1, IN_Q_OVERFLOW, 0, 0)

    events = parse_inotify_buffer(raw)

    assert events[0].overflow is True


def test_inotify_parser_decodes_multiple_unicode_records() -> None:
    raw = _record(3, IN_CREATE, 7, "算法.py".encode()) + _record(4, IN_CREATE, 0, b"next")

    events = parse_inotify_buffer(raw)

    assert [(event.watch_descriptor, event.cookie, event.name) for event in events] == [
        (3, 7, "算法.py"),
        (4, 0, "next"),
    ]


@pytest.mark.parametrize("raw", [b"short", struct.pack("iIII", 1, IN_CREATE, 0, 16) + b"x"])
def test_inotify_parser_rejects_truncated_records(raw: bytes) -> None:
    with pytest.raises(ValueError, match="truncated"):
        parse_inotify_buffer(raw)


def _router(root: Path, store: RemoteStore) -> InotifyBackend:
    backend = object.__new__(InotifyBackend)
    backend._root = root
    backend._store = store
    backend._watch_paths = {1: root}
    backend._path_watches = {root: 1}
    backend._pending_moves = {}
    return backend


def test_inotify_router_pairs_moves_by_cookie_without_linux(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)

        backend._process_events(
            [
                InotifyEvent(1, IN_MOVED_FROM, 9, "old.py", False),
                InotifyEvent(1, IN_MOVED_TO, 9, "new.py", False),
            ]
        )

        events = store.events_after(0)
        assert [(event.kind, event.path, event.destination_path) for event in events] == [
            ("move", "old.py", "new.py")
        ]


def test_inotify_router_records_overflow_as_rescan_required_without_linux(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)
        rebuilt = False

        def rebuild() -> None:
            nonlocal rebuilt
            rebuilt = True

        backend._rebuild_watches = rebuild

        backend._process_events([InotifyEvent(-1, IN_Q_OVERFLOW, 0, "", True)])

        assert rebuilt is True
        assert [(event.kind, event.path) for event in store.events_after(0)] == [
            ("rescan-required", "*")
        ]


def test_inotify_router_ignores_workspace_root_attribute_events(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)

        backend._process_events([InotifyEvent(1, IN_ATTRIB, 0, "", False)])

        assert store.events_after(0) == []


def test_recursive_inotify_adds_each_watch_before_enumerating_children(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    (root / "child").mkdir(parents=True)
    backend = object.__new__(InotifyBackend)
    backend._root = root
    actions: list[tuple[str, Path | None]] = []
    original_scandir = inotify_module.os.scandir

    def add_watch(path: Path, *, watch_source: Path | None = None) -> None:
        del watch_source
        actions.append(("watch", path))

    def scandir(path: Any) -> Any:
        actions.append(("scan", None))
        return original_scandir(path)

    backend._add_watch = add_watch
    monkeypatch.setattr(inotify_module.os, "scandir", scandir)

    backend._add_tree(root)

    assert actions == [
        ("watch", root),
        ("scan", None),
        ("watch", root / "child"),
        ("scan", None),
    ]


def test_rapid_directory_create_delete_is_transient(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)

        def disappeared(_path: Path) -> None:
            raise FileNotFoundError(errno.ENOENT, "gone")

        backend._add_tree = disappeared

        backend._process_events(
            [
                InotifyEvent(1, IN_CREATE | IN_ISDIR, 0, "temporary", False),
                InotifyEvent(1, IN_DELETE | IN_ISDIR, 0, "temporary", False),
            ]
        )

        assert [(event.kind, event.path) for event in store.events_after(0)] == [
            ("delete", "temporary")
        ]


def test_rapid_directory_move_in_delete_is_transient(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)

        def disappeared(_path: Path) -> None:
            raise NotADirectoryError(errno.ENOTDIR, "gone")

        backend._add_tree = disappeared

        backend._process_events(
            [
                InotifyEvent(1, IN_MOVED_TO | IN_ISDIR, 17, "incoming", False),
                InotifyEvent(1, IN_DELETE | IN_ISDIR, 0, "incoming", False),
            ]
        )

        assert [(event.kind, event.path) for event in store.events_after(0)] == [
            ("delete", "incoming")
        ]


def test_runtime_tree_resource_failure_requests_rescan_without_killing_router(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with RemoteStore(tmp_path / "state.sqlite3") as store:
        backend = _router(root, store)

        def exhausted(_path: Path) -> None:
            raise OSError(errno.EMFILE, "too many files")

        backend._add_tree = exhausted

        backend._process_events(
            [InotifyEvent(1, IN_CREATE | IN_ISDIR, 0, "new", False)]
        )

        assert isinstance(backend.last_error, OSError)
        assert [(event.kind, event.path) for event in store.events_after(0)] == [
            ("create", "new"),
            ("rescan-required", "*"),
        ]


def test_initial_recursive_setup_surfaces_permission_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    (root / "blocked").mkdir(parents=True)
    backend = object.__new__(InotifyBackend)
    backend._root = root
    backend._add_watch = lambda _path, watch_source=None: None
    original_open = inotify_module.os.open

    def open_directory(path: Any, flags: int, *, dir_fd: int | None = None) -> int:
        if path == "blocked":
            raise PermissionError(errno.EACCES, "denied")
        return original_open(path, flags, dir_fd=dir_fd)

    monkeypatch.setattr(inotify_module.os, "open", open_directory)

    with pytest.raises(PermissionError, match="denied"):
        backend._add_tree(root)


def test_recursive_setup_bounds_open_descriptors_by_depth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    for index in range(64):
        (root / f"child-{index:02d}").mkdir()
    backend = object.__new__(InotifyBackend)
    backend._root = root
    backend._add_watch = lambda _path, watch_source=None: None
    original_open = inotify_module.os.open
    original_close = inotify_module.os.close
    tracked: set[int] = set()
    peak = 0

    def open_directory(path: Any, flags: int, *, dir_fd: int | None = None) -> int:
        nonlocal peak
        descriptor = original_open(path, flags, dir_fd=dir_fd)
        tracked.add(descriptor)
        peak = max(peak, len(tracked))
        return descriptor

    def close_directory(descriptor: int) -> None:
        tracked.discard(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(inotify_module.os, "open", open_directory)
    monkeypatch.setattr(inotify_module.os, "close", close_directory)

    backend._add_tree(root)

    assert peak <= 2
    assert not tracked
