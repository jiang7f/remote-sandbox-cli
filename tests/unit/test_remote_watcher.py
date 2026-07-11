from __future__ import annotations

from pathlib import Path

import pytest

import remote_sandbox.remote_agent.inotify as inotify_module
import remote_sandbox.remote_agent.watcher as watcher_module
from remote_sandbox.remote_agent.store import RemoteStore
from remote_sandbox.remote_agent.watcher import PollingWatcher, WatcherService


def _store(tmp_path: Path, root: Path) -> RemoteStore:
    store = RemoteStore(tmp_path / "state.sqlite3")
    store.register_workspace("workspace", root)
    return store


def test_polling_watcher_records_one_rescan_for_repeated_root_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = _store(tmp_path, root)
    watcher = PollingWatcher(root, store, interval=0.01)
    monkeypatch.setattr(
        watcher_module,
        "_root_identity",
        lambda _root: (_ for _ in ()).throw(FileNotFoundError("gone")),
    )
    try:
        watcher._poll_once()
        watcher._poll_once()
        events = store.events_after(0)
    finally:
        store.close()

    assert [(event.kind, event.path) for event in events] == [("rescan-required", "*")]
    assert isinstance(watcher.last_error, FileNotFoundError)


def test_polling_watcher_recovers_after_root_identity_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = _store(tmp_path, root)
    watcher = PollingWatcher(root, store, interval=0.01)
    changed = watcher_module._RootIdentity(999, 999)
    monkeypatch.setattr(watcher_module, "_root_identity", lambda _root: changed)
    monkeypatch.setattr(watcher_module, "scan_snapshot", lambda _root: {})
    try:
        watcher._poll_once()
        events = store.events_after(0)
    finally:
        store.close()

    assert [(event.kind, event.path) for event in events] == [("rescan-required", "*")]
    assert watcher.last_error is None
    assert watcher._root_identity == changed


def test_watcher_service_falls_back_when_inotify_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = _store(tmp_path, root)
    monkeypatch.setattr(watcher_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        inotify_module,
        "InotifyBackend",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("unsupported")),
    )
    try:
        service = WatcherService(root, store, poll_interval=0.01)
    finally:
        store.close()

    assert service.backend_name == "polling"
    assert isinstance(service._backend, PollingWatcher)


class _Backend:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.last_error: BaseException | None = None
        self.stopped = False

    def run(self) -> None:
        if self.error is not None:
            raise self.error

    def stop(self) -> None:
        self.stopped = True


def test_watcher_service_records_success_failure_and_generation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = _store(tmp_path, root)
    try:
        store.record_watcher(None, "starting", backend=None, token="token")
        service = WatcherService(root, store, token="token")
        backend = _Backend()
        service._backend = backend
        service.backend_name = "fake"
        service.run()
        assert store.watcher_state().status == "stopped"

        store.record_watcher(None, "stopped", backend=None, token="other")
        failing = WatcherService(root, store)
        failing_backend = _Backend(RuntimeError("crash"))
        failing._backend = failing_backend
        failing.backend_name = "fake"
        with pytest.raises(RuntimeError, match="crash"):
            failing.run()
        assert store.watcher_state().status == "failed"
        assert store.watcher_state().error == "crash"
        failing.stop()
        assert failing_backend.stopped is True
    finally:
        store.close()
