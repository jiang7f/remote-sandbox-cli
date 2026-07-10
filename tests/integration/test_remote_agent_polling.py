from __future__ import annotations

import json
import os
import platform
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from remote_sandbox.agent import build_agent_zipapp
from remote_sandbox.remote_agent.store import RemoteStore
from remote_sandbox.remote_agent.watcher import PollingWatcher, WatcherService, snapshot_entries
from remote_sandbox.remote_protocol import AgentRequest, decode_response, encode_request


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for remote watcher state")


def test_polling_watcher_records_a_delete(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    file = root / "x.txt"
    file.write_text("x", encoding="utf-8")
    store = RemoteStore(tmp_path / "state.sqlite3")
    watcher = PollingWatcher(root, store, interval=0.05)
    thread = threading.Thread(target=watcher.run, daemon=True)
    thread.start()
    try:
        time.sleep(0.1)
        file.unlink()
        _wait_until(lambda: any(event.kind == "delete" for event in store.events_after(0)))
    finally:
        watcher.stop()
        thread.join(timeout=2)
        store.close()

    assert not thread.is_alive()


def test_polling_watcher_does_not_follow_symlinks_or_record_hard_ignored_paths(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / ".git").mkdir()
    link = root / "linked"
    link.symlink_to(outside, target_is_directory=True)
    store = RemoteStore(tmp_path / "state.sqlite3")
    watcher = PollingWatcher(root, store, interval=0.02)
    thread = threading.Thread(target=watcher.run, daemon=True)
    thread.start()
    try:
        (outside / "secret.txt").write_text("outside", encoding="utf-8")
        (root / ".git" / "index").write_text("ignored", encoding="utf-8")
        (root / "visible.txt").write_text("visible", encoding="utf-8")
        _wait_until(lambda: any(event.path == "visible.txt" for event in store.events_after(0)))
    finally:
        watcher.stop()
        thread.join(timeout=2)
        events = store.events_after(0)
        store.close()

    assert all(event.path not in {"linked/secret.txt", ".git/index"} for event in events)


def test_remote_snapshot_uses_manifest_kinds_and_preserves_symlink_text(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "pkg").mkdir()
    (root / "current").symlink_to("pkg", target_is_directory=True)

    entries = {entry["path"]: entry for entry in snapshot_entries(root)}

    assert entries["pkg"]["kind"] == "dir"
    assert entries["current"]["kind"] == "symlink"
    assert entries["current"]["link_target"] == "pkg"


@pytest.mark.skipif(platform.system() != "Linux", reason="inotify is Linux-only")
def test_linux_service_selects_inotify_and_watches_new_directories(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = RemoteStore(tmp_path / "state.sqlite3")
    service = WatcherService(root, store, poll_interval=0.05)
    thread = threading.Thread(target=service.run, daemon=True)
    thread.start()
    try:
        assert service.backend_name == "inotify"
        (root / "new").mkdir()
        (root / "new" / "file.txt").write_text("x", encoding="utf-8")
        _wait_until(lambda: any(event.path == "new/file.txt" for event in store.events_after(0)))
    finally:
        service.stop()
        thread.join(timeout=2)
        store.close()


def _agent_call(
    archive: Path,
    request: AgentRequest,
    env: dict[str, str],
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["python3", str(archive)],
        input=encode_request(request),
        capture_output=True,
        check=False,
        env=env,
        timeout=10,
    )


def test_zipapp_manages_detached_watcher_journal_and_safe_forget(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = tmp_path / "control"
    root.mkdir()
    home.mkdir()
    env = {
        **os.environ,
        "HOME": str(home),
        "CODEX_REMOTE_SANDBOX_HOME": str(control),
    }
    workspace_id = "00000000-0000-4000-8000-000000000007"

    register = _agent_call(
        archive,
        AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
        env,
    )
    assert register.returncode == 0
    assert decode_response(register.stdout).ok

    started = _agent_call(archive, AgentRequest("start", {"workspace_id": workspace_id}), env)
    assert started.returncode == 0
    started_payload = decode_response(started.stdout).payload
    assert started_payload["status"] in {"starting", "running"}
    assert int(started_payload["pid"]) > 0

    def watcher_running() -> bool:
        status = _agent_call(archive, AgentRequest("status", {"workspace_id": workspace_id}), env)
        return (
            status.returncode == 0 and decode_response(status.stdout).payload["status"] == "running"
        )

    _wait_until(watcher_running, timeout=5)
    changed = root / "算法.py"
    changed.write_text("changed\n", encoding="utf-8")

    def event_lines() -> list[dict[str, object]]:
        result = _agent_call(
            archive,
            AgentRequest(
                "events",
                {"workspace_id": workspace_id, "after_sequence": 0, "follow": False},
            ),
            env,
        )
        assert result.returncode == 0
        return [json.loads(line) for line in result.stdout.splitlines()]

    _wait_until(lambda: any(line["path"] == "算法.py" for line in event_lines()), timeout=5)
    lines = event_lines()
    last_sequence = max(int(line["sequence"]) for line in lines)

    snapshot = _agent_call(
        archive,
        AgentRequest("snapshot", {"workspace_id": workspace_id}),
        env,
    )
    assert snapshot.returncode == 0
    snapshot_entries = decode_response(snapshot.stdout).payload["entries"]
    assert any(entry["path"] == "算法.py" for entry in snapshot_entries)

    acknowledged = _agent_call(
        archive,
        AgentRequest("ack", {"workspace_id": workspace_id, "sequence": last_sequence}),
        env,
    )
    assert decode_response(acknowledged.stdout).payload["acknowledged_sequence"] == last_sequence

    refused = _agent_call(archive, AgentRequest("forget", {"workspace_id": workspace_id}), env)
    assert refused.returncode == 2
    assert "running" in (decode_response(refused.stdout).error or "")

    stopped = _agent_call(archive, AgentRequest("stop", {"workspace_id": workspace_id}), env)
    assert stopped.returncode == 0
    assert decode_response(stopped.stdout).payload["status"] == "stopped"

    forgotten = _agent_call(archive, AgentRequest("forget", {"workspace_id": workspace_id}), env)
    assert forgotten.returncode == 0
    assert not (control / "workspaces" / workspace_id).exists()
    assert root.exists()
