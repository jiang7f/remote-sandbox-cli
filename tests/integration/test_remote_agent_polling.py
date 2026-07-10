from __future__ import annotations

import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

import remote_sandbox.remote_agent.__main__ as remote_agent_main
from remote_sandbox.agent import build_agent_zipapp
from remote_sandbox.remote_agent.store import RemoteStore, WatcherState, process_is_alive
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


def test_remote_snapshot_refuses_a_registered_root_replaced_by_a_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    moved_root = tmp_path / "moved-root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_text("outside", encoding="utf-8")
    root.rename(moved_root)
    root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(NotADirectoryError):
        snapshot_entries(root)


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
    runtime = tmp_path / "runtime"
    root.mkdir()
    home.mkdir()
    env = {
        **os.environ,
        "HOME": str(home),
        "CODEX_REMOTE_SANDBOX_HOME": str(control),
        "CODEX_REMOTE_SANDBOX_RUNTIME_DIR": str(runtime),
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
    runtime_workspace = runtime / "workspaces" / workspace_id
    assert runtime.stat().st_mode & 0o777 == 0o700
    assert runtime_workspace.stat().st_mode & 0o777 == 0o700
    assert (runtime / "index.lock").stat().st_mode & 0o777 == 0o600
    assert (runtime_workspace / "control.lock").stat().st_mode & 0o777 == 0o600
    assert (runtime_workspace / "watcher.log").stat().st_mode & 0o777 == 0o600
    persistent_workspace = control / "workspaces" / workspace_id
    assert not (control / "index.lock").exists()
    assert not (persistent_workspace / "control.lock").exists()
    assert not (persistent_workspace / "watcher.log").exists()

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
    assert not (runtime_workspace / "watcher.log").exists()
    assert root.exists()


def test_default_remote_runtime_uses_isolated_codex_tmp_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CODEX_REMOTE_SANDBOX_RUNTIME_DIR", raising=False)

    assert remote_agent_main._runtime_root() == (
        Path("/tmp") / f"codex-remote-sandbox-{os.getuid()}"
    )


def test_register_conflict_does_not_leave_unreachable_workspace_metadata(tmp_path: Path) -> None:
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
    first_id = "00000000-0000-4000-8000-000000000071"
    conflicting_id = "00000000-0000-4000-8000-000000000072"

    first = _agent_call(
        archive,
        AgentRequest("register", {"workspace_id": first_id, "root": str(root)}),
        env,
    )
    assert first.returncode == 0

    conflict = _agent_call(
        archive,
        AgentRequest("register", {"workspace_id": conflicting_id, "root": str(root)}),
        env,
    )

    assert conflict.returncode == 2
    assert "already registered" in (decode_response(conflict.stdout).error or "")
    assert not (control / "workspaces" / conflicting_id).exists()

    forgotten = _agent_call(archive, AgentRequest("forget", {"workspace_id": first_id}), env)
    assert forgotten.returncode == 0


def test_register_rejects_control_home_inside_workspace_before_creating_it(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = root / ".codex-remote-sandbox"
    root.mkdir()
    home.mkdir()
    env = {
        **os.environ,
        "HOME": str(home),
        "CODEX_REMOTE_SANDBOX_HOME": str(control),
    }

    registered = _agent_call(
        archive,
        AgentRequest(
            "register",
            {
                "workspace_id": "00000000-0000-4000-8000-000000000077",
                "root": str(root),
            },
        ),
        env,
    )

    assert registered.returncode == 2
    assert "must not overlap" in (decode_response(registered.stdout).error or "")
    assert not control.exists()


def test_stop_never_signals_a_reused_unrelated_pid(tmp_path: Path) -> None:
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
    workspace_id = "00000000-0000-4000-8000-000000000073"
    registered = _agent_call(
        archive,
        AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
        env,
    )
    assert registered.returncode == 0

    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", "not-a-watcher"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        state_path = control / "workspaces" / workspace_id / "state.sqlite3"
        with RemoteStore(state_path) as store:
            store.record_watcher(
                unrelated.pid,
                "running",
                backend="polling",
                token="stale-watcher-token",
            )

        stopped = _agent_call(archive, AgentRequest("stop", {"workspace_id": workspace_id}), env)

        assert stopped.returncode == 2
        assert "another process" in (decode_response(stopped.stdout).error or "")
        assert unrelated.poll() is None

        forgotten = _agent_call(
            archive,
            AgentRequest("forget", {"workspace_id": workspace_id}),
            env,
        )
        assert forgotten.returncode == 0
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_commands_reject_workspace_state_that_disagrees_with_protected_index(
    tmp_path: Path,
) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    root = tmp_path / "workspace"
    outside = tmp_path / "outside"
    home = tmp_path / "home"
    control = tmp_path / "control"
    root.mkdir()
    outside.mkdir()
    home.mkdir()
    (outside / "secret.txt").write_text("outside", encoding="utf-8")
    env = {
        **os.environ,
        "HOME": str(home),
        "CODEX_REMOTE_SANDBOX_HOME": str(control),
    }
    workspace_id = "00000000-0000-4000-8000-000000000074"
    registered = _agent_call(
        archive,
        AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
        env,
    )
    assert registered.returncode == 0

    state_path = control / "workspaces" / workspace_id / "state.sqlite3"
    with sqlite3.connect(state_path) as connection:
        connection.execute("UPDATE workspace SET root = ?", (str(outside),))

    snapshot = _agent_call(
        archive,
        AgentRequest("snapshot", {"workspace_id": workspace_id}),
        env,
    )

    assert snapshot.returncode == 2
    assert "protected index" in (decode_response(snapshot.stdout).error or "")
    assert b"secret.txt" not in snapshot.stdout


def test_concurrent_start_calls_share_one_watcher_generation(tmp_path: Path) -> None:
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
    workspace_id = "00000000-0000-4000-8000-000000000075"
    assert (
        _agent_call(
            archive,
            AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
            env,
        ).returncode
        == 0
    )
    barrier = threading.Barrier(3)
    results: list[subprocess.CompletedProcess[bytes]] = []

    def start() -> None:
        barrier.wait()
        results.append(
            _agent_call(archive, AgentRequest("start", {"workspace_id": workspace_id}), env)
        )

    threads = [threading.Thread(target=start) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert [result.returncode for result in results] == [0, 0]
    assert len({int(decode_response(result.stdout).payload["pid"]) for result in results}) == 1

    stopped = _agent_call(archive, AgentRequest("stop", {"workspace_id": workspace_id}), env)
    assert stopped.returncode == 0
    assert (
        _agent_call(archive, AgentRequest("forget", {"workspace_id": workspace_id}), env).returncode
        == 0
    )


def test_concurrent_start_and_forget_never_recreate_persistent_metadata(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = tmp_path / "control"
    runtime = tmp_path / "runtime"
    root.mkdir()
    home.mkdir()
    env = {
        **os.environ,
        "HOME": str(home),
        "CODEX_REMOTE_SANDBOX_HOME": str(control),
        "CODEX_REMOTE_SANDBOX_RUNTIME_DIR": str(runtime),
    }
    workspace_id = "00000000-0000-4000-8000-000000000079"
    assert (
        _agent_call(
            archive,
            AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
            env,
        ).returncode
        == 0
    )
    barrier = threading.Barrier(3)
    results: dict[str, subprocess.CompletedProcess[bytes]] = {}

    def call(command: str) -> None:
        barrier.wait()
        results[command] = _agent_call(
            archive,
            AgentRequest(command, {"workspace_id": workspace_id}),
            env,
        )

    threads = [
        threading.Thread(target=call, args=("start",)),
        threading.Thread(target=call, args=("forget",)),
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)
    assert all(not thread.is_alive() for thread in threads)

    if results["start"].returncode == 0:
        assert results["forget"].returncode == 2
        assert _agent_call(
            archive,
            AgentRequest("stop", {"workspace_id": workspace_id}),
            env,
        ).returncode == 0
        assert _agent_call(
            archive,
            AgentRequest("forget", {"workspace_id": workspace_id}),
            env,
        ).returncode == 0
    else:
        assert results["start"].returncode == 2
        assert results["forget"].returncode == 0

    time.sleep(0.1)
    persistent_workspace = control / "workspaces" / workspace_id
    assert not persistent_workspace.exists()
    with RemoteStore(control / "index.sqlite3") as index:
        assert index.index_entry(workspace_id) is None
    assert not (persistent_workspace / "control.lock").exists()
    assert not (persistent_workspace / "watcher.log").exists()


def test_status_leaves_running_state_unchanged_when_identity_is_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = tmp_path / "control"
    runtime = tmp_path / "runtime"
    root.mkdir()
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(control))
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_RUNTIME_DIR", str(runtime))
    workspace_id = "00000000-0000-4000-8000-000000000080"
    remote_agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})
    state_path = control / "workspaces" / workspace_id / "state.sqlite3"
    with RemoteStore(state_path) as store:
        store.record_watcher(
            os.getpid(),
            "running",
            backend="polling",
            token="generation-a",
        )
    monkeypatch.setattr(remote_agent_main, "_watcher_identity", lambda _state, _id: "unknown")

    payload = remote_agent_main._handle_status({"workspace_id": workspace_id})

    assert payload["status"] == "running"
    with RemoteStore(state_path) as store:
        state = store.watcher_state()
        assert state.status == "running"
        assert state.token == "generation-a"


def test_status_cannot_overwrite_a_new_start_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = tmp_path / "control"
    runtime = tmp_path / "runtime"
    root.mkdir()
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(control))
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_RUNTIME_DIR", str(runtime))
    workspace_id = "00000000-0000-4000-8000-000000000081"
    remote_agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})
    state_path = control / "workspaces" / workspace_id / "state.sqlite3"
    with RemoteStore(state_path) as store:
        store.record_watcher(
            os.getpid(),
            "running",
            backend="polling",
            token="generation-a",
        )

    status_entered = threading.Event()
    generation_b_seen = threading.Event()
    stop_monitor = threading.Event()
    status_result: dict[str, object] = {}
    start_result: dict[str, object] = {}

    def identity(state: WatcherState, _workspace_id: str) -> str:
        if threading.current_thread().name == "status-thread" and state.token == "generation-a":
            status_entered.set()
            generation_b_seen.wait(timeout=1)
            return "mismatch"
        return "dead"

    def sleeper_command(current_id: str, current_home: Path, token: str) -> list[str]:
        return [
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
            "_watch",
            current_id,
            str(current_home),
            token,
        ]

    def monitor_generation() -> None:
        while not stop_monitor.wait(0.01):
            with RemoteStore(state_path) as store:
                if store.watcher_state().token not in {None, "generation-a"}:
                    generation_b_seen.set()
                    return

    def read_status() -> None:
        status_result.update(remote_agent_main._handle_status({"workspace_id": workspace_id}))

    def start_watcher() -> None:
        start_result.update(remote_agent_main._handle_start({"workspace_id": workspace_id}))

    monkeypatch.setattr(remote_agent_main, "_watcher_identity", identity)
    monkeypatch.setattr(remote_agent_main, "_watcher_command", sleeper_command)
    monitor = threading.Thread(target=monitor_generation)
    status_thread = threading.Thread(target=read_status, name="status-thread")
    start_thread = threading.Thread(target=start_watcher, name="start-thread")
    monitor.start()
    status_thread.start()
    assert status_entered.wait(timeout=2)
    start_thread.start()
    status_thread.join(timeout=5)
    start_thread.join(timeout=5)
    stop_monitor.set()
    monitor.join(timeout=2)

    assert not status_thread.is_alive()
    assert not start_thread.is_alive()
    with RemoteStore(state_path) as store:
        final_state = store.watcher_state()
    assert final_state.token not in {None, "generation-a"}
    assert final_state.status == "starting"
    pid = int(start_result["pid"])
    assert process_is_alive(pid)
    os.kill(pid, signal.SIGTERM)
    waited_pid, _status = os.waitpid(pid, 0)
    assert waited_pid == pid
    assert not process_is_alive(pid)


def test_forget_restores_index_and_metadata_when_deletion_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    home = tmp_path / "home"
    control = tmp_path / "control"
    root.mkdir()
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(control))
    workspace_id = "00000000-0000-4000-8000-000000000076"
    remote_agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})
    metadata = control / "workspaces" / workspace_id
    original_rmtree = remote_agent_main.shutil.rmtree

    def fail_delete(path: Path) -> None:
        raise OSError(f"cannot remove {path}")

    monkeypatch.setattr(remote_agent_main.shutil, "rmtree", fail_delete)
    with pytest.raises(OSError, match="cannot remove"):
        remote_agent_main._handle_forget({"workspace_id": workspace_id})

    assert metadata.exists()
    with RemoteStore(control / "index.sqlite3") as index:
        assert index.index_entry(workspace_id) is not None

    monkeypatch.setattr(remote_agent_main.shutil, "rmtree", original_rmtree)
    assert remote_agent_main._handle_forget({"workspace_id": workspace_id})["forgotten"] is True


def test_non_follow_event_stream_drains_multiple_bounded_batches(tmp_path: Path) -> None:
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
    workspace_id = "00000000-0000-4000-8000-000000000078"
    assert (
        _agent_call(
            archive,
            AgentRequest("register", {"workspace_id": workspace_id, "root": str(root)}),
            env,
        ).returncode
        == 0
    )
    with RemoteStore(control / "workspaces" / workspace_id / "state.sqlite3") as store:
        for index in range(300):
            store.append_event("create", f"files/{index:03d}.txt", None)

    streamed = _agent_call(
        archive,
        AgentRequest(
            "events",
            {"workspace_id": workspace_id, "after_sequence": 0, "follow": False},
        ),
        env,
    )

    assert streamed.returncode == 0
    lines = [json.loads(line) for line in streamed.stdout.splitlines()]
    assert len(lines) == 300
    assert lines[0]["path"] == "files/000.txt"
    assert lines[-1]["path"] == "files/299.txt"
