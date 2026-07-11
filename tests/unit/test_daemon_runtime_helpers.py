from __future__ import annotations

import fcntl
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import remote_sandbox.daemon as daemon
from remote_sandbox.daemon import (
    DaemonError,
    DaemonStatus,
    StopResult,
    SupervisorRuntime,
)
from remote_sandbox.journal import EventKind
from remote_sandbox.registry import BindingRecord
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def _runtime(tmp_path: Path) -> SupervisorRuntime:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    return SupervisorRuntime("workspace-1", metadata, tmp_path / "runtime")


def _status(
    phase: WorkspacePhase,
    *,
    running: bool = True,
    pid: int | None = 1234,
    conn_state: str = "ok",
) -> DaemonStatus:
    return DaemonStatus(running, pid, conn_state=conn_state, phase=phase)


def test_runtime_paths_and_local_path_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    local = tmp_path / "local"
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)

    assert runtime.state_db == runtime.metadata_root / "state.sqlite3"
    assert runtime.logfile == runtime.metadata_root / "daemon.log"
    assert runtime.pidfile == runtime.metadata_root / "daemon.pid"
    assert runtime.lockfile == runtime.metadata_root / "daemon.lock"
    assert runtime.socket == runtime.runtime_root / "workspace-1.sock"
    assert daemon.meta_dir(local) == runtime.metadata_root
    assert daemon.pidfile_path(local) == runtime.pidfile
    assert daemon.daemon_lock_path(local) == runtime.lockfile
    assert daemon.logfile_path(local) == runtime.logfile
    assert daemon.socket_path(local) == runtime.socket


def test_daemon_status_wrappers_and_workspace_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    ready = _status(WorkspacePhase.READY)
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: ready)
    monkeypatch.setattr(daemon.SupervisorClient, "control_status", lambda _self: ready)

    assert daemon.daemon_status(tmp_path) is ready
    assert daemon.daemon_control_status(tmp_path) is ready

    monkeypatch.setattr(
        daemon.SupervisorClient,
        "status",
        lambda _self: (_ for _ in ()).throw(DaemonError("bad state")),
    )
    assert daemon.daemon_status(tmp_path).phase is WorkspacePhase.STOPPED

    projected = daemon._project_workspace_status(
        DaemonStatus(
            True,
            2,
            last_error="offline",
            phase=WorkspacePhase.DISCONNECTED,
        )
    )
    assert projected == WorkspaceStatus(
        WorkspacePhase.DISCONNECTED,
        SyncProgress("disconnected"),
        last_error="offline",
    )


def test_workspace_status_requires_control_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        daemon,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(root=tmp_path / "metadata"),
    )
    monkeypatch.setattr(daemon, "runtime_dir", lambda: tmp_path / "runtime")
    monkeypatch.setattr(
        daemon.SupervisorClient,
        "control_status",
        lambda _self: _status(WorkspacePhase.STARTING),
    )

    with pytest.raises(DaemonError, match="did not publish workspace status"):
        daemon.daemon_workspace_status("workspace-1")


def test_wait_for_control_retries_starting_and_times_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    replies: list[object] = [
        DaemonError("missing"),
        _status(WorkspacePhase.STARTING),
        _status(WorkspacePhase.READY),
    ]

    def control(_self: object) -> DaemonStatus:
        value = replies.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(daemon.SupervisorClient, "control_status", control)
    monkeypatch.setattr(daemon.time, "sleep", lambda _seconds: None)
    assert daemon.wait_for_daemon_control(tmp_path, 1.0).phase is WorkspacePhase.READY

    monkeypatch.setattr(daemon.time, "monotonic", iter((0.0, 0.0, 2.0)).__next__)
    monkeypatch.setattr(
        daemon.SupervisorClient,
        "control_status",
        lambda _self: (_ for _ in ()).throw(DaemonError("missing")),
    )
    with pytest.raises(DaemonError, match="endpoint is unresponsive"):
        daemon.wait_for_daemon_control(tmp_path, 1.0)


def test_stop_and_ensure_daemon_lifecycle_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    state = {"status": _status(WorkspacePhase.STOPPED, running=False, pid=None), "stop": True}

    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: state["status"])
    monkeypatch.setattr(daemon.SupervisorClient, "stop", lambda _self: state["stop"])
    monkeypatch.setattr(daemon.SupervisorClient, "sync", lambda _self: True)
    monkeypatch.setattr(daemon, "_wait_until_stopped", lambda _runtime: True)

    assert daemon.poke_daemon(tmp_path, "test") is True
    assert daemon.stop_daemon_result(tmp_path) is StopResult.NOT_RUNNING

    state["status"] = _status(WorkspacePhase.READY)
    state["stop"] = False
    assert daemon.stop_daemon_result(tmp_path) is StopResult.TIMEOUT
    state["stop"] = True
    assert daemon.stop_daemon_result(tmp_path) is StopResult.STOPPED
    assert daemon.stop_daemon(tmp_path) is True

    assert daemon.ensure_daemon(tmp_path) is state["status"]
    disconnected = _status(
        WorkspacePhase.DISCONNECTED,
        conn_state="disconnected",
    )
    resumed = _status(WorkspacePhase.READY)
    state["status"] = disconnected
    resume_calls: list[bool] = []
    monkeypatch.setattr(
        daemon.SupervisorClient,
        "resume",
        lambda _self: resume_calls.append(True) or True,
    )
    monkeypatch.setattr(
        daemon.SupervisorClient,
        "status",
        lambda _self: resumed if resume_calls else state["status"],
    )
    assert daemon.ensure_daemon(tmp_path) is resumed
    assert resume_calls == [True]

    stopped = _status(WorkspacePhase.STOPPED, running=False, pid=None)
    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: stopped)
    monkeypatch.setattr(daemon, "start_daemon", lambda root, runner=None: (root, runner))
    assert daemon.ensure_daemon(tmp_path, runner="runner") == (tmp_path, "runner")  # type: ignore[arg-type]


def test_start_daemon_existing_and_parent_process_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    local = tmp_path / "local"
    local.mkdir()
    existing = _status(WorkspacePhase.READY)
    state = {"status": existing}
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: state["status"])
    monkeypatch.setattr(
        daemon.SupervisorClient,
        "wait_until_running",
        lambda _self, _timeout: existing,
    )

    assert daemon.start_daemon(local) is existing

    state["status"] = _status(WorkspacePhase.STOPPED, running=False, pid=None)
    waited: list[tuple[int, int]] = []
    monkeypatch.setattr(daemon.os, "fork", lambda: 4321)
    monkeypatch.setattr(daemon.os, "waitpid", lambda pid, flags: waited.append((pid, flags)))
    assert daemon.start_daemon(local) is existing
    assert waited == [(4321, 0)]


def test_start_daemon_first_child_exits_after_second_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    stopped = _status(WorkspacePhase.STOPPED, running=False, pid=None)
    forks = iter((0, 4321))
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: stopped)
    monkeypatch.setattr(daemon.os, "fork", lambda: next(forks))
    monkeypatch.setattr(daemon.os, "setsid", lambda: None)
    monkeypatch.setattr(
        daemon.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )

    with pytest.raises(SystemExit) as stopped_process:
        daemon.start_daemon(tmp_path)
    assert stopped_process.value.code == 0


def test_start_daemon_second_child_configures_and_runs_supervisor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    stopped = _status(WorkspacePhase.STOPPED, running=False, pid=None)
    forks = iter((0, 0))
    calls: list[object] = []
    supervisor = SimpleNamespace(run=lambda: calls.append("run"))
    monkeypatch.setattr(daemon, "_runtime_for_local", lambda _root: runtime)
    monkeypatch.setattr(daemon.SupervisorClient, "status", lambda _self: stopped)
    monkeypatch.setattr(daemon.os, "fork", lambda: next(forks))
    monkeypatch.setattr(daemon.os, "setsid", lambda: calls.append("setsid"))
    monkeypatch.setattr(daemon.os, "umask", lambda mask: calls.append(("umask", mask)))
    monkeypatch.setattr(daemon, "_configure_daemon_logging", lambda path: calls.append(path))
    monkeypatch.setattr(daemon, "_detach_standard_streams", lambda path: calls.append(("io", path)))
    monkeypatch.setattr(daemon, "_build_supervisor", lambda *_args, **_kwargs: supervisor)
    monkeypatch.setattr(
        daemon.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )

    with pytest.raises(SystemExit) as stopped_process:
        daemon.start_daemon(tmp_path, runner="runner")  # type: ignore[arg-type]
    assert stopped_process.value.code == 0
    assert "setsid" in calls and "run" in calls
    assert ("umask", 0o077) in calls


def test_record_and_runtime_lookup_use_external_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    record = BindingRecord(
        "dq",
        "workspace-1",
        "host",
        "/work/dq",
        str(local),
        "2026-01-01T00:00:00+00:00",
    )
    monkeypatch.setattr(daemon, "list_binding_records", lambda: [record])
    monkeypatch.setattr(
        daemon,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(root=tmp_path / "metadata"),
    )
    monkeypatch.setattr(daemon, "runtime_dir", lambda: tmp_path / "runtime")

    assert daemon._record_for_local(local / ".") == record
    runtime = daemon._runtime_for_local(local)
    assert runtime.workspace_id == "workspace-1"
    assert runtime.metadata_root == tmp_path / "metadata"

    monkeypatch.setattr(daemon, "list_binding_records", lambda: [])
    with pytest.raises(DaemonError, match="not a bound workspace"):
        daemon._record_for_local(local)


def test_detach_streams_duplicates_and_closes_extra_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptors = iter((10, 11))
    duplicated: list[tuple[int, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(daemon.os, "open", lambda *_args: next(descriptors))
    monkeypatch.setattr(
        daemon.os,
        "dup2",
        lambda source, target: duplicated.append((source, target)),
    )
    monkeypatch.setattr(daemon.os, "close", lambda descriptor: closed.append(descriptor))

    daemon._detach_standard_streams(tmp_path / "daemon.log")

    assert duplicated == [(10, 0), (11, 1), (11, 2)]
    assert closed == [10, 11]


def test_wait_and_lock_helpers_cover_success_busy_and_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.runtime_root.mkdir()
    lock_is_free = daemon._daemon_lock_is_free
    monkeypatch.setattr(daemon, "_daemon_lock_is_free", lambda _path: True)
    assert daemon._wait_until_stopped(runtime) is True

    runtime.socket.touch()
    times = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(daemon.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(daemon.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(daemon, "_STOP_TIMEOUT_S", 1.0)
    assert daemon._wait_until_stopped(runtime) is False
    monkeypatch.setattr(daemon, "_daemon_lock_is_free", lock_is_free)

    missing_parent = tmp_path / "missing" / "lock"
    assert daemon._daemon_lock_is_free(missing_parent) is False

    lock_path = tmp_path / "held.lock"
    held = lock_path.open("a+b")
    fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert daemon._daemon_lock_is_free(lock_path) is False
    finally:
        fcntl.flock(held.fileno(), fcntl.LOCK_UN)
        held.close()
    assert daemon._daemon_lock_is_free(lock_path) is True

    releasable = lock_path.open("a+b")
    fcntl.flock(releasable.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    daemon._release_daemon_lock(releasable)
    assert releasable.closed


def test_status_payload_round_trip_and_malformed_payloads() -> None:
    workspace = WorkspaceStatus(
        WorkspacePhase.DEGRADED,
        SyncProgress(
            "auditing",
            files_done=2,
            files_total=3,
            bytes_done=4,
            bytes_total=5,
            current_path="model.py",
            elapsed_seconds=1.5,
        ),
        pending=6,
        conflicts=7,
        last_error="warning",
        last_sync_at=8.0,
    )
    expected = DaemonStatus(
        True,
        1234,
        2,
        "warning",
        "degraded",
        WorkspacePhase.DEGRADED,
        workspace,
        3,
    )
    assert daemon._daemon_status_from_payload(daemon._daemon_status_payload(expected)) == expected

    invalid_daemon_payloads: tuple[object, ...] = (
        [],
        {},
        {
            "running": True,
            "pid": None,
            "consecutive_failures": 0,
            "last_error": None,
            "conn_state": "ok",
            "phase": "unknown",
            "workspace_status": None,
        },
        {
            "running": True,
            "pid": None,
            "consecutive_failures": 0,
            "last_error": None,
            "conn_state": "ok",
            "phase": "ready",
            "workspace_status": [],
        },
    )
    for payload in invalid_daemon_payloads:
        with pytest.raises(DaemonError, match="malformed supervisor status"):
            daemon._daemon_status_from_payload(payload)

    for payload in (None, {}, {"progress": {}}, {"progress": [], "phase": "ready"}):
        with pytest.raises(DaemonError, match="malformed workspace status"):
            daemon._workspace_status_from_payload(payload)


def test_pid_durable_status_and_process_probes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    assert daemon._read_runtime_pidfile(runtime) is None
    runtime.pidfile.write_text("bad\n", encoding="utf-8")
    assert daemon._read_runtime_pidfile(runtime) is None
    runtime.pidfile.write_text("42\n", encoding="utf-8")
    assert daemon._read_runtime_pidfile(runtime) == 42

    assert daemon._read_durable_status(runtime).phase is WorkspacePhase.STOPPED
    with WorkspaceStore.open(runtime.state_db) as store:
        store.set_status(WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle")))
    assert daemon._read_durable_status(runtime).phase is WorkspacePhase.READY

    assert daemon._process_exists(0) is False
    monkeypatch.setattr(
        daemon.os,
        "kill",
        lambda *_args: (_ for _ in ()).throw(ProcessLookupError()),
    )
    assert daemon._process_exists(42) is False
    monkeypatch.setattr(
        daemon.os,
        "kill",
        lambda *_args: (_ for _ in ()).throw(PermissionError()),
    )
    assert daemon._process_exists(42) is True
    monkeypatch.setattr(daemon.os, "kill", lambda *_args: None)
    assert daemon._process_exists(42) is True


def test_build_supervisor_wires_components_events_and_mutations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    runtime = _runtime(tmp_path)
    record = BindingRecord(
        "dq",
        "workspace-1",
        "host",
        "/work/dq",
        str(local),
        "2026-01-01T00:00:00+00:00",
    )
    spec = SimpleNamespace(target="host", workspace_id="workspace-1", remote_root="/work/dq")
    captured: dict[str, Any] = {}

    class FakeWatcher:
        def __init__(self) -> None:
            self.starts = 0

        def start(self) -> None:
            self.starts += 1

        def stop(self) -> None:
            return

    watcher = FakeWatcher()
    remote = SimpleNamespace()
    engine = SimpleNamespace(run_once=lambda _reason: None)

    class FakeInitial:
        def __init__(self, **kwargs: Any) -> None:
            captured["initial_kwargs"] = kwargs

        def run(self) -> None:
            return

    monkeypatch.setattr(daemon, "_record_for_local", lambda _root: record)
    monkeypatch.setattr(
        daemon,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(workspace_file=tmp_path / "workspace.json"),
    )
    monkeypatch.setattr(daemon, "read_workspace_spec", lambda _path: spec)
    monkeypatch.setattr(daemon, "RemoteWorkspaceClient", lambda *_args, **_kwargs: remote)
    monkeypatch.setattr(daemon, "RemoteAgentManager", lambda _runner: "manager")
    monkeypatch.setattr(daemon.StaticPolicyEngine, "from_file", lambda *_args, **_kwargs: "policy")
    monkeypatch.setattr(daemon, "BatchTransport", lambda *_args, **_kwargs: "transport")
    monkeypatch.setattr(daemon, "SyncEngine", lambda **_kwargs: engine)
    monkeypatch.setattr(
        daemon,
        "create_local_watcher",
        lambda _root, _policy, callback: captured.update(local_event=callback) or watcher,
    )
    monkeypatch.setattr(daemon, "InitialSyncCoordinator", FakeInitial)
    monkeypatch.setattr(daemon, "load_settings", lambda: SimpleNamespace(placeholder_limit=4096))
    monkeypatch.setattr(
        daemon,
        "resolve_conflict_transaction",
        lambda **_kwargs: SimpleNamespace(path="model.py", conflict_id=9),
    )
    monkeypatch.setattr(daemon, "fetch_placeholders", lambda **_kwargs: (3, False))

    supervisor = daemon._build_supervisor(local, runtime, runner="runner")  # type: ignore[arg-type]
    try:
        supervisor._load_components()
        assert supervisor.remote is remote
        assert supervisor.engine is engine
        assert supervisor.local_watcher is watcher

        callback = captured["local_event"]
        callback(EventKind.CREATE, "model.py", None)
        assert supervisor._sync_requested.is_set()
        assert supervisor.store.latest_sequence("local") == 1

        start_watcher = captured["initial_kwargs"]["start_local_watcher"]
        assert start_watcher() == 1
        assert watcher.starts == 1

        mutate = supervisor._mutation_handler
        assert mutate is not None
        assert mutate("resolve", {"path": "model.py", "use_local": True}) == {
            "path": "model.py",
            "conflict_id": 9,
        }
        assert mutate("fetch", {"path": None, "fetch_all": True}) == {
            "count": 3,
            "cancelled": False,
        }

        invalid = (
            ("resolve", {"path": "model.py"}),
            ("resolve", {"path": 1, "use_local": True}),
            ("fetch", {"path": None}),
            ("fetch", {"path": 1, "fetch_all": False}),
            ("fetch", {"path": None, "fetch_all": 1}),
            ("unknown", {}),
        )
        for kind, payload in invalid:
            with pytest.raises(ValueError, match="malformed|unsupported"):
                mutate(kind, payload)
    finally:
        supervisor.store.close()
