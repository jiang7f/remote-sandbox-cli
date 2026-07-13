from __future__ import annotations

import argparse
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from helpers.sync_harness import CliHarness, make_cli_harness

import remote_sandbox.cli as cli
from remote_sandbox.cli import ConnectedWorkspace, RemoteCommandResult
from remote_sandbox.daemon import DaemonError, StopResult
from remote_sandbox.registry import BindingRecord, RegistryError
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


@pytest.fixture
def cli_fixture(tmp_path: Path) -> Iterator[CliHarness]:
    harness = make_cli_harness(tmp_path)
    try:
        yield harness
    finally:
        harness.store.close()
        harness.pair.remote_client.close()


def _record(local: Path, *, name: str = "dq", workspace_id: str = "workspace-1") -> BindingRecord:
    return BindingRecord(
        name,
        workspace_id,
        "host",
        "/work/dq",
        str(local),
        "2026-01-01T00:00:00+00:00",
    )


def _workspace_status(
    phase: WorkspacePhase,
    stage: str,
    *,
    last_error: str | None = None,
) -> WorkspaceStatus:
    return WorkspaceStatus(phase, SyncProgress(stage), last_error=last_error)


def test_invoke_cli_debug_traceback_and_init_existing_file(
    cli_fixture: CliHarness,
    tmp_path: Path,
) -> None:
    cli_fixture.services.cwd = lambda: tmp_path
    first = cli.invoke_cli(["init"], services=cli_fixture.services)
    second = cli.invoke_cli(["init"], services=cli_fixture.services)
    assert "Initialized" in first.stdout
    assert "Already initialized" in second.stdout

    cli_fixture.services.list_records = lambda _path: (_ for _ in ()).throw(
        RuntimeError("status failed")
    )
    failed = cli.invoke_cli(["--debug", "status"], services=cli_fixture.services)
    assert failed.exit_code == 2
    assert "Traceback" in failed.stderr


def test_status_empty_missing_and_watch_refresh_paths(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.services.list_records = lambda _path: []
    assert "No bound workspaces" in cli.invoke_cli(
        ["status"], services=cli_fixture.services
    ).stdout

    missing = cli.invoke_cli(["status", "missing"], services=cli_fixture.services)
    assert missing.exit_code == 2
    assert "no connection named" in missing.stderr

    cli_fixture.services.list_records = lambda _path: [cli_fixture.record]
    cli_fixture.services.watch_limit = 2
    cli_fixture.services.watch_sleep = lambda _seconds: None
    watched = cli.invoke_cli(["status", "--watch"], services=cli_fixture.services)
    assert watched.exit_code == 0
    assert "\x1b[H\x1b[2J" in watched.stdout

    cli_fixture.services.watch_limit = None
    cli_fixture.services.watch_sleep = lambda _seconds: (_ for _ in ()).throw(
        KeyboardInterrupt()
    )
    interrupted = cli.invoke_cli(["status", "--watch"], services=cli_fixture.services)
    assert interrupted.exit_code == 0


def test_run_reports_sync_failures_without_changing_remote_exit_code(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.services.run_remote = lambda _record, _argv: RemoteCommandResult(
        7,
        "stdout\n",
        "stderr\n",
    )
    cli_fixture.services.request_sync = lambda _record: False
    result = cli.invoke_cli(["run", "dq", "--", "false"], services=cli_fixture.services)
    assert result.exit_code == 7
    assert "stdout" in result.stdout and "stderr" in result.stderr
    assert "sync after run failed" in result.stderr

    cli_fixture.services.request_sync = lambda _record: (_ for _ in ()).throw(
        RuntimeError("sync exploded")
    )
    debug = cli.invoke_cli(
        ["--debug", "run", "dq", "--", "false"],
        services=cli_fixture.services,
    )
    assert debug.exit_code == 7
    assert "Traceback" in debug.stderr


def test_no_shell_connect_and_reconnect_failure_paths(
    cli_fixture: CliHarness,
) -> None:
    ready = _workspace_status(WorkspacePhase.READY, "idle")
    cli_fixture.services.connect_workspace = lambda *_args: ConnectedWorkspace(
        cli_fixture.record,
        False,
        0,
    )
    cli_fixture.services.workspace_status = lambda _record: ready
    connected = cli.invoke_cli(
        ["connect", "host", "--remote", "/work/dq", "--no-shell"],
        services=cli_fixture.services,
    )
    assert connected.exit_code == 0
    assert "Workspace is ready" in connected.stdout

    failed = _workspace_status(WorkspacePhase.FAILED, "failed", last_error="sync failed")
    cli_fixture.services.connect_workspace = lambda *_args: ConnectedWorkspace(
        cli_fixture.record,
        True,
        0,
    )
    cli_fixture.services.wait_initial_sync = lambda _record, _generation: failed
    rejected = cli.invoke_cli(
        ["connect", "host", "--remote", "/work/dq", "--no-shell"],
        services=cli_fixture.services,
    )
    assert rejected.exit_code == 2
    assert "sync failed" in rejected.stderr

    captured: list[tuple[str, str, Path, str | None, bool]] = []

    def connect_automatic(
        target: str,
        remote: str,
        local: Path,
        name: str | None,
        assume_yes: bool,
    ) -> ConnectedWorkspace:
        captured.append((target, remote, local, name, assume_yes))
        return ConnectedWorkspace(cli_fixture.record, False, 0)

    cli_fixture.services.connect_workspace = connect_automatic
    cli_fixture.services.workspace_status = lambda _record: ready
    automatic = cli.invoke_cli(
        [
            "connect",
            "host",
            "--auto-remote",
            "--local",
            str(cli_fixture.pair.local),
            "--name",
            "project",
            "--no-shell",
        ],
        services=cli_fixture.services,
    )
    assert automatic.exit_code == 0
    assert captured[0][1] == cli.automatic_remote_workspace_path(
        cli_fixture.pair.local,
        "project",
    )

    cli_fixture.services.find_record = lambda _name, _path: None
    reconnect = cli.invoke_cli(
        ["reconnect", "missing", "--no-shell"],
        services=cli_fixture.services,
    )
    assert reconnect.exit_code == 2
    assert "run rsb status" in reconnect.stderr


def test_fetch_peek_resolve_and_service_record_output_paths(
    cli_fixture: CliHarness,
) -> None:
    outcomes = iter(((1, False), (0, True), (0, False)))
    cli_fixture.services.fetch_placeholders = lambda *_args: next(outcomes)
    assert "Fetched 1 placeholder file" in cli.invoke_cli(
        ["fetch", "model.bin"], services=cli_fixture.services
    ).stdout
    assert "Fetch cancelled" in cli.invoke_cli(
        ["fetch", "model.bin"], services=cli_fixture.services
    ).stdout
    assert "No placeholders found" in cli.invoke_cli(
        ["fetch", "model.bin"], services=cli_fixture.services
    ).stdout

    cli_fixture.services.peek_placeholder = lambda *_args: b"head\xfftail"
    peeked = cli.invoke_cli(["peek", "model.bin", "--tail", "2"], services=cli_fixture.services)
    assert peeked.exit_code == 0
    assert peeked.stdout.encode("utf-8", errors="surrogateescape") == b"head\xfftail"

    calls: list[tuple[str, bool]] = []
    cli_fixture.services.resolve_conflict = (
        lambda _record, path, use_local: calls.append((path, use_local))
    )
    resolved = cli.invoke_cli(
        ["resolve", "model.py", "--use-remote"],
        services=cli_fixture.services,
    )
    assert "using remote" in resolved.stdout
    assert calls == [("model.py", False)]

    cli_fixture.services.current_record = lambda _path, _cwd: None
    unbound = cli.invoke_cli(["fetch", "model.bin"], services=cli_fixture.services)
    assert unbound.exit_code == 2
    assert "current directory is not bound" in unbound.stderr


def test_dispatch_rejects_unimplemented_service_command(cli_fixture: CliHarness) -> None:
    args = argparse.Namespace(command="unknown")
    with pytest.raises(ValueError, match="not implemented"):
        cli._dispatch_services(args, cli_fixture.services)


def test_default_adapters_starting_fetch_cancel_and_malformed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(tmp_path / "state"))
    local = tmp_path / "local"
    local.mkdir()
    record = _record(local)
    starting = SimpleNamespace(phase=SimpleNamespace(value="starting"))
    ready = SimpleNamespace(phase=SimpleNamespace(value="ready"))
    monkeypatch.setattr(cli, "_require_live_workspace", lambda _record: local)
    monkeypatch.setattr(cli, "ensure_daemon", lambda _root: starting)
    monkeypatch.setattr(cli, "wait_for_daemon_control", lambda _root, timeout: ready)
    services = cli.default_cli_services()
    assert services.ensure_supervisor(record) is ready

    state_db = tmp_path / "state.sqlite3"
    monkeypatch.setattr(
        cli,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(state_db=state_db, root=tmp_path / "metadata"),
    )
    monkeypatch.setattr(cli, "fetch_all_prompt", lambda _root, _store: "Fetch all? ")
    assert services.fetch_placeholders(record, None, True, lambda _prompt: False) == (0, True)

    class Client:
        def mutate(self, _kind: str, _payload: dict[str, object]) -> dict[str, object]:
            return {"count": True, "cancelled": False}

    monkeypatch.setattr(cli, "_supervisor_client", lambda _record: Client())
    monkeypatch.setattr(cli, "ensure_daemon", lambda _root: ready)
    with pytest.raises(DaemonError, match="result is malformed"):
        services.fetch_placeholders(record, "model.bin", False, lambda _prompt: True)


def test_default_cleanup_adapters_detect_shared_agent_and_changed_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(tmp_path / "state"))
    local = tmp_path / "local"
    local.mkdir()
    record = _record(local)
    other = _record(tmp_path / "other", name="other", workspace_id="workspace-2")
    deleted: list[str] = []
    monkeypatch.setattr(cli, "list_binding_records", lambda _registry: [record, other])
    monkeypatch.setattr(
        cli,
        "_connected_runner",
        lambda _target: SimpleNamespace(delete_path=lambda *_args: deleted.append("agent")),
    )
    services = cli.default_cli_services()
    services.prune_remote_agent(record)
    assert deleted == []

    monkeypatch.setattr(cli, "delete_binding_record", lambda *_args, **_kwargs: False)
    with pytest.raises(RegistryError, match="changed during cleanup"):
        services.delete_registry_record(record)

    metadata = tmp_path / "metadata"
    metadata.mkdir()
    monkeypatch.setattr(
        cli,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(root=metadata),
    )
    monkeypatch.setattr(cli.shutil, "rmtree", lambda _path: None)
    with pytest.raises(RegistryError, match="still exists"):
        services.delete_local_workspace(record)


def test_remote_component_and_supervisor_client_factories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    record = _record(local)
    runner = object()
    remote = SimpleNamespace(close=lambda: None)
    remote.ensure_agent = lambda: setattr(remote, "ensured", True)
    monkeypatch.setattr(cli, "_connected_runner", lambda _target: runner)
    monkeypatch.setattr(cli, "RemoteAgentManager", lambda _runner: "manager")
    monkeypatch.setattr(cli, "RemoteWorkspaceClient", lambda *_args, **_kwargs: remote)
    monkeypatch.setattr(cli, "BatchTransport", lambda *_args, **_kwargs: "transport")

    assert cli._production_remote_components(record) == (runner, remote, "transport")
    assert remote.ensured is True

    monkeypatch.setattr(
        cli,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(root=tmp_path / "metadata"),
    )
    monkeypatch.setattr(cli, "runtime_dir", lambda: tmp_path / "runtime")
    monkeypatch.setattr(
        cli,
        "supervisor_runtime_dir",
        lambda root: root / "supervisors",
    )
    client = cli._supervisor_client(record)
    assert client.runtime.workspace_id == record.workspace_id
    assert client.runtime.runtime_root == tmp_path / "runtime" / "supervisors"

    remote.closed = False
    remote.close = lambda: setattr(remote, "closed", True)
    monkeypatch.setattr(
        cli,
        "_production_remote_components",
        lambda _record: (runner, remote, "transport"),
    )
    assert cli._with_remote_client(record, lambda value: value) is remote
    assert remote.closed is True


def test_confirm_server_and_daemon_command_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("builtins.input", lambda _prompt: "YES")
    assert cli.confirm_prompt("Continue? ") is True
    monkeypatch.setattr(
        "builtins.input",
        lambda _prompt: (_ for _ in ()).throw(EOFError()),
    )
    with pytest.raises(ValueError, match="input unavailable"):
        cli.confirm_prompt("Continue? ")

    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(placeholder_limit=1024))
    monkeypatch.setattr(cli, "load_configured_hosts", lambda **_kwargs: [])
    assert cli.list_servers() == 0
    assert "No SSH hosts found" in capsys.readouterr().out

    local = tmp_path / "local"
    local.mkdir()
    record = _record(local)
    monkeypatch.setattr(cli, "_record_for_execution", lambda _name: record)
    monkeypatch.setattr(cli, "_require_live_workspace", lambda _record: local)
    monkeypatch.setattr(cli, "_ensure_master", lambda _target: None)
    monkeypatch.setattr(
        cli,
        "ensure_daemon",
        lambda _root: SimpleNamespace(pid=4321),
    )
    assert cli.start_binding_daemon("dq") == 0

    results = iter((StopResult.STOPPED, StopResult.NOT_RUNNING, StopResult.TIMEOUT))
    monkeypatch.setattr(cli, "stop_daemon_result", lambda _root: next(results))
    assert cli.stop_binding_daemon("dq") == 0
    assert cli.stop_binding_daemon("dq") == 0
    assert cli.stop_binding_daemon("dq") == 0
    output = capsys.readouterr().out
    assert "Stopped daemon" in output
    assert "No daemon running" in output
    assert "still shutting down" in output


def test_shell_poke_parse_record_and_runner_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    record = _record(local)

    class Runner:
        def __init__(self) -> None:
            self.master_targets: list[str] = []
            self.shell_identity: tuple[str | None, str | None] | None = None

        def ensure_master(self, target: str) -> None:
            self.master_targets.append(target)

        def interactive_shell(
            self,
            _target: str,
            _remote: str,
            *,
            on_barrier: Any,
            name: str | None = None,
            workspace_id: str | None = None,
        ) -> int:
            self.shell_identity = (name, workspace_id)
            on_barrier(0)
            return 9

    runner = Runner()
    monkeypatch.setattr(cli, "SubprocessSshRunner", lambda: runner)
    cli._ensure_master("host")
    assert cli._connected_runner("host") is runner
    assert runner.master_targets == ["host", "host"]

    monkeypatch.setattr(cli, "_require_live_workspace", lambda _record: local)
    monkeypatch.setattr(cli, "_connected_runner", lambda _target: runner)
    monkeypatch.setattr(cli, "ensure_daemon", lambda _root: None)
    record_for_execution = cli._record_for_execution
    monkeypatch.setattr(cli, "_record_for_execution", lambda _name: record)
    poke_calls: list[str] = []
    poke_or_restart = cli._poke_or_restart_daemon
    monkeypatch.setattr(
        cli,
        "_poke_or_restart_daemon",
        lambda _root, source: poke_calls.append(source),
    )
    assert cli.open_wrapped_shell("dq") == 9
    assert runner.shell_identity == (record.name, record.workspace_id)
    assert poke_calls == ["shell"]

    monkeypatch.setattr(cli, "_poke_or_restart_daemon", poke_or_restart)
    pokes = iter((False, False))
    monkeypatch.setattr(cli, "poke_daemon", lambda *_args: next(pokes))
    monkeypatch.setattr(cli, "ensure_daemon", lambda _root: None)
    cli._poke_or_restart_daemon(local, "test")
    assert "did not accept sync notification" in capsys.readouterr().err

    assert cli._parse_run_items(["--", "true"]) == (None, ["true"])
    for items, message in (
        (["true"], "requires --"),
        (["--"], "requires a command"),
        (["one", "two", "--", "true"], "at most one"),
    ):
        with pytest.raises(ValueError, match=message):
            cli._parse_run_items(items)

    monkeypatch.setattr(cli, "_record_for_execution", record_for_execution)
    monkeypatch.setattr(cli, "find_binding_record", lambda _name: None)
    with pytest.raises(RegistryError, match="no connection named"):
        cli._record_for_execution("missing")
    monkeypatch.setattr(cli, "current_workspace_record", lambda *_args: None)
    with pytest.raises(RegistryError, match="current directory is not bound"):
        cli._record_for_execution(None)


def test_live_workspace_validation_and_formatting_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = _record(tmp_path / "missing")
    with pytest.raises(RegistryError, match="missing local path"):
        cli._require_live_workspace(missing)

    local = tmp_path / "local"
    local.mkdir()
    record = _record(local)
    monkeypatch.setattr(
        cli,
        "workspace_paths",
        lambda _workspace_id: SimpleNamespace(workspace_file=tmp_path / "workspace.json"),
    )
    monkeypatch.setattr(
        cli,
        "read_workspace_spec",
        lambda _path: (_ for _ in ()).throw(ValueError("bad spec")),
    )
    with pytest.raises(RegistryError, match="invalid external workspace state"):
        cli._require_live_workspace(record)

    monkeypatch.setattr(
        cli,
        "read_workspace_spec",
        lambda _path: SimpleNamespace(local_root=str(tmp_path / "other")),
    )
    with pytest.raises(RegistryError, match="disagrees"):
        cli._require_live_workspace(record)
    monkeypatch.setattr(
        cli,
        "read_workspace_spec",
        lambda _path: SimpleNamespace(local_root=str(local)),
    )
    assert cli._require_live_workspace(record) == local

    assert cli._display_width("e\u0301中") == 3
    assert cli._one_line("a\n b", max_len=10) == "a b"
    assert cli._one_line("abcdefgh", max_len=5) == "abcd..."
    table = cli._format_table(["A", "B"], [["中", "x"]])
    assert "A" in table and "中" in table
    vertical = cli._format_records_vertical(["NAME", "ITEM"], [["dq", "a\nb"]])
    assert "a" in vertical and "b" in vertical

    monkeypatch.setattr(cli, "_terminal_width", lambda: 3)
    assert cli._table_fits("abcd") is False
    monkeypatch.setattr(cli, "_terminal_width", lambda: None)
    assert cli._table_fits("abcd") is True

    failed = SimpleNamespace(error="probe failed", resources=None)
    assert cli._list_resource_columns(failed)[0].startswith("error")
    no_gpu = SimpleNamespace(
        error=None,
        resources=SimpleNamespace(
            cpu=SimpleNamespace(load_1m=1.0, count=4),
            memory=SimpleNamespace(used_pct=50.0),
            gpus=(),
        ),
    )
    assert cli._list_resource_columns(no_gpu) == ("1.00/4", "50.0%", "none")
    gpu = SimpleNamespace(
        error=None,
        resources=SimpleNamespace(
            cpu=SimpleNamespace(load_1m=2.0, count=8),
            memory=SimpleNamespace(used_pct=25.0),
            gpus=(SimpleNamespace(index=0, util_pct=90, mem_used_mb=10, mem_total_mb=20),),
        ),
    )
    assert "0:90% 10/20MB" in cli._list_resource_columns(gpu)[2]


def test_print_no_shell_status_variants(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    record = _record(tmp_path)
    cli._print_no_shell_status(
        record,
        _workspace_status(WorkspacePhase.INITIAL_SYNCING, "scanning"),
        initial=True,
    )
    cli._print_no_shell_status(
        record,
        _workspace_status(WorkspacePhase.READY, "idle"),
        initial=True,
    )
    cli._print_no_shell_status(
        record,
        _workspace_status(WorkspacePhase.DEGRADED, "audit"),
        initial=False,
    )
    output = capsys.readouterr().out
    assert "continues in background" in output
    assert "Initial sync completed" in output
    assert "phase: degraded" in output
