from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from helpers.sync_harness import CliHarness

import remote_sandbox.cli as cli
import remote_sandbox.daemon as daemon_module
from remote_sandbox.daemon import StopResult
from remote_sandbox.registry import BindingRecord, upsert_binding_record
from remote_sandbox.ssh import CommandResult
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.workspace import workspace_paths


def test_main_routes_service_commands_and_double_dash(
    cli_fixture: CliHarness,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "default_cli_services", lambda: cli_fixture.services)
    cli_fixture.remote_command_result(returncode=7, stdout="ran\n", stderr="warn\n")

    assert cli.main(["status", "dq"]) == 0
    assert cli.main(["run", "dq", "--", "python", "-c", "print('--flag')"]) == 7
    assert cli.main(["conflicts", "dq"]) == 0
    assert cli.main(["forget", "missing"]) == 0

    output = capsys.readouterr()
    assert "dq" in output.out
    assert "ran" in output.out
    assert "warn" in output.err
    assert cli_fixture.services.run_remote is not None


def test_main_routes_non_service_commands_and_tty_guards(
    cli_fixture: CliHarness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cli, "default_cli_services", lambda: cli_fixture.services)
    monkeypatch.setattr(cli, "list_servers", lambda: 11)
    monkeypatch.setattr(cli, "start_binding_daemon", lambda name: 12 if name == "dq" else 1)
    monkeypatch.setattr(cli, "stop_binding_daemon", lambda name: 13 if name == "dq" else 1)
    monkeypatch.setattr(cli, "open_wrapped_shell", lambda name: 14 if name == "dq" else 1)
    monkeypatch.setattr(cli, "enter_and_bind", lambda **kwargs: 15)
    monkeypatch.setattr(cli, "_open_wrapped_shell_for_record", lambda record: 16)
    monkeypatch.setattr(cli, "_has_tty", lambda: True)
    cli_fixture.services.connect_workspace = lambda *args: cli.ConnectedWorkspace(
        cli_fixture.record,
        False,
        0,
    )
    monkeypatch.setattr(
        cli,
        "set_placeholder_limit",
        lambda _value: SimpleNamespace(placeholder_limit=10_000_000),
    )
    monkeypatch.setattr(cli, "settings_path", lambda: tmp_path / "config.toml")

    assert cli.main(["list"]) == 11
    assert cli.main(["start", "dq"]) == 12
    assert cli.main(["stop", "dq"]) == 13
    assert cli.main(["shell", "dq"]) == 14
    assert cli.main(["set", "placeholder-limit", "10MB"]) == 0
    assert cli.main(["enter", "host"]) == 15
    assert cli.main(["connect", "host", "--remote", "/work/dq"]) == 16
    assert cli.main(["reconnect", "dq"]) == 16

    monkeypatch.setattr(cli, "_has_tty", lambda: False)
    assert cli.main(["shell", "dq"]) == 2
    assert cli.main(["enter", "host"]) == 2
    assert cli.main(["connect", "host", "--remote", "/work/dq"]) == 2
    assert cli.main(["reconnect", "dq"]) == 2


def test_main_formats_concise_and_debug_errors(
    cli_fixture: CliHarness,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli_fixture.services.list_records = lambda _path: (_ for _ in ()).throw(
        RuntimeError("first line\nsecond line")
    )
    monkeypatch.setattr(cli, "default_cli_services", lambda: cli_fixture.services)

    assert cli.main(["status"]) == 2
    concise = capsys.readouterr().err
    assert "first line second line" in concise
    assert "Traceback" not in concise

    assert cli.main(["--debug", "status"]) == 2
    assert "Traceback" in capsys.readouterr().err


class _Runner:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult:
        return CommandResult(9, f"{target}:{cwd}:{argv[0]}", "stderr")

    def delete_path(self, target: str, path: str) -> None:
        self.deleted.append((target, path))


class _Remote:
    def __init__(self) -> None:
        self.stopped = False
        self.forgotten = False
        self.closed = False

    def stop_watcher(self) -> None:
        self.stopped = True

    def forget(self) -> None:
        self.forgotten = True

    def close(self) -> None:
        self.closed = True


def test_default_services_adapters_use_isolated_runtime_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_home = tmp_path / "state"
    runtime = tmp_path / "runtime"
    local = tmp_path / "local"
    local.mkdir()
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(state_home))
    monkeypatch.setenv("REMOTE_SANDBOX_RUNTIME_DIR", str(runtime))
    record = BindingRecord(
        "dq",
        "00000000-0000-4000-8000-000000000180",
        "host",
        "/work/dq",
        str(local),
        "2026-01-01T00:00:00+00:00",
    )
    upsert_binding_record(None, record)
    runner = _Runner()
    remote = _Remote()
    ready = WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle"))
    monkeypatch.setattr(cli, "_connected_runner", lambda _target: runner)
    monkeypatch.setattr(cli, "_require_live_workspace", lambda _record: local)
    monkeypatch.setattr(cli, "ensure_daemon", lambda _root: ready)
    monkeypatch.setattr(cli, "poke_daemon", lambda root, reason: root == local and reason == "cli")
    monkeypatch.setattr(daemon_module, "daemon_workspace_status", lambda _workspace_id: ready)
    monkeypatch.setattr(cli, "stop_daemon_result", lambda _root: StopResult.STOPPED)
    monkeypatch.setattr(
        cli,
        "_production_remote_components",
        lambda _record: (runner, remote, SimpleNamespace()),
    )

    services = cli.default_cli_services()

    assert services.workspace_status(record) == ready
    assert services.ensure_supervisor(record) == ready
    assert services.request_sync(record) is True
    assert services.run_remote(record, ("false",)).returncode == 9

    paths = workspace_paths(record.workspace_id)
    with WorkspaceStore.open(paths.state_db):
        pass
    monkeypatch.setattr(
        cli,
        "bind_workspace",
        lambda **kwargs: SimpleNamespace(
            connection=record,
            workspace=SimpleNamespace(workspace_id=record.workspace_id),
            created=True,
        ),
    )
    connected = services.connect_workspace("host", "/work/dq", local, "dq")
    assert connected.record == record and connected.created is True

    class Client:
        def wait_for_initial_sync_started(self, generation: int, timeout: float) -> object:
            assert generation == 0 and timeout == 5.0
            return SimpleNamespace(workspace_status=ready)

        def mutate(self, kind: str, payload: dict[str, object]) -> dict[str, object]:
            if kind == "fetch":
                return {"count": 1, "cancelled": False}
            return {"kind": kind, **payload}

    monkeypatch.setattr(cli, "_supervisor_client", lambda _record: Client())
    assert services.wait_initial_sync(record, 0) == ready
    assert services.list_conflicts(record) == []
    services.resolve_conflict(record, "value.txt", True)
    monkeypatch.setattr(cli, "fetch_all_prompt", lambda local_root, store: None)
    assert services.fetch_placeholders(record, None, True, lambda _prompt: True) == (0, False)
    assert services.fetch_placeholders(record, "value.txt", False, lambda _prompt: True) == (
        1,
        False,
    )
    monkeypatch.setattr(cli, "peek_placeholder", lambda **kwargs: b"preview")
    remote.closed = False
    assert services.peek_placeholder(record, "value.txt", 5, False) == b"preview"
    assert remote.closed is True

    services.stop_local_supervisor(record)
    services.stop_remote_watcher(record)
    assert remote.stopped is True and remote.closed is True
    remote.closed = False
    services.delete_remote_workspace(record)
    assert remote.forgotten is True and remote.closed is True

    metadata = workspace_paths(record.workspace_id).root
    metadata.mkdir(parents=True, exist_ok=True)
    services.delete_local_workspace(record)
    assert not metadata.exists()
    services.prune_remote_agent(record)
    assert runner.deleted and "agent.pyz" in runner.deleted[0][1]
    services.delete_registry_record(record)
    assert services.find_record("dq", services.registry) is None


def test_default_stop_adapter_reports_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(tmp_path / "state"))
    services = cli.default_cli_services()
    record = BindingRecord(
        "dq",
        "00000000-0000-4000-8000-000000000181",
        "host",
        "/work/dq",
        str(tmp_path),
        "2026-01-01T00:00:00+00:00",
    )
    monkeypatch.setattr(cli, "stop_daemon_result", lambda _root: StopResult.TIMEOUT)

    with pytest.raises(cli.DaemonError, match="did not stop"):
        services.stop_local_supervisor(record)
