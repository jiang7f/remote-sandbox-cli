from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import remote_sandbox.cli as cli
from remote_sandbox.daemon import DaemonError
from remote_sandbox.registry import BindingRecord
from remote_sandbox.shell import ConnectRequestEvent, ConnectResponse, EnterShellResult


def _connection(local: Path) -> BindingRecord:
    return BindingRecord(
        name="dq",
        workspace_id="00000000-0000-4000-8000-000000000217",
        target="host",
        remote_path="/work/dq",
        local_path=str(local),
        updated_at="2026-07-11T00:00:00+00:00",
    )


def _install_enter_fakes(
    monkeypatch: pytest.MonkeyPatch,
    local: Path,
    *,
    remote_entries: list[str],
    status: object,
    captured: list[ConnectResponse],
) -> None:
    connection = _connection(local)

    class FakeRunner:
        def listdir(self, target: str, remote: str) -> list[str]:
            assert (target, remote) == ("host", "/work/dq")
            return remote_entries

    monkeypatch.setattr(cli, "SubprocessSshRunner", FakeRunner)
    monkeypatch.setattr(
        cli,
        "bind_workspace",
        lambda **_kwargs: SimpleNamespace(
            workspace=SimpleNamespace(workspace_id=connection.workspace_id),
            connection=connection,
        ),
    )
    monkeypatch.setattr(cli, "ensure_daemon", lambda *_args, **_kwargs: status)
    monkeypatch.setattr(cli, "wait_for_daemon_control", lambda *_args: status)
    monkeypatch.setattr(cli, "_print_connection", lambda _record: None)

    def fake_enter_shell_loop(
        _target: str,
        _cwd: str,
        *,
        nonce: str,
        on_connect_request: Any,
    ) -> EnterShellResult:
        assert nonce
        captured.append(
            on_connect_request(ConnectRequestEvent(remote="/work/dq", local=None, name="dq"))
        )
        return EnterShellResult(7, "/work/dq", str(local), "dq")

    monkeypatch.setattr(cli, "enter_shell_loop", fake_enter_shell_loop)


def test_enter_and_bind_treats_missing_local_and_empty_remote_as_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local = tmp_path / "not-created"
    captured: list[ConnectResponse] = []
    running = SimpleNamespace(
        running=True,
        pid=1234,
        phase=SimpleNamespace(value="initial-syncing"),
        last_error=None,
    )
    _install_enter_fakes(
        monkeypatch,
        local,
        remote_entries=[],
        status=running,
        captured=captured,
    )

    assert cli.enter_and_bind(target="host", remote="~", local=local, open_shell=False) == 7
    assert captured[0].direction == "empty"
    assert captured[0].ready_probe is None


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (
            SimpleNamespace(
                running=False,
                pid=None,
                phase=SimpleNamespace(value="starting"),
                last_error=None,
            ),
            "did not publish its process state",
        ),
        (
            SimpleNamespace(
                running=True,
                pid=1234,
                phase=SimpleNamespace(value="failed"),
                last_error="initial sync failed",
            ),
            "initial sync failed",
        ),
        (
            SimpleNamespace(
                running=True,
                pid=1234,
                phase=SimpleNamespace(value="stopped"),
                last_error=None,
            ),
            "workspace supervisor failed to start",
        ),
    ],
)
def test_enter_and_bind_rejects_invalid_control_startup_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: object,
    message: str,
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    captured: list[ConnectResponse] = []
    _install_enter_fakes(
        monkeypatch,
        local,
        remote_entries=["source.py"],
        status=status,
        captured=captured,
    )

    with pytest.raises(DaemonError, match=message):
        cli.enter_and_bind(target="host", remote="~", local=local, open_shell=True)

    assert captured == []


def test_enter_and_bind_uses_symmetric_immediate_shell_entry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local = tmp_path / "local"
    local.mkdir()
    (local / "source.py").write_text("local", encoding="utf-8")
    captured: list[ConnectResponse] = []
    running = SimpleNamespace(
        running=True,
        pid=1234,
        phase=SimpleNamespace(value="initial-syncing"),
        last_error=None,
    )
    _install_enter_fakes(
        monkeypatch,
        local,
        remote_entries=[],
        status=running,
        captured=captured,
    )

    assert cli.enter_and_bind(target="host", remote="~", local=local, open_shell=True) == 7
    assert captured[0].ready_probe is None
    assert captured[0].enter_immediately is True
