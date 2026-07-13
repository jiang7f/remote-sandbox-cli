from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import remote_sandbox.ssh as ssh_module
from remote_sandbox.ssh import SshError, SubprocessSshRunner


def test_cancellable_subprocess_stops_promptly() -> None:
    cancelled = threading.Event()
    timer = threading.Timer(0.1, cancelled.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(SshError, match="cancelled"):
            ssh_module._run_cancellable_bytes(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                input_data=b"",
                timeout=30.0,
                cancel_event=cancelled,
            )
    finally:
        timer.cancel()

    assert time.monotonic() - started < 2.0


def test_control_master_reuses_establishes_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    results = iter(
        (
            subprocess.CompletedProcess(["ssh"], 0, "", ""),
            subprocess.CompletedProcess(["ssh"], 1, "", ""),
            subprocess.CompletedProcess(["ssh"], 0, "", ""),
            subprocess.CompletedProcess(["ssh"], 1, "", ""),
            subprocess.CompletedProcess(["ssh"], 1, "", "denied"),
        )
    )

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert kwargs.get("check") is False
        calls.append(argv)
        return next(results)

    monkeypatch.setattr(ssh_module.subprocess, "run", run)
    runner = SubprocessSshRunner()
    runner.ensure_master("host")
    runner.ensure_master("host")
    with pytest.raises(SshError, match="could not open"):
        runner.ensure_master("host")
    runner.clear_master("host")

    assert all(isinstance(call, list) for call in calls)
    assert any("-O" in call and "check" in call for call in calls)


def test_shared_control_master_owns_keepalive_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("REMOTE_SANDBOX_CONTROL_DIR", str(tmp_path / "control"))

    options = ssh_module.ssh_control_opts()

    control_path = next(value for value in options if value.startswith("ControlPath="))
    assert Path(control_path.removeprefix("ControlPath=")).name.startswith("v2-")
    assert "ControlPersist=30m" in options
    assert "ServerAliveInterval=15" in options
    assert "ServerAliveCountMax=4" in options
    assert "TCPKeepAlive=yes" in options


def test_clear_master_retires_without_terminating_existing_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(ssh_module.subprocess, "run", run)

    SubprocessSshRunner().clear_master("host")

    assert len(calls) == 1
    assert "-O" in calls[0]
    assert "stop" in calls[0]
    assert "exit" not in calls[0]


def test_probe_connection_classifies_success_auth_network_and_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = SubprocessSshRunner()
    outcomes: list[object] = [
        subprocess.CompletedProcess(["ssh"], 0, "", ""),
        subprocess.CompletedProcess(["ssh"], 255, "", "Permission denied"),
        subprocess.CompletedProcess(["ssh"], 255, "", "Connection timed out"),
        OSError("network"),
    ]

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(ssh_module.subprocess, "run", run)

    assert runner.probe_connection("host") == "ok"
    assert runner.probe_connection("host") == "auth"
    assert runner.probe_connection("host") == "network"
    assert runner.probe_connection("host") == "network"


def test_subprocess_runner_text_and_binary_operations_use_safe_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[object]:
        assert kwargs.get("check") is False
        calls.append((argv, kwargs))
        if kwargs.get("text") is True:
            return subprocess.CompletedProcess(argv, 0, "item\n", "")
        return subprocess.CompletedProcess(argv, 0, b"item\n", b"")

    monkeypatch.setattr(ssh_module.subprocess, "run", run)
    runner = SubprocessSshRunner()

    assert runner.exists("host", "/work/value") is True
    assert runner.is_dir("host", "/work") is True
    assert runner.is_symlink("host", "/work/link") is True
    assert runner.listdir("host", "/work") == ["item"]
    runner.mkdir_p("host", "/work/new")
    assert runner.read_text("host", "/work/value") == "item\n"
    runner.write_text_atomic("host", "/work/value", "text")
    assert runner.read_bytes("host", "/work/value") == b"item\n"
    assert runner.read_head("host", "/work/value", 2) == b"item\n"
    assert runner.read_tail("host", "/work/value", 2) == b"item\n"
    runner.write_bytes_atomic("host", "/work/value", b"bytes")
    runner.delete_path("host", "/work/value")
    assert runner.run_python_file("host", "/work/agent.py", ("self-check",)) == "item\n"
    assert runner.run_python_file_bytes("host", "/work/agent.py", b"input").stdout == b"item\n"
    workspace = runner.run_workspace_python_bytes(
        "host",
        "/work",
        "print('ok')",
        b"input",
        ("arg",),
    )
    assert workspace.returncode == 0
    runner.delete_workspace_path("host", "/work", "nested/value.txt")
    result = runner.run_command("host", "/work", ("false",))
    assert result.returncode == 0
    assert result.stdout == "item\n"

    assert calls
    assert all(isinstance(argv, list) for argv, _kwargs in calls)


def test_subprocess_runner_reports_failures_and_validates_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[object]:
        if kwargs.get("text") is True:
            return subprocess.CompletedProcess(argv, 2, "", "remote failed")
        return subprocess.CompletedProcess(argv, 2, b"", b"remote failed")

    monkeypatch.setattr(ssh_module.subprocess, "run", failed)
    runner = SubprocessSshRunner()

    with pytest.raises(FileNotFoundError):
        runner.read_text("host", "/missing")
    with pytest.raises(FileNotFoundError):
        runner.read_bytes("host", "/missing")
    with pytest.raises(SshError, match="remote mkdir failed"):
        runner.mkdir_p("host", "/work")
    with pytest.raises(ValueError, match="positive"):
        runner.read_head("host", "/work/value", 0)
    with pytest.raises(ValueError, match="workspace Python code"):
        runner.run_workspace_python_bytes("host", "/work", "", b"")
    with pytest.raises(ValueError, match="workspace Python argument"):
        runner.run_workspace_python_bytes("host", "/work", "pass", b"", ("bad\narg",))
    with pytest.raises(SshError, match="empty"):
        runner.run_command("host", "/work", ())


def test_run_command_does_not_use_the_internal_operation_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    argvs: list[list[str]] = []
    calls: list[dict[str, object]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        argvs.append(argv)
        calls.append(kwargs)
        return subprocess.CompletedProcess(argv, 0, "done\n", "")

    monkeypatch.setattr(ssh_module.subprocess, "run", run)

    result = SubprocessSshRunner().run_command("host", "/work", ("sleep", "60"))

    assert result.returncode == 0
    assert result.transport_complete is False
    assert calls[-1]["timeout"] is None
    assert "ServerAliveInterval=15" in argvs[-1]
    assert "ServerAliveCountMax=4" in argvs[-1]


def test_run_command_extracts_remote_completion_without_polluting_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ssh_module.secrets, "token_hex", lambda _size: "abc123")

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            255,
            "done\n",
            "warning\n\x1eRSB-COMMAND-COMPLETE-abc123:7\x1f",
        )

    monkeypatch.setattr(ssh_module.subprocess, "run", run)

    result = SubprocessSshRunner().run_command("host", "/work", ("command",))

    assert result.returncode == 7
    assert result.stdout == "done\n"
    assert result.stderr == "warning\n"
    assert result.transport_complete is True


def test_run_command_reports_incomplete_transport_when_marker_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ssh_module.subprocess,
        "run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 255, "", ""),
    )

    result = SubprocessSshRunner().run_command("host", "/work", ("command",))

    assert result.returncode == 255
    assert result.transport_complete is False


def test_interactive_shell_requires_a_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ssh_module.os, "isatty", lambda _fd: False)

    with pytest.raises(SshError, match="requires a TTY"):
        SubprocessSshRunner().interactive_shell("host", "/work")
