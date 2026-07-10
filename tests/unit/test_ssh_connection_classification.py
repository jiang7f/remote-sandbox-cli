from __future__ import annotations

import io
import subprocess
from pathlib import Path

import pytest

import remote_sandbox.ssh as ssh_module
from remote_sandbox.ssh import SubprocessSshRunner, _classify_ssh_failure, ssh_control_opts


def test_permission_denied_is_classified_as_authentication() -> None:
    assert _classify_ssh_failure("Permission denied (publickey,password).") == "auth"


def test_timeout_is_classified_as_network() -> None:
    assert _classify_ssh_failure("Connection timed out") == "network"


def test_structured_python_call_sends_payload_only_on_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        observed["args"] = args
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(args, 0, b'{"ok":true,"payload":{}}\n', b"")

    monkeypatch.setattr(ssh_module.subprocess, "run", fake_run)
    payload = b'{"command":"register","payload":{"root":"/tmp/$(touch nope)"}}\n'

    result = SubprocessSshRunner().run_python_file_bytes(
        "example-host",
        "~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
        payload,
    )

    assert result.stdout == b'{"ok":true,"payload":{}}\n'
    assert observed["kwargs"] == {
        "check": False,
        "input": payload,
        "capture_output": True,
        "timeout": 30.0,
    }
    assert b"$(touch nope)" not in " ".join(observed["args"]).encode()


class _CapturingInput(io.BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.closed_with = b""

    def close(self) -> None:
        self.closed_with = self.getvalue()
        super().close()


class _StreamingProcess:
    def __init__(self) -> None:
        self.stdin = _CapturingInput()
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()


def test_streaming_python_call_writes_request_and_leaves_stdout_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _StreamingProcess()
    observed: dict[str, object] = {}

    def fake_popen(args: list[str], **kwargs: object) -> _StreamingProcess:
        observed["args"] = args
        observed["kwargs"] = kwargs
        return process

    monkeypatch.setattr(ssh_module.subprocess, "Popen", fake_popen)
    payload = b'{"command":"events","payload":{"after_sequence":7}}\n'

    returned = SubprocessSshRunner().stream_python_file(
        "example-host",
        "~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
        payload,
    )

    assert returned is process
    assert process.stdin.closed_with == payload
    assert not process.stdout.closed
    assert observed["kwargs"] == {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }


def test_control_path_uses_isolated_codex_runtime_namespace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "codex-remote-sandbox-123"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_RUNTIME_DIR", str(runtime))
    monkeypatch.setenv("REMOTE_SANDBOX_CONTROL_DIR", str(tmp_path / "installed-rsb"))

    options = ssh_control_opts()

    control_path = next(value for value in options if value.startswith("ControlPath="))
    assert control_path == f"ControlPath={runtime}/cm/%C"
    assert (runtime / "cm").stat().st_mode & 0o777 == 0o700
