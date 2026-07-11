from __future__ import annotations

import base64
import os
import shutil
import subprocess
from pathlib import Path

import conftest as e2e
import pytest


def test_isolated_ssh_wrapper_forces_fixture_config_and_environment(tmp_path: Path) -> None:
    ssh = shutil.which("ssh")
    assert ssh is not None
    config = tmp_path / "config"
    config.write_text("Host fixture\n  HostName 127.0.0.1\n", encoding="utf-8")
    wrapper = tmp_path / "bin" / "ssh"

    e2e._write_isolated_ssh_wrapper(wrapper, Path(ssh), config)
    env = e2e._isolated_ssh_environment(
        {"PATH": "/usr/bin", "HOME": "/should/not/control/openssh"},
        wrapper,
    )

    script = wrapper.read_text(encoding="utf-8")
    syntax = subprocess.run(
        ["bash", "-n", str(wrapper)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert syntax.returncode == 0, syntax.stderr
    assert f"-F {config}" in script
    assert "IdentityAgent=none" in script
    assert env["PATH"].split(os.pathsep)[0] == str(wrapper.parent)
    assert env["RSYNC_RSH"] == str(wrapper)


def test_fixture_cleanup_aggregates_failures_and_removes_all_local_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = "00000000-0000-4000-8000-000000000017"
    state_home = tmp_path / "state"
    metadata = state_home / "workspaces" / workspace_id
    metadata.mkdir(parents=True)
    runtime = tmp_path / "runtime"
    socket = runtime / "supervisors" / f"{workspace_id}.sock"
    socket.parent.mkdir(parents=True)
    socket.write_text("socket", encoding="utf-8")
    key = tmp_path / "client-key"
    key.write_text("private", encoding="utf-8")
    key.with_suffix(".pub").write_text("public", encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()

    daemon = subprocess.Popen(["sleep", "30"])
    (metadata / "daemon.pid").write_text(f"{daemon.pid}\n", encoding="utf-8")

    class BrokenShell:
        def close(self) -> None:
            raise RuntimeError("shell close failed")

    fixture = e2e.SshFixture(
        container_id="container-id",
        image="fixture-image",
        host="fixture-key",
        password_host="fixture-password",
        port=2222,
        key_file=key,
        state_home=state_home,
        runtime_dir=runtime,
        home=home,
        env={},
        cli_executable=tmp_path / "codex-rsb",
        _tmp_path=tmp_path,
        _shells=[BrokenShell()],
    )
    cli_calls: list[tuple[str, ...]] = []
    docker_calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        e2e.SshFixture,
        "_binding_records",
        lambda _self: [{"name": "broken", "workspace_id": workspace_id}],
    )

    def failing_cli(_self: e2e.SshFixture, *argv: str) -> subprocess.CompletedProcess[str]:
        cli_calls.append(argv)
        raise RuntimeError("forget failed")

    def fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        docker_calls.append(tuple(argv))
        returncode = 1 if "inspect" in argv else 0
        return subprocess.CompletedProcess(argv, returncode, "", "")

    monkeypatch.setattr(e2e.SshFixture, "cli", failing_cli)
    monkeypatch.setattr(e2e.subprocess, "run", fake_run)

    try:
        with pytest.raises(AssertionError, match="(?s)shell close failed.*forget failed"):
            fixture.close()
    finally:
        if daemon.poll() is None:
            daemon.terminate()
            daemon.wait(timeout=5.0)

    assert ("forget", "broken") in cli_calls
    assert daemon.poll() is not None
    assert not key.exists()
    assert not key.with_suffix(".pub").exists()
    assert not state_home.exists()
    assert not runtime.exists()
    assert ("docker", "rm", "-f", "container-id") in docker_calls
    assert ("docker", "image", "rm", "-f", "fixture-image") in docker_calls


def test_prompt_wait_ignores_matching_text_from_prior_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shell = object.__new__(e2e.PtyShell)
    shell._output = bytearray(b"[codex:fixture sync 99%]")
    monkeypatch.setattr(e2e.PtyShell, "_read_available", lambda _self, _deadline: None)

    with pytest.raises(AssertionError, match="PTY did not emit"):
        shell.wait_for_prompt_text("sync ", timeout=0.001)


def test_terminal_state_is_parsed_from_a_new_readline_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shell = object.__new__(e2e.PtyShell)
    shell._output = bytearray(b"cached input must not be returned")
    payload = base64.b64encode(b"python tra")

    def emit_probe(_self: e2e.PtyShell, _data: bytes) -> None:
        shell._output.extend(b"\n__E2E_TERMINAL__" + payload + b":6:7301\n")

    monkeypatch.setattr(e2e.PtyShell, "_send", emit_probe)

    state = shell.terminal_state()

    assert state.visible_input == "python tra"
    assert state.cursor_offset == 6
    assert state.remote_shell_pid == 7301


def test_foreground_probe_waits_for_child_ready_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shell = object.__new__(e2e.PtyShell)
    shell._output = bytearray()
    events: list[tuple[str, bytes]] = []

    monkeypatch.setattr(
        e2e.PtyShell,
        "_send",
        lambda _self, data: events.append(("send", data)),
    )
    monkeypatch.setattr(
        e2e.PtyShell,
        "_wait_for",
        lambda _self, expected, **_kwargs: events.append(("wait", expected)),
    )

    shell.run_foreground_probe(seconds=0.01)

    assert events[0][0] == "send"
    assert events[1] == ("wait", b"__E2E_FOREGROUND_READY__")
