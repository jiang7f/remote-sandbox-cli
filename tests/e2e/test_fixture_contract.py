from __future__ import annotations

import base64
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

_E2E_FIXTURE_MODULE = "_codex_remote_sandbox_e2e_fixture"


def _load_e2e_fixture_module() -> ModuleType:
    path = Path(__file__).with_name("conftest.py").resolve()
    existing = sys.modules.get(_E2E_FIXTURE_MODULE)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(_E2E_FIXTURE_MODULE, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load E2E fixture module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_E2E_FIXTURE_MODULE] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(_E2E_FIXTURE_MODULE, None)
        raise
    return module


e2e = _load_e2e_fixture_module()


def test_fixture_contract_loads_sibling_conftest_under_stable_name() -> None:
    expected = Path(__file__).with_name("conftest.py").resolve()

    assert Path(e2e.__file__).resolve() == expected
    assert e2e.__name__ == "_codex_remote_sandbox_e2e_fixture"


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


def test_start_fixture_cleans_generated_artifacts_when_docker_build_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = tmp_path / "client-key"
    home = tmp_path / "home"
    state = tmp_path / "codex-state"
    runtime = tmp_path / "codex-runtime"
    for path in (home, state, runtime):
        path.mkdir()
    docker_calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        e2e.shutil,
        "which",
        lambda name: f"/usr/bin/{name}" if name in {"docker", "ssh"} else None,
    )

    def fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[0] == "ssh-keygen":
            key.write_text("private", encoding="utf-8")
            key.with_suffix(".pub").write_text("public", encoding="utf-8")
            return subprocess.CompletedProcess(argv, 0, "", "")
        docker_calls.append(tuple(argv))
        if "build" in argv:
            raise subprocess.CalledProcessError(1, argv, stderr="build failed")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(e2e.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError, match="docker.*build"):
        e2e.start_ssh_fixture(tmp_path)

    assert not key.exists()
    assert not key.with_suffix(".pub").exists()
    assert not home.exists()
    assert not state.exists()
    assert not runtime.exists()
    assert any(call[1:4] == ("image", "rm", "-f") for call in docker_calls)


def test_start_fixture_preserves_startup_and_all_cleanup_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = tmp_path / "client-key"
    cleanup_calls: list[tuple[str, ...]] = []
    original_remove = e2e._remove_path

    monkeypatch.setattr(
        e2e.shutil,
        "which",
        lambda name: f"/usr/bin/{name}" if name in {"docker", "ssh"} else None,
    )

    def fake_remove(path: Path) -> None:
        if path == key:
            raise RuntimeError("private key cleanup failed")
        original_remove(path)

    def fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[0] == "ssh-keygen":
            key.write_text("private", encoding="utf-8")
            key.with_suffix(".pub").write_text("public", encoding="utf-8")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "build" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "run" in argv:
            return subprocess.CompletedProcess(argv, 0, "container-id\n", "")
        if "port" in argv:
            raise RuntimeError("port lookup failed")
        cleanup_calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 1, "", "docker cleanup failed")

    monkeypatch.setattr(e2e, "_remove_path", fake_remove)
    monkeypatch.setattr(e2e.subprocess, "run", fake_run)

    with pytest.raises(BaseExceptionGroup) as raised:
        e2e.start_ssh_fixture(tmp_path)

    failures = tuple(str(failure) for failure in raised.value.exceptions)
    assert any("port lookup failed" in failure for failure in failures)
    assert any("private key cleanup failed" in failure for failure in failures)
    assert sum("docker cleanup failed" in failure for failure in failures) == 2
    assert ("/usr/bin/docker", "rm", "-f", "container-id") in cleanup_calls
    assert any(call[1:4] == ("image", "rm", "-f") for call in cleanup_calls)


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
