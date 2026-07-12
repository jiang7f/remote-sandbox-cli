import os
from pathlib import Path

from remote_sandbox.namespace import TOOL_NAMESPACE, runtime_dir, ssh_control_dir, tool_home


def test_remote_sandbox_namespace_honors_formal_overrides(tmp_path: Path) -> None:
    runtime = Path("/tmp/rsb-test-runtime")
    env = {
        "HOME": str(tmp_path),
        "REMOTE_SANDBOX_HOME": str(tmp_path / "state"),
        "REMOTE_SANDBOX_RUNTIME_DIR": str(runtime),
    }

    assert TOOL_NAMESPACE.distribution == "remote-sandbox"
    assert TOOL_NAMESPACE.command == "rsb"
    assert tool_home(env) == tmp_path / "state"
    assert runtime_dir(env) == runtime
    assert ssh_control_dir(env) == runtime / "cm"
    assert TOOL_NAMESPACE.home_dirname == ".remote-sandbox"
    assert TOOL_NAMESPACE.runtime_prefix == "remote-sandbox"


def test_remote_sandbox_namespace_uses_formal_default_paths(tmp_path: Path) -> None:
    env = {"HOME": str(tmp_path)}
    expected_runtime_dir = Path("/tmp") / f"remote-sandbox-{os.getuid()}"

    assert tool_home(env) == tmp_path / ".remote-sandbox"
    assert runtime_dir(env) == expected_runtime_dir
    assert ssh_control_dir(env) == expected_runtime_dir / "cm"


def test_formal_environment_variables_redirect_paths(tmp_path: Path) -> None:
    runtime = Path("/tmp/rsb-test-runtime-redirected")
    env = {
        "HOME": str(tmp_path / "home"),
        "REMOTE_SANDBOX_HOME": str(tmp_path / "state"),
        "REMOTE_SANDBOX_RUNTIME_DIR": str(runtime),
    }

    assert tool_home(env) == tmp_path / "state"
    assert runtime_dir(env) == runtime
    assert ssh_control_dir(env) == runtime / "cm"


def test_long_runtime_override_uses_an_isolated_short_control_path(tmp_path: Path) -> None:
    runtime = tmp_path / ("deep-runtime-" * 8)
    env = {"REMOTE_SANDBOX_RUNTIME_DIR": str(runtime)}

    control = ssh_control_dir(env)

    assert control.parent == Path("/tmp") / f"remote-sandbox-cm-{os.getuid()}"
    assert len(os.fsencode(control / ("0" * 40))) < 100
    assert control == ssh_control_dir(env)
    assert control != runtime / "cm"
