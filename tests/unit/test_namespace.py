import os
from pathlib import Path

from remote_sandbox.namespace import DEV_NAMESPACE, runtime_dir, ssh_control_dir, tool_home


def test_development_namespace_is_fully_isolated(tmp_path: Path) -> None:
    env = {
        "HOME": str(tmp_path),
        "CODEX_REMOTE_SANDBOX_HOME": str(tmp_path / "state"),
        "CODEX_REMOTE_SANDBOX_RUNTIME_DIR": str(tmp_path / "runtime"),
    }

    assert DEV_NAMESPACE.distribution == "codex-remote-sandbox"
    assert DEV_NAMESPACE.command == "codex-rsb"
    assert tool_home(env) == tmp_path / "state"
    assert runtime_dir(env) == tmp_path / "runtime"
    assert ssh_control_dir(env) == tmp_path / "runtime" / "cm"
    assert ".remote-sandbox" not in str(tool_home(env))


def test_development_namespace_uses_isolated_default_paths(tmp_path: Path) -> None:
    env = {"HOME": str(tmp_path)}
    expected_runtime_dir = Path("/tmp") / f"codex-remote-sandbox-{os.getuid()}"

    assert tool_home(env) == tmp_path / ".codex-remote-sandbox"
    assert runtime_dir(env) == expected_runtime_dir
    assert ssh_control_dir(env) == expected_runtime_dir / "cm"


def test_legacy_environment_variables_cannot_redirect_development_paths(
    tmp_path: Path,
) -> None:
    env = {
        "HOME": str(tmp_path / "home"),
        "REMOTE_SANDBOX_HOME": str(tmp_path / "legacy-state"),
        "REMOTE_SANDBOX_RUNTIME_DIR": str(tmp_path / "legacy-runtime"),
    }
    expected_runtime_dir = Path("/tmp") / f"codex-remote-sandbox-{os.getuid()}"

    assert tool_home(env) == tmp_path / "home" / ".codex-remote-sandbox"
    assert runtime_dir(env) == expected_runtime_dir
    assert ssh_control_dir(env) == expected_runtime_dir / "cm"
