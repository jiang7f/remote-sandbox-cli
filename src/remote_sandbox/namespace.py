from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ToolNamespace:
    distribution: str
    command: str
    long_command: str
    home_dirname: str
    runtime_prefix: str


DEV_NAMESPACE = ToolNamespace(
    distribution="codex-remote-sandbox",
    command="codex-rsb",
    long_command="codex-remote-sandbox",
    home_dirname=".codex-remote-sandbox",
    runtime_prefix="codex-remote-sandbox",
)


def tool_home(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("CODEX_REMOTE_SANDBOX_HOME")
    if override:
        return Path(override).expanduser()
    return Path(values.get("HOME", str(Path.home()))) / DEV_NAMESPACE.home_dirname


def runtime_dir(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("CODEX_REMOTE_SANDBOX_RUNTIME_DIR")
    if override:
        return Path(override).expanduser()
    return Path("/tmp") / f"{DEV_NAMESPACE.runtime_prefix}-{os.getuid()}"


def ssh_control_dir(env: Mapping[str, str] | None = None) -> Path:
    return runtime_dir(env) / "cm"


def program_name() -> str:
    return DEV_NAMESPACE.command
