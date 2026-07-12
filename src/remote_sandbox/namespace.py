from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ToolNamespace:
    distribution: str
    command: str
    home_dirname: str
    runtime_prefix: str


TOOL_NAMESPACE = ToolNamespace(
    distribution="remote-sandbox",
    command="rsb",
    home_dirname=".remote-sandbox",
    runtime_prefix="remote-sandbox",
)


def tool_home(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("REMOTE_SANDBOX_HOME")
    if override:
        return Path(override).expanduser()
    return Path(values.get("HOME", str(Path.home()))) / TOOL_NAMESPACE.home_dirname


def runtime_dir(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("REMOTE_SANDBOX_RUNTIME_DIR")
    if override:
        return Path(override).expanduser()
    return Path("/tmp") / f"{TOOL_NAMESPACE.runtime_prefix}-{os.getuid()}"


def ssh_control_dir(env: Mapping[str, str] | None = None) -> Path:
    desired = runtime_dir(env) / "cm"
    return _bounded_socket_dir(desired, label="cm", socket_name_bytes=40)


def supervisor_runtime_dir(runtime_root: Path) -> Path:
    desired = runtime_root / "supervisors"
    return _bounded_socket_dir(desired, label="supervisors", socket_name_bytes=41)


def _bounded_socket_dir(desired: Path, *, label: str, socket_name_bytes: int) -> Path:
    socket_path = desired / ("0" * socket_name_bytes)
    if len(os.fsencode(socket_path)) < 100:
        return desired
    digest = hashlib.sha256(os.fsencode(desired)).hexdigest()[:16]
    return Path("/tmp") / f"remote-sandbox-{label}-{os.getuid()}" / digest


def program_name() -> str:
    return TOOL_NAMESPACE.command
