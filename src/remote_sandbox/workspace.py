from __future__ import annotations

import json
import os
import tempfile
import tomllib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from remote_sandbox.namespace import tool_home

SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class WorkspaceSpec:
    """Durable identity and endpoints for one synchronized workspace."""

    schema_version: int
    workspace_id: str
    name: str
    target: str
    local_root: str
    remote_root: str
    created_at: str


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    """External durable metadata paths owned by one workspace."""

    root: Path
    workspace_file: Path
    state_db: Path
    daemon_log: Path


def new_workspace_spec(
    *,
    name: str,
    target: str,
    local_root: Path,
    remote_root: str,
) -> WorkspaceSpec:
    """Create a workspace specification with canonical local identity."""
    return WorkspaceSpec(
        schema_version=SCHEMA_VERSION,
        workspace_id=str(uuid.uuid4()),
        name=name,
        target=target,
        local_root=str(local_root.expanduser().resolve(strict=False)),
        remote_root=remote_root,
        created_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )


def validate_workspace_id(value: str) -> str:
    """Return a canonical UUID or reject unsafe and noncanonical forms."""
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("invalid workspace id") from exc
    canonical = str(parsed)
    if value != canonical:
        raise ValueError("workspace id must use canonical UUID form")
    return canonical


def workspace_paths(workspace_id: str) -> WorkspacePaths:
    """Return metadata paths outside both synchronized workspace trees."""
    safe_id = validate_workspace_id(workspace_id)
    root = tool_home() / "workspaces" / safe_id
    return WorkspacePaths(
        root=root,
        workspace_file=root / "workspace.toml",
        state_db=root / "state.sqlite3",
        daemon_log=root / "daemon.log",
    )


def write_workspace_spec(path: Path, spec: WorkspaceSpec) -> None:
    """Persist a workspace specification atomically with user-only access."""
    _validate_spec(spec)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    content = _spec_to_toml(spec)
    fd, tmp_name = tempfile.mkstemp(
        prefix="workspace.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp_path.chmod(0o600)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def read_workspace_spec(path: Path) -> WorkspaceSpec:
    """Read and validate a persisted workspace specification."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    spec = WorkspaceSpec(
        schema_version=_expect_int(data, "schema_version"),
        workspace_id=_expect_str(data, "workspace_id"),
        name=_expect_str(data, "name"),
        target=_expect_str(data, "target"),
        local_root=_expect_str(data, "local_root"),
        remote_root=_expect_str(data, "remote_root"),
        created_at=_expect_str(data, "created_at"),
    )
    _validate_spec(spec)
    return spec


def _validate_spec(spec: WorkspaceSpec) -> None:
    if spec.schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported workspace schema version: {spec.schema_version}")
    validate_workspace_id(spec.workspace_id)


def _spec_to_toml(spec: WorkspaceSpec) -> str:
    values = (
        ("workspace_id", spec.workspace_id),
        ("name", spec.name),
        ("target", spec.target),
        ("local_root", spec.local_root),
        ("remote_root", spec.remote_root),
        ("created_at", spec.created_at),
    )
    lines = [f"schema_version = {spec.schema_version}"]
    lines.extend(f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in values)
    return "\n".join(lines) + "\n"


def _expect_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"workspace field {key} must be an integer")
    return value


def _expect_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"workspace field {key} must be a non-empty string")
    return value
