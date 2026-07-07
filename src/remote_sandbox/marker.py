from __future__ import annotations

import os
import tempfile
import tomllib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

METADATA_DIR = ".remote-sandbox"
WORKSPACE_FILE = "workspace.toml"
SCHEMA_VERSION = 1
VALID_SYNC_STATES = {"none"}


@dataclass(frozen=True)
class BindingInfo:
    target: str
    local_path: str
    remote_path: str


@dataclass(frozen=True)
class WorkspaceMarker:
    schema_version: int
    workspace_id: str
    binding_id: str
    local_replica_id: str
    remote_replica_id: str
    created_at: str
    sync_state: str
    binding: BindingInfo

    @classmethod
    def new(cls, *, target: str, local_path: str, remote_path: str) -> WorkspaceMarker:
        now = datetime.now(UTC).isoformat(timespec="seconds")
        return cls(
            schema_version=SCHEMA_VERSION,
            workspace_id=str(uuid.uuid4()),
            binding_id=str(uuid.uuid4()),
            local_replica_id=str(uuid.uuid4()),
            remote_replica_id=str(uuid.uuid4()),
            created_at=now,
            sync_state="none",
            binding=BindingInfo(
                target=target,
                local_path=local_path,
                remote_path=remote_path,
            ),
        )

    def with_binding(self, *, target: str, local_path: str, remote_path: str) -> WorkspaceMarker:
        return WorkspaceMarker(
            schema_version=self.schema_version,
            workspace_id=self.workspace_id,
            binding_id=self.binding_id,
            local_replica_id=self.local_replica_id,
            remote_replica_id=self.remote_replica_id,
            created_at=self.created_at,
            sync_state=self.sync_state,
            binding=BindingInfo(target=target, local_path=local_path, remote_path=remote_path),
        )


def marker_path(root: Path) -> Path:
    return root / METADATA_DIR / WORKSPACE_FILE


def read_local_marker(root: Path) -> WorkspaceMarker | None:
    path = marker_path(root)
    if not path.exists():
        return None
    return marker_from_toml(path.read_text(encoding="utf-8"))


def write_local_marker(root: Path, marker: WorkspaceMarker) -> None:
    metadata_dir = root / METADATA_DIR
    metadata_dir.mkdir(parents=True, exist_ok=True)
    content = marker_to_toml(marker)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{WORKSPACE_FILE}.",
        suffix=".tmp",
        dir=metadata_dir,
        text=True,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, marker_path(root))
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def marker_to_toml(marker: WorkspaceMarker) -> str:
    lines = [
        f"schema_version = {marker.schema_version}",
        f'workspace_id = "{marker.workspace_id}"',
        f'binding_id = "{marker.binding_id}"',
        f'local_replica_id = "{marker.local_replica_id}"',
        f'remote_replica_id = "{marker.remote_replica_id}"',
        f'created_at = "{marker.created_at}"',
        f'sync_state = "{marker.sync_state}"',
        "",
        "[binding]",
        f'target = "{_toml_escape(marker.binding.target)}"',
        f'local_path = "{_toml_escape(marker.binding.local_path)}"',
        f'remote_path = "{_toml_escape(marker.binding.remote_path)}"',
        "",
    ]
    return "\n".join(lines)


def marker_from_toml(content: str) -> WorkspaceMarker:
    data = tomllib.loads(content)
    binding = _expect_dict(data.get("binding"), "binding")
    schema_version = _expect_int(data.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"Invalid marker: unsupported schema_version {schema_version}")
    sync_state = _expect_str(data.get("sync_state"), "sync_state")
    if sync_state not in VALID_SYNC_STATES:
        raise ValueError(f"Invalid marker: sync_state must be one of {sorted(VALID_SYNC_STATES)}")
    return WorkspaceMarker(
        schema_version=schema_version,
        workspace_id=_expect_str(data.get("workspace_id"), "workspace_id"),
        binding_id=_expect_str(data.get("binding_id"), "binding_id"),
        local_replica_id=_expect_str(data.get("local_replica_id"), "local_replica_id"),
        remote_replica_id=_expect_str(data.get("remote_replica_id"), "remote_replica_id"),
        created_at=_expect_str(data.get("created_at"), "created_at"),
        sync_state=sync_state,
        binding=BindingInfo(
            target=_expect_str(binding.get("target"), "binding.target"),
            local_path=_expect_str(binding.get("local_path"), "binding.local_path"),
            remote_path=_expect_str(binding.get("remote_path"), "binding.remote_path"),
        ),
    )


def _expect_dict(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Invalid marker: {field} must be a table")
    return value


def _expect_str(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid marker: {field} must be a non-empty string")
    return value


def _expect_int(value: object, field: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Invalid marker: {field} must be an integer")
    return value


def _toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
