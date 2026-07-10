from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import tomllib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from remote_sandbox.settings import remote_sandbox_home

# Legacy in-tree metadata directory. New workspaces keep local metadata OUT of the working
# tree (see local_meta_dir); this constant is still the *remote* metadata dir name and the
# source location that migrate_local_metadata() moves away from.
METADATA_DIR = ".remote-sandbox"
WORKSPACE_FILE = "workspace.toml"
STATE_FILE = "state.sqlite3"
SCHEMA_VERSION = 1
VALID_SYNC_STATES = {"none"}


def local_meta_dir(root: Path) -> Path:
    """Per-workspace local metadata dir, OUTSIDE the working tree.

    Keyed by a hash of the resolved local path (the same scheme the daemon already uses for
    its control socket), so it is locatable without first reading the marker — avoiding a
    chicken-and-egg with the workspace id, which lives *inside* the marker. Keeping the
    marker, sync state, lock, and daemon files here means the working directory the AI reads
    and writes stays clean: no `.remote-sandbox` in it.
    """
    resolved = root.expanduser().resolve()
    digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:16]
    return remote_sandbox_home() / "workspaces" / f"L-{digest}"


def remote_meta_dir(remote_root: str) -> str:
    """Per-workspace remote metadata dir (POSIX), OUTSIDE the remote working tree.

    Returns `~/.remote-sandbox/workspaces/R-<hash>`; `~` is expanded on the remote by the
    shell helper at call time. Keyed by a hash of the remote root string (kept consistent by
    the registry) so it is locatable without reading anything, mirroring local_meta_dir. This
    keeps the remote working directory clean — no `.remote-sandbox` inside it.
    """
    import posixpath

    digest = hashlib.sha256(remote_root.encode("utf-8")).hexdigest()[:16]
    return posixpath.join("~", METADATA_DIR, "workspaces", f"R-{digest}")


def legacy_remote_meta_dir(remote_root: str) -> str:
    import posixpath

    return posixpath.join(remote_root.rstrip("/") or "/", METADATA_DIR)


def _legacy_meta_dir(root: Path) -> Path:
    return root.expanduser() / METADATA_DIR


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
    """Current location of the local marker (out-of-tree home workspace dir)."""
    return local_meta_dir(root) / WORKSPACE_FILE


def legacy_marker_path(root: Path) -> Path:
    return _legacy_meta_dir(root) / WORKSPACE_FILE


def read_local_marker(root: Path) -> WorkspaceMarker | None:
    """Read the marker, preferring the out-of-tree home dir, falling back to legacy in-tree.

    The legacy fallback keeps pre-relocation bindings working until they are migrated (which
    happens on the next connect/reconnect/start via migrate_local_metadata).
    """
    path = marker_path(root)
    if path.exists():
        return marker_from_toml(path.read_text(encoding="utf-8"))
    legacy = legacy_marker_path(root)
    if legacy.exists():
        return marker_from_toml(legacy.read_text(encoding="utf-8"))
    return None


def write_local_marker(root: Path, marker: WorkspaceMarker) -> None:
    metadata_dir = local_meta_dir(root)
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


def migrate_local_metadata(root: Path) -> bool:
    """Move a legacy in-tree ``.remote-sandbox`` into the out-of-tree home workspace dir.

    Idempotent: returns True only when it actually migrated something. Copies the marker and
    sync state (the durable files) into the home dir, then removes the whole in-tree dir so
    the working directory is left clean. A running daemon's transient files (pid/lock/socket)
    are intentionally NOT carried over — the caller restarts the daemon after migrating.
    """
    legacy_dir = _legacy_meta_dir(root)
    if not legacy_dir.is_dir() or legacy_dir.is_symlink():
        return False
    legacy_marker = legacy_dir / WORKSPACE_FILE
    if not legacy_marker.exists():
        return False
    dest_dir = local_meta_dir(root)
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in (WORKSPACE_FILE, STATE_FILE):
        src = legacy_dir / name
        if src.exists() and not (dest_dir / name).exists():
            shutil.copy2(src, dest_dir / name)
    shutil.rmtree(legacy_dir, ignore_errors=True)
    return True


def remove_local_metadata(root: Path) -> bool:
    """Delete the local metadata (out-of-tree home dir and any legacy in-tree dir).

    Returns True if something was removed. Used by ``forget`` so a forgotten connection
    leaves no local binding metadata behind, in either location.
    """
    removed = False
    for metadata_dir in (local_meta_dir(root), _legacy_meta_dir(root)):
        if metadata_dir.is_symlink():
            metadata_dir.unlink()
            removed = True
        elif metadata_dir.is_dir():
            shutil.rmtree(metadata_dir)
            removed = True
        elif metadata_dir.exists():
            metadata_dir.unlink()
            removed = True
    return removed


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
