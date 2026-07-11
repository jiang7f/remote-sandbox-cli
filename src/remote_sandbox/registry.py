from __future__ import annotations

import fcntl
import os
import re
import tempfile
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from remote_sandbox.settings import remote_sandbox_home
from remote_sandbox.workspace import WorkspaceSpec, validate_workspace_id

_VALID_CONNECTION_NAME = re.compile(r"^[A-Za-z0-9_.@-]+$")


class RegistryError(RuntimeError):
    pass


@dataclass(frozen=True)
class BindingRecord:
    name: str
    workspace_id: str
    target: str
    remote_path: str
    local_path: str
    updated_at: str


def registry_path() -> Path:
    override = os.environ.get("REMOTE_SANDBOX_CONNECTIONS")
    if override:
        return Path(override).expanduser()
    return remote_sandbox_home() / "connections.toml"


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def list_binding_records(path: Path | None = None) -> list[BindingRecord]:
    registry = path or registry_path()
    return _list_binding_records_unlocked(registry)


def _list_binding_records_unlocked(registry: Path) -> list[BindingRecord]:
    if not registry.exists():
        return []
    try:
        data = tomllib.loads(registry.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise RegistryError(f"Invalid connections file {registry}: {exc}") from exc
    raw_records = data.get("connections", data.get("bindings", []))
    if not isinstance(raw_records, list):
        raise RegistryError(f"Invalid connections file {registry}: connections must be a list")
    records: list[BindingRecord] = []
    for index, item in enumerate(raw_records, start=1):
        if not isinstance(item, dict):
            raise RegistryError(
                f"Invalid connection record {index} in {registry}: expected table"
            )
        try:
            record = _record_from_dict(item)
        except ValueError as exc:
            raise RegistryError(
                f"Invalid connection record {index} in {registry}: {exc}"
            ) from exc
        records.append(record)
    return sorted(records, key=lambda record: record.name)


def upsert_binding_record(path: Path | None, record: BindingRecord) -> None:
    validate_connection_name(record.name)
    validate_workspace_id(record.workspace_id)
    registry = path or registry_path()
    record_local = _resolved_local_path(record.local_path)
    with _registry_transaction(registry):
        records = []
        for existing in _list_binding_records_unlocked(registry):
            existing_local = _resolved_local_path(existing.local_path)
            same_workspace = existing.workspace_id == record.workspace_id
            same_local = existing_local == record_local
            same_name = existing.name == record.name
            if same_name and not same_workspace:
                raise RegistryError(f"Connection name already exists: {record.name}")
            if same_local and not same_workspace:
                raise RegistryError(f"Local path is already registered: {record_local}")
            if same_workspace or same_local:
                continue
            records.append(existing)
        records.append(record)
        records.sort(key=lambda item: item.name)
        _write_binding_records_unlocked(registry, records)


def delete_binding_record(
    name: str,
    path: Path | None = None,
    *,
    workspace_id: str | None = None,
) -> bool:
    validate_connection_name(name)
    if workspace_id is not None:
        validate_workspace_id(workspace_id)
    registry = path or registry_path()
    with _registry_transaction(registry):
        records = _list_binding_records_unlocked(registry)
        matched = next((record for record in records if record.name == name), None)
        if matched is None or (
            workspace_id is not None and matched.workspace_id != workspace_id
        ):
            return False
        kept = [record for record in records if record.name != name]
        _write_binding_records_unlocked(registry, kept)
    return True


@contextmanager
def _registry_transaction(registry: Path) -> Iterator[None]:
    registry.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    registry.parent.chmod(0o700)
    lock_path = registry.with_name(f"{registry.name}.lock")
    with lock_path.open("a+b") as handle:
        lock_path.chmod(0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_binding_records_unlocked(
    registry: Path,
    records: list[BindingRecord],
) -> None:
    content = _records_to_toml(records)
    fd, tmp_name = tempfile.mkstemp(
        prefix="connections.",
        suffix=".tmp",
        dir=registry.parent,
        text=True,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp_path.chmod(0o600)
        os.replace(tmp_path, registry)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def register_workspace(
    spec: WorkspaceSpec,
    *,
    registry: Path | None = None,
) -> BindingRecord:
    """Register a workspace from its external durable specification."""
    validate_workspace_id(spec.workspace_id)
    local_path = str(_resolved_local_path(spec.local_root))
    record = BindingRecord(
        name=spec.name,
        workspace_id=spec.workspace_id,
        target=spec.target,
        remote_path=spec.remote_root,
        local_path=local_path,
        updated_at=spec.created_at,
    )
    upsert_binding_record(registry, record)
    return record


def validate_connection_name(name: str) -> str:
    if not name:
        raise RegistryError("Connection name cannot be empty")
    if len(name) > 80:
        raise RegistryError("Connection name is too long; keep it under 80 characters")
    if not _VALID_CONNECTION_NAME.fullmatch(name):
        raise RegistryError(
            "Invalid connection name; use letters, numbers, '.', '_', '-' or '@'"
        )
    return name


def ensure_connection_name_available(
    *,
    name: str,
    target: str,
    remote_path: str,
    local_path: str,
    path: Path | None = None,
) -> None:
    validate_connection_name(name)
    for record in list_binding_records(path):
        if record.name != name:
            continue
        if record.target == target and record.remote_path == remote_path:
            return
        raise RegistryError(
            f"Connection name already exists: {name}. "
            "Choose another --name or run codex-rsb reconnect with the existing name."
        )


def find_binding_record(name: str, path: Path | None = None) -> BindingRecord | None:
    validate_connection_name(name)
    for record in list_binding_records(path):
        if record.name == name:
            return record
    return None


def existing_or_generated_name(
    records: list[BindingRecord],
    *,
    workspace_id: str,
    target: str,
    local_path: str,
) -> str:
    resolved_local = _resolved_local_path(local_path)
    for record in records:
        same_workspace = record.workspace_id == workspace_id
        same_local = _resolved_local_path(record.local_path) == resolved_local
        if same_workspace or same_local:
            return record.name
    return generate_connection_name(target=target, local_path=local_path, records=records)


def generate_connection_name(
    *,
    target: str,
    local_path: str,
    records: list[BindingRecord],
) -> str:
    existing = {record.name for record in records}
    local_name = Path(local_path).name or "workspace"
    base = _sanitize_name(f"{target}-{local_name}") or "session"
    if base not in existing:
        return base
    index = 2
    while f"{base}-{index}" in existing:
        index += 1
    return f"{base}-{index}"


def current_workspace_record(path: Path | None, cwd: Path) -> BindingRecord | None:
    resolved = cwd.expanduser().resolve(strict=False)
    matches: list[tuple[int, BindingRecord]] = []
    for record in list_binding_records(path):
        local_resolved = _resolved_local_path(record.local_path)
        try:
            resolved.relative_to(local_resolved)
        except ValueError:
            continue
        matches.append((len(local_resolved.parts), record))
    if not matches:
        return None
    return max(matches, key=lambda item: item[0])[1]


def _record_from_dict(item: dict[str, Any]) -> BindingRecord:
    try:
        workspace_id = _expect_str(item["workspace_id"])
        validate_workspace_id(workspace_id)
        target = _expect_str(item["target"])
        remote_path = _expect_str(item["remote_path"])
        local_path = _expect_str(item["local_path"])
        updated_at = _expect_str(item["updated_at"])
    except KeyError as exc:
        raise ValueError(f"missing field {exc.args[0]}") from exc
    raw_name = item.get("name")
    if isinstance(raw_name, str) and raw_name:
        name = raw_name
        try:
            validate_connection_name(name)
        except RegistryError as exc:
            raise ValueError(str(exc)) from exc
    else:
        name = _sanitize_name(f"{target}-{Path(local_path).name}") or workspace_id[:8]
    return BindingRecord(
        name=name,
        workspace_id=workspace_id,
        target=target,
        remote_path=remote_path,
        local_path=local_path,
        updated_at=updated_at,
    )


def _resolved_local_path(path: str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _records_to_toml(records: list[BindingRecord]) -> str:
    lines: list[str] = []
    for record in records:
        lines.extend(
            [
                "[[connections]]",
                f'name = "{_toml_escape(record.name)}"',
                f'workspace_id = "{_toml_escape(record.workspace_id)}"',
                f'target = "{_toml_escape(record.target)}"',
                f'remote_path = "{_toml_escape(record.remote_path)}"',
                f'local_path = "{_toml_escape(record.local_path)}"',
                f'updated_at = "{_toml_escape(record.updated_at)}"',
                "",
            ]
        )
    return "\n".join(lines)


def _expect_str(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("expected non-empty string")
    return value


def _toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _sanitize_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.@-]+", "-", value.strip())
    sanitized = sanitized.strip(".-_")
    return sanitized[:80]
