from __future__ import annotations

import posixpath
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.agent import bootstrap_agent
from remote_sandbox.lock import WorkspaceLockError
from remote_sandbox.marker import (
    METADATA_DIR,
    WorkspaceMarker,
    marker_to_toml,
    read_local_marker,
    write_local_marker,
)
from remote_sandbox.registry import (
    BindingRecord,
    ensure_connection_name_available,
    record_binding_from_marker,
)
from remote_sandbox.ssh import SshRunner, remote_marker_path, validate_remote_path, validate_target
from remote_sandbox.syncsession import SyncSession


class BindError(RuntimeError):
    pass


@dataclass(frozen=True)
class BindResult:
    workspace: WorkspaceMarker
    connection: BindingRecord
    created: bool


ConfirmCallback = Callable[[str], bool]


def bind_workspace(
    *,
    target: str,
    remote: str,
    local: Path,
    runner: SshRunner,
    confirm: ConfirmCallback | None = None,
    connection_name: str | None = None,
) -> BindResult:
    if sys.platform == "win32":
        raise BindError("remote-sandbox phase 1 supports macOS/Linux/WSL only")
    try:
        safe_target = validate_target(target)
        safe_remote = validate_remote_path(remote)
    except ValueError as exc:
        raise BindError(str(exc)) from exc
    if safe_remote == "/":
        raise BindError("Refusing to bind dangerous remote path: /")
    local_root = local.expanduser().resolve()
    dangerous_local_reason = _dangerous_local_root_reason(local_root)
    if dangerous_local_reason is not None:
        raise BindError(
            f"Refusing to bind dangerous local path: {local_root} ({dangerous_local_reason}). "
            "Use --local to choose a project directory."
        )
    if _has_control_char(str(local_root)):
        raise BindError("Invalid local path")
    if connection_name is not None:
        ensure_connection_name_available(
            name=connection_name,
            target=safe_target,
            remote_path=safe_remote,
            local_path=str(local_root),
        )
    local_root.mkdir(parents=True, exist_ok=True)
    if (local_root / METADATA_DIR).is_symlink():
        raise BindError("Local metadata path is a symlink")
    remote_exists = runner.exists(safe_target, safe_remote)
    if runner.is_symlink(safe_target, safe_remote):
        raise BindError(f"Remote path is a symlink: {safe_remote}")
    if remote_exists and not runner.is_dir(safe_target, safe_remote):
        raise BindError(f"Remote path is not a directory: {safe_remote}")
    if runner.is_symlink(safe_target, posixpath.dirname(remote_marker_path(safe_remote))):
        raise BindError("Remote metadata path is a symlink")
    runner.mkdir_p(safe_target, safe_remote)

    local_marker = _read_local_marker_checked(local_root)
    remote_marker = _read_remote_marker_checked(runner, safe_target, safe_remote)

    if local_marker is None and remote_marker is None:
        _require_new_binding_confirmation_if_needed(
            runner,
            safe_target,
            safe_remote,
            local_root,
            confirm,
        )
        marker = WorkspaceMarker.new(
            target=safe_target,
            local_path=str(local_root),
            remote_path=safe_remote,
        )
        created = True
        _write_remote_marker(runner, safe_target, safe_remote, marker)
        write_local_marker(local_root, marker)
    elif local_marker is not None and remote_marker is None:
        if (
            local_marker.binding.target != safe_target
            or local_marker.binding.remote_path != safe_remote
        ):
            raise BindError(
                "Local workspace is already bound to "
                f"{local_marker.binding.target}:{local_marker.binding.remote_path}"
            )
        marker = local_marker.with_binding(
            target=safe_target,
            local_path=str(local_root),
            remote_path=safe_remote,
        )
        created = False
        _require_new_binding_confirmation_if_needed(
            runner,
            safe_target,
            safe_remote,
            local_root,
            confirm,
        )
        _write_remote_marker(runner, safe_target, safe_remote, marker)
        write_local_marker(local_root, marker)
    elif local_marker is None and remote_marker is not None:
        if not _same_remote_binding(remote_marker, safe_target, safe_remote):
            raise BindError("Remote workspace marker points to a different binding")
        marker = remote_marker.with_binding(
            target=safe_target,
            local_path=str(local_root),
            remote_path=safe_remote,
        )
        created = False
        write_local_marker(local_root, marker)
    else:
        assert local_marker is not None
        assert remote_marker is not None
        if local_marker.workspace_id != remote_marker.workspace_id:
            raise BindError(
                "Local and remote are bound to different workspace ids: "
                f"local={local_marker.workspace_id} remote={remote_marker.workspace_id}"
            )
        if not _same_identity(local_marker, remote_marker):
            raise BindError("Local and remote have inconsistent workspace metadata")
        if not _same_remote_binding(remote_marker, safe_target, safe_remote):
            raise BindError("Remote workspace marker points to a different binding")
        marker = local_marker.with_binding(
            target=safe_target,
            local_path=str(local_root),
            remote_path=safe_remote,
        )
        created = False
        _write_remote_marker(runner, safe_target, safe_remote, marker)
        write_local_marker(local_root, marker)

    session = SyncSession(
        local_root=local_root,
        runner=runner,
        target=safe_target,
        remote=safe_remote,
    )
    bootstrap_agent(runner, safe_target, safe_remote)
    _sync_once_as_bind(session)
    connection = record_binding_from_marker(
        workspace_id=marker.workspace_id,
        target=safe_target,
        remote_path=safe_remote,
        local_path=str(local_root),
        name=connection_name,
    )
    return BindResult(
        workspace=marker,
        connection=connection,
        created=created,
    )


def _read_local_marker_checked(local_root: Path) -> WorkspaceMarker | None:
    try:
        return read_local_marker(local_root)
    except Exception as exc:
        raise BindError(f"Invalid local workspace marker: {exc}") from exc


def _read_remote_marker_checked(
    runner: SshRunner,
    target: str,
    remote: str,
) -> WorkspaceMarker | None:
    marker_path = remote_marker_path(remote)
    if not runner.exists(target, marker_path):
        return None
    try:
        from remote_sandbox.marker import marker_from_toml

        return marker_from_toml(runner.read_text(target, marker_path))
    except Exception as exc:
        raise BindError(f"Invalid remote workspace marker: {exc}") from exc


def _write_remote_marker(
    runner: SshRunner,
    target: str,
    remote: str,
    marker: WorkspaceMarker,
) -> None:
    try:
        runner.write_text_atomic(target, remote_marker_path(remote), marker_to_toml(marker))
    except Exception as exc:
        raise BindError(f"Failed to write remote workspace marker: {exc}") from exc


def _remote_has_user_content(runner: SshRunner, target: str, remote: str) -> bool:
    return any(name != METADATA_DIR for name in runner.listdir(target, remote))


def _require_new_binding_confirmation_if_needed(
    runner: SshRunner,
    target: str,
    remote: str,
    local_root: Path,
    confirm: ConfirmCallback | None,
) -> None:
    has_user_content = _remote_has_user_content(runner, target, remote)
    if confirm is None:
        if has_user_content:
            raise BindError(f"Binding cancelled for non-empty remote directory: {target}:{remote}")
        return
    remote_note = (
        "Remote directory is not empty and is not bound yet."
        if has_user_content
        else "Remote directory is empty or newly created."
    )
    prompt = (
        "Create a new remote-sandbox binding?\n"
        f"  remote: {_display_safe(target)}:{_display_safe(remote)}\n"
        f"  local:  {_display_safe(str(local_root))}\n"
        f"  note:   {remote_note}\n\n"
        "Continue? [y/N] "
    )
    if not confirm(prompt):
        if has_user_content:
            raise BindError(f"Binding cancelled for non-empty remote directory: {target}:{remote}")
        raise BindError(f"Binding cancelled: {target}:{remote} -> {local_root}")


def _same_identity(left: WorkspaceMarker, right: WorkspaceMarker) -> bool:
    return (
        left.workspace_id == right.workspace_id
        and left.binding_id == right.binding_id
        and left.local_replica_id == right.local_replica_id
        and left.remote_replica_id == right.remote_replica_id
        and left.created_at == right.created_at
        and left.sync_state == right.sync_state
    )


def _same_remote_binding(marker: WorkspaceMarker, target: str, remote_path: str) -> bool:
    return marker.binding.target == target and marker.binding.remote_path == remote_path


def _sync_once_as_bind(session: SyncSession) -> None:
    try:
        session.sync_once()
    except WorkspaceLockError as exc:
        raise BindError(str(exc)) from exc



def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def _dangerous_local_root_reason(local_root: Path) -> str | None:
    home = Path.home().resolve()
    filesystem_root = Path(local_root.anchor).resolve()
    if local_root == filesystem_root:
        return "filesystem root"
    if local_root == home:
        return "home directory"
    protected = {
        (home / ".ssh").resolve(): "SSH secrets directory",
        (home / ".remote-sandbox").resolve(): "remote-sandbox settings directory",
    }
    for path, reason in protected.items():
        try:
            local_root.relative_to(path)
        except ValueError:
            continue
        else:
            return reason
    return None


def _display_safe(value: str) -> str:
    return "".join(char if char.isprintable() else repr(char)[1:-1] for char in value)
