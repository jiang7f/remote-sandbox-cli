from __future__ import annotations

import posixpath
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.agent import bootstrap_agent
from remote_sandbox.marker import (
    METADATA_DIR,
    WorkspaceMarker,
    legacy_remote_meta_dir,
    marker_to_toml,
    migrate_local_metadata,
    read_local_marker,
    write_local_marker,
)
from remote_sandbox.policy import POLICY_FILE_NAME, PolicyEngine, StaticPolicyEngine
from remote_sandbox.registry import (
    BindingRecord,
    ensure_connection_name_available,
    record_binding_from_marker,
)
from remote_sandbox.settings import load_settings
from remote_sandbox.ssh import (
    SshRunner,
    legacy_remote_marker_path,
    remote_marker_path,
    validate_remote_path,
    validate_target,
)


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
    # Relocate any legacy in-tree metadata into the out-of-tree home dir before reading the
    # marker, so an existing binding is recognized (and its sync base kept) after upgrade.
    migrate_local_metadata(local_root)
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
    # Relocate a legacy in-tree remote .remote-sandbox into the out-of-tree home dir (keeps
    # the remote working directory clean, preserves the workspace id), best-effort.
    _migrate_remote_metadata(runner, safe_target, safe_remote)

    local_marker = _read_local_marker_checked(local_root)
    remote_marker = _read_remote_marker_checked(runner, safe_target, safe_remote)

    if local_marker is None and remote_marker is None:
        _guard_new_pairing(
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
        _guard_new_pairing(
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
        _guard_new_pairing(
            runner,
            safe_target,
            safe_remote,
            local_root,
            confirm,
        )
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

    # Bootstrap the remote agent now so any connectivity / missing-python3 problem surfaces
    # here, as a clear connect-time error, rather than silently in the background daemon.
    # The *initial sync itself* is intentionally NOT run here: the sync daemon (started by
    # the caller) owns it, so `connect` returns immediately and the sync happens once, in the
    # background, with live progress — instead of blocking here and then again in the daemon.
    bootstrap_agent(runner, safe_target, safe_remote)
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


def _migrate_remote_metadata(runner: SshRunner, target: str, remote: str) -> None:
    """Move a legacy in-tree remote `.remote-sandbox` into the out-of-tree home dir.

    Best-effort and idempotent: if the new home-dir marker already exists, or there is no
    legacy in-tree marker, do nothing. Otherwise copy the legacy marker's workspace identity
    into the home dir and delete the whole legacy in-tree metadata dir so the remote working
    directory is left clean. The agent + hashcache are re-bootstrapped fresh in the home dir
    by the normal bind flow, so only the marker needs carrying over.
    """
    new_marker = remote_marker_path(remote)
    legacy_marker = legacy_remote_marker_path(remote)
    legacy_dir = legacy_remote_meta_dir(remote)
    try:
        if runner.exists(target, new_marker):
            # Already migrated (or fresh): just make sure no stale in-tree dir lingers.
            if runner.exists(target, legacy_marker):
                runner.remove_metadata_tree(target, legacy_dir)
            return
        if not runner.exists(target, legacy_marker):
            return
        content = runner.read_text(target, legacy_marker)
        runner.write_text_atomic(target, new_marker, content)
        runner.remove_metadata_tree(target, legacy_dir)
    except Exception:  # noqa: BLE001 - migration is best-effort; bind proceeds regardless
        return


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


def _remote_user_content(
    runner: SshRunner, target: str, remote: str, policy: PolicyEngine
) -> list[str]:
    """Top-level remote names that would actually sync (ignored/junk excluded)."""
    return sorted(name for name in runner.listdir(target, remote) if not policy.is_ignored(name))


def _local_user_content(local_root: Path, policy: PolicyEngine) -> list[str]:
    """Top-level local names that would actually sync (ignored/junk excluded).

    A directory that is empty, or holds only ignored entries (`.remote-sandbox`, OS
    cruft like `.DS_Store`), counts as empty for binding purposes.
    """
    try:
        return sorted(
            entry.name for entry in local_root.iterdir() if not policy.is_ignored(entry.name)
        )
    except FileNotFoundError:
        return []


def _guard_new_pairing(
    runner: SshRunner,
    target: str,
    remote: str,
    local_root: Path,
    confirm: ConfirmCallback | None,
) -> None:
    """Gate a *new* local<->remote pairing on directory emptiness.

    Binding runs a bidirectional merge, so pairing two independent non-empty
    directories would silently union them (or fail mid-sync on conflicts). Require
    at least one empty side; when exactly one side has content, confirm the sync
    direction first. Files the policy ignores (`.remote-sandbox`, OS cruft) don't count.
    Not called for an already-bound same-workspace reconnect.
    """
    policy = StaticPolicyEngine.from_file(
        local_root / POLICY_FILE_NAME,
        default_ignore_patterns=load_settings().default_ignores,
    )
    local_names = _local_user_content(local_root, policy)
    remote_names = _remote_user_content(runner, target, remote, policy)
    local_content = bool(local_names)
    remote_content = bool(remote_names)
    if local_content and remote_content:
        raise BindError(
            "Refusing to bind two non-empty directories: "
            f"local {_display_safe(str(local_root))} (e.g. {_display_safe(local_names[0])}) "
            f"and remote {_display_safe(target)}:{_display_safe(remote)} "
            f"(e.g. {_display_safe(remote_names[0])}) both contain files. "
            "Binding needs at least one empty side; clear or move one side first."
        )
    if confirm is None:
        return
    if remote_content:
        note = "Remote has files; they will sync down into the empty local directory."
    elif local_content:
        note = "Local has files; they will sync up into the empty remote directory."
    else:
        note = "Both directories are empty or newly created."
    prompt = (
        "Create a new remote-sandbox binding?\n"
        f"  remote: {_display_safe(target)}:{_display_safe(remote)}\n"
        f"  local:  {_display_safe(str(local_root))}\n"
        f"  note:   {note}\n\n"
        "Continue? [y/N] "
    )
    if not confirm(prompt):
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
