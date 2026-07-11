from __future__ import annotations

import contextlib
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol, cast

from remote_sandbox.agent import RemoteAgentManager
from remote_sandbox.policy import POLICY_FILE_NAME, PolicyEngine, StaticPolicyEngine
from remote_sandbox.registry import (
    BindingRecord,
    ensure_connection_name_available,
    generate_connection_name,
    list_binding_records,
    register_workspace,
)
from remote_sandbox.remote_client import RemoteWorkspaceClient
from remote_sandbox.ssh import SshRunner, validate_remote_path, validate_target
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.workspace import (
    WorkspacePaths,
    WorkspaceSpec,
    new_workspace_spec,
    read_workspace_spec,
    workspace_paths,
    write_workspace_spec,
)


class BindError(RuntimeError):
    pass


@dataclass(frozen=True)
class BindResult:
    workspace: WorkspaceSpec
    connection: BindingRecord
    created: bool


class _RemoteRegistration(Protocol):
    def forget(self) -> dict[str, object]: ...

    def close(self) -> None: ...


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
    try:
        if connection_name is not None:
            ensure_connection_name_available(
                name=connection_name,
                target=safe_target,
                remote_path=safe_remote,
                local_path=str(local_root),
            )
        existing = _existing_workspace(
            target=safe_target,
            remote=safe_remote,
            local=local_root,
            connection_name=connection_name,
        )
    except Exception as exc:
        raise BindError(str(exc)) from exc

    local_root.mkdir(parents=True, exist_ok=True)
    if runner.is_symlink(safe_target, safe_remote):
        raise BindError(f"Remote path is a symlink: {safe_remote}")
    remote_exists = runner.exists(safe_target, safe_remote)
    if remote_exists and not runner.is_dir(safe_target, safe_remote):
        raise BindError(f"Remote path is not a directory: {safe_remote}")
    runner.mkdir_p(safe_target, safe_remote)

    is_same_binding = existing is not None and Path(existing.local_root) == local_root
    if not is_same_binding:
        _guard_new_pairing(runner, safe_target, safe_remote, local_root, confirm)

    records = list_binding_records()
    if existing is None:
        name = connection_name or generate_connection_name(
            target=safe_target,
            local_path=str(local_root),
            records=records,
        )
        spec = new_workspace_spec(
            name=name,
            target=safe_target,
            local_root=local_root,
            remote_root=safe_remote,
        )
        created = True
    else:
        spec = replace(
            existing,
            name=connection_name or existing.name,
            target=safe_target,
            local_root=str(local_root),
            remote_root=safe_remote,
        )
        created = False

    paths = workspace_paths(spec.workspace_id)
    prior_workspace = paths.workspace_file.read_bytes() if paths.workspace_file.exists() else None
    metadata_existed = paths.root.exists()
    remote_registration: _RemoteRegistration | None = None
    try:
        remote_registration = _register_remote_workspace(
            runner,
            target=safe_target,
            remote=safe_remote,
            workspace_id=spec.workspace_id,
        )
        write_workspace_spec(paths.workspace_file, spec)
        with WorkspaceStore.open(paths.state_db):
            pass
        connection = register_workspace(spec)
    except BaseException as exc:
        _rollback_local_metadata(paths, metadata_existed, prior_workspace)
        if remote_registration is not None:
            with contextlib.suppress(BaseException):
                remote_registration.forget()
        if isinstance(exc, BindError):
            raise
        raise BindError(f"Failed to create workspace binding: {exc}") from exc
    finally:
        if remote_registration is not None:
            remote_registration.close()

    return BindResult(workspace=spec, connection=connection, created=created)


def _register_remote_workspace(
    runner: SshRunner,
    *,
    target: str,
    remote: str,
    workspace_id: str,
) -> _RemoteRegistration:
    manager = RemoteAgentManager(runner)
    install = manager.ensure(target)
    client = RemoteWorkspaceClient(
        cast(Any, runner),
        target=target,
        workspace_id=workspace_id,
        agent_path=install.remote_path,
    )
    try:
        client.register(remote)
    except BaseException:
        client.close()
        raise
    return client


def _existing_workspace(
    *,
    target: str,
    remote: str,
    local: Path,
    connection_name: str | None,
) -> WorkspaceSpec | None:
    candidates = []
    for record in list_binding_records():
        same_name = connection_name is not None and record.name == connection_name
        same_endpoint = record.target == target and record.remote_path == remote
        same_local = Path(record.local_path).expanduser().resolve(strict=False) == local
        if same_name or (same_endpoint and same_local):
            candidates.append(record)
    if not candidates:
        return None
    if len({record.workspace_id for record in candidates}) != 1:
        raise BindError("binding registry contains inconsistent workspace identities")
    paths = workspace_paths(candidates[0].workspace_id)
    try:
        spec = read_workspace_spec(paths.workspace_file)
    except Exception as exc:
        raise BindError(f"Invalid external workspace metadata: {exc}") from exc
    if spec.target != target or spec.remote_root != remote:
        raise BindError("existing workspace metadata points to a different remote binding")
    return spec


def _rollback_local_metadata(
    paths: WorkspacePaths,
    metadata_existed: bool,
    prior_workspace: bytes | None,
) -> None:
    if not metadata_existed:
        shutil.rmtree(paths.root, ignore_errors=True)
        for directory in (paths.root.parent, paths.root.parent.parent):
            with contextlib.suppress(OSError):
                directory.rmdir()
        return
    if prior_workspace is not None:
        paths.workspace_file.parent.mkdir(parents=True, exist_ok=True)
        paths.workspace_file.write_bytes(prior_workspace)


def _remote_user_content(
    runner: SshRunner, target: str, remote: str, policy: PolicyEngine
) -> list[str]:
    return sorted(name for name in runner.listdir(target, remote) if not policy.is_ignored(name))


def _local_user_content(local_root: Path, policy: PolicyEngine) -> list[str]:
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
    policy = StaticPolicyEngine.from_file(local_root / POLICY_FILE_NAME)
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
