from __future__ import annotations

import posixpath
from pathlib import Path

from remote_sandbox.manifest import normalize_relative_path
from remote_sandbox.marker import WorkspaceMarker, read_local_marker
from remote_sandbox.scan import read_placeholder_entry
from remote_sandbox.ssh import SshRunner


class PeekError(RuntimeError):
    pass


def peek_placeholder(
    *,
    local_root: Path,
    runner: SshRunner,
    path: str,
    lines: int,
    tail: bool,
) -> bytes:
    if lines <= 0:
        raise PeekError("lines must be positive")
    marker = _read_marker(local_root)
    rel_path = normalize_relative_path(path)
    local_path = _safe_local_path(local_root, rel_path)
    try:
        entry = read_placeholder_entry(
            local_path,
            expected_path=rel_path,
            raise_on_path_mismatch=True,
            raise_on_invalid_placeholder=True,
        )
    except ValueError as exc:
        raise PeekError(str(exc)) from exc
    if entry is None:
        raise PeekError(f"not a placeholder: {rel_path}")
    remote_path = _remote_path(marker, entry.path)
    if runner.is_symlink(marker.binding.target, remote_path):
        raise PeekError(f"remote path is a symlink: {entry.path}")
    try:
        if tail:
            return runner.read_tail(marker.binding.target, remote_path, lines)
        return runner.read_head(marker.binding.target, remote_path, lines)
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise PeekError(str(exc)) from exc


def _read_marker(local_root: Path) -> WorkspaceMarker:
    marker = read_local_marker(local_root)
    if marker is None:
        raise PeekError("current directory is not bound; run rsb connect first")
    return marker


def _remote_path(marker: WorkspaceMarker, path: str) -> str:
    return posixpath.join(marker.binding.remote_path.rstrip("/") or "/", path)


def _safe_local_path(local_root: Path, relative_path: str) -> Path:
    candidate = local_root / relative_path
    resolved_root = local_root.resolve()
    resolved_parent = candidate.parent.resolve()
    try:
        resolved_parent.relative_to(resolved_root)
    except ValueError as exc:
        raise PeekError(f"path escapes local workspace: {relative_path}") from exc
    if candidate.exists() or candidate.is_symlink():
        try:
            candidate.resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise PeekError(f"path escapes local workspace: {relative_path}") from exc
    return candidate
