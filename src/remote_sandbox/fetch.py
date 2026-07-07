from __future__ import annotations

import hashlib
import os
import posixpath
import tempfile
from collections.abc import Callable
from pathlib import Path

from remote_sandbox.lock import WorkspaceLockError, workspace_lock
from remote_sandbox.manifest import FileEntry, normalize_relative_path
from remote_sandbox.marker import METADATA_DIR, WorkspaceMarker, read_local_marker
from remote_sandbox.scan import read_placeholder_entry
from remote_sandbox.ssh import SshRunner
from remote_sandbox.state import StateStore


class FetchError(RuntimeError):
    pass


ConfirmCallback = Callable[[str], bool]


def fetch_placeholders(
    *,
    local_root: Path,
    runner: SshRunner,
    path: str | None,
    fetch_all: bool,
    confirm: ConfirmCallback,
) -> tuple[int, bool]:
    marker = _read_marker(local_root)
    placeholders = _select_placeholders(local_root, path=path, fetch_all=fetch_all)
    if not placeholders:
        return 0, False
    if fetch_all and not confirm(_fetch_all_prompt(placeholders)):
        return 0, True
    try:
        with (
            workspace_lock(local_root),
            StateStore.open(local_root / METADATA_DIR / "state.sqlite3") as state,
        ):
            for entry in placeholders:
                _ensure_placeholder_still_matches(local_root, entry)
                remote_path = _remote_path(marker, entry.path)
                if runner.is_symlink(marker.binding.target, remote_path):
                    raise FetchError(f"remote path is a symlink: {entry.path}")
                content = runner.read_bytes(
                    marker.binding.target,
                    remote_path,
                )
                if entry.hash is not None and hashlib.sha256(content).hexdigest() != entry.hash:
                    raise FetchError(f"hash mismatch while fetching {entry.path}")
                _ensure_placeholder_still_matches(local_root, entry)
                _write_local_bytes_atomic(_safe_local_path(local_root, entry.path), content)
                state.upsert_base(
                    FileEntry(
                        kind=entry.kind,
                        path=entry.path,
                        size=entry.size,
                        mtime=entry.mtime,
                        hash=entry.hash,
                        is_placeholder=False,
                    )
                )
    except WorkspaceLockError as exc:
        raise FetchError(str(exc)) from exc
    return len(placeholders), False


def _ensure_placeholder_still_matches(local_root: Path, expected: FileEntry) -> None:
    try:
        current = read_placeholder_entry(
            _safe_local_path(local_root, expected.path),
            expected_path=expected.path,
            raise_on_path_mismatch=True,
            raise_on_invalid_placeholder=True,
        )
    except ValueError as exc:
        raise FetchError(str(exc)) from exc
    if current != expected:
        raise FetchError(f"placeholder changed before fetch completed: {expected.path}")


def _read_marker(local_root: Path) -> WorkspaceMarker:
    marker = read_local_marker(local_root)
    if marker is None:
        raise FetchError("current directory is not bound; run rsb connect first")
    return marker


def _select_placeholders(
    local_root: Path,
    *,
    path: str | None,
    fetch_all: bool,
) -> list[FileEntry]:
    if fetch_all:
        return _find_all_placeholders(local_root)
    if path is None:
        raise FetchError("fetch requires a path or --all")
    rel_path = normalize_relative_path(path)
    placeholder_path = _safe_local_path(local_root, rel_path)
    try:
        entry = read_placeholder_entry(
            placeholder_path,
            expected_path=rel_path,
            raise_on_path_mismatch=True,
            raise_on_invalid_placeholder=True,
        )
    except ValueError as exc:
        raise FetchError(str(exc)) from exc
    if entry is None:
        raise FetchError(f"not a placeholder: {rel_path}")
    return [entry]


def _find_all_placeholders(local_root: Path) -> list[FileEntry]:
    placeholders: list[FileEntry] = []
    for candidate in sorted(local_root.rglob("*")):
        if candidate.is_symlink() or not candidate.is_file():
            continue
        rel = candidate.relative_to(local_root).as_posix()
        if rel == METADATA_DIR or rel.startswith(METADATA_DIR + "/"):
            continue
        rel = candidate.relative_to(local_root).as_posix()
        try:
            entry = read_placeholder_entry(
                candidate,
                expected_path=rel,
                raise_on_path_mismatch=True,
                raise_on_invalid_placeholder=True,
            )
        except ValueError as exc:
            raise FetchError(str(exc)) from exc
        if entry is not None:
            placeholders.append(entry)
    return placeholders


def _fetch_all_prompt(placeholders: list[FileEntry]) -> str:
    total = sum(entry.size or 0 for entry in placeholders)
    return (
        f"This will fetch {len(placeholders)} placeholder files, "
        f"total {_format_size(total)}.\n"
        "Continue? [y/N] "
    )


def _remote_path(marker: WorkspaceMarker, path: str) -> str:
    return posixpath.join(marker.binding.remote_path.rstrip("/") or "/", path)


def _safe_local_path(local_root: Path, relative_path: str) -> Path:
    candidate = local_root / relative_path
    resolved_root = local_root.resolve()
    resolved_parent = candidate.parent.resolve()
    try:
        resolved_parent.relative_to(resolved_root)
    except ValueError as exc:
        raise FetchError(f"path escapes local workspace: {relative_path}") from exc
    if candidate.exists() or candidate.is_symlink():
        try:
            candidate.resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise FetchError(f"path escapes local workspace: {relative_path}") from exc
    return candidate


def _write_local_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".remote-sandbox.tmp",
        dir=path.parent,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _format_size(size: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1000 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1000
    return f"{size} B"
