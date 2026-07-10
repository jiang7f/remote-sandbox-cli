from __future__ import annotations

import hashlib
import os
import posixpath
import shlex
import tempfile
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from remote_sandbox.manifest import MISSING, EntryKind, EntryState, FileEntry, is_missing
from remote_sandbox.reconcile import PlanAction, PlanActionType, SyncPlan
from remote_sandbox.scan import read_placeholder_entry
from remote_sandbox.ssh import SshRunner, TransferCancelled
from remote_sandbox.state import StateStore

if TYPE_CHECKING:
    from remote_sandbox.syncsession import ProgressCallback


class SyncExecutionError(RuntimeError):
    pass


class ConcurrentModification(SyncExecutionError):
    """One file changed between scan and transfer (e.g. git rewriting .git/index).

    Skippable: the offending file is left for the next sync cycle instead of aborting the
    whole plan, which is why it is distinct from a fatal SyncExecutionError.
    """


class SyncCancelled(SyncExecutionError):
    """The sync was aborted because a cancel signal fired (daemon stop)."""


def execute_plan(
    plan: SyncPlan,
    *,
    local_root: Path,
    runner: SshRunner,
    target: str,
    remote_root: str,
    state: StateStore,
    on_progress: ProgressCallback | None = None,
    cancel: Callable[[], bool] | None = None,
) -> None:
    from remote_sandbox.syncsession import SyncProgress

    local_root = local_root.expanduser().resolve()
    transfer_actions = [
        action
        for action in plan.actions
        if action.type
        not in {PlanActionType.UPDATE_BASE, PlanActionType.CONFLICT, PlanActionType.NEEDS_HASH}
    ]
    files_total = len(transfer_actions)
    bytes_total = sum(_action_bytes(action) for action in transfer_actions)
    files_done = 0
    bytes_done = 0
    if on_progress is not None and files_total:
        on_progress(
            SyncProgress(
                phase="transferring",
                files_total=files_total,
                files_done=0,
                bytes_total=bytes_total,
                bytes_done=0,
            )
        )
    ordered = _execution_order(plan.actions)
    if cancel is not None and cancel():
        raise SyncCancelled("sync cancelled")
    # Bulk-transfer regular files in a single tar-over-ssh stream instead of one ssh round
    # trip per file. Falls back to the per-file verified path for any group that errors, so
    # correctness never depends on tar succeeding.
    bulk_done = _bulk_transfer_files(
        ordered,
        local_root=local_root,
        runner=runner,
        target=target,
        remote_root=remote_root,
        cancel=cancel,
    )
    skipped = 0
    conflicts = 0
    for action in ordered:
        if cancel is not None and cancel():
            raise SyncCancelled("sync cancelled")
        if action.type in {PlanActionType.CONFLICT, PlanActionType.NEEDS_HASH}:
            # A single unsyncable path — a symlink/unsupported file (e.g. .venv/bin/python),
            # a both-sides-changed conflict, or a missing hash — must NOT abort the whole
            # sync. Skip just this path (base untouched, so it is revisited next cycle) and
            # keep going. This is what lets `rsb connect` work on a tree that contains a venv.
            conflicts += 1
            if on_progress is not None:
                on_progress(
                    SyncProgress(
                        phase="transferring",
                        files_total=files_total,
                        files_done=files_done,
                        bytes_total=bytes_total,
                        bytes_done=bytes_done,
                        current_path=f"skipped ({action.reason or 'conflict'}): {action.path}",
                    )
                )
            continue
        key = (action.type, action.path)
        try:
            if key not in bulk_done:
                _execute_action(
                    action,
                    local_root=local_root,
                    runner=runner,
                    target=target,
                    remote_root=remote_root,
                )
        except ConcurrentModification as exc:
            # One file moved under us (classically git rewriting .git/index or a pack tmp
            # file mid-sync). Skip just this path and leave its base untouched so the next
            # cycle reconciles it — do NOT abort the whole plan the way this used to.
            skipped += 1
            if on_progress is not None:
                on_progress(
                    SyncProgress(
                        phase="transferring",
                        files_total=files_total,
                        files_done=files_done,
                        bytes_total=bytes_total,
                        bytes_done=bytes_done,
                        current_path=f"skipped (changed): {action.path}",
                    )
                )
            del exc
            continue
        _update_base(state, action)
        if action.type != PlanActionType.UPDATE_BASE:
            files_done += 1
            bytes_done += _action_bytes(action)
            if on_progress is not None:
                on_progress(
                    SyncProgress(
                        phase="transferring",
                        files_total=files_total,
                        files_done=files_done,
                        bytes_total=bytes_total,
                        bytes_done=bytes_done,
                        current_path=action.path,
                    )
                )
    if (skipped or conflicts) and on_progress is not None:
        notes = []
        if skipped:
            notes.append(f"{skipped} changed mid-sync")
        if conflicts:
            notes.append(f"{conflicts} conflicting/unsupported skipped")
        on_progress(
            SyncProgress(
                phase="transferring",
                files_total=files_total,
                files_done=files_done,
                bytes_total=bytes_total,
                bytes_done=bytes_done,
                current_path="; ".join(notes) + "; will revisit next cycle",
            )
        )


# Below this many same-direction file transfers, the per-file verified path (with its
# mid-sync change detection) is used; at or above it, one tar stream is worth the small
# loss of per-file verification.
_BULK_MIN = 4


def _is_regular_file_transfer(action: PlanAction) -> bool:
    if action.type == PlanActionType.PUSH:
        entry = action.local
    elif action.type == PlanActionType.PULL:
        entry = action.remote
    else:
        return False
    return isinstance(entry, FileEntry) and entry.kind == EntryKind.FILE


def _bulk_transfer_files(
    ordered: tuple[PlanAction, ...],
    *,
    local_root: Path,
    runner: SshRunner,
    target: str,
    remote_root: str,
    cancel: Callable[[], bool] | None = None,
) -> set[tuple[PlanActionType, str]]:
    """Transfer regular-file PUSH/PULL actions in one tar stream each.

    Returns the set of (type, path) actions successfully handled in bulk so the caller can
    skip re-transferring them (but still update base + progress). Any group whose tar fails
    is simply left out, so the per-file loop retransfers it the slow-but-verified way.
    """
    pushes = [
        a.path
        for a in ordered
        if a.type == PlanActionType.PUSH and _is_regular_file_transfer(a)
    ]
    pulls = [
        a.path
        for a in ordered
        if a.type == PlanActionType.PULL and _is_regular_file_transfer(a)
    ]
    done: set[tuple[PlanActionType, str]] = set()
    if len(pushes) >= _BULK_MIN:
        try:
            runner.push_files(target, str(local_root), remote_root, pushes, cancel)
            done.update((PlanActionType.PUSH, path) for path in pushes)
        except TransferCancelled as exc:
            # A stop request killed the tar — abort the whole sync, don't fall back.
            raise SyncCancelled("sync cancelled") from exc
        except Exception:  # noqa: BLE001 - any other failure falls back to the per-file path
            pass
    if len(pulls) >= _BULK_MIN:
        try:
            runner.pull_files(target, str(local_root), remote_root, pulls, cancel)
            done.update((PlanActionType.PULL, path) for path in pulls)
        except TransferCancelled as exc:
            raise SyncCancelled("sync cancelled") from exc
        except Exception:  # noqa: BLE001 - any other failure falls back to the per-file path
            pass
    return done


def _action_bytes(action: PlanAction) -> int:
    for entry in (action.local, action.remote):
        if isinstance(entry, FileEntry) and entry.size:
            return entry.size
    return 0


def placeholder_text(*, remote: FileEntry, target: str, remote_root: str) -> str:
    return "\n".join(
        [
            "REMOTE-SANDBOX PLACEHOLDER",
            "reason: large remote file",
            f"path: {remote.path}",
            f"remote: {target}:{posixpath.join(remote_root.rstrip('/') or '/', remote.path)}",
            f"size: {_format_size(remote.size or 0)}",
            f"bytes: {remote.size or 0}",
            f"mtime: {_format_mtime(remote.mtime)}",
            f"hash: {remote.hash or ''}",
            f"fetch: rsb fetch -- {shlex.quote(remote.path)}",
            "",
        ]
    )


def _execution_order(actions: tuple[PlanAction, ...]) -> tuple[PlanAction, ...]:
    ordered = sorted(
        enumerate(actions),
        key=lambda item: _execution_sort_key(item[0], item[1]),
    )
    return tuple(action for _index, action in ordered)


def _execution_sort_key(index: int, action: PlanAction) -> tuple[int, int]:
    if action.type in {PlanActionType.DELETE_LOCAL, PlanActionType.DELETE_REMOTE}:
        return (0, -action.path.count("/"))
    return (1, index)


def _execute_action(
    action: PlanAction,
    *,
    local_root: Path,
    runner: SshRunner,
    target: str,
    remote_root: str,
) -> None:
    if action.type == PlanActionType.PUSH:
        _ensure_local_matches(local_root, action.path, action.local)
        _ensure_remote_parents_safe(runner, target, remote_root, action.path)
        _ensure_remote_matches(runner, target, remote_root, action.path, action.remote)
        _push(action, local_root=local_root, runner=runner, target=target, remote_root=remote_root)
    elif action.type == PlanActionType.PULL:
        _ensure_local_matches(local_root, action.path, action.local)
        _ensure_remote_parents_safe(runner, target, remote_root, action.path)
        _ensure_remote_matches(runner, target, remote_root, action.path, action.remote)
        _pull(action, local_root=local_root, runner=runner, target=target, remote_root=remote_root)
    elif action.type == PlanActionType.PLACEHOLDER:
        _ensure_local_matches(local_root, action.path, action.local)
        _ensure_remote_parents_safe(runner, target, remote_root, action.path)
        _ensure_remote_matches(runner, target, remote_root, action.path, action.remote)
        _write_placeholder(action, local_root=local_root, target=target, remote_root=remote_root)
    elif action.type == PlanActionType.DELETE_LOCAL:
        _ensure_local_matches(local_root, action.path, action.local)
        _delete_local(_safe_local_path(local_root, action.path))
    elif action.type == PlanActionType.DELETE_REMOTE:
        _ensure_remote_parents_safe(runner, target, remote_root, action.path)
        _ensure_remote_matches(runner, target, remote_root, action.path, action.remote)
        _ensure_remote_directory_empty_if_needed(
            runner,
            target,
            remote_root,
            action.path,
            action.remote,
        )
        runner.delete_path(target, _remote_path(remote_root, action.path))
    elif action.type == PlanActionType.UPDATE_BASE:
        return


def _push(
    action: PlanAction,
    *,
    local_root: Path,
    runner: SshRunner,
    target: str,
    remote_root: str,
) -> None:
    entry = _require_file_entry(action.local, action.path)
    if entry.kind == EntryKind.DIR:
        runner.mkdir_p(target, _remote_path(remote_root, action.path))
        return
    runner.write_bytes_atomic(
        target,
        _remote_path(remote_root, action.path),
        _safe_local_path(local_root, action.path).read_bytes(),
    )


def _pull(
    action: PlanAction,
    *,
    local_root: Path,
    runner: SshRunner,
    target: str,
    remote_root: str,
) -> None:
    entry = _require_file_entry(action.remote, action.path)
    local_path = _safe_local_path(local_root, action.path)
    if entry.kind == EntryKind.DIR:
        local_path.mkdir(parents=True, exist_ok=True)
        return
    content = runner.read_bytes(target, _remote_path(remote_root, action.path))
    _write_local_bytes_atomic(local_path, content)


def _write_placeholder(
    action: PlanAction,
    *,
    local_root: Path,
    target: str,
    remote_root: str,
) -> None:
    remote = _require_file_entry(action.remote, action.path)
    text = placeholder_text(remote=remote, target=target, remote_root=remote_root)
    _write_local_bytes_atomic(_safe_local_path(local_root, action.path), text.encode("utf-8"))


def _delete_local(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        path.rmdir()
    elif path.exists() or path.is_symlink():
        path.unlink()


def _safe_local_path(local_root: Path, relative_path: str) -> Path:
    candidate = local_root / relative_path
    parent = candidate.parent
    resolved_root = local_root.resolve()
    resolved_parent = parent.resolve()
    try:
        resolved_parent.relative_to(resolved_root)
    except ValueError as exc:
        raise SyncExecutionError(f"path escapes local workspace: {relative_path}") from exc
    if candidate.exists() or candidate.is_symlink():
        resolved_candidate = candidate.resolve()
        try:
            resolved_candidate.relative_to(resolved_root)
        except ValueError as exc:
            raise SyncExecutionError(f"path escapes local workspace: {relative_path}") from exc
    return candidate


def _update_base(state: StateStore, action: PlanAction) -> None:
    if action.type in {PlanActionType.DELETE_LOCAL, PlanActionType.DELETE_REMOTE}:
        state.delete_base(action.path)
        return
    if action.type == PlanActionType.PLACEHOLDER:
        remote = _require_file_entry(action.remote, action.path)
        state.upsert_base(replace(remote, is_placeholder=True))
        return
    base_after = action.base_after
    if base_after is None:
        return
    if is_missing(base_after):
        state.delete_base(action.path)
        return
    assert isinstance(base_after, FileEntry)
    state.upsert_base(base_after)


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


def _ensure_local_matches(local_root: Path, path: str, expected: EntryState) -> None:
    actual = _local_entry(local_root, path)
    if not _entry_matches(actual, expected):
        raise ConcurrentModification(f"local changed during sync: {path}")


def _ensure_remote_matches(
    runner: SshRunner,
    target: str,
    remote_root: str,
    path: str,
    expected: EntryState,
) -> None:
    actual = _remote_entry(runner, target, remote_root, path)
    if not _entry_matches(actual, expected):
        raise ConcurrentModification(f"remote changed during sync: {path}")


def _ensure_remote_directory_empty_if_needed(
    runner: SshRunner,
    target: str,
    remote_root: str,
    path: str,
    expected: EntryState,
) -> None:
    if not isinstance(expected, FileEntry) or expected.kind != EntryKind.DIR:
        return
    remote_path = _remote_path(remote_root, path)
    if runner.listdir(target, remote_path):
        raise ConcurrentModification(f"remote directory changed during sync: {path}")


def _local_entry(local_root: Path, path: str) -> EntryState:
    candidate = _safe_local_path(local_root, path)
    if not candidate.exists() and not candidate.is_symlink():
        return MISSING
    if candidate.is_symlink():
        return FileEntry(
            kind=EntryKind.UNSUPPORTED,
            path=path,
            size=None,
            mtime=None,
            hash=None,
        )
    if candidate.is_file():
        try:
            placeholder = read_placeholder_entry(
                candidate,
                expected_path=path,
                raise_on_path_mismatch=True,
                raise_on_invalid_placeholder=True,
            )
            if placeholder is not None:
                return placeholder
        except ValueError as exc:
            raise SyncExecutionError(str(exc)) from exc
    stat = candidate.stat()
    if candidate.is_dir():
        return FileEntry(
            kind=EntryKind.DIR,
            path=path,
            size=None,
            mtime=stat.st_mtime,
            hash=None,
        )
    if candidate.is_file():
        return FileEntry(
            kind=EntryKind.FILE,
            path=path,
            size=stat.st_size,
            mtime=stat.st_mtime,
            hash=_sha256_file(candidate),
        )
    return FileEntry(
        kind=EntryKind.UNSUPPORTED,
        path=path,
        size=None,
        mtime=None,
        hash=None,
    )


def _remote_entry(
    runner: SshRunner,
    target: str,
    remote_root: str,
    path: str,
) -> EntryState:
    remote_path = _remote_path(remote_root, path)
    if runner.is_symlink(target, remote_path):
        return FileEntry(
            kind=EntryKind.UNSUPPORTED,
            path=path,
            size=None,
            mtime=None,
            hash=None,
        )
    if not runner.exists(target, remote_path):
        return MISSING
    if runner.is_dir(target, remote_path):
        return FileEntry(
            kind=EntryKind.DIR,
            path=path,
            size=None,
            mtime=None,
            hash=None,
        )
    content = runner.read_bytes(target, remote_path)
    return FileEntry(
        kind=EntryKind.FILE,
        path=path,
        size=len(content),
        mtime=None,
        hash=hashlib.sha256(content).hexdigest(),
    )


def _entry_matches(actual: EntryState, expected: EntryState) -> bool:
    if is_missing(actual) or is_missing(expected):
        return actual is expected
    assert isinstance(actual, FileEntry)
    assert isinstance(expected, FileEntry)
    if actual.kind != expected.kind:
        return False
    if actual.kind == EntryKind.DIR:
        return True
    if actual.kind == EntryKind.UNSUPPORTED:
        return False
    return actual.size == expected.size and actual.hash == expected.hash


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_path(remote_root: str, path: str) -> str:
    return posixpath.join(remote_root.rstrip("/") or "/", path)


def _ensure_remote_parents_safe(
    runner: SshRunner,
    target: str,
    remote_root: str,
    path: str,
) -> None:
    current = remote_root.rstrip("/") or "/"
    if runner.is_symlink(target, current):
        raise SyncExecutionError(f"remote path uses symlinked parent: {path}")
    parent = posixpath.dirname(path)
    if not parent:
        return
    for part in parent.split("/"):
        current = posixpath.join(current, part)
        if runner.is_symlink(target, current):
            raise SyncExecutionError(f"remote path uses symlinked parent: {path}")


def _require_file_entry(entry: EntryState, path: str) -> FileEntry:
    if is_missing(entry):
        raise SyncExecutionError(f"missing entry for {path}")
    assert isinstance(entry, FileEntry)
    return entry


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


def _format_mtime(mtime: float | None) -> str:
    if mtime is None:
        return "unknown"
    return datetime.fromtimestamp(mtime, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
