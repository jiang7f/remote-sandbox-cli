from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias, cast

from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.policy import PolicyEngine

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry


class ActionType(StrEnum):
    PUSH = "push"
    PULL = "pull"
    DELETE_LOCAL = "delete-local"
    DELETE_REMOTE = "delete-remote"
    UPDATE_BASE = "update-base"


@dataclass(frozen=True, slots=True)
class HashRequest:
    side: str
    path: str

    def __post_init__(self) -> None:
        if type(self.side) is not str or self.side not in {"local", "remote"}:
            raise ValueError("hash request side must be local or remote")
        object.__setattr__(self, "path", _normalized_model_path(self.path, "hash request"))


@dataclass(frozen=True, slots=True)
class ConflictDecision:
    path: str
    reason: str
    local: FingerprintState
    remote: FingerprintState

    def __post_init__(self) -> None:
        normalized = _normalized_model_path(self.path, "conflict")
        object.__setattr__(self, "path", normalized)
        _validate_reason(self.reason, "conflict")
        _validate_snapshot(self.local, normalized, "conflict local")
        _validate_snapshot(self.remote, normalized, "conflict remote")


@dataclass(frozen=True, slots=True)
class PlanWarning:
    path: str
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _normalized_model_path(self.path, "warning"))
        _validate_reason(self.reason, "warning")


@dataclass(frozen=True, slots=True)
class SyncAction:
    type: ActionType
    path: str
    expected_local: FingerprintState
    expected_remote: FingerprintState
    base_after: FingerprintState

    def __post_init__(self) -> None:
        if type(self.type) is not ActionType:
            raise ValueError("sync action type must be an ActionType")
        normalized = _normalized_model_path(self.path, "sync action")
        object.__setattr__(self, "path", normalized)
        _validate_snapshot(self.expected_local, normalized, "sync action expected local")
        _validate_snapshot(self.expected_remote, normalized, "sync action expected remote")
        _validate_snapshot(self.base_after, normalized, "sync action base after")


@dataclass(frozen=True, slots=True)
class SyncPlan:
    hash_requests: tuple[HashRequest, ...] = ()
    actions: tuple[SyncAction, ...] = ()
    conflicts: tuple[ConflictDecision, ...] = ()
    warnings: tuple[PlanWarning, ...] = ()

    def __post_init__(self) -> None:
        _validate_tuple(self.hash_requests, HashRequest, "hash_requests")
        _validate_tuple(self.actions, SyncAction, "actions")
        _validate_tuple(self.conflicts, ConflictDecision, "conflicts")
        _validate_tuple(self.warnings, PlanWarning, "warnings")


def build_incremental_plan(
    base: Mapping[str, FingerprintState],
    local: Mapping[str, FingerprintState],
    remote: Mapping[str, FingerprintState],
    dirty_paths: Iterable[str],
    policy: PolicyEngine,
) -> SyncPlan:
    _validate_mapping(base, "base")
    _validate_mapping(local, "local")
    _validate_mapping(remote, "remote")
    paths = _normalized_dirty_paths(dirty_paths)
    hash_requests: list[HashRequest] = []
    actions: list[SyncAction] = []
    conflicts: list[ConflictDecision] = []
    warnings: list[PlanWarning] = []

    for path in paths:
        if policy.is_ignored(path):
            continue
        base_entry = _fingerprint_at(base, path, "base")
        local_entry = _fingerprint_at(local, path, "local")
        remote_entry = _fingerprint_at(remote, path, "remote")

        if _contains_special(base_entry, local_entry, remote_entry):
            warnings.append(PlanWarning(path, "special-entry-not-transferred"))
            continue
        if _placeholder_changed(base_entry, local_entry):
            conflicts.append(
                ConflictDecision(path, "placeholder-changed", local_entry, remote_entry)
            )
            continue
        if _kind_diverged(local_entry, remote_entry):
            conflicts.append(ConflictDecision(path, "kind-divergence", local_entry, remote_entry))
            continue

        requests = _missing_hash_requests(path, base_entry, local_entry, remote_entry)
        if requests:
            hash_requests.extend(requests)
            continue

        action, conflict = _plan_incremental_path(
            path,
            base_entry,
            local_entry,
            remote_entry,
        )
        if action is not None:
            actions.append(action)
        if conflict is not None:
            conflicts.append(conflict)

    return SyncPlan(
        hash_requests=tuple(hash_requests),
        actions=tuple(actions),
        conflicts=tuple(conflicts),
        warnings=tuple(warnings),
    )


def _plan_incremental_path(
    path: str,
    base: FingerprintState,
    local: FingerprintState,
    remote: FingerprintState,
) -> tuple[SyncAction | None, ConflictDecision | None]:
    if _fingerprint_content_equal(local, remote):
        if isinstance(local, MissingEntry) and isinstance(base, MissingEntry):
            return None, None
        base_after = _matching_base_after(local, remote)
        return _new_action(ActionType.UPDATE_BASE, path, local, remote, base_after), None

    if isinstance(base, MissingEntry):
        if isinstance(local, MissingEntry):
            return _new_action(ActionType.PULL, path, local, remote, remote), None
        if isinstance(remote, MissingEntry):
            return _new_action(ActionType.PUSH, path, local, remote, local), None
        return None, ConflictDecision(path, "both-modified", local, remote)

    local_matches_base = _fingerprint_matches_base(local, base)
    remote_matches_base = _fingerprint_matches_base(remote, base)
    if local_matches_base and remote_matches_base:
        return None, None
    if local_matches_base and not remote_matches_base:
        if isinstance(remote, MissingEntry):
            return _new_action(
                ActionType.DELETE_LOCAL,
                path,
                local,
                remote,
                MissingEntry(path),
            ), None
        return _new_action(ActionType.PULL, path, local, remote, remote), None
    if remote_matches_base and not local_matches_base:
        if isinstance(local, MissingEntry):
            return _new_action(
                ActionType.DELETE_REMOTE,
                path,
                local,
                remote,
                MissingEntry(path),
            ), None
        return _new_action(ActionType.PUSH, path, local, remote, local), None

    reason = (
        "delete-versus-modify"
        if isinstance(local, MissingEntry) or isinstance(remote, MissingEntry)
        else "both-modified"
    )
    return None, ConflictDecision(path, reason, local, remote)


def _new_action(
    action_type: ActionType,
    path: str,
    local: FingerprintState,
    remote: FingerprintState,
    base_after: FingerprintState,
) -> SyncAction:
    return SyncAction(action_type, path, local, remote, base_after)


def _fingerprint_content_equal(left: FingerprintState, right: FingerprintState) -> bool:
    if isinstance(left, MissingEntry) or isinstance(right, MissingEntry):
        return isinstance(left, MissingEntry) and isinstance(right, MissingEntry)
    if left.kind is not right.kind:
        return False
    if left.kind is EntryKind.FILE:
        return left.content_hash is not None and left.content_hash == right.content_hash
    if left.kind is EntryKind.SYMLINK:
        return left.link_target == right.link_target
    return left.kind is EntryKind.DIR


def _matching_base_after(
    local: FingerprintState,
    remote: FingerprintState,
) -> FingerprintState:
    if isinstance(local, MissingEntry):
        return local
    if local.is_placeholder:
        return local
    return remote


def _missing_hash_requests(
    path: str,
    base: FingerprintState,
    local: FingerprintState,
    remote: FingerprintState,
) -> tuple[HashRequest, ...]:
    requests: list[HashRequest] = []
    if _file_needs_hash(local) and not _quick_file_equal(local, base):
        requests.append(HashRequest("local", path))
    if _file_needs_hash(remote) and not _quick_file_equal(remote, base):
        requests.append(HashRequest("remote", path))
    return tuple(requests)


def _file_needs_hash(entry: FingerprintState) -> bool:
    return (
        isinstance(entry, EntryFingerprint)
        and entry.kind is EntryKind.FILE
        and entry.content_hash is None
    )


def _fingerprint_matches_base(current: FingerprintState, base: FingerprintState) -> bool:
    if (
        isinstance(current, EntryFingerprint)
        and isinstance(base, EntryFingerprint)
        and current.kind is EntryKind.FILE
        and base.kind is EntryKind.FILE
        and current.content_hash is None
    ):
        return _quick_file_equal(current, base)
    return _fingerprint_content_equal(current, base)


def _quick_file_equal(left: FingerprintState, right: FingerprintState) -> bool:
    return (
        isinstance(left, EntryFingerprint)
        and isinstance(right, EntryFingerprint)
        and left.kind is EntryKind.FILE
        and right.kind is EntryKind.FILE
        and left.size == right.size
        and left.mtime_ns == right.mtime_ns
        and left.mode == right.mode
    )


def _placeholder_changed(base: FingerprintState, local: FingerprintState) -> bool:
    if not isinstance(base, EntryFingerprint) or not base.is_placeholder:
        return False
    return local != base


def _kind_diverged(local: FingerprintState, remote: FingerprintState) -> bool:
    return (
        isinstance(local, EntryFingerprint)
        and isinstance(remote, EntryFingerprint)
        and local.kind is not remote.kind
    )


def _contains_special(*entries: FingerprintState) -> bool:
    return any(
        isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.SPECIAL
        for entry in entries
    )


def _fingerprint_at(
    entries: Mapping[str, FingerprintState],
    path: str,
    label: str,
) -> FingerprintState:
    absent = object()
    entry = cast(object, entries.get(path, absent))
    if entry is absent:
        return MissingEntry(path)
    _validate_snapshot(entry, path, f"{label} entry")
    return cast(FingerprintState, entry)


def _normalized_dirty_paths(dirty_paths: Iterable[str]) -> tuple[str, ...]:
    if isinstance(dirty_paths, (str, bytes)):
        raise ValueError("dirty_paths must be an iterable of relative paths")
    normalized: set[str] = set()
    try:
        iterator = iter(dirty_paths)
    except TypeError as exc:
        raise ValueError("dirty_paths must be an iterable of relative paths") from exc
    for path in iterator:
        normalized.add(_normalized_model_path(path, "dirty path"))
    return tuple(sorted(normalized))


def _normalized_model_path(path: object, label: str) -> str:
    if type(path) is not str:
        raise ValueError(f"{label} path must be a string")
    return normalize_relative_path(path)


def _validate_snapshot(entry: object, path: str, label: str) -> None:
    if type(entry) not in {EntryFingerprint, MissingEntry}:
        raise ValueError(f"{label} must be an EntryFingerprint or MissingEntry")
    entry_path = cast(FingerprintState, entry).path
    if entry_path is None or entry_path != path:
        raise ValueError(f"{label} path must match {path}")


def _validate_mapping(entries: object, label: str) -> None:
    if not isinstance(entries, Mapping):
        raise ValueError(f"{label} must be a mapping")


def _validate_reason(reason: object, label: str) -> None:
    if type(reason) is not str or not reason:
        raise ValueError(f"{label} reason must be a non-empty string")


def _validate_tuple(values: object, item_type: type[object], label: str) -> None:
    if type(values) is not tuple or any(type(value) is not item_type for value in values):
        raise ValueError(f"{label} must be a tuple of {item_type.__name__} values")
