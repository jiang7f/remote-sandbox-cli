from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from remote_sandbox.manifest import (
    MISSING,
    EntryKind,
    EntryState,
    FileEntry,
    MissingEntry,
    is_missing,
)
from remote_sandbox.policy import PolicyDecision, PolicyEngine, ReplicaSide


class PlanActionType(StrEnum):
    PULL = "pull"
    PUSH = "push"
    DELETE_LOCAL = "delete_local"
    DELETE_REMOTE = "delete_remote"
    PLACEHOLDER = "placeholder"
    UPDATE_BASE = "update_base"
    CONFLICT = "conflict"
    NEEDS_HASH = "needs_hash"


@dataclass(frozen=True, slots=True)
class PlanAction:
    type: PlanActionType
    path: str
    base: EntryState
    local: EntryState
    remote: EntryState
    base_after: EntryState | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True)
class SyncPlan:
    actions: tuple[PlanAction, ...]


def build_plan(
    base_entries: Mapping[str, FileEntry],
    local_entries: Mapping[str, FileEntry],
    remote_entries: Mapping[str, FileEntry],
    policy_engine: PolicyEngine,
) -> SyncPlan:
    paths = sorted(set(base_entries) | set(local_entries) | set(remote_entries))
    actions: list[PlanAction] = []
    for path in paths:
        if policy_engine.is_ignored(path):
            continue
        base = base_entries.get(path, MISSING)
        local = local_entries.get(path, MISSING)
        remote = remote_entries.get(path, MISSING)
        action = _plan_path(path, base, local, remote, policy_engine)
        if action is not None:
            actions.append(action)
    return SyncPlan(actions=tuple(actions))


def _plan_path(
    path: str,
    base: EntryState,
    local: EntryState,
    remote: EntryState,
    policy_engine: PolicyEngine,
) -> PlanAction | None:
    if _has_unsupported(local, remote):
        return PlanAction(
            PlanActionType.CONFLICT,
            path,
            base,
            local,
            remote,
            reason="unsupported file type",
        )
    if _kind_changed_from_base(base, local, remote):
        return PlanAction(
            PlanActionType.CONFLICT,
            path,
            base,
            local,
            remote,
            reason="entry kind changed",
        )
    if _needs_hash(local, remote):
        return PlanAction(
            PlanActionType.NEEDS_HASH,
            path,
            base,
            local,
            remote,
            reason=_missing_hash_reason(local, remote),
        )
    if _needs_hash(local, base):
        return PlanAction(
            PlanActionType.NEEDS_HASH,
            path,
            base,
            local,
            remote,
            reason=_missing_hash_reason(local, base),
        )
    if _needs_hash(remote, base):
        return PlanAction(
            PlanActionType.NEEDS_HASH,
            path,
            base,
            local,
            remote,
            reason=_missing_hash_reason(remote, base).replace("local", "remote"),
        )

    local_remote_equal = content_equal(local, remote)
    local_base_equal = content_equal(local, base)
    remote_base_equal = content_equal(remote, base)

    if local_remote_equal:
        if is_missing(local) and is_missing(base):
            return None
        base_after = _base_after_matching_replicas(base, local, remote)
        return PlanAction(
            PlanActionType.UPDATE_BASE,
            path,
            base,
            local,
            remote,
            base_after=base_after,
            reason="local and remote match",
        )

    if local_base_equal and not remote_base_equal:
        if is_missing(remote):
            return PlanAction(
                PlanActionType.DELETE_LOCAL,
                path,
                base,
                local,
                remote,
                base_after=MISSING,
                reason="remote deleted",
            )
        assert isinstance(remote, FileEntry)
        return PlanAction(
            _pull_action_type(remote, policy_engine),
            path,
            base,
            local,
            remote,
            base_after=remote,
            reason="remote changed",
        )

    if remote_base_equal and not local_base_equal:
        if _local_placeholder_deleted(base, local, remote):
            return PlanAction(
                PlanActionType.CONFLICT,
                path,
                base,
                local,
                remote,
                reason="local placeholder deleted; remote file still exists",
            )
        if is_missing(local):
            return PlanAction(
                PlanActionType.DELETE_REMOTE,
                path,
                base,
                local,
                remote,
                base_after=MISSING,
                reason="local deleted",
            )
        return PlanAction(
            PlanActionType.PUSH,
            path,
            base,
            local,
            remote,
            base_after=local,
            reason="local changed",
        )

    return PlanAction(
        PlanActionType.CONFLICT,
        path,
        base,
        local,
        remote,
        reason="both sides changed",
    )


def content_equal(left: EntryState, right: EntryState) -> bool:
    if is_missing(left) or is_missing(right):
        return left is right
    assert isinstance(left, FileEntry)
    assert isinstance(right, FileEntry)
    if left.kind != right.kind:
        return False
    if left.kind == "dir":
        return True
    if left.kind == EntryKind.UNSUPPORTED:
        return False
    if left.hash is None or right.hash is None:
        return False
    return left.hash == right.hash


def _pull_action_type(entry: FileEntry, policy_engine: PolicyEngine) -> PlanActionType:
    if entry.kind != EntryKind.FILE:
        return PlanActionType.PULL
    decision = policy_engine.classify(entry, side=ReplicaSide.REMOTE)
    if decision == PolicyDecision.PLACEHOLDER:
        return PlanActionType.PLACEHOLDER
    return PlanActionType.PULL


def _needs_hash(left: EntryState, right: EntryState) -> bool:
    if isinstance(left, MissingEntry) or isinstance(right, MissingEntry):
        return False
    assert isinstance(left, FileEntry)
    assert isinstance(right, FileEntry)
    if left.kind != right.kind:
        return False
    if left.kind == "dir":
        return False
    if left.kind == EntryKind.UNSUPPORTED:
        return False
    return left.hash is None or right.hash is None


def _kind_changed_from_base(base: EntryState, local: EntryState, remote: EntryState) -> bool:
    if is_missing(base):
        return False
    assert isinstance(base, FileEntry)
    return (
        isinstance(local, FileEntry)
        and local.kind != base.kind
        or isinstance(remote, FileEntry)
        and remote.kind != base.kind
    )


def _base_after_matching_replicas(
    base: EntryState,
    local: EntryState,
    remote: EntryState,
) -> EntryState:
    if is_missing(local):
        return MISSING
    if isinstance(local, FileEntry) and local.is_placeholder:
        return local
    if isinstance(base, FileEntry) and base.is_placeholder and not is_missing(remote):
        assert isinstance(remote, FileEntry)
        return remote
    return remote if not is_missing(remote) else local


def _local_placeholder_deleted(base: EntryState, local: EntryState, remote: EntryState) -> bool:
    return (
        isinstance(base, FileEntry)
        and base.is_placeholder
        and is_missing(local)
        and isinstance(remote, FileEntry)
    )


def _has_unsupported(left: EntryState, right: EntryState) -> bool:
    return (
        isinstance(left, FileEntry)
        and left.kind == EntryKind.UNSUPPORTED
        or isinstance(right, FileEntry)
        and right.kind == EntryKind.UNSUPPORTED
    )


def _missing_hash_reason(left: EntryState, right: EntryState) -> str:
    del right
    if isinstance(left, FileEntry) and left.hash is None:
        return "missing local hash"
    return "missing remote hash"
