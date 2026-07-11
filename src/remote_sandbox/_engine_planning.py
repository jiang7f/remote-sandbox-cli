from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol, TypeAlias

from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import PolicyEngine
from remote_sandbox.reconcile import ActionType, SyncPlan, build_incremental_plan

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry


class LocalHasher(Protocol):
    def paths(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
        base: Mapping[str, EntryFingerprint],
    ) -> dict[str, FingerprintState]: ...


class RemoteHasher(Protocol):
    def hash_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]: ...


def satisfy_hash_requests(
    plan: SyncPlan,
    base: Mapping[str, EntryFingerprint],
    local: dict[str, FingerprintState],
    remote: dict[str, FingerprintState],
    dirty: tuple[str, ...],
    *,
    local_hasher: LocalHasher,
    remote_hasher: RemoteHasher,
    policy: PolicyEngine,
) -> tuple[SyncPlan, dict[str, FingerprintState], dict[str, FingerprintState]]:
    local_paths = tuple(request.path for request in plan.hash_requests if request.side == "local")
    remote_paths = tuple(
        request.path for request in plan.hash_requests if request.side == "remote"
    )
    if local_paths:
        local.update(local_hasher.paths(local_paths, with_hash=True, base=base))
    if remote_paths:
        remote.update(remote_hasher.hash_paths(remote_paths))
    if plan.hash_requests:
        plan = build_incremental_plan(base, local, remote, dirty, policy)
    if plan.hash_requests:
        raise RuntimeError("incremental planner requested unresolved hashes")
    return plan, local, remote


def strengthen_deletion_targets(
    plan: SyncPlan,
    base: Mapping[str, EntryFingerprint],
    local: dict[str, FingerprintState],
    remote: dict[str, FingerprintState],
    dirty: tuple[str, ...],
    *,
    local_hasher: LocalHasher,
    remote_hasher: RemoteHasher,
    policy: PolicyEngine,
) -> tuple[SyncPlan, dict[str, FingerprintState], dict[str, FingerprintState]]:
    local_paths = tuple(
        action.path
        for action in plan.actions
        if action.type is ActionType.DELETE_LOCAL
        and _regular_file_without_hash(action.expected_local)
    )
    remote_paths = tuple(
        action.path
        for action in plan.actions
        if action.type is ActionType.DELETE_REMOTE
        and _regular_file_without_hash(action.expected_remote)
    )
    if local_paths:
        local.update(local_hasher.paths(local_paths, with_hash=True, base=base))
    if remote_paths:
        remote.update(remote_hasher.hash_paths(remote_paths))
    if local_paths or remote_paths:
        plan = build_incremental_plan(base, local, remote, dirty, policy)
    return plan, local, remote


def strengthen_requeued_files(
    base: Mapping[str, EntryFingerprint],
    local: dict[str, FingerprintState],
    remote: dict[str, FingerprintState],
    paths: tuple[str, ...],
    *,
    local_hasher: LocalHasher,
    remote_hasher: RemoteHasher,
) -> tuple[dict[str, FingerprintState], dict[str, FingerprintState]]:
    local_paths = tuple(path for path in paths if _regular_file_without_hash(local[path]))
    remote_paths = tuple(path for path in paths if _regular_file_without_hash(remote[path]))
    if local_paths:
        local.update(local_hasher.paths(local_paths, with_hash=True, base=base))
    if remote_paths:
        remote.update(remote_hasher.hash_paths(remote_paths))
    return local, remote


def _regular_file_without_hash(entry: FingerprintState) -> bool:
    return (
        isinstance(entry, EntryFingerprint)
        and entry.kind is EntryKind.FILE
        and entry.content_hash is None
    )
