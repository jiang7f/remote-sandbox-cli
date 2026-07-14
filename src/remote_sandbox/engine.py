from __future__ import annotations

import hashlib
import os
import stat
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeAlias

from remote_sandbox._engine_audit import AuditCoordinator
from remote_sandbox._engine_events import (
    acknowledge_pending,
    contains_rescan,
    dirty_sources,
)
from remote_sandbox._engine_metadata import LocalMetadata
from remote_sandbox._engine_planning import (
    satisfy_hash_requests,
    strengthen_deletion_targets,
    strengthen_requeued_files,
)
from remote_sandbox._transport_fingerprint import LocalPathChanged, ProtectedLocalRoot
from remote_sandbox.journal import EventKind, JournalEvent, coalesce_events
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.policy import PolicyEngine
from remote_sandbox.reconcile import (
    ActionType,
    ConflictDecision,
    PlanWarning,
    SyncAction,
    SyncPlan,
    build_incremental_plan,
)
from remote_sandbox.remote_client import RemoteSnapshot
from remote_sandbox.state import AuditSignature, ConflictRecord, WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferPreflightError,
    TransferResult,
)

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry


class RemoteReplica(Protocol):
    def events_after(self, after_sequence: int) -> list[JournalEvent]: ...

    def metadata_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]: ...

    def hash_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]: ...

    def snapshot(self) -> RemoteSnapshot: ...

    def read_path(self, path: str) -> bytes | None: ...

    def acknowledge(self, sequence: int) -> int: ...

    def audit_signatures(
        self,
        paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]: ...

    def observations(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
    ) -> tuple[
        dict[str, FingerprintState],
        dict[str, AuditSignature | None],
    ]: ...


class SyncTransport(Protocol):
    def transfer(
        self,
        batch: TransferBatch,
        on_progress: Callable[[TransferResult], None],
    ) -> TransferResult: ...

    def delete_local(
        self,
        expected: Mapping[str, FingerprintState],
    ) -> TransferResult: ...

    def delete_remote(
        self,
        expected: Mapping[str, FingerprintState],
    ) -> TransferResult: ...


@dataclass(frozen=True, slots=True)
class EngineResult:
    transferred: tuple[str, ...] = ()
    completed: tuple[str, ...] = ()
    requeued: tuple[str, ...] = ()
    echoes: tuple[str, ...] = ()
    conflict_ids: tuple[str, ...] = ()
    warnings: tuple[PlanWarning, ...] = ()


@dataclass(frozen=True, slots=True)
class _Echo:
    side: str
    path: str
    observed: FingerprintState


class SyncEngine:
    def __init__(
        self,
        *,
        store: WorkspaceStore,
        local_root: Path,
        remote: RemoteReplica,
        transport: SyncTransport,
        policy: PolicyEngine,
    ) -> None:
        self.store = store
        self.local_root = local_root.expanduser().resolve()
        self.remote = remote
        self.transport = transport
        self.policy = policy
        self.local_metadata = LocalMetadata(self.local_root, policy)
        self.audit_coordinator = AuditCoordinator(
            store=store,
            local=self.local_metadata,
            remote=remote,
            policy=policy,
        )
        self._cycle_lock = threading.RLock()
        self._status_override: tuple[WorkspacePhase, str] | None = None

    def run_once(self, reason: str) -> EngineResult:
        if type(reason) is not str or not reason:
            raise ValueError("sync reason must not be empty")
        with self._cycle_lock:
            previous_override = self._status_override
            if reason == "initial-replay":
                self._status_override = (WorkspacePhase.INITIAL_SYNCING, "replaying")
            try:
                return self._run_once(reason)
            except BaseException as exc:
                self._record_error(exc)
                raise
            finally:
                self._status_override = previous_override

    def _run_once(self, reason: str) -> EngineResult:
        remote_after = self.store.acknowledged_sequence("remote")
        imported = self.remote.events_after(remote_after)
        self.store.record_events(imported)
        local_events = self.store.pending_events(
            "local", self.store.acknowledged_sequence("local")
        )
        remote_events = self.store.pending_events("remote", remote_after)
        if contains_rescan(local_events, remote_events) or self._directory_move_requires_audit(
            local_events,
            remote_events,
        ):
            self._record_audit_drift()
        requeued_before = set(self.store.list_requeued_paths())
        sources = dirty_sources(local_events, remote_events, requeued_before)
        if reason != "initial-replay":
            for conflict in self.store.list_conflicts(unresolved_only=True):
                sources.setdefault(conflict.path, set()).add("conflict")
        dirty = tuple(sorted(sources))
        if not dirty:
            return self._commit_noop(local_events, remote_events)

        base = self.store.list_base()
        local = self.local_metadata.paths(dirty, with_hash=False, base=base)
        remote = self.remote.metadata_paths(dirty)
        echoes = self._matching_echoes(
            local_events,
            remote_events,
            local,
            remote,
            requeued_before,
        )
        for echo in echoes:
            path_sources = sources.get(echo.path)
            if path_sources is not None:
                path_sources.discard(echo.side)
                if not path_sources:
                    sources.pop(echo.path)
        dirty = tuple(sorted(sources))
        if not dirty:
            return self._commit_echoes(local_events, remote_events, echoes)
        self._set_status(WorkspacePhase.SYNCING, "collecting")

        local = {path: local[path] for path in dirty}
        remote = {path: remote[path] for path in dirty}
        local, remote = strengthen_requeued_files(
            base,
            local,
            remote,
            tuple(path for path in dirty if path in requeued_before),
            local_hasher=self.local_metadata,
            remote_hasher=self.remote,
        )
        plan = build_incremental_plan(base, local, remote, dirty, self.policy)
        plan, local, remote = satisfy_hash_requests(
            plan,
            base,
            local,
            remote,
            dirty,
            local_hasher=self.local_metadata,
            remote_hasher=self.remote,
            policy=self.policy,
        )
        plan, local, remote = strengthen_deletion_targets(
            plan,
            base,
            local,
            remote,
            dirty,
            local_hasher=self.local_metadata,
            remote_hasher=self.remote,
            policy=self.policy,
        )
        return self._execute_and_commit(
            plan,
            local_events,
            remote_events,
            echoes,
            requeued_before,
        )

    def audit(self) -> EngineResult:
        with self._cycle_lock:
            self._record_audit_drift()
            return self.run_once("audit")

    def _directory_move_requires_audit(
        self,
        local_events: list[JournalEvent],
        remote_events: list[JournalEvent],
    ) -> bool:
        moves = tuple(
            event
            for event in coalesce_events([*local_events, *remote_events])
            if event.kind is EventKind.MOVE
        )
        if not moves:
            return False
        base = self.store.list_base()
        if any(
            isinstance((entry := base.get(event.path)), EntryFingerprint)
            and entry.kind is EntryKind.DIR
            for event in moves
        ):
            return True
        local_destinations = tuple(
            sorted(
                {
                    event.destination_path
                    for event in moves
                    if event.side == "local" and event.destination_path is not None
                }
            )
        )
        remote_destinations = tuple(
            sorted(
                {
                    event.destination_path
                    for event in moves
                    if event.side == "remote" and event.destination_path is not None
                }
            )
        )
        local = (
            self.local_metadata.paths(local_destinations, with_hash=False, base=base)
            if local_destinations
            else {}
        )
        remote = self.remote.metadata_paths(remote_destinations) if remote_destinations else {}
        return any(
            isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.DIR
            for entry in (*local.values(), *remote.values())
        )

    def seed_base_from_transfer(
        self,
        batch: TransferBatch,
        completed_paths: Iterable[str],
    ) -> None:
        completed = tuple(sorted({normalize_relative_path(path) for path in completed_paths}))
        batch_paths = {item.path for item in batch.items}
        if not set(completed) <= batch_paths:
            raise ValueError("completed transfer paths must belong to the batch")
        fingerprints = self._destination_hashes(batch.direction, completed)
        verified: list[EntryFingerprint] = []
        for path in completed:
            fingerprint = fingerprints[path]
            if isinstance(fingerprint, MissingEntry):
                raise RuntimeError(f"completed transfer destination is missing: {path}")
            verified.append(fingerprint)
        self.seed_verified_transfer(batch.direction, tuple(verified))

    def seed_verified_transfer(
        self,
        direction: TransferDirection,
        entries: tuple[EntryFingerprint, ...],
    ) -> None:
        if type(direction) is not TransferDirection:
            raise ValueError("verified transfer direction must be a TransferDirection")
        if not entries or any(type(entry) is not EntryFingerprint for entry in entries):
            raise ValueError("verified transfer entries must contain fingerprints")
        destination_side = "remote" if direction is TransferDirection.PUSH else "local"
        self.store.seed_verified_transfer(destination_side, entries)
        self.audit_coordinator.refresh(entry.path for entry in entries)

    def requeue_paths(self, paths: Iterable[str], reason: str) -> None:
        self.store.requeue_paths(paths, reason)

    def apply_initial_placeholders(
        self,
        placeholders: Mapping[str, EntryFingerprint] | Iterable[EntryFingerprint],
    ) -> None:
        if isinstance(placeholders, Mapping):
            entries = tuple(placeholders.values())
            if any(path != entry.path for path, entry in placeholders.items()):
                raise ValueError("placeholder mapping keys must match fingerprint paths")
        else:
            entries = tuple(placeholders)
        if any(
            type(entry) is not EntryFingerprint or not entry.is_placeholder for entry in entries
        ):
            raise ValueError("initial placeholders must be placeholder fingerprints")
        with self.store.transaction():
            for entry in sorted(entries, key=lambda item: item.path):
                self.store.upsert_base(entry)
                self.store.set_expected_echo("local", entry)
        self.audit_coordinator.refresh(entry.path for entry in entries)

    def _execute_and_commit(
        self,
        plan: SyncPlan,
        local_events: list[JournalEvent],
        remote_events: list[JournalEvent],
        echoes: tuple[_Echo, ...],
        requeued_before: set[str],
    ) -> EngineResult:
        transferred: set[str] = set()
        completed: set[str] = set()
        changed: set[str] = set()
        adopted_changed: set[str] = set()
        base_after: dict[str, FingerprintState] = {}
        expected_echoes: dict[tuple[str, str], FingerprintState] = {}
        deferred_deletes = self._conflicting_directory_deletes(plan)
        if deferred_deletes:
            plan = SyncPlan(
                hash_requests=plan.hash_requests,
                actions=tuple(
                    action
                    for action in plan.actions
                    if action.path not in deferred_deletes
                ),
                conflicts=plan.conflicts,
                warnings=plan.warnings,
            )
        intents = self._expected_echo_intents(plan)
        successful_mutations: set[tuple[str, str]] = set()
        attempted_mutations: set[tuple[str, str]] = set()
        known_unused: set[tuple[str, str]] = set()
        current_attempt: set[tuple[str, str]] = set()
        self._commit_expected_echo_intents(intents)
        try:
            for direction, action_type in (
                (TransferDirection.PUSH, ActionType.PUSH),
                (TransferDirection.PULL, ActionType.PULL),
            ):
                actions = tuple(
                    sorted(
                        (action for action in plan.actions if action.type is action_type),
                        key=lambda action: action.path,
                    )
                )
                if not actions:
                    continue
                destination_side = (
                    "remote" if direction is TransferDirection.PUSH else "local"
                )

                def record_progress(
                    result: TransferResult,
                    side: str = destination_side,
                ) -> None:
                    successful_mutations.update(
                        (side, path) for path in result.completed
                    )

                current_attempt = {
                    (destination_side, action.path) for action in actions
                }
                attempted_mutations.update(current_attempt)
                result = self.transport.transfer(
                    TransferBatch.from_actions(actions),
                    record_progress,
                )
                transferred.update(result.completed)
                completed.update(result.completed)
                changed.update(result.changed_during_transfer)
                adopted = self._adoptable_changed_destinations(
                    direction,
                    actions,
                    result.changed_during_transfer,
                )
                adopted_changed.update(adopted)
                base_after.update(adopted)
                expected_echoes.update(
                    ((destination_side, path), fingerprint)
                    for path, fingerprint in adopted.items()
                )
                known_unused.update(
                    (destination_side, path) for path in result.changed_during_transfer
                )
                successful_mutations.update(
                    (destination_side, path) for path in result.completed
                )
                current_attempt = set()
                final = self._destination_hashes(direction, result.completed)
                for path in result.completed:
                    fingerprint = final[path]
                    if isinstance(fingerprint, MissingEntry):
                        raise RuntimeError(f"transfer destination is missing: {path}")
                    base_after[path] = fingerprint
                    expected_echoes[(destination_side, path)] = fingerprint

            for action_type, side in (
                (ActionType.DELETE_LOCAL, "local"),
                (ActionType.DELETE_REMOTE, "remote"),
            ):
                actions = tuple(
                    sorted(
                        (action for action in plan.actions if action.type is action_type),
                        key=lambda action: action.path.count("/"),
                        reverse=True,
                    )
                )
                if not actions:
                    continue
                expected = {
                    action.path: (
                        action.expected_local
                        if action_type is ActionType.DELETE_LOCAL
                        else action.expected_remote
                    )
                    for action in actions
                }
                current_attempt = {(side, action.path) for action in actions}
                attempted_mutations.update(current_attempt)
                result = (
                    self.transport.delete_local(expected)
                    if action_type is ActionType.DELETE_LOCAL
                    else self.transport.delete_remote(expected)
                )
                changed.update(result.changed_during_transfer)
                known_unused.update(
                    (side, path) for path in result.changed_during_transfer
                )
                successful_mutations.update((side, path) for path in result.completed)
                current_attempt = set()
                if action_type is ActionType.DELETE_LOCAL:
                    observed = self.local_metadata.paths(
                        result.completed,
                        with_hash=False,
                        base=self.store.list_base(),
                    )
                else:
                    observed = self.remote.metadata_paths(result.completed)
                for path in result.completed:
                    if not isinstance(observed[path], MissingEntry):
                        raise RuntimeError(f"delete verification failed: {path}")
                    completed.add(path)
                    base_after[path] = MissingEntry(path)
                    expected_echoes[(side, path)] = MissingEntry(path)

            for action in plan.actions:
                if action.type is ActionType.UPDATE_BASE:
                    completed.add(action.path)
                    base_after[action.path] = action.base_after

            conflict_payloads = []
            for conflict in plan.conflicts:
                payload = self._prepare_conflict(conflict)
                if payload is None:
                    changed.add(conflict.path)
                else:
                    conflict_payloads.append(payload)
        except BaseException as exc:
            unused = known_unused | (set(intents) - attempted_mutations)
            if isinstance(exc, TransferPreflightError):
                unused.update(current_attempt)
            self._clear_echo_intents(intents, unused)
            raise
        clearable = (
            completed
            | {conflict.path for conflict in plan.conflicts}
            | {warning.path for warning in plan.warnings}
            | requeued_before
        ) - changed - deferred_deletes
        conflict_ids: list[str] = []
        acknowledge_events = not changed
        try:
            with self.store.transaction():
                for path in sorted(completed | adopted_changed):
                    state = base_after[path]
                    if isinstance(state, MissingEntry):
                        self.store.delete_base(path)
                    else:
                        self.store.upsert_base(state)
                    if path in completed:
                        self.store.resolve_conflicts_for_path(path)
                for (side, _path), fingerprint in sorted(expected_echoes.items()):
                    self.store.set_expected_echo(side, fingerprint)
                for echo in echoes:
                    self.store.consume_expected_echo(echo.side, echo.observed)
                for (side, path), fingerprint in intents.items():
                    if path in changed and path not in adopted_changed:
                        self.store.consume_expected_echo(side, fingerprint)
                for conflict, local_blob, remote_blob in conflict_payloads:
                    record = self._existing_conflict(conflict)
                    if record is None:
                        record = self.store.create_conflict(
                            path=conflict.path,
                            reason=conflict.reason,
                            local_blob=local_blob,
                            remote_blob=remote_blob,
                            local_fingerprint=_entry_or_none(conflict.local),
                            remote_fingerprint=_entry_or_none(conflict.remote),
                        )
                    conflict_ids.append(record.conflict_id)
                if changed:
                    self.store.requeue_paths(changed, "changed-during-transfer")
                if deferred_deletes:
                    self.store.requeue_paths(
                        deferred_deletes,
                        "descendant-conflict",
                    )
                self.store.clear_requeued_paths(clearable)
                if acknowledge_events:
                    acknowledge_pending(self.store, "local", local_events)
                    acknowledge_pending(self.store, "remote", remote_events)
                self._set_status(
                    (
                        WorkspacePhase.DEGRADED
                        if (
                            plan.conflicts
                            or plan.warnings
                            or self.store.list_conflicts(unresolved_only=True)
                        )
                        else WorkspacePhase.READY
                    ),
                    "idle",
                )
        except BaseException:
            self._clear_unused_echo_intents(intents, successful_mutations)
            raise

        self.audit_coordinator.refresh(completed)
        if acknowledge_events and remote_events:
            self.remote.acknowledge(remote_events[-1].sequence)
        return EngineResult(
            transferred=tuple(sorted(transferred)),
            completed=tuple(sorted(completed)),
            requeued=tuple(sorted(changed | deferred_deletes)),
            echoes=tuple(sorted({echo.path for echo in echoes})),
            conflict_ids=tuple(conflict_ids),
            warnings=plan.warnings,
        )

    def _conflicting_directory_deletes(self, plan: SyncPlan) -> set[str]:
        conflict_paths = {conflict.path for conflict in plan.conflicts}
        conflict_paths.update(
            conflict.path
            for conflict in self.store.list_conflicts(unresolved_only=True)
        )
        if not conflict_paths:
            return set()
        deferred: set[str] = set()
        for action in plan.actions:
            if action.type is ActionType.DELETE_LOCAL:
                destination = action.expected_local
            elif action.type is ActionType.DELETE_REMOTE:
                destination = action.expected_remote
            else:
                continue
            if not (
                isinstance(destination, EntryFingerprint)
                and destination.kind is EntryKind.DIR
            ):
                continue
            if any(_path_is_within(path, action.path) for path in conflict_paths):
                deferred.add(action.path)
        return deferred

    def _commit_noop(
        self,
        local_events: list[JournalEvent],
        remote_events: list[JournalEvent],
    ) -> EngineResult:
        with self.store.transaction():
            acknowledge_pending(self.store, "local", local_events)
            acknowledge_pending(self.store, "remote", remote_events)
            self._set_status(WorkspacePhase.READY, "idle")
        remote_sequence = self.store.acknowledged_sequence("remote")
        if remote_sequence:
            self.remote.acknowledge(remote_sequence)
        return EngineResult()

    def _commit_echoes(
        self,
        local_events: list[JournalEvent],
        remote_events: list[JournalEvent],
        echoes: tuple[_Echo, ...],
    ) -> EngineResult:
        with self.store.transaction():
            for echo in echoes:
                self.store.consume_expected_echo(echo.side, echo.observed)
            acknowledge_pending(self.store, "local", local_events)
            acknowledge_pending(self.store, "remote", remote_events)
            self._set_status(WorkspacePhase.READY, "idle")
        if remote_events:
            self.remote.acknowledge(remote_events[-1].sequence)
        return EngineResult(echoes=tuple(sorted({echo.path for echo in echoes})))

    def _matching_echoes(
        self,
        local_events: list[JournalEvent],
        remote_events: list[JournalEvent],
        local: dict[str, FingerprintState],
        remote: dict[str, FingerprintState],
        requeued: set[str],
    ) -> tuple[_Echo, ...]:
        candidates: list[tuple[str, str, FingerprintState]] = []
        for event in coalesce_events([*local_events, *remote_events]):
            if event.kind in {EventKind.MOVE, EventKind.RESCAN_REQUIRED} or event.path in requeued:
                continue
            expected = self.store.get_expected_echo(event.side, event.path)
            if expected is not None:
                candidates.append((event.side, event.path, expected))
        local_hashes = tuple(
            sorted(
                {
                    path
                    for side, path, expected in candidates
                    if side == "local" and _regular_file(expected)
                }
            )
        )
        remote_hashes = tuple(
            sorted(
                {
                    path
                    for side, path, expected in candidates
                    if side == "remote" and _regular_file(expected)
                }
            )
        )
        if local_hashes:
            local.update(
                self.local_metadata.paths(
                    local_hashes,
                    with_hash=True,
                    base=self.store.list_base(),
                )
            )
        if remote_hashes:
            remote.update(self.remote.hash_paths(remote_hashes))
        matches = []
        for side, path, expected in candidates:
            observed = local[path] if side == "local" else remote[path]
            if observed == expected:
                matches.append(_Echo(side, path, observed))
        return tuple(matches)

    def _prepare_conflict(
        self,
        conflict: ConflictDecision,
    ) -> tuple[ConflictDecision, bytes | None, bytes | None] | None:
        with ProtectedLocalRoot(self.local_root) as local_root:
            local_observed, local_blob = local_root.read_entry(conflict.path)
        remote_blob = self.remote.read_path(conflict.path)
        remote_observed = self.remote.hash_paths((conflict.path,))[conflict.path]
        if (
            not _capture_matches(conflict.local, local_observed)
            or not _capture_matches(conflict.remote, remote_observed)
            or not _blob_matches(local_observed, local_blob)
            or not _blob_matches(remote_observed, remote_blob)
        ):
            return None
        if local_blob is None and remote_blob is None:
            local_blob = b""
        captured = ConflictDecision(
            conflict.path,
            conflict.reason,
            local_observed,
            remote_observed,
        )
        return captured, local_blob, remote_blob

    def _existing_conflict(self, conflict: ConflictDecision) -> ConflictRecord | None:
        local = _entry_or_none(conflict.local)
        remote = _entry_or_none(conflict.remote)
        for record in self.store.list_conflicts(unresolved_only=True):
            if (
                record.path == conflict.path
                and record.reason == conflict.reason
                and record.local_fingerprint == local
                and record.remote_fingerprint == remote
            ):
                return record
        return None

    def _expected_echo_intents(
        self,
        plan: SyncPlan,
    ) -> dict[tuple[str, str], FingerprintState]:
        intents: dict[tuple[str, str], FingerprintState] = {}
        for action in plan.actions:
            side: str | None = None
            if action.type in {ActionType.PUSH, ActionType.DELETE_REMOTE}:
                side = "remote"
            elif action.type in {ActionType.PULL, ActionType.DELETE_LOCAL}:
                side = "local"
            if side is not None:
                intents[(side, action.path)] = action.base_after
        return intents

    def _commit_expected_echo_intents(
        self,
        intents: Mapping[tuple[str, str], FingerprintState],
    ) -> None:
        with self.store.transaction():
            for (side, _path), fingerprint in sorted(intents.items()):
                self.store.set_expected_echo(side, fingerprint)

    def _clear_unused_echo_intents(
        self,
        intents: Mapping[tuple[str, str], FingerprintState],
        successful: set[tuple[str, str]],
    ) -> None:
        with self.store.transaction():
            for (side, path), fingerprint in intents.items():
                if (side, path) not in successful:
                    self.store.consume_expected_echo(side, fingerprint)

    def _clear_echo_intents(
        self,
        intents: Mapping[tuple[str, str], FingerprintState],
        selected: set[tuple[str, str]],
    ) -> None:
        with self.store.transaction():
            for side, path in selected:
                fingerprint = intents.get((side, path))
                if fingerprint is not None:
                    self.store.consume_expected_echo(side, fingerprint)

    def _destination_hashes(
        self,
        direction: TransferDirection,
        paths: Iterable[str],
    ) -> dict[str, FingerprintState]:
        requested = tuple(paths)
        if not requested:
            return {}
        if direction is TransferDirection.PUSH:
            return self.remote.hash_paths(requested)
        return self.local_metadata.paths(
            requested,
            with_hash=True,
            base=self.store.list_base(),
        )

    def _adoptable_changed_destinations(
        self,
        direction: TransferDirection,
        actions: tuple[SyncAction, ...],
        paths: tuple[str, ...],
    ) -> dict[str, EntryFingerprint]:
        if not paths:
            return {}
        planned = {action.path: action.base_after for action in actions}
        adopted: dict[str, EntryFingerprint] = {}
        for path in paths:
            expected = planned.get(path)
            if not isinstance(expected, EntryFingerprint):
                continue
            try:
                observed = self._destination_hashes(direction, (path,))[path]
            except LocalPathChanged:
                continue
            if _matches_installed_snapshot(expected, observed):
                assert isinstance(observed, EntryFingerprint)
                adopted[path] = observed
        return adopted

    def _record_audit_drift(self) -> None:
        self.audit_coordinator.record_drift()

    def _set_status(self, phase: WorkspacePhase, stage: str) -> None:
        if self._status_override is not None:
            phase, stage = self._status_override
        pending = len(self.store.pending_events("local", 0))
        pending += len(self.store.pending_events("remote", 0))
        pending += len(self.store.list_requeued_paths())
        conflicts = len(self.store.list_conflicts(unresolved_only=True))
        self.store.set_status(
            WorkspaceStatus(
                phase,
                SyncProgress(stage),
                pending=pending,
                conflicts=conflicts,
                last_sync_at=(
                    time.time()
                    if phase in {WorkspacePhase.READY, WorkspacePhase.DEGRADED}
                    else None
                ),
            )
        )

    def _record_error(self, exc: BaseException) -> None:
        try:
            pending = len(self.store.pending_events("local", 0))
            pending += len(self.store.pending_events("remote", 0))
            pending += len(self.store.list_requeued_paths())
            conflicts = len(self.store.list_conflicts(unresolved_only=True))
            self.store.set_status(
                WorkspaceStatus(
                    WorkspacePhase.DEGRADED,
                    SyncProgress("error"),
                    pending=pending,
                    conflicts=conflicts,
                    last_error=str(exc),
                )
            )
        except BaseException:
            pass


def _regular_file(entry: FingerprintState) -> bool:
    return isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.FILE


def _entry_or_none(entry: FingerprintState) -> EntryFingerprint | None:
    return entry if isinstance(entry, EntryFingerprint) else None


def _path_is_within(path: str, directory: str) -> bool:
    return path == directory or path.startswith(f"{directory}/")


def _blob_matches(state: FingerprintState, blob: bytes | None) -> bool:
    if isinstance(state, MissingEntry):
        return blob is None
    if state.kind is EntryKind.FILE:
        return (
            blob is not None
            and state.size == len(blob)
            and state.content_hash is not None
            and hashlib.sha256(blob).hexdigest() == state.content_hash
        )
    if state.kind is EntryKind.SYMLINK:
        return blob is not None and os.fsdecode(blob) == state.link_target
    return blob in {None, b""}


def _capture_matches(planned: FingerprintState, observed: FingerprintState) -> bool:
    if planned == observed:
        return True
    return (
        isinstance(planned, EntryFingerprint)
        and isinstance(observed, EntryFingerprint)
        and planned.kind is EntryKind.FILE
        and observed.kind is EntryKind.FILE
        and planned.content_hash is None
        and planned.size == observed.size
        and planned.mtime_ns == observed.mtime_ns
        and planned.mode == observed.mode
    )


def _matches_installed_snapshot(
    planned: EntryFingerprint,
    observed: FingerprintState,
) -> bool:
    if not isinstance(observed, EntryFingerprint) or planned.kind is not observed.kind:
        return False
    if planned.is_placeholder or observed.is_placeholder:
        return False
    if planned.kind is EntryKind.FILE:
        return (
            planned.content_hash is not None
            and planned.content_hash == observed.content_hash
            and planned.size == observed.size
            and stat.S_IMODE(planned.mode or 0) == stat.S_IMODE(observed.mode or 0)
        )
    if planned.kind is EntryKind.SYMLINK:
        return planned.link_target == observed.link_target
    return False
