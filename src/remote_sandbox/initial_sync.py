from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, TypeAlias

from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.engine import EngineResult, SyncEngine
from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.placeholder import PlaceholderMetadata, decode_placeholder, encode_placeholder
from remote_sandbox.policy import PolicyDecision, PolicyEngine, ReplicaSide
from remote_sandbox.remote_client import RemoteSnapshot
from remote_sandbox.settings import DEFAULT_PLACEHOLDER_LIMIT
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferResult,
)

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry
WatcherStarter: TypeAlias = Callable[[], int | None]


class InitialSyncError(RuntimeError):
    pass


class InitialDirection(StrEnum):
    LOCAL_TO_REMOTE = "local-to-remote"
    REMOTE_TO_LOCAL = "remote-to-local"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class InitialSyncPlan:
    transfer_batch: TransferBatch | None
    placeholders: Mapping[str, EntryFingerprint]


@dataclass(frozen=True, slots=True)
class InitialSyncResult:
    direction: InitialDirection
    files: int
    bytes: int
    placeholders: int


class InitialRemote(Protocol):
    def snapshot(self) -> RemoteSnapshot: ...

    def hash_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]: ...

    def acknowledge(self, sequence: int) -> int: ...

    def events_after(self, after_sequence: int) -> list[JournalEvent]: ...

    def close(self) -> None: ...


class InitialTransport(Protocol):
    def transfer(
        self,
        batch: TransferBatch,
        on_progress: Callable[[TransferResult], None],
    ) -> TransferResult: ...


class InitialSyncCoordinator:
    def __init__(
        self,
        *,
        store: WorkspaceStore,
        local_root: Path,
        remote: InitialRemote,
        transport: InitialTransport,
        engine: SyncEngine,
        start_local_watcher: WatcherStarter,
        start_remote_watcher: WatcherStarter | None = None,
        placeholder_limit: int = DEFAULT_PLACEHOLDER_LIMIT,
        quiet_seconds: float = 0.5,
        poll_interval: float = 0.05,
        max_replay_seconds: float = 30.0,
        progress_interval: float = 0.25,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if placeholder_limit < 0:
            raise ValueError("placeholder_limit must be non-negative")
        if (
            quiet_seconds < 0
            or poll_interval <= 0
            or max_replay_seconds <= 0
            or progress_interval < 0.25
        ):
            raise ValueError("initial sync timing values are invalid")
        self.store = store
        self.local_root = local_root.expanduser().resolve()
        self.remote = remote
        self.transport = transport
        self.engine = engine
        self.policy = engine.policy
        self.start_local_watcher = start_local_watcher
        self.start_remote_watcher = start_remote_watcher or self._start_remote
        self.placeholder_limit = placeholder_limit
        self.quiet_seconds = quiet_seconds
        self.poll_interval = poll_interval
        self.max_replay_seconds = max_replay_seconds
        self.progress_interval = progress_interval
        self._clock = clock
        self._sleep = sleep
        self._started_at = 0.0
        self._last_progress_at = 0.0
        self._scan_files = 0
        self._scan_bytes = 0

    def run(self) -> InitialSyncResult:
        self._started_at = self._clock()
        self._scan_files = 0
        self._scan_bytes = 0
        try:
            self._publish("scanning")
            sampled_local_start = self.start_local_watcher() or 0
            sampled_remote_start = self.start_remote_watcher() or 0
            checkpoint = self.store.get_initial_sync_watermarks()
            if checkpoint is None:
                self.store.set_initial_sync_watermarks(
                    sampled_local_start,
                    sampled_remote_start,
                )
                local_start = sampled_local_start
                remote_start = sampled_remote_start
                self._discard_preexisting_events(local_start, remote_start)
            else:
                local_start, remote_start = checkpoint
            local_snapshot = self._local_snapshot()
            remote_snapshot = self._remote_snapshot()

            self._publish("planning")
            direction = choose_initial_direction(
                local_snapshot,
                remote_snapshot,
                store=self.store,
            )
            plan = build_initial_plan(
                direction,
                local_snapshot,
                remote_snapshot,
                policy=self.policy,
                placeholder_limit=self.placeholder_limit,
                remote=self.remote,
                base=self.store.list_base(),
            )
            dirty_during_scan = self._changes_since(local_start, remote_start)
            if dirty_during_scan:
                plan = _exclude_changed_paths(plan, dirty_during_scan)
                self.engine.requeue_paths(
                    dirty_during_scan,
                    "changed-during-initial-scan",
                )

            completed: set[str] = set()
            changed: set[str] = set()
            batch = plan.transfer_batch
            self._publish(
                "transferring",
                files_total=len(batch.items) if batch is not None else 0,
                bytes_total=(
                    sum(_item_size(item) for item in batch.items)
                    if batch is not None
                    else 0
                ),
            )
            if batch is not None:
                def on_progress(progress: TransferResult) -> None:
                    newly_completed = set(progress.completed) - completed
                    if newly_completed:
                        self.engine.seed_base_from_transfer(batch, newly_completed)
                    completed.update(progress.completed)
                    changed.update(progress.changed_during_transfer)
                    current = progress.completed[-1] if progress.completed else None
                    self._publish_progress(
                        "transferring",
                        files_done=len(completed),
                        files_total=len(batch.items),
                        bytes_done=sum(
                            _item_size(item)
                            for item in batch.items
                            if item.path in completed
                        ),
                        bytes_total=sum(_item_size(item) for item in batch.items),
                        current_path=current,
                        force=len(completed) == len(batch.items),
                    )

                result = self.transport.transfer(batch, on_progress)
                remaining_completed = set(result.completed) - completed
                if remaining_completed:
                    self.engine.seed_base_from_transfer(batch, remaining_completed)
                completed.update(result.completed)
                changed.update(result.changed_during_transfer)
                if changed:
                    self.engine.requeue_paths(changed, "changed-during-initial-transfer")

            if plan.placeholders:
                self._write_placeholders(plan.placeholders)
                self.engine.apply_initial_placeholders(plan.placeholders)

            self._replay_until_quiet()
            self.store.complete_initial_sync(
                self._status("ready", phase=WorkspacePhase.READY)
            )
            return InitialSyncResult(
                direction,
                len(batch.items) if batch is not None else 0,
                sum(_item_size(item) for item in batch.items) if batch is not None else 0,
                len(plan.placeholders),
            )
        except BaseException as exc:
            self._record_failure(exc)
            raise

    def _start_remote(self) -> int:
        starter = getattr(self.remote, "start_watcher", None)
        if starter is None:
            raise InitialSyncError("remote watcher starter is unavailable")
        payload = starter()
        if isinstance(payload, int):
            return payload
        if isinstance(payload, dict):
            sequence = payload.get("latest_sequence", 0)
            if type(sequence) is int and sequence >= 0:
                return sequence
        return 0

    def _discard_preexisting_events(self, local_start: int, remote_start: int) -> None:
        if local_start:
            self.store.acknowledge("local", local_start)
        if remote_start:
            self.remote.acknowledge(remote_start)

    def _local_snapshot(self) -> dict[str, EntryFingerprint]:
        with ProtectedLocalRoot(self.local_root) as root:
            paths = root.walk_paths(self.policy.is_ignored)
            entries: dict[str, EntryFingerprint] = {}
            bytes_seen = 0
            for index, path in enumerate(paths, start=1):
                entry = root.fingerprint(path, with_hash=False)
                if isinstance(entry, EntryFingerprint):
                    if entry.kind is EntryKind.FILE and (entry.size or 0) <= 8_192:
                        _observed, content = root.read_entry(path)
                        try:
                            placeholder = decode_placeholder(content or b"", expected_path=path)
                        except ValueError as exc:
                            raise InitialSyncError(
                                f"invalid local placeholder metadata: {path}"
                            ) from exc
                        if placeholder is not None:
                            entry = EntryFingerprint(
                                path,
                                EntryKind.FILE,
                                placeholder.size,
                                placeholder.mtime_ns,
                                entry.mode,
                                content_hash=placeholder.content_hash,
                                is_placeholder=True,
                            )
                    entries[path] = entry
                    bytes_seen += entry.size or 0
                self._scan_files = index
                self._scan_bytes = bytes_seen
                self._publish_progress(
                    "scanning",
                    files_done=index,
                    bytes_done=bytes_seen,
                    force=index == len(paths),
                )
            return entries

    def _remote_snapshot(self) -> dict[str, EntryFingerprint]:
        snapshot = self.remote.snapshot()
        entries = {
            path: entry
            for path, entry in snapshot.entries.items()
            if isinstance(entry, EntryFingerprint) and not self.policy.is_ignored(path)
        }
        bytes_seen = sum(entry.size or 0 for entry in entries.values())
        self._publish_progress(
            "scanning",
            files_done=self._scan_files + len(entries),
            bytes_done=self._scan_bytes + bytes_seen,
            force=True,
        )
        return entries

    def _write_placeholders(self, placeholders: Mapping[str, EntryFingerprint]) -> None:
        with tempfile.TemporaryDirectory(prefix="remote-sandbox-placeholders-") as temporary:
            staging = Path(temporary)
            paths = tuple(sorted(placeholders))
            for path in paths:
                entry = placeholders[path]
                if entry.size is None or entry.mtime_ns is None or entry.content_hash is None:
                    raise InitialSyncError(f"placeholder fingerprint is incomplete: {path}")
                destination = staging / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                content = encode_placeholder(
                    PlaceholderMetadata(path, entry.size, entry.mtime_ns, entry.content_hash)
                )
                destination.write_bytes(content)
                if decode_placeholder(content, expected_path=path) is None:
                    raise InitialSyncError(f"placeholder validation failed: {path}")
            with ProtectedLocalRoot(self.local_root) as root:
                root.finalize(staging, paths, error_type=InitialSyncError)

    def _changes_since(self, local_start: int, remote_start: int) -> set[str]:
        self._sleep(self.poll_interval)
        imported = self.remote.events_after(remote_start)
        self.store.record_events(imported)
        events = [
            *self.store.pending_events("local", local_start),
            *self.store.pending_events("remote", remote_start),
        ]
        dirty: set[str] = set()
        for event in events:
            if event.kind is EventKind.RESCAN_REQUIRED:
                continue
            dirty.add(event.path)
            if event.destination_path is not None:
                dirty.add(event.destination_path)
        return dirty

    def _replay_until_quiet(self) -> None:
        self._publish("replaying")
        deadline = self._clock() + self.max_replay_seconds
        quiet_since: float | None = None
        while self._clock() < deadline:
            result = self.engine.run_once("initial-replay")
            self._publish("replaying")
            if _engine_did_work(result) or self.store.list_requeued_paths():
                quiet_since = None
            elif quiet_since is None:
                quiet_since = self._clock()
            elif self._clock() - quiet_since >= self.quiet_seconds:
                return
            self._sleep(self.poll_interval)
        raise InitialSyncError("initial replay did not reach a quiet window")

    def _publish(
        self,
        stage: str,
        *,
        phase: WorkspacePhase = WorkspacePhase.INITIAL_SYNCING,
        files_done: int = 0,
        files_total: int = 0,
        bytes_done: int = 0,
        bytes_total: int = 0,
        current_path: str | None = None,
    ) -> None:
        self.store.set_status(
            self._status(
                stage,
                phase=phase,
                files_done=files_done,
                files_total=files_total,
                bytes_done=bytes_done,
                bytes_total=bytes_total,
                current_path=current_path,
            )
        )

    def _status(
        self,
        stage: str,
        *,
        phase: WorkspacePhase,
        files_done: int = 0,
        files_total: int = 0,
        bytes_done: int = 0,
        bytes_total: int = 0,
        current_path: str | None = None,
    ) -> WorkspaceStatus:
        return WorkspaceStatus(
            phase,
            SyncProgress(
                stage,
                files_done=files_done,
                files_total=files_total,
                bytes_done=bytes_done,
                bytes_total=bytes_total,
                current_path=current_path,
                elapsed_seconds=max(0.0, self._clock() - self._started_at),
            ),
            pending=_pending_count(self.store),
            conflicts=len(self.store.list_conflicts(unresolved_only=True)),
            last_sync_at=time.time() if phase is WorkspacePhase.READY else None,
        )

    def _publish_progress(
        self,
        stage: str,
        *,
        files_done: int = 0,
        files_total: int = 0,
        bytes_done: int = 0,
        bytes_total: int = 0,
        current_path: str | None = None,
        force: bool = False,
    ) -> None:
        now = self._clock()
        if not force and now - self._last_progress_at < self.progress_interval:
            return
        self._last_progress_at = now
        self._publish(
            stage,
            files_done=files_done,
            files_total=files_total,
            bytes_done=bytes_done,
            bytes_total=bytes_total,
            current_path=current_path,
        )

    def _record_failure(self, exc: BaseException) -> None:
        try:
            current = self.store.get_status()
            self.store.set_status(
                WorkspaceStatus(
                    WorkspacePhase.DEGRADED,
                    current.progress,
                    pending=_pending_count(self.store),
                    conflicts=len(self.store.list_conflicts(unresolved_only=True)),
                    last_error=str(exc),
                )
            )
        except BaseException:
            pass


def choose_initial_direction(
    local: Mapping[str, EntryFingerprint],
    remote: Mapping[str, EntryFingerprint],
    *,
    store: WorkspaceStore | None = None,
) -> InitialDirection:
    if not local and not remote:
        return InitialDirection.EMPTY
    if local and not remote:
        return InitialDirection.LOCAL_TO_REMOTE
    if remote and not local:
        return InitialDirection.REMOTE_TO_LOCAL
    if store is not None:
        for path in sorted(store.list_base()):
            if store.get_expected_echo("remote", path) is not None:
                return InitialDirection.LOCAL_TO_REMOTE
            if store.get_expected_echo("local", path) is not None:
                return InitialDirection.REMOTE_TO_LOCAL
    raise InitialSyncError("refusing initial sync for two non-empty replicas")


def build_initial_plan(
    direction: InitialDirection,
    local: Mapping[str, EntryFingerprint],
    remote_snapshot: Mapping[str, EntryFingerprint],
    *,
    policy: PolicyEngine,
    placeholder_limit: int,
    remote: InitialRemote,
    base: Mapping[str, EntryFingerprint] | None = None,
) -> InitialSyncPlan:
    if direction is InitialDirection.EMPTY:
        return InitialSyncPlan(None, {})
    source = local if direction is InitialDirection.LOCAL_TO_REMOTE else remote_snapshot
    destination = remote_snapshot if direction is InitialDirection.LOCAL_TO_REMOTE else local
    persisted = base or {}
    placeholder_paths: tuple[str, ...] = ()
    if direction is InitialDirection.REMOTE_TO_LOCAL:
        placeholder_paths = tuple(
            sorted(
                path
                for path, entry in source.items()
                if entry.kind is EntryKind.FILE
                and (
                    (entry.size or 0) > placeholder_limit
                    or policy.classify(entry, side=ReplicaSide.REMOTE)
                    is PolicyDecision.PLACEHOLDER
                )
            )
        )
    strong_placeholders = remote.hash_paths(placeholder_paths) if placeholder_paths else {}
    placeholders: dict[str, EntryFingerprint] = {}
    for path in placeholder_paths:
        entry = strong_placeholders[path]
        if not isinstance(entry, EntryFingerprint) or entry.content_hash is None:
            raise InitialSyncError(f"remote placeholder source changed: {path}")
        placeholders[path] = EntryFingerprint(
            path,
            entry.kind,
            entry.size,
            entry.mtime_ns,
            entry.mode,
            link_target=entry.link_target,
            content_hash=entry.content_hash,
            is_placeholder=True,
        )

    items = []
    for path, entry in source.items():
        if entry.kind is EntryKind.SPECIAL or path in placeholders:
            continue
        if direction is InitialDirection.LOCAL_TO_REMOTE and entry.is_placeholder:
            continue
        destination_entry: FingerprintState = destination.get(path, MissingEntry(path))
        base_entry = persisted.get(path)
        if base_entry is not None and _quick_matches(entry, base_entry) and _quick_matches(
            destination_entry, base_entry
        ):
            continue
        items.append(TransferItem(path, entry, destination_entry))
    items.sort(
        key=lambda item: (
            not isinstance(item.expected_source, EntryFingerprint)
            or item.expected_source.kind is not EntryKind.DIR,
            item.path,
        )
    )
    if not items:
        return InitialSyncPlan(None, placeholders)
    transfer_direction = (
        TransferDirection.PUSH
        if direction is InitialDirection.LOCAL_TO_REMOTE
        else TransferDirection.PULL
    )
    return InitialSyncPlan(TransferBatch(transfer_direction, tuple(items)), placeholders)


def _quick_matches(observed: FingerprintState, expected: EntryFingerprint) -> bool:
    if not isinstance(observed, EntryFingerprint) or observed.kind is not expected.kind:
        return False
    if observed.kind is EntryKind.FILE:
        return (
            observed.size == expected.size
            and observed.mtime_ns == expected.mtime_ns
            and observed.mode == expected.mode
        )
    if observed.kind is EntryKind.SYMLINK:
        return observed.link_target == expected.link_target
    return observed.mode == expected.mode


def _exclude_changed_paths(
    plan: InitialSyncPlan,
    changed: set[str],
) -> InitialSyncPlan:
    placeholders = {
        path: entry for path, entry in plan.placeholders.items() if path not in changed
    }
    if plan.transfer_batch is None:
        return InitialSyncPlan(None, placeholders)
    items = tuple(item for item in plan.transfer_batch.items if item.path not in changed)
    batch = (
        TransferBatch(plan.transfer_batch.direction, items)
        if items
        else None
    )
    return InitialSyncPlan(batch, placeholders)


def _item_size(item: TransferItem) -> int:
    source = item.expected_source
    return (source.size or 0) if isinstance(source, EntryFingerprint) else 0


def _engine_did_work(result: EngineResult) -> bool:
    return any(
        (
            result.transferred,
            result.completed,
            result.requeued,
            result.echoes,
            result.conflict_ids,
            result.warnings,
        )
    )


def _pending_count(store: WorkspaceStore) -> int:
    return (
        len(store.pending_events("local", 0))
        + len(store.pending_events("remote", 0))
        + len(store.list_requeued_paths())
    )
