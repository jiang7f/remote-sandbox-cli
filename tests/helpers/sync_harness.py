from __future__ import annotations

import hashlib
import multiprocessing
import os
import shutil
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Literal

from remote_sandbox.daemon import (
    SupervisorClient,
    SupervisorRuntime,
    WorkspaceSupervisor,
)
from remote_sandbox.engine import SyncEngine
from remote_sandbox.initial_sync import InitialSyncCoordinator
from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    fingerprint_local,
)
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.remote_agent.store import RemoteStore
from remote_sandbox.remote_client import RemoteSnapshot
from remote_sandbox.state import AuditSignature, WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferResult,
)
from remote_sandbox.watch import PollingLocalWatcher

FingerprintState = EntryFingerprint | MissingEntry


class LocalReplicaClient:
    def __init__(
        self,
        root: Path,
        database: Path,
        *,
        order_log: Path | None = None,
        subscription_gate: Path | None = None,
        local_state_db: Path | None = None,
        ack_commit_marker: Path | None = None,
    ) -> None:
        self.root = root
        self._store = RemoteStore(database)
        self._store.register_workspace("test-workspace", root)
        self._order_log = order_log
        self._subscription_gate = subscription_gate
        self._local_state_db = local_state_db
        self._ack_commit_marker = ack_commit_marker
        self._subscriptions: set[LocalReplicaSubscription] = set()
        self.metadata_calls: list[tuple[str, ...]] = []
        self.hash_calls: list[tuple[str, ...]] = []
        self.snapshot_calls = 0
        self.acknowledge_calls: list[int] = []
        self.on_before_snapshot: Callable[[], object] | None = None

    def close(self) -> None:
        for subscription in tuple(self._subscriptions):
            subscription.close()
        self._store.close()

    def ensure_agent(self) -> None:
        return

    def start_watcher(self) -> dict[str, int]:
        _append_order(self._order_log, "remote-watcher")
        return {"latest_sequence": self.latest_sequence()}

    def subscribe(self, after_sequence: int) -> LocalReplicaSubscription:
        subscription = LocalReplicaSubscription(
            self,
            after_sequence,
            order_log=self._order_log,
            gate=self._subscription_gate,
        )
        self._subscriptions.add(subscription)
        return subscription

    def clear_master(self) -> None:
        return

    def probe_connection(self) -> Literal["ok", "auth", "network"]:
        return "ok"

    def append_event(
        self,
        kind: EventKind,
        path: str,
        destination_path: str | None = None,
    ) -> None:
        self._store.append_event(kind.value, path, destination_path)

    def events_after(self, sequence: int) -> list[JournalEvent]:
        return [
            JournalEvent(
                "remote",
                event.sequence,
                EventKind(event.kind),
                event.path,
                event.destination_path,
            )
            for event in self._store.events_after(sequence)
        ]

    def acknowledge(self, sequence: int) -> int:
        if self._local_state_db is not None and self._ack_commit_marker is not None:
            pending = [event for event in self._store.events_after(0) if event.sequence <= sequence]
            with WorkspaceStore.open(self._local_state_db) as local_store:
                base = local_store.list_base()
            committed = all(event.path in base for event in pending)
            self._ack_commit_marker.write_text("1" if committed else "0", encoding="utf-8")
        self.acknowledge_calls.append(sequence)
        self._store.acknowledge(sequence)
        return self._store.acknowledged_sequence()

    def acknowledged_sequence(self) -> int:
        return self._store.acknowledged_sequence()

    def metadata_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]:
        requested = tuple(paths)
        self.metadata_calls.append(requested)
        return {path: fingerprint_local(self.root, path, with_hash=False) for path in requested}

    def hash_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]:
        requested = tuple(paths)
        self.hash_calls.append(requested)
        return {path: fingerprint_local(self.root, path, with_hash=True) for path in requested}

    def snapshot(self) -> RemoteSnapshot:
        self.snapshot_calls += 1
        if self.on_before_snapshot is not None:
            callback = self.on_before_snapshot
            self.on_before_snapshot = None
            callback()
        entries = snapshot_tree(self.root, with_hash=False)
        signatures = {
            path: signature
            for path in entries
            if (signature := _audit_signature(self.root, path)) is not None
        }
        return RemoteSnapshot(entries, self._store.latest_sequence(), signatures)

    def latest_sequence(self) -> int:
        return self._store.latest_sequence()

    def _discard_subscription(self, subscription: LocalReplicaSubscription) -> None:
        self._subscriptions.discard(subscription)

    def audit_signatures(
        self,
        paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]:
        return {path: _audit_signature(self.root, path) for path in paths}

    def observations(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
    ) -> tuple[dict[str, FingerprintState], dict[str, AuditSignature | None]]:
        requested = tuple(paths)
        if with_hash:
            self.hash_calls.append(requested)
        else:
            self.metadata_calls.append(requested)
        return (
            {
                path: fingerprint_local(self.root, path, with_hash=with_hash)
                for path in requested
            },
            {path: _audit_signature(self.root, path) for path in requested},
        )

    def read_path(self, path: str) -> bytes | None:
        entry = fingerprint_local(self.root, path, with_hash=False)
        if isinstance(entry, MissingEntry):
            return None
        candidate = self.root / path
        if entry.kind is EntryKind.FILE:
            return candidate.read_bytes()
        if entry.kind is EntryKind.SYMLINK:
            return candidate.readlink().as_posix().encode()
        return None


class LocalReplicaSubscription:
    def __init__(
        self,
        client: LocalReplicaClient,
        after_sequence: int,
        *,
        order_log: Path | None,
        gate: Path | None,
    ) -> None:
        self._client = client
        self._after_sequence = after_sequence
        self._order_log = order_log
        self._gate = gate
        self._closed = threading.Event()

    def __iter__(self) -> Iterator[JournalEvent]:
        _append_order(self._order_log, "subscription")
        while not self._closed.wait(0.01):
            if self._gate is not None and not self._gate.exists():
                continue
            events = self._client.events_after(self._after_sequence)
            for event in events:
                if self._closed.is_set():
                    return
                self._after_sequence = event.sequence
                yield event

    def close(self) -> None:
        self._closed.set()
        self._client._discard_subscription(self)


class ControllableLocalPairTransport:
    def __init__(self, local: Path, remote: Path) -> None:
        self.local = local
        self.remote = remote
        self.transfer_calls = 0
        self.batches: list[TransferBatch] = []
        self._mutate_before_commit: set[str] = set()
        self._mutate_before_delete: dict[tuple[str, str], bytes] = {}
        self.before_destination_change: Callable[[str, str], None] | None = None
        self.on_first_progress: Callable[[], object] | None = None
        self.operation_order: OperationOrder | None = None
        self.transfer_call_number: int | None = None
        self._progress_triggered = False
        self.fail_after_first_progress = False

    def change_source_before_commit(self, path: str) -> None:
        self._mutate_before_commit.add(path)

    def change_destination_before_delete(self, side: str, path: str, content: bytes) -> None:
        self._mutate_before_delete[(side, path)] = content

    def transfer(self, batch: TransferBatch, on_progress: object) -> TransferResult:
        self.transfer_calls += 1
        if self.operation_order is not None:
            self.transfer_call_number = self.operation_order.record("transfer")
        self.batches.append(batch)
        source, destination = (
            (self.local, self.remote)
            if batch.direction is TransferDirection.PUSH
            else (self.remote, self.local)
        )
        completed: list[str] = []
        changed: list[str] = []
        for item in batch.items:
            before = fingerprint_local(source, item.path, with_hash=True)
            destination_before = fingerprint_local(destination, item.path, with_hash=True)
            if not _matches_expected(item.expected_source, before) or not _matches_expected(
                item.expected_destination,
                destination_before,
            ):
                changed.append(item.path)
                continue
            if item.path in self._mutate_before_commit:
                candidate = source / item.path
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_bytes(candidate.read_bytes() + b"-changed")
            after = fingerprint_local(source, item.path, with_hash=True)
            if after != before:
                changed.append(item.path)
                continue
            if self.before_destination_change is not None:
                destination_side = (
                    "remote" if batch.direction is TransferDirection.PUSH else "local"
                )
                self.before_destination_change(destination_side, item.path)
            _copy_entry(source / item.path, destination / item.path)
            destination_after = fingerprint_local(destination, item.path, with_hash=True)
            if _content_identity(before) != _content_identity(destination_after):
                raise RuntimeError(f"verification failed for {item.path}")
            completed.append(item.path)
            on_progress(TransferResult(tuple(completed), ()))  # type: ignore[operator]
            if not self._progress_triggered and self.on_first_progress is not None:
                self._progress_triggered = True
                self.on_first_progress()
            if self.fail_after_first_progress:
                self.fail_after_first_progress = False
                raise RuntimeError("injected transfer interruption")
        return TransferResult(tuple(completed), tuple(changed))

    def delete_local(
        self,
        expected: Mapping[str, FingerprintState],
    ) -> TransferResult:
        return self._delete_expected("local", self.local, expected)

    def delete_remote(
        self,
        expected: Mapping[str, FingerprintState],
    ) -> TransferResult:
        return self._delete_expected("remote", self.remote, expected)

    def _delete_expected(
        self,
        side: str,
        root: Path,
        expected: Mapping[str, FingerprintState],
    ) -> TransferResult:
        completed: list[str] = []
        changed: list[str] = []
        for path, expected_entry in expected.items():
            replacement = self._mutate_before_delete.get((side, path))
            if replacement is not None:
                candidate = root / path
                _remove(candidate)
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_bytes(replacement)
            observed = fingerprint_local(root, path, with_hash=True)
            if observed != expected_entry:
                changed.append(path)
                continue
            if self.before_destination_change is not None:
                self.before_destination_change(side, path)
            _remove(root / path)
            completed.append(path)
        return TransferResult(tuple(completed), tuple(changed))


@dataclass(slots=True)
class EngineHarness:
    local: Path
    remote: Path
    store: WorkspaceStore
    transport: ControllableLocalPairTransport
    remote_client: LocalReplicaClient
    engine: SyncEngine

    def append_local_modify(self, path: str, content: bytes) -> None:
        destination = self.local / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.store.append_event("local", EventKind.MODIFY, path)

    def append_remote_event_for_current_fingerprint(self, path: str) -> None:
        self.remote_client.append_event(EventKind.MODIFY, path)


@dataclass(slots=True)
class SyncPair:
    local: Path
    remote: Path
    store: WorkspaceStore
    remote_client: LocalReplicaClient
    transport: ControllableLocalPairTransport
    engine: SyncEngine

    def seed_current_base(self) -> None:
        entries = snapshot_matching_replicas(self.local, self.remote, with_hash=True)
        self.store.replace_base(entries)
        self.engine.audit_coordinator.refresh(entries)

    def append_local_modify(self, path: str, content: bytes) -> None:
        destination = self.local / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.store.append_event("local", EventKind.MODIFY, path)

    def append_remote_delete(self, path: str) -> None:
        (self.remote / path).unlink()
        self.remote_client.append_event(EventKind.DELETE, path)


class OperationOrder:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sequence = 0
        self.calls: list[tuple[int, str]] = []

    def record(self, label: str) -> int:
        with self._lock:
            self._sequence += 1
            self.calls.append((self._sequence, label))
            return self._sequence


class RecordingWatcher:
    def __init__(
        self,
        order: OperationOrder,
        label: str,
        root: Path,
        on_event: Callable[[EventKind, str, str | None], None],
        latest_sequence: Callable[[], int],
        transport: ControllableLocalPairTransport,
    ) -> None:
        self.order = order
        self.label = label
        self.transport = transport
        self._root = root
        self._on_event = on_event
        self._latest_sequence = latest_sequence
        self._watcher: PollingLocalWatcher | None = None
        self.start_call_number: int | None = None

    def start(self) -> int:
        self.start_call_number = self.order.record(self.label)
        sequence = self._latest_sequence()
        if self._watcher is None:
            self._watcher = PollingLocalWatcher(
                self._root,
                StaticPolicyEngine(),
                self._on_event,
                interval=0.001,
            )
        self._watcher.start()
        return sequence

    def stop(self) -> None:
        if self._watcher is not None:
            self._watcher.stop()

    @property
    def started_before_transfer(self) -> bool:
        return (
            self.start_call_number is not None
            and self.transport.transfer_call_number is not None
            and self.start_call_number < self.transport.transfer_call_number
        )


@dataclass(slots=True)
class InitialPairHarness:
    local: Path
    remote: Path
    store: WorkspaceStore
    local_watcher: RecordingWatcher
    remote_watcher: RecordingWatcher
    remote_client: LocalReplicaClient
    transport: ControllableLocalPairTransport
    engine: SyncEngine
    coordinator: InitialSyncCoordinator

    def set_placeholder_limit(self, value: int) -> None:
        self.coordinator.placeholder_limit = value

    def close(self) -> None:
        self.local_watcher.stop()
        self.remote_watcher.stop()
        self.remote_client.close()
        self.store.close()


class BlockingInitialSync:
    def __init__(self) -> None:
        self._scan_allowed = threading.Event()

    def block_before_scan(self) -> None:
        self._scan_allowed.clear()

    def run(self) -> None:
        self._scan_allowed.wait(timeout=2.0)

    def unblock(self) -> None:
        self._scan_allowed.set()


class ControllableRemoteClient:
    def __init__(self) -> None:
        self.probe_result: Literal["ok", "auth", "network"] = "ok"
        self.failure = RuntimeError("subscription failed")
        self.clear_master_calls = 0

    def ensure_agent(self) -> None:
        return

    def start_watcher(self) -> None:
        return

    def subscribe(self, after_sequence: int) -> Iterator[JournalEvent]:
        del after_sequence
        return iter(())

    def close(self) -> None:
        return

    def raise_auth_failure(self) -> None:
        self.probe_result = "auth"
        self.failure = RuntimeError("password authentication required")

    def raise_network_failure(self) -> None:
        self.probe_result = "network"
        self.failure = RuntimeError("network unavailable")

    def raise_watcher_crash(self) -> None:
        self.probe_result = "ok"
        self.failure = RuntimeError("remote watcher crashed")

    def clear_master(self) -> None:
        self.clear_master_calls += 1

    def probe_connection(self) -> Literal["ok", "auth", "network"]:
        return self.probe_result


@dataclass(slots=True)
class SupervisorHarness:
    store: WorkspaceStore
    remote: ControllableRemoteClient
    initial_sync: BlockingInitialSync
    engine: RecordingEngine
    supervisor: WorkspaceSupervisor
    client: SupervisorClient
    thread: threading.Thread | None = None

    def start_in_thread(self) -> None:
        self.thread = threading.Thread(target=self.supervisor.run, daemon=True)
        self.thread.start()
        self.client.wait_until_running(timeout=2.0)

    def close(self) -> None:
        self.initial_sync.unblock()
        self.client.stop()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
        self.store.close()

    def publish_live_pid_without_socket(self) -> None:
        self.supervisor.runtime.metadata_root.mkdir(parents=True, exist_ok=True)
        self.supervisor.runtime.pidfile.write_text(f"{os.getpid()}\n", encoding="utf-8")
        self.supervisor.runtime.socket.unlink(missing_ok=True)


class RecordingEngine:
    def __init__(self, store: WorkspaceStore) -> None:
        self.store = store
        self.reasons: list[str] = []

    def run_once(self, reason: str) -> None:
        self.reasons.append(reason)
        self.store.set_status(WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle")))


@dataclass(frozen=True, slots=True)
class DaemonProcessConfig:
    local: Path
    remote: Path
    state_db: Path
    remote_state_db: Path
    runtime: SupervisorRuntime
    order_log: Path
    subscription_gate: Path
    ack_commit_marker: Path
    initial_sync_marker: Path


class ProcessLocalWatcher:
    def __init__(self, config: DaemonProcessConfig, store: WorkspaceStore) -> None:
        self._config = config
        self._store = store
        self._watcher: PollingLocalWatcher | None = None
        self.last_error: BaseException | None = None

    def start(self) -> None:
        _append_order(self._config.order_log, "local-watcher")
        if self._watcher is None:
            self._watcher = PollingLocalWatcher(
                self._config.local,
                StaticPolicyEngine(),
                lambda kind, path, destination: self._store.append_event(
                    "local", kind, path, destination
                ),
                interval=0.01,
            )
        self._watcher.start()

    def stop(self) -> None:
        if self._watcher is not None:
            self._watcher.stop()
            self.last_error = self._watcher.last_error


class OrderedEngine:
    def __init__(self, engine: SyncEngine, order_log: Path) -> None:
        self._engine = engine
        self._order_log = order_log

    def run_once(self, reason: str) -> object:
        _append_order(self._order_log, f"engine:{reason}")
        return self._engine.run_once(reason)


class ForbiddenInitialSync:
    def __init__(self, marker: Path) -> None:
        self._marker = marker

    def run(self) -> None:
        self._marker.write_text("called", encoding="utf-8")
        raise AssertionError("initial sync repeated after durable completion")


def _run_daemon_pair_process(config: DaemonProcessConfig) -> None:
    store = WorkspaceStore.open(config.state_db)
    remote = LocalReplicaClient(
        config.remote,
        config.remote_state_db,
        order_log=config.order_log,
        subscription_gate=config.subscription_gate,
        local_state_db=config.state_db,
        ack_commit_marker=config.ack_commit_marker,
    )
    transport = ControllableLocalPairTransport(config.local, config.remote)
    engine = SyncEngine(
        store=store,
        local_root=config.local,
        remote=remote,
        transport=transport,
        policy=StaticPolicyEngine(),
    )
    supervisor = WorkspaceSupervisor(
        config.runtime,
        store=store,
        initial_sync=ForbiddenInitialSync(config.initial_sync_marker),
        remote=remote,
        engine=OrderedEngine(engine, config.order_log),
        local_watcher=ProcessLocalWatcher(config, store),
        close_store=True,
    )
    supervisor.run()


@dataclass(slots=True)
class DaemonPairHarness:
    local: Path
    remote: Path
    store: WorkspaceStore
    client: SupervisorClient
    remote_client: LocalReplicaClient
    process: BaseProcess
    runtime: SupervisorRuntime
    config: DaemonProcessConfig

    def append_remote_change(self, path: str, content: bytes) -> None:
        destination = self.remote / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.remote_client.append_event(EventKind.MODIFY, path)

    def kill_local_daemon(self) -> None:
        self.process.terminate()
        self.process.join(timeout=2.0)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=2.0)
        if self.process.exitcode in {None, 0}:
            raise AssertionError("daemon process did not terminate abruptly")
        self.config.order_log.write_text("", encoding="utf-8")

    def start_local_daemon(self) -> None:
        self.config.subscription_gate.write_text("open", encoding="utf-8")
        self.config.ack_commit_marker.unlink(missing_ok=True)
        self.process = _start_daemon_process(self.config)
        self.client.wait_until_running(timeout=2.0)

    def wait_until_ready(self) -> None:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if self.client.status().phase is WorkspacePhase.READY:
                _wait_for_order(self.config.order_log, "subscription", timeout=2.0)
                return
            time.sleep(0.01)
        raise AssertionError("supervisor did not become ready")

    def close(self) -> None:
        if self.process.is_alive():
            self.client.stop()
            self.process.join(timeout=2.0)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=2.0)
        self.remote_client.close()
        self.store.close()

    @property
    def restart_order(self) -> list[str]:
        return _read_order(self.config.order_log)

    @property
    def ack_after_commit(self) -> bool:
        return self.config.ack_commit_marker.read_text(encoding="utf-8") == "1"

    @property
    def initial_sync_repeated(self) -> bool:
        return self.config.initial_sync_marker.exists()


def make_supervisor_harness(tmp_path: Path) -> SupervisorHarness:
    store = WorkspaceStore.open(tmp_path / "state.sqlite3")
    initial_sync = BlockingInitialSync()
    remote = ControllableRemoteClient()
    engine = RecordingEngine(store)
    supervisor = WorkspaceSupervisor.for_test(
        workspace_id="00000000-0000-4000-8000-000000000013",
        metadata_root=tmp_path / "metadata",
        store=store,
        initial_sync=initial_sync,
        remote=remote,
        engine=engine,
    )
    client = SupervisorClient(supervisor.runtime)
    return SupervisorHarness(store, remote, initial_sync, engine, supervisor, client)


def _append_order(path: Path | None, label: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{label}\n")


def _read_order(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


def _wait_for_order(path: Path, label: str, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if label in _read_order(path):
            return
        time.sleep(0.01)
    raise AssertionError(f"daemon process did not record {label}")


def _start_daemon_process(config: DaemonProcessConfig) -> BaseProcess:
    process = multiprocessing.get_context("spawn").Process(
        target=_run_daemon_pair_process,
        args=(config,),
        daemon=True,
    )
    process.start()
    return process


def make_daemon_pair(tmp_path: Path) -> DaemonPairHarness:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    metadata_root = tmp_path / "metadata"
    state_db = metadata_root / "state.sqlite3"
    remote_state_db = tmp_path / "remote-state.sqlite3"
    runtime_key = hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12]
    runtime = SupervisorRuntime(
        "00000000-0000-4000-8000-000000000113",
        metadata_root,
        Path("/tmp") / f"codex-rsb-process-{runtime_key}",
    )
    runtime.pidfile.unlink(missing_ok=True)
    runtime.socket.unlink(missing_ok=True)
    store = WorkspaceStore.open(state_db)
    store.set_initial_sync_watermarks(0, 0)
    store.complete_initial_sync(
        WorkspaceStatus(WorkspacePhase.READY, SyncProgress("ready"), last_sync_at=time.time())
    )
    remote_client = LocalReplicaClient(remote, remote_state_db)
    config = DaemonProcessConfig(
        local=local,
        remote=remote,
        state_db=state_db,
        remote_state_db=remote_state_db,
        runtime=runtime,
        order_log=tmp_path / "restart-order.log",
        subscription_gate=tmp_path / "subscription-open",
        ack_commit_marker=tmp_path / "ack-after-commit",
        initial_sync_marker=tmp_path / "initial-sync-repeated",
    )
    for path in (
        config.order_log,
        config.subscription_gate,
        config.ack_commit_marker,
        config.initial_sync_marker,
    ):
        path.unlink(missing_ok=True)
    process = _start_daemon_process(config)
    client = SupervisorClient(runtime)
    client.wait_until_running(timeout=2.0)
    client.wait_for_phase(WorkspacePhase.READY, timeout=5.0)
    _wait_for_order(config.order_log, "subscription", timeout=2.0)
    return DaemonPairHarness(
        local,
        remote,
        store,
        client,
        remote_client,
        process,
        runtime,
        config,
    )


def make_engine_harness(tmp_path: Path) -> EngineHarness:
    pair = make_sync_pair(tmp_path)
    return EngineHarness(
        pair.local,
        pair.remote,
        pair.store,
        pair.transport,
        pair.remote_client,
        pair.engine,
    )


def make_sync_pair(tmp_path: Path) -> SyncPair:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    store = WorkspaceStore.open(tmp_path / "state.sqlite3")
    remote_client = LocalReplicaClient(remote, tmp_path / "remote-state.sqlite3")
    transport = ControllableLocalPairTransport(local, remote)
    engine = SyncEngine(
        store=store,
        local_root=local,
        remote=remote_client,
        transport=transport,
        policy=StaticPolicyEngine(),
    )
    return SyncPair(local, remote, store, remote_client, transport, engine)


def make_initial_pair(tmp_path: Path) -> InitialPairHarness:
    pair = make_sync_pair(tmp_path)
    order = OperationOrder()
    pair.transport.operation_order = order
    local_watcher = RecordingWatcher(
        order,
        "local-watcher",
        pair.local,
        lambda kind, path, destination: pair.store.append_event(
            "local", kind, path, destination
        ),
        lambda: pair.store.latest_sequence("local"),
        pair.transport,
    )
    remote_watcher = RecordingWatcher(
        order,
        "remote-watcher",
        pair.remote,
        lambda kind, path, destination: pair.remote_client.append_event(
            kind, path, destination
        ),
        pair.remote_client.latest_sequence,
        pair.transport,
    )
    coordinator = InitialSyncCoordinator(
        store=pair.store,
        local_root=pair.local,
        remote=pair.remote_client,
        transport=pair.transport,
        engine=pair.engine,
        start_local_watcher=local_watcher.start,
        start_remote_watcher=remote_watcher.start,
        quiet_seconds=0.05,
        poll_interval=0.005,
    )
    return InitialPairHarness(
        pair.local,
        pair.remote,
        pair.store,
        local_watcher,
        remote_watcher,
        pair.remote_client,
        pair.transport,
        pair.engine,
        coordinator,
    )


def snapshot_tree(root: Path, *, with_hash: bool) -> dict[str, EntryFingerprint]:
    paths = sorted(
        candidate.relative_to(root).as_posix()
        for candidate in root.rglob("*")
        if ".remote-sandbox" not in candidate.parts
    )
    return {
        path: entry
        for path in paths
        if isinstance(
            (entry := fingerprint_local(root, path, with_hash=with_hash)),
            EntryFingerprint,
        )
    }


def snapshot_matching_replicas(
    local: Path,
    remote: Path,
    *,
    with_hash: bool,
) -> dict[str, EntryFingerprint]:
    local_entries = snapshot_tree(local, with_hash=with_hash)
    remote_entries = snapshot_tree(remote, with_hash=with_hash)
    assert set(local_entries) == set(remote_entries)
    return local_entries


def _copy_entry(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    _remove(destination)
    if source.is_symlink():
        destination.symlink_to(source.readlink())
    elif source.is_dir():
        shutil.copytree(source, destination, symlinks=True)
    else:
        shutil.copy2(source, destination, follow_symlinks=False)


def _remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def _content_identity(entry: FingerprintState) -> tuple[object, ...]:
    if isinstance(entry, MissingEntry):
        return ("missing",)
    if entry.kind is EntryKind.FILE:
        return (entry.kind, entry.content_hash)
    if entry.kind is EntryKind.SYMLINK:
        return (entry.kind, entry.link_target)
    return (entry.kind,)


def _matches_expected(expected: FingerprintState | None, observed: FingerprintState) -> bool:
    if expected is None or expected == observed:
        return True
    return (
        isinstance(expected, EntryFingerprint)
        and isinstance(observed, EntryFingerprint)
        and expected.kind is EntryKind.FILE
        and observed.kind is EntryKind.FILE
        and expected.content_hash is None
        and expected.size == observed.size
        and expected.mtime_ns == observed.mtime_ns
        and expected.mode == observed.mode
    )


def _audit_signature(root: Path, path: str) -> AuditSignature | None:
    candidate = root / path
    try:
        metadata = candidate.lstat()
    except FileNotFoundError:
        return None
    entry = fingerprint_local(root, path, with_hash=False)
    if not isinstance(entry, EntryFingerprint):
        return None
    return AuditSignature(
        path,
        entry.kind,
        metadata.st_ctime_ns,
        metadata.st_dev,
        metadata.st_ino,
    )
