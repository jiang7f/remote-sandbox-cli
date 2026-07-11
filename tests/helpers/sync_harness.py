from __future__ import annotations

import hashlib
import multiprocessing
import os
import shutil
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import remote_sandbox._transport_fingerprint as transport_fingerprint_module
import remote_sandbox.manifest as manifest_module
from remote_sandbox.cli import (
    CapturedCliResult,
    CliServices,
    ConnectedWorkspace,
    RemoteCommandResult,
    invoke_cli,
)
from remote_sandbox.conflicts import resolve_conflict_transaction
from remote_sandbox.daemon import (
    SupervisorClient,
    SupervisorRuntime,
    WorkspaceSupervisor,
)
from remote_sandbox.engine import SyncEngine
from remote_sandbox.fetch import fetch_placeholders as fetch_placeholder_transaction
from remote_sandbox.initial_sync import InitialSyncCoordinator
from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    fingerprint_local,
    normalize_relative_path,
    workspace_path,
)
from remote_sandbox.peek import peek_placeholder as peek_placeholder_transaction
from remote_sandbox.placeholder import PlaceholderMetadata, encode_placeholder
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.registry import (
    BindingRecord,
    delete_binding_record,
    find_binding_record,
    list_binding_records,
    now_iso,
    upsert_binding_record,
)
from remote_sandbox.remote_agent.store import RemoteStore
from remote_sandbox.remote_client import RemoteSnapshot
from remote_sandbox.shell import ConnectResponse, ManagedShellSession
from remote_sandbox.state import AuditSignature, WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import (
    LocalPairTransport,
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferResult,
)
from remote_sandbox.watch import PollingLocalWatcher


@dataclass(slots=True)
class FakePtyBackend:
    remote_shell_pid: int


@dataclass(slots=True)
class FakeManagedPtySession:
    remote_shell_pid: int
    output: str
    prompt_mode: str
    _session: ManagedShellSession

    def type(self, text: str) -> None:
        self._session.feed_user_input(text.encode())

    def accept_binding(self) -> None:
        self._session.handle_connect_response(
            ConnectResponse(
                ok=True,
                workspace_id="w1",
                name="dq",
                remote_root="/work/dq",
                direction="remote-to-local",
            )
        )
        self.output = self._session.captured_output()
        self.prompt_mode = self._session.prompt_mode

    def reject_binding(self, error: str) -> None:
        self._session.handle_connect_response(ConnectResponse(ok=False, error=error))
        self.output = self._session.captured_output()
        self.prompt_mode = self._session.prompt_mode

    def connect(self, *, direction: str, remote_root: str) -> None:
        self._session.activate_workspace(
            ConnectResponse(
                ok=True,
                workspace_id="w1",
                name="dq",
                remote_root=remote_root,
                direction=direction,
            ),
            direction=direction,
        )

    def publish_ready(self) -> None:
        self._session.publish_ready()

    @property
    def remote_cwd(self) -> str:
        return self._session.remote_cwd


@dataclass(slots=True)
class FakePtyBackendHarness:
    def open_enter_shell(self) -> FakeManagedPtySession:
        backend = FakePtyBackend(remote_shell_pid=4242)
        session = ManagedShellSession(backend=backend, nonce="test-nonce")
        return FakeManagedPtySession(4242, "", "enter", session)


@dataclass(slots=True)
class PromptShellHarness:
    session: ManagedShellSession

    def type_without_enter(self, text: str) -> None:
        self.session.feed_user_input(text.encode("utf-8"))

    def move_cursor_left(self, count: int) -> None:
        self.session.feed_user_input(b"\x02" * count)

    def submit(self, text: str) -> None:
        self.session.feed_user_input(text.encode("utf-8") + b"\n")

    def publish_progress(self, percent: int) -> None:
        self.session.publish_status(
            WorkspaceStatus(
                WorkspacePhase.INITIAL_SYNCING,
                SyncProgress("transferring", files_done=percent, files_total=100),
            ),
            now=percent / 100.0,
        )

    def publish_prompt(self) -> None:
        self.session.publish_prompt()

    @property
    def visible_input(self) -> str:
        return self.session.readline_buffer

    @property
    def cursor_offset(self) -> int:
        return self.session.readline_cursor

    @property
    def current_prompt(self) -> str:
        return self.session.rendered_prompt

    @property
    def redraw_count(self) -> int:
        return self.session.redraw_count


def make_prompt_shell_harness() -> PromptShellHarness:
    backend = FakePtyBackend(remote_shell_pid=4242)
    session = ManagedShellSession(
        backend=backend,
        nonce="prompt-test-nonce",
        target="ZJU_2",
    )
    session.activate_workspace(
        ConnectResponse(
            ok=True,
            workspace_id="00000000-0000-4000-8000-000000000015",
            name="dq",
            remote_root="/work/dq",
            direction="remote-to-local",
        ),
        direction="remote-to-local",
    )
    session.publish_status(
        WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("scanning")),
        now=0.0,
    )
    return PromptShellHarness(session)


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
        self.fail_after_progress_count: int | None = None
        self._progress_count = 0

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
            if not isinstance(destination_after, EntryFingerprint):
                raise RuntimeError(f"verified destination is missing for {item.path}")
            on_progress(  # type: ignore[operator]
                TransferResult((item.path,), (), (destination_after,))
            )
            self._progress_count += 1
            if not self._progress_triggered and self.on_first_progress is not None:
                self._progress_triggered = True
                self.on_first_progress()
            if self.fail_after_first_progress:
                self.fail_after_first_progress = False
                raise RuntimeError("injected transfer interruption")
            if self.fail_after_progress_count == self._progress_count:
                self.fail_after_progress_count = None
                raise RuntimeError("injected transfer interruption")
        verified = tuple(
            entry
            for path in completed
            if isinstance(
                (entry := fingerprint_local(destination, path, with_hash=True)),
                EntryFingerprint,
            )
        )
        return TransferResult(tuple(completed), tuple(changed), verified)

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


class CountingLocalPairTransport(LocalPairTransport):
    def __init__(self, local: Path, remote: Path) -> None:
        super().__init__(local, remote, engine="rsync")
        self.process_count = 0
        self.progress_payload_sizes: list[int] = []

    def transfer(
        self,
        batch: TransferBatch,
        on_progress: Callable[[TransferResult], None],
    ) -> TransferResult:
        def record_progress(progress: TransferResult) -> None:
            self.progress_payload_sizes.append(len(progress.completed))
            on_progress(progress)

        real_popen = subprocess.Popen

        def counted_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
            self.process_count += 1
            return real_popen(*args, **kwargs)

        with patch.object(subprocess, "Popen", side_effect=counted_popen):
            return super().transfer(batch, record_progress)


@dataclass(slots=True)
class SqlTransactionCounter:
    commits: int = 0
    measurement_started_at: float | None = None
    first_progress_seconds: float | None = None

    def trace(self, statement: str) -> None:
        normalized = statement.lstrip().upper()
        if normalized.startswith("COMMIT"):
            self.commits += 1
        if (
            self.measurement_started_at is not None
            and self.first_progress_seconds is None
            and "INSERT INTO WORKSPACE_STATUS" in normalized
        ):
            self.first_progress_seconds = time.monotonic() - self.measurement_started_at

    def start_measurement(self) -> None:
        self.measurement_started_at = time.monotonic()
        self.first_progress_seconds = None


class CountingHashProvider:
    def __init__(self) -> None:
        self._local_total = 0
        self._remote_total = 0
        self._local_baseline = 0
        self._remote_baseline = 0
        self._original_local = transport_fingerprint_module._hash_descriptor
        self._original_remote = manifest_module._sha256_file

        def count_local(descriptor: int) -> str:
            self._local_total += 1
            return self._original_local(descriptor)

        def count_remote(path: Path) -> str:
            self._remote_total += 1
            return self._original_remote(path)

        self._local_wrapper = count_local
        self._remote_wrapper = count_remote
        transport_fingerprint_module._hash_descriptor = count_local
        manifest_module._sha256_file = count_remote

    @property
    def local_count(self) -> int:
        return self._local_total - self._local_baseline

    @property
    def remote_count(self) -> int:
        return self._remote_total - self._remote_baseline

    @property
    def count(self) -> int:
        return self.local_count + self.remote_count

    def reset(self) -> None:
        self._local_baseline = self._local_total
        self._remote_baseline = self._remote_total

    def close(self) -> None:
        if transport_fingerprint_module._hash_descriptor is self._local_wrapper:
            transport_fingerprint_module._hash_descriptor = self._original_local
        if manifest_module._sha256_file is self._remote_wrapper:
            manifest_module._sha256_file = self._original_remote


@dataclass(slots=True)
class PerformancePair(SyncPair):
    transport: CountingLocalPairTransport
    coordinator: InitialSyncCoordinator
    hash_counter: CountingHashProvider
    direct: Path
    transaction_counter: SqlTransactionCounter

    def close(self) -> None:
        self.store._connection.set_trace_callback(None)
        self.hash_counter.close()
        self.remote_client.close()
        self.store.close()

    def populate(self, files: int) -> None:
        if files <= 0:
            raise ValueError("performance population must be positive")
        for index in range(files):
            path = self.local / f"files/{index // 100:03d}/file-{index:05d}.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = f"{index:08d}".encode("ascii").ljust(128, b"x")
            path.write_bytes(payload)

    def initial_sync(self) -> None:
        self.coordinator.run()

    def initial_transfer_batch(self) -> TransferBatch:
        entries = snapshot_tree(self.local, with_hash=False)
        return TransferBatch(
            TransferDirection.PUSH,
            tuple(
                TransferItem(path, entry, MissingEntry(path))
                for path, entry in entries.items()
            ),
        )

    def measure_direct_rsync(self, destination: Path | None = None) -> float:
        target = destination or self.direct
        shutil.rmtree(target, ignore_errors=True)
        target.mkdir(parents=True)
        started = time.monotonic()
        subprocess.run(
            ["rsync", "-a", f"{self.local}/", f"{target}/"],
            check=True,
            capture_output=True,
            timeout=120.0,
        )
        return time.monotonic() - started

    def measure_batch_transport(
        self,
        batch: TransferBatch,
        destination: Path,
    ) -> tuple[float, int, int]:
        shutil.rmtree(destination, ignore_errors=True)
        destination.mkdir(parents=True)
        transport = CountingLocalPairTransport(self.local, destination)
        started = time.monotonic()
        result = transport.transfer(batch, lambda _progress: None)
        elapsed = time.monotonic() - started
        assert result.completed == tuple(item.path for item in batch.items)
        return (
            elapsed,
            transport.process_count,
            max(transport.progress_payload_sizes),
        )

    def measure_initial_sync(self) -> float:
        self.transaction_counter.start_measurement()
        assert self.transaction_counter.measurement_started_at is not None
        started = self.transaction_counter.measurement_started_at
        self.initial_sync()
        return time.monotonic() - started

    def assert_final_base_and_echoes(self) -> None:
        local = snapshot_tree(self.local, with_hash=True)
        remote = snapshot_tree(self.remote, with_hash=True)
        assert set(local) == set(remote)
        assert all(
            _content_identity(local[path]) == _content_identity(remote[path])
            for path in local
        )
        assert self.store.list_base() == remote
        assert all(
            self.store.get_expected_echo("remote", path) == entry
            for path, entry in remote.items()
        )

    def wait_until_synced(self, remote_path: Path, timeout: float = 5.0) -> None:
        relative = remote_path.relative_to(self.remote).as_posix()
        self.remote_client.append_event(EventKind.MODIFY, relative)
        self.engine.run_once("performance-remote-change")
        self._wait_until(
            lambda: (self.local / relative).is_file(),
            f"local replica of {relative}",
            timeout,
        )

    def wait_until_missing(self, local_path: Path, timeout: float = 5.0) -> None:
        relative = local_path.relative_to(self.local).as_posix()
        self.remote_client.append_event(EventKind.DELETE, relative)
        self.engine.run_once("performance-remote-delete")
        self._wait_until(
            lambda: not local_path.exists(),
            f"local deletion of {relative}",
            timeout,
        )

    def _wait_until(
        self,
        predicate: Callable[[], bool],
        label: str,
        timeout: float,
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        status = self.store.get_status()
        raise AssertionError(
            f"timed out waiting for {label}. Last status was "
            f"{status.phase.value}/{status.progress.stage}, error={status.error!r}"
        )


@dataclass(slots=True)
class CliHarness:
    pair: SyncPair
    store: WorkspaceStore
    registry: Path
    services: CliServices
    record: BindingRecord
    cleanup_calls: list[str]
    _command_result: RemoteCommandResult
    _followup_error: str | None = None
    _remote_forget_error: str | None = None
    _registry_delete_failures: int = 0
    _fast_initial_sync: bool = False
    _next_connection_created: bool = True

    def run(self, argv: list[str]) -> CapturedCliResult:
        return invoke_cli(argv, services=self.services)

    def create_conflict(
        self,
        *,
        path: str,
        base: bytes,
        local: bytes,
        remote: bytes,
    ) -> object:
        normalized = normalize_relative_path(path)
        for root, content in ((self.pair.local, base), (self.pair.remote, base)):
            target = workspace_path(root, normalized)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        self.pair.seed_current_base()
        workspace_path(self.pair.local, normalized).write_bytes(local)
        workspace_path(self.pair.remote, normalized).write_bytes(remote)
        return self.store.create_conflict(
            path=normalized,
            reason="both-modified",
            local_blob=local,
            remote_blob=remote,
            local_fingerprint=fingerprint_local(self.pair.local, normalized, with_hash=True),
            remote_fingerprint=fingerprint_local(self.pair.remote, normalized, with_hash=True),
        )

    def local_bytes(self, path: str) -> bytes:
        return workspace_path(self.pair.local, path).read_bytes()

    def remote_bytes(self, path: str) -> bytes:
        return workspace_path(self.pair.remote, path).read_bytes()

    def remote_command_result(
        self,
        *,
        returncode: int,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self._command_result = RemoteCommandResult(returncode, stdout, stderr)

    def followup_sync_fails(self, message: str) -> None:
        self._followup_error = message

    def remote_forget_fails(self, message: str) -> None:
        self._remote_forget_error = message

    def registry_delete_fails_once(self) -> None:
        self._registry_delete_failures = 1

    def registry_has(self, name: str) -> bool:
        return find_binding_record(name, self.registry) is not None

    def set_workspace_state(self, name: str, phase: str, *, error: str | None = None) -> None:
        assert name == self.record.name
        self.store.set_status(
            WorkspaceStatus(
                WorkspacePhase(phase),
                SyncProgress(phase),
                last_error=error,
            )
        )

    def create_remote_placeholder(self, path: str, content: bytes) -> None:
        normalized = normalize_relative_path(path)
        remote_path = workspace_path(self.pair.remote, normalized)
        remote_path.parent.mkdir(parents=True, exist_ok=True)
        remote_path.write_bytes(content)
        remote = fingerprint_local(self.pair.remote, normalized, with_hash=True)
        assert isinstance(remote, EntryFingerprint)
        assert remote.size is not None
        assert remote.mtime_ns is not None
        assert remote.content_hash is not None
        local_path = workspace_path(self.pair.local, normalized)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(
            encode_placeholder(
                PlaceholderMetadata(
                    normalized,
                    remote.size,
                    remote.mtime_ns,
                    remote.content_hash,
                )
            )
        )
        self.store.upsert_base(
            EntryFingerprint(
                normalized,
                EntryKind.FILE,
                remote.size,
                remote.mtime_ns,
                remote.mode,
                content_hash=remote.content_hash,
                is_placeholder=True,
            )
        )

    def block_initial_sync(self) -> None:
        return

    def complete_initial_sync_immediately(self) -> None:
        self._fast_initial_sync = True

    def reconnect_existing_workspace(self) -> None:
        self._next_connection_created = False
        self.store.mark_initial_sync_completed()
        self.store.set_status(WorkspaceStatus(WorkspacePhase.READY, SyncProgress("ready")))

    def change_selected_source_before_transfer(self, path: str) -> None:
        self.pair.transport.change_source_before_commit(path)


def make_cli_harness(tmp_path: Path) -> CliHarness:
    pair = make_sync_pair(tmp_path)
    registry = tmp_path / "home" / "connections.toml"
    record = BindingRecord(
        name="dq",
        workspace_id=str(uuid.uuid4()),
        target="host",
        remote_path="/work/dq",
        local_path=str(pair.local),
        updated_at=now_iso(),
    )
    upsert_binding_record(registry, record)
    cleanup_calls: list[str] = []
    command_result = RemoteCommandResult(0)

    harness: CliHarness

    def request_sync(_record: BindingRecord) -> bool:
        if harness._followup_error is not None:
            raise RuntimeError(harness._followup_error)
        return True

    def run_remote(_record: BindingRecord, _argv: tuple[str, ...]) -> RemoteCommandResult:
        return harness._command_result

    def connect_workspace(
        target: str,
        remote: str,
        local: Path,
        name: str | None,
    ) -> ConnectedWorkspace:
        assert target == "host"
        assert remote == "/work/dq"
        assert local.resolve() == pair.local.resolve()
        assert name in {None, "dq"}
        created = harness._next_connection_created
        generation = pair.store.initial_sync_started_generation()
        if created:
            pair.store.publish_initial_sync_started(
                WorkspaceStatus(
                    WorkspacePhase.INITIAL_SYNCING,
                    SyncProgress("initial-syncing"),
                )
            )
        if created and harness._fast_initial_sync:
            pair.store.complete_initial_sync(
                WorkspaceStatus(WorkspacePhase.READY, SyncProgress("ready"))
            )
        return ConnectedWorkspace(record, created, generation)

    def wait_initial_sync(
        _record: BindingRecord,
        generation: int,
    ) -> WorkspaceStatus:
        if pair.store.initial_sync_started_generation() <= generation:
            raise RuntimeError("initial sync acknowledgement was not published")
        return pair.store.get_status()

    def fetch_registered(
        _record: BindingRecord,
        path: str | None,
        fetch_all: bool,
        confirm: Callable[[str], bool],
    ) -> tuple[int, bool]:
        return fetch_placeholder_transaction(
            local_root=pair.local,
            store=pair.store,
            remote=pair.remote_client,
            transport=pair.transport,
            path=path,
            fetch_all=fetch_all,
            confirm=confirm,
        )

    def peek_registered(
        _record: BindingRecord,
        path: str,
        lines: int,
        tail: bool,
    ) -> bytes:
        return peek_placeholder_transaction(
            local_root=pair.local,
            store=pair.store,
            remote=pair.remote_client,
            path=path,
            lines=lines,
            tail=tail,
        )

    def resolve_registered(_record: BindingRecord, path: str, use_local: bool) -> None:
        resolve_conflict_transaction(
            store=pair.store,
            local_root=pair.local,
            remote=pair.remote_client,
            transport=pair.transport,
            path=path,
            use_local=use_local,
        )

    def stop_local(_record: BindingRecord) -> None:
        cleanup_calls.append("stop-local-supervisor")

    def stop_remote(_record: BindingRecord) -> None:
        cleanup_calls.append("stop-remote-watcher")
        if harness._remote_forget_error is not None:
            raise RuntimeError(harness._remote_forget_error)

    def delete_remote(_record: BindingRecord) -> None:
        cleanup_calls.append("delete-remote-workspace")

    def prune_remote(_record: BindingRecord) -> None:
        cleanup_calls.append("prune-unused-remote-agent")

    def delete_local(_record: BindingRecord) -> None:
        cleanup_calls.append("delete-local-workspace")

    def delete_registry(record_to_delete: BindingRecord) -> None:
        cleanup_calls.append("delete-registry-record")
        if harness._registry_delete_failures:
            harness._registry_delete_failures -= 1
            raise RuntimeError("registry busy")
        if not delete_binding_record(
            record_to_delete.name,
            registry,
            workspace_id=record_to_delete.workspace_id,
        ):
            raise RuntimeError(
                f"Connection {record_to_delete.name} changed during cleanup"
            )

    services = CliServices(
        registry=registry,
        cwd=lambda: pair.local,
        list_records=lambda path: list_binding_records(path),
        find_record=lambda name, path: find_binding_record(name, path),
        current_record=lambda path, cwd: next(
            (
                candidate
                for candidate in list_binding_records(path)
                if cwd.resolve().is_relative_to(Path(candidate.local_path).resolve())
            ),
            None,
        ),
        workspace_status=lambda _record: pair.store.get_status(),
        daemon_status=lambda _record: pair.store.get_status(),
        ensure_supervisor=lambda _record: pair.store.get_status(),
        request_sync=request_sync,
        run_remote=run_remote,
        connect_workspace=connect_workspace,
        wait_initial_sync=wait_initial_sync,
        fetch_placeholders=fetch_registered,
        peek_placeholder=peek_registered,
        list_conflicts=lambda _record: pair.store.list_conflicts(unresolved_only=True),
        resolve_conflict=resolve_registered,
        stop_local_supervisor=stop_local,
        stop_remote_watcher=stop_remote,
        delete_remote_workspace=delete_remote,
        prune_remote_agent=prune_remote,
        delete_local_workspace=delete_local,
        delete_registry_record=delete_registry,
    )
    harness = CliHarness(
        pair,
        pair.store,
        registry,
        services,
        record,
        cleanup_calls,
        command_result,
    )
    return harness


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
        self.start_watcher_calls = 0
        self.subscribe_calls = 0
        self.closed = False

    def ensure_agent(self) -> None:
        return

    def start_watcher(self) -> None:
        self.start_watcher_calls += 1

    def subscribe(self, after_sequence: int) -> Iterator[JournalEvent]:
        del after_sequence
        self.subscribe_calls += 1
        return iter(())

    def close(self) -> None:
        self.closed = True

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
        self.active = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def run_once(self, reason: str) -> None:
        self.reasons.append(reason)
        self.active.set()
        try:
            self.release.wait(timeout=2.0)
            self.store.set_status(WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle")))
        finally:
            self.active.clear()


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

    def mutate(kind: str, payload: dict[str, object]) -> dict[str, object]:
        if kind != "resolve":
            raise ValueError(f"unsupported test mutation: {kind}")
        path = payload.get("path")
        use_local = payload.get("use_local")
        if not isinstance(path, str) or type(use_local) is not bool:
            raise ValueError("resolve mutation payload is malformed")
        conflict = resolve_conflict_transaction(
            store=store,
            local_root=config.local,
            remote=remote,
            transport=transport,
            path=path,
            use_local=use_local,
        )
        return {"path": conflict.path, "conflict_id": conflict.conflict_id}

    supervisor = WorkspaceSupervisor(
        config.runtime,
        store=store,
        initial_sync=ForbiddenInitialSync(config.initial_sync_marker),
        remote=remote,
        engine=OrderedEngine(engine, config.order_log),
        local_watcher=ProcessLocalWatcher(config, store),
        mutation_handler=mutate,
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


def make_performance_pair(tmp_path: Path) -> PerformancePair:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    direct = tmp_path / "direct"
    local.mkdir()
    remote.mkdir()
    direct.mkdir()
    store = WorkspaceStore.open(tmp_path / "state.sqlite3")
    remote_client = LocalReplicaClient(remote, tmp_path / "remote-state.sqlite3")
    transport = CountingLocalPairTransport(local, remote)
    engine = SyncEngine(
        store=store,
        local_root=local,
        remote=remote_client,
        transport=transport,
        policy=StaticPolicyEngine(),
    )
    coordinator = InitialSyncCoordinator(
        store=store,
        local_root=local,
        remote=remote_client,
        transport=transport,
        engine=engine,
        start_local_watcher=lambda: store.latest_sequence("local"),
        start_remote_watcher=remote_client.latest_sequence,
        quiet_seconds=0.0,
        poll_interval=0.001,
    )
    transaction_counter = SqlTransactionCounter()
    store._connection.set_trace_callback(transaction_counter.trace)
    return PerformancePair(
        local,
        remote,
        store,
        remote_client,
        transport,
        engine,
        coordinator,
        CountingHashProvider(),
        direct,
        transaction_counter,
    )


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
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copystat(source, destination, follow_symlinks=False)
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
