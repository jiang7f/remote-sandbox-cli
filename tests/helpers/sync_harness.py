from __future__ import annotations

import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.engine import SyncEngine
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
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferResult,
)

FingerprintState = EntryFingerprint | MissingEntry


class LocalReplicaClient:
    def __init__(self, root: Path, database: Path) -> None:
        self.root = root
        self._store = RemoteStore(database)
        self._store.register_workspace("test-workspace", root)
        self.metadata_calls: list[tuple[str, ...]] = []
        self.hash_calls: list[tuple[str, ...]] = []
        self.snapshot_calls = 0
        self.acknowledge_calls: list[int] = []

    def close(self) -> None:
        self._store.close()

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
        entries = snapshot_tree(self.root, with_hash=False)
        signatures = {
            path: signature
            for path in entries
            if (signature := _audit_signature(self.root, path)) is not None
        }
        return RemoteSnapshot(entries, self._store.latest_sequence(), signatures)

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


class ControllableLocalPairTransport:
    def __init__(self, local: Path, remote: Path) -> None:
        self.local = local
        self.remote = remote
        self.transfer_calls = 0
        self.batches: list[TransferBatch] = []
        self._mutate_before_commit: set[str] = set()
        self._mutate_before_delete: dict[tuple[str, str], bytes] = {}
        self.before_destination_change: Callable[[str, str], None] | None = None

    def change_source_before_commit(self, path: str) -> None:
        self._mutate_before_commit.add(path)

    def change_destination_before_delete(self, side: str, path: str, content: bytes) -> None:
        self._mutate_before_delete[(side, path)] = content

    def transfer(self, batch: TransferBatch, on_progress: object) -> TransferResult:
        self.transfer_calls += 1
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
