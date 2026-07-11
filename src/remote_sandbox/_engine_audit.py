from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, TypeAlias

from remote_sandbox._engine_metadata import LocalMetadata, quick_matches_base
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import PolicyEngine
from remote_sandbox.remote_client import RemoteSnapshot
from remote_sandbox.state import AuditSignature, WorkspaceStore

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry


class AuditRemote(Protocol):
    def snapshot(self) -> RemoteSnapshot: ...

    def hash_paths(self, paths: Iterable[str]) -> dict[str, FingerprintState]: ...

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


class AuditCoordinator:
    def __init__(
        self,
        *,
        store: WorkspaceStore,
        local: LocalMetadata,
        remote: AuditRemote,
        policy: PolicyEngine,
    ) -> None:
        self.store = store
        self.local = local
        self.remote = remote
        self.policy = policy

    def record_drift(self) -> None:
        base = self.store.list_base()
        local_entries, local_signatures = self.local.snapshot_with_signatures(base)
        remote_snapshot = self.remote.snapshot()
        remote_entries = remote_snapshot.entries
        paths = sorted(set(base) | set(local_entries) | set(remote_entries))
        stored_local = self.store.list_audit_signatures("local")
        stored_remote = self.store.list_audit_signatures("remote")
        dirty: set[str] = set()
        ambiguous_local: list[str] = []
        ambiguous_remote: list[str] = []

        for path in paths:
            if self.policy.is_ignored(path):
                continue
            base_entry = base.get(path)
            local_entry = local_entries.get(path, MissingEntry(path))
            remote_entry = remote_entries.get(path, MissingEntry(path))
            if not quick_matches_base(local_entry, base_entry) or not quick_matches_base(
                remote_entry, base_entry
            ):
                dirty.add(path)
                continue
            if base_entry is None or base_entry.kind is not EntryKind.FILE:
                continue
            if base_entry.is_placeholder:
                continue
            if stored_local.get(path) != local_signatures.get(path):
                ambiguous_local.append(path)
            if stored_remote.get(path) != remote_snapshot.signatures.get(path):
                ambiguous_remote.append(path)

        local_hashes = (
            self.local.paths(ambiguous_local, with_hash=True, base=base)
            if ambiguous_local
            else {}
        )
        remote_hashes = self.remote.hash_paths(ambiguous_remote) if ambiguous_remote else {}
        for path in ambiguous_local:
            if not _strong_matches_base(local_hashes[path], base.get(path)):
                dirty.add(path)
        for path in ambiguous_remote:
            if not _strong_matches_base(remote_hashes[path], base.get(path)):
                dirty.add(path)

        clean_local = {
            path: signature
            for path, signature in local_signatures.items()
            if path not in dirty and path in base
        }
        clean_remote = {
            path: signature
            for path, signature in remote_snapshot.signatures.items()
            if path not in dirty and path in base
        }
        with self.store.transaction():
            self.store.replace_audit_signatures("local", clean_local)
            self.store.replace_audit_signatures("remote", clean_remote)
            if dirty:
                self.store.requeue_paths(dirty, "audit-drift")

    def refresh(self, paths: Iterable[str]) -> None:
        normalized = tuple(sorted(set(paths)))
        if not normalized:
            return
        base = self.store.list_base()
        local_entries, local_signatures = self.local.observations(
            normalized,
            with_hash=True,
            base=base,
        )
        remote_entries, remote_signatures = self.remote.observations(
            normalized,
            with_hash=True,
        )
        local_updates: dict[str, AuditSignature | None] = {}
        remote_updates: dict[str, AuditSignature | None] = {}
        dirty: set[str] = set()
        for path in normalized:
            base_entry = base.get(path)
            if _observation_matches_base(local_entries[path], base_entry):
                local_updates[path] = local_signatures[path]
            else:
                dirty.add(path)
            if _observation_matches_base(remote_entries[path], base_entry):
                remote_updates[path] = remote_signatures[path]
            else:
                dirty.add(path)
        with self.store.transaction():
            self.store.update_audit_signatures("local", local_updates)
            self.store.update_audit_signatures("remote", remote_updates)
            if dirty:
                self.store.requeue_paths(dirty, "signature-refresh-mismatch")


def _strong_matches_base(
    current: FingerprintState,
    base: EntryFingerprint | None,
) -> bool:
    return (
        isinstance(current, EntryFingerprint)
        and base is not None
        and current.kind is EntryKind.FILE
        and base.kind is EntryKind.FILE
        and current.content_hash is not None
        and base.content_hash is not None
        and current.content_hash == base.content_hash
    )


def _observation_matches_base(
    current: FingerprintState,
    base: EntryFingerprint | None,
) -> bool:
    if base is not None and base.kind is EntryKind.FILE and not base.is_placeholder:
        return _strong_matches_base(current, base)
    return quick_matches_base(current, base)
