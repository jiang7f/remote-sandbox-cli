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
        stored_local = self.store.list_audit_signatures("local")
        stored_remote = self.store.list_audit_signatures("remote")
        paths = sorted(
            set(base)
            | set(local_entries)
            | set(remote_entries)
            | set(stored_local)
            | set(stored_remote)
        )
        dirty: set[str] = set()
        ambiguous_local: list[str] = []
        ambiguous_remote: list[str] = []
        local_updates: dict[str, AuditSignature | None] = {}
        remote_updates: dict[str, AuditSignature | None] = {}

        for path in paths:
            if self.policy.is_ignored(path):
                continue
            base_entry = base.get(path)
            local_entry = local_entries.get(path, MissingEntry(path))
            remote_entry = remote_entries.get(path, MissingEntry(path))
            local_quick = _side_quick_matches(local_entry, base_entry, side="local")
            remote_quick = _side_quick_matches(remote_entry, base_entry, side="remote")
            if not local_quick or not remote_quick:
                dirty.add(path)
            if base_entry is None or base_entry.kind is not EntryKind.FILE:
                if local_quick:
                    local_updates[path] = local_signatures.get(path)
                if remote_quick:
                    remote_updates[path] = remote_snapshot.signatures.get(path)
                continue
            if base_entry.is_placeholder:
                if local_quick:
                    local_updates[path] = local_signatures.get(path)
                if remote_quick:
                    if stored_remote.get(path) != remote_snapshot.signatures.get(path):
                        ambiguous_remote.append(path)
                    else:
                        remote_updates[path] = remote_snapshot.signatures.get(path)
                continue
            if local_quick and stored_local.get(path) != local_signatures.get(path):
                ambiguous_local.append(path)
            elif local_quick:
                local_updates[path] = local_signatures.get(path)
            if remote_quick and stored_remote.get(path) != remote_snapshot.signatures.get(path):
                ambiguous_remote.append(path)
            elif remote_quick:
                remote_updates[path] = remote_snapshot.signatures.get(path)

        if ambiguous_local:
            local_hashes, local_observed_signatures = self.local.observations(
                ambiguous_local,
                with_hash=True,
                base=base,
            )
        else:
            local_hashes, local_observed_signatures = {}, {}
        if ambiguous_remote:
            remote_hashes, remote_observed_signatures = self.remote.observations(
                ambiguous_remote,
                with_hash=True,
            )
        else:
            remote_hashes, remote_observed_signatures = {}, {}
        for path in ambiguous_local:
            if _strong_matches_base(local_hashes[path], base.get(path)):
                local_updates[path] = local_observed_signatures[path]
            else:
                dirty.add(path)
        for path in ambiguous_remote:
            if _strong_matches_base(remote_hashes[path], base.get(path)):
                remote_updates[path] = remote_observed_signatures[path]
            else:
                dirty.add(path)

        with self.store.transaction():
            self.store.update_audit_signatures("local", local_updates)
            self.store.update_audit_signatures("remote", remote_updates)
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


def _side_quick_matches(
    current: FingerprintState,
    base: EntryFingerprint | None,
    *,
    side: str,
) -> bool:
    if base is None or not base.is_placeholder:
        return quick_matches_base(current, base)
    if side == "local":
        return current == base
    return (
        isinstance(current, EntryFingerprint)
        and current.kind is EntryKind.FILE
    )


def _observation_matches_base(
    current: FingerprintState,
    base: EntryFingerprint | None,
) -> bool:
    if base is not None and base.kind is EntryKind.FILE and not base.is_placeholder:
        return _strong_matches_base(current, base)
    return quick_matches_base(current, base)
