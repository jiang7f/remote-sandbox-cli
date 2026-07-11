from __future__ import annotations

from pathlib import Path

from remote_sandbox.engine import RemoteReplica, SyncTransport
from remote_sandbox.manifest import (
    EntryFingerprint,
    MissingEntry,
    fingerprint_local,
    normalize_relative_path,
    workspace_path,
)
from remote_sandbox.state import ConflictRecord, WorkspaceStore
from remote_sandbox.transport import TransferBatch, TransferDirection, TransferItem


def resolve_conflict_transaction(
    *,
    store: WorkspaceStore,
    local_root: Path,
    remote: RemoteReplica,
    transport: SyncTransport,
    path: str,
    use_local: bool,
) -> ConflictRecord:
    normalized = normalize_relative_path(path)
    workspace_path(local_root, normalized)
    conflicts = [
        conflict
        for conflict in store.list_conflicts(unresolved_only=True)
        if conflict.path == normalized
    ]
    if not conflicts:
        raise ValueError(f"no unresolved conflict for {normalized}")
    conflict = conflicts[-1]
    direction = TransferDirection.PUSH if use_local else TransferDirection.PULL
    expected_source = conflict.local_fingerprint if use_local else conflict.remote_fingerprint
    if expected_source is None:
        raise ValueError(f"conflict has no verified selected-source fingerprint: {normalized}")
    observed_local = fingerprint_local(local_root, normalized, with_hash=True)
    observed_remote = remote.hash_paths((normalized,))[normalized]
    observed_source = observed_local if use_local else observed_remote
    if observed_source != expected_source:
        raise ValueError(f"selected source changed: {normalized}")
    expected_destination = observed_remote if use_local else observed_local
    result = transport.transfer(
        TransferBatch(
            direction,
            (TransferItem(normalized, expected_source, expected_destination),),
        ),
        lambda _result: None,
    )
    if result.completed != (normalized,):
        raise ValueError(f"selected source changed: {normalized}")
    final_state = (
        remote.hash_paths((normalized,))[normalized]
        if use_local
        else fingerprint_local(local_root, normalized, with_hash=True)
    )
    if isinstance(final_state, MissingEntry):
        raise RuntimeError(f"resolved destination is missing: {normalized}")
    if not isinstance(final_state, EntryFingerprint):
        raise RuntimeError(f"resolved destination fingerprint is invalid: {normalized}")
    destination_side = "remote" if use_local else "local"
    with store.transaction():
        store.upsert_base(final_state)
        store.set_expected_echo(destination_side, final_state)
        return store.resolve_conflict(conflict.conflict_id)
