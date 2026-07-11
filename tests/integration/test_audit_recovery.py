import hashlib
import os
import shutil
from pathlib import Path

import pytest
from helpers.sync_harness import SyncPair

from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import EntryFingerprint, EntryKind
from remote_sandbox.placeholder import PlaceholderMetadata, encode_placeholder


def test_audit_finds_change_when_watcher_event_was_lost(sync_pair: SyncPair) -> None:
    (sync_pair.remote / "lost.txt").write_text("remote", encoding="utf-8")
    result = sync_pair.engine.audit()
    assert "lost.txt" in result.completed
    assert (sync_pair.local / "lost.txt").read_text(encoding="utf-8") == "remote"


def test_audit_recovers_lost_local_change_and_remote_deletion(sync_pair: SyncPair) -> None:
    (sync_pair.local / "local.txt").write_bytes(b"base")
    (sync_pair.remote / "local.txt").write_bytes(b"base")
    (sync_pair.local / "deleted.txt").write_bytes(b"base")
    (sync_pair.remote / "deleted.txt").write_bytes(b"base")
    sync_pair.seed_current_base()
    (sync_pair.local / "local.txt").write_bytes(b"local-new")
    (sync_pair.remote / "deleted.txt").unlink()

    result = sync_pair.engine.audit()

    assert set(result.completed) == {"deleted.txt", "local.txt"}
    assert (sync_pair.remote / "local.txt").read_bytes() == b"local-new"
    assert not (sync_pair.local / "deleted.txt").exists()


def test_noop_audit_does_not_hash_regular_file_contents(sync_pair: SyncPair) -> None:
    (sync_pair.local / "stable.txt").write_bytes(b"stable")
    shutil.copy2(sync_pair.local / "stable.txt", sync_pair.remote / "stable.txt")
    sync_pair.seed_current_base()
    sync_pair.remote_client.hash_calls.clear()

    result = sync_pair.engine.audit()

    assert result == type(result)()
    assert sync_pair.remote_client.hash_calls == []


def test_missing_audit_signature_hashes_once_then_restores_noop_fast_path(
    sync_pair: SyncPair,
) -> None:
    path = "stable.txt"
    (sync_pair.local / path).write_bytes(b"stable")
    shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    sync_pair.store.replace_audit_signatures("local", {})
    sync_pair.store.replace_audit_signatures("remote", {})
    sync_pair.remote_client.hash_calls.clear()

    first = sync_pair.engine.audit()
    first_hashes = tuple(sync_pair.remote_client.hash_calls)
    sync_pair.remote_client.hash_calls.clear()
    second = sync_pair.engine.audit()

    assert first == type(first)()
    assert second == type(second)()
    assert first_hashes == ((path,),)
    assert sync_pair.remote_client.hash_calls == []


def test_ambiguous_audit_stores_signature_from_same_strong_observation(
    sync_pair: SyncPair,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = "stable.txt"
    (sync_pair.local / path).write_bytes(b"stable")
    shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    _replace_bytes_preserving_quick_metadata(sync_pair.local / path, b"stable")
    original_paths = sync_pair.engine.local_metadata.paths
    original_observations = sync_pair.engine.local_metadata.observations
    mutated = False

    def mutate_once() -> None:
        nonlocal mutated
        if not mutated:
            mutated = True
            _replace_bytes_preserving_quick_metadata(sync_pair.local / path, b"stable")

    def paths_wrapper(*args: object, **kwargs: object) -> object:
        if kwargs.get("with_hash") is True:
            mutate_once()
        return original_paths(*args, **kwargs)  # type: ignore[arg-type]

    def observations_wrapper(*args: object, **kwargs: object) -> object:
        if kwargs.get("with_hash") is True:
            mutate_once()
        return original_observations(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(sync_pair.engine.local_metadata, "paths", paths_wrapper)
    monkeypatch.setattr(
        sync_pair.engine.local_metadata,
        "observations",
        observations_wrapper,
    )

    sync_pair.engine.audit_coordinator.record_drift()

    current = sync_pair.engine.local_metadata.audit_signatures((path,))[path]
    assert mutated
    assert sync_pair.store.list_audit_signatures("local")[path] == current
    assert sync_pair.store.list_requeued_paths() == ()


def test_audit_mismatch_keeps_prior_side_signature(sync_pair: SyncPair) -> None:
    path = "stable.txt"
    (sync_pair.local / path).write_bytes(b"base")
    shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    prior = sync_pair.store.list_audit_signatures("local")[path]
    _replace_bytes_preserving_quick_metadata(sync_pair.local / path, b"evil")

    sync_pair.engine.audit_coordinator.record_drift()

    assert sync_pair.store.list_requeued_paths() == (path,)
    assert sync_pair.store.list_audit_signatures("local")[path] == prior


def test_placeholder_remote_real_file_matches_by_strong_content_identity(
    sync_pair: SyncPair,
) -> None:
    path = "weights.bin"
    content = b"real-content"
    digest = hashlib.sha256(content).hexdigest()
    (sync_pair.remote / path).write_bytes(content)
    remote_stat = (sync_pair.remote / path).stat(follow_symlinks=False)
    placeholder = EntryFingerprint(
        path,
        EntryKind.FILE,
        len(content),
        remote_stat.st_mtime_ns,
        remote_stat.st_mode,
        content_hash=digest,
        is_placeholder=True,
    )
    (sync_pair.local / path).write_bytes(
        encode_placeholder(
            PlaceholderMetadata(path, len(content), remote_stat.st_mtime_ns, digest)
        )
    )
    sync_pair.store.replace_base({path: placeholder})
    local_signature = sync_pair.engine.local_metadata.audit_signatures((path,))[path]
    assert local_signature is not None
    sync_pair.store.update_audit_signatures("local", {path: local_signature})
    sync_pair.store.replace_audit_signatures("remote", {})
    sync_pair.remote_client.hash_calls.clear()

    sync_pair.engine.audit_coordinator.record_drift()

    assert sync_pair.store.list_requeued_paths() == ()
    assert sync_pair.remote_client.hash_calls == [(path,)]
    assert path in sync_pair.store.list_audit_signatures("remote")


def _replace_bytes_preserving_quick_metadata(path: Path, content: bytes) -> None:
    metadata = path.stat(follow_symlinks=False)
    path.write_bytes(content)
    path.chmod(metadata.st_mode)
    os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))


def test_audit_recovers_lost_local_modification_with_unchanged_quick_metadata(
    sync_pair: SyncPair,
) -> None:
    path = "same-quick.txt"
    (sync_pair.local / path).write_bytes(b"base")
    shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    _replace_bytes_preserving_quick_metadata(sync_pair.local / path, b"evil")

    result = sync_pair.engine.audit()

    assert result.completed == (path,)
    assert (sync_pair.remote / path).read_bytes() == b"evil"


def test_audit_recovers_lost_remote_modification_with_unchanged_quick_metadata(
    sync_pair: SyncPair,
) -> None:
    path = "same-quick.txt"
    (sync_pair.local / path).write_bytes(b"base")
    shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    _replace_bytes_preserving_quick_metadata(sync_pair.remote / path, b"evil")

    result = sync_pair.engine.audit()

    assert result.completed == (path,)
    assert (sync_pair.local / path).read_bytes() == b"evil"


def test_rescan_event_audits_kind_change_and_persists_conflict(sync_pair: SyncPair) -> None:
    (sync_pair.local / "entry").write_bytes(b"base")
    shutil.copy2(sync_pair.local / "entry", sync_pair.remote / "entry")
    sync_pair.seed_current_base()
    (sync_pair.local / "entry").unlink()
    (sync_pair.local / "entry").mkdir()
    sync_pair.store.append_event("local", EventKind.RESCAN_REQUIRED, "*")

    result = sync_pair.engine.run_once("overflow")

    assert len(result.conflict_ids) == 1
    assert sync_pair.store.get_conflict(result.conflict_ids[0]).reason == "kind-divergence"
    assert sync_pair.remote_client.snapshot_calls == 1
