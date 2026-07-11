from __future__ import annotations

import os
from pathlib import Path

import pytest

import remote_sandbox._transport_fingerprint as fingerprint_module
import remote_sandbox._transport_paths as transport_paths
from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.manifest import MissingEntry, fingerprint_local
from remote_sandbox.transport import (
    LocalPairTransport,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferItem,
)


def test_bulk_fingerprint_reuses_one_verified_parent_walk_for_siblings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    parent = root / "files"
    parent.mkdir(parents=True)
    paths = tuple(f"files/file-{index:04d}.txt" for index in range(100))
    for path in paths:
        (root / path).write_text(path, encoding="utf-8")
    original = fingerprint_module._open_verified_parent
    parent_walks = 0

    def counted_parent_walk(root_fd: int, parts: list[str]):
        nonlocal parent_walks
        parent_walks += 1
        return original(root_fd, parts)

    monkeypatch.setattr(fingerprint_module, "_open_verified_parent", counted_parent_walk)

    with ProtectedLocalRoot(root) as protected:
        observed = protected.fingerprints(paths, with_hash=True)

    assert tuple(observed) == paths
    assert all(observed[path].content_hash is not None for path in paths)
    assert parent_walks == 1


def test_local_transport_does_not_repeat_bulk_observations_after_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    paths = ("files/first.txt", "files/second.txt")
    for path in paths:
        destination = local / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(path, encoding="utf-8")
    batch = TransferBatch(
        TransferDirection.PUSH,
        tuple(
            TransferItem(
                path,
                fingerprint_local(local, path, with_hash=True),
                MissingEntry(path),
            )
            for path in paths
        ),
    )
    original = ProtectedLocalRoot.observations
    bulk_calls = 0

    def race_once(
        self: ProtectedLocalRoot,
        selected: tuple[str, ...],
        *,
        with_hash: bool,
    ):
        nonlocal bulk_calls
        bulk_calls += 1
        return original(self, selected, with_hash=with_hash)

    monkeypatch.setattr(ProtectedLocalRoot, "observations", race_once)

    result = LocalPairTransport(local, remote).transfer(batch, lambda _progress: None)

    assert result.completed == paths
    assert bulk_calls == 1


def test_local_transport_reuses_source_signature_without_postflight_content_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    path = "value.txt"
    (local / path).write_text("value", encoding="utf-8")
    batch = TransferBatch(
        TransferDirection.PUSH,
        (
            TransferItem(
                path,
                fingerprint_local(local, path, with_hash=True),
                MissingEntry(path),
            ),
        ),
    )
    original = ProtectedLocalRoot.observations
    calls: list[tuple[Path, bool]] = []

    def record_observations(
        self: ProtectedLocalRoot,
        selected: tuple[str, ...],
        *,
        with_hash: bool,
    ):
        calls.append((self.path, with_hash))
        return original(self, selected, with_hash=with_hash)

    monkeypatch.setattr(ProtectedLocalRoot, "observations", record_observations)

    result = LocalPairTransport(local, remote).transfer(batch, lambda _progress: None)

    assert result.completed == (path,)
    assert calls == [(remote, True)]


def test_finalize_reuses_one_parent_walk_for_sibling_observations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "destination"
    staging = tmp_path / "staging"
    destination.mkdir()
    (staging / "files").mkdir(parents=True)
    paths = ("files", *(f"files/file-{index:04d}.txt" for index in range(100)))
    for path in paths[1:]:
        (staging / path).write_text(path, encoding="utf-8")
    original_walk = transport_paths._walk_parent
    parent_walks: list[bool] = []

    def counted_walk(root_fd: int, parts: list[str], *, create: bool) -> int:
        parent_walks.append(create)
        return original_walk(root_fd, parts, create=create)

    monkeypatch.setattr(transport_paths, "_walk_parent", counted_walk)

    with ProtectedLocalRoot(destination) as protected:
        observed = protected.finalize(staging, paths, error_type=TransferError)

    assert tuple(observed) == paths
    assert parent_walks.count(False) == 2


def test_source_signature_detects_same_size_in_place_rewrite_with_restored_mtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "value.txt"
    source.write_bytes(b"before")
    original_stat = source.stat()
    transport = LocalPairTransport(local, remote)
    original_transfer = transport._transfer_rsync

    def mutate_after_transfer(*args: object, **kwargs: object) -> None:
        original_transfer(*args, **kwargs)
        source.write_bytes(b"AFTER!")
        os.utime(
            source,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )

    monkeypatch.setattr(transport, "_transfer_rsync", mutate_after_transfer)

    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem(
                    "value.txt",
                    fingerprint_local(local, "value.txt", with_hash=True),
                    MissingEntry("value.txt"),
                ),
            ),
        ),
        lambda _progress: None,
    )

    assert result.completed == ()
    assert result.changed_during_transfer == ("value.txt",)


def test_stage_revalidates_preflight_signature_before_linking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "value.txt"
    source.write_bytes(b"before")
    original_stat = source.stat()
    original_copy = transport_paths._copy_from_descriptor
    mutated = False

    def mutate_before_stage(*args: object, **kwargs: object) -> None:
        nonlocal mutated
        if not mutated:
            mutated = True
            source.write_bytes(b"AFTER!")
            os.utime(
                source,
                ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
            )
        original_copy(*args, **kwargs)

    monkeypatch.setattr(transport_paths, "_copy_from_descriptor", mutate_before_stage)

    result = LocalPairTransport(local, remote).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem(
                    "value.txt",
                    fingerprint_local(local, "value.txt", with_hash=True),
                    MissingEntry("value.txt"),
                ),
            ),
        ),
        lambda _progress: None,
    )

    assert result.completed == ()
    assert result.changed_during_transfer == ("value.txt",)
    assert not (remote / "value.txt").exists()


def test_source_postflight_runs_while_private_hardlink_is_alive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "value.txt"
    source.write_bytes(b"value")
    original = ProtectedLocalRoot.verify_and_cleanup_stage
    postflight_link_counts: list[int] = []

    def record_postflight(
        self: ProtectedLocalRoot,
        selected: tuple[str, ...],
        staging: Path,
        *,
        expected_entries: object,
        expected_signatures: object,
        error_type: type[Exception],
    ):
        if self.path == local:
            postflight_link_counts.append(source.stat().st_nlink)
        return original(
            self,
            selected,
            staging,
            expected_entries=expected_entries,  # type: ignore[arg-type]
            expected_signatures=expected_signatures,  # type: ignore[arg-type]
            error_type=error_type,
        )

    monkeypatch.setattr(
        ProtectedLocalRoot,
        "verify_and_cleanup_stage",
        record_postflight,
    )

    result = LocalPairTransport(local, remote).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem(
                    "value.txt",
                    fingerprint_local(local, "value.txt", with_hash=True),
                    MissingEntry("value.txt"),
                ),
            ),
        ),
        lambda _progress: None,
    )

    assert result.completed == ("value.txt",)
    assert postflight_link_counts and min(postflight_link_counts) >= 2
    assert source.stat().st_nlink == 1


def test_rsync_postflight_reuses_protocol_verified_content_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    path = "value.txt"
    (local / path).write_bytes(b"value")
    batch = TransferBatch(
        TransferDirection.PUSH,
        (
            TransferItem(
                path,
                fingerprint_local(local, path, with_hash=True),
                MissingEntry(path),
            ),
        ),
    )
    original_hash = fingerprint_module._hash_descriptor
    content_hashes = 0

    def count_hash(descriptor: int) -> str:
        nonlocal content_hashes
        content_hashes += 1
        return original_hash(descriptor)

    monkeypatch.setattr(fingerprint_module, "_hash_descriptor", count_hash)

    result = LocalPairTransport(local, remote).transfer(batch, lambda _progress: None)

    assert result.completed == (path,)
    assert (
        result.verified_fingerprints[0].content_hash
        == batch.items[0].expected_source.content_hash
    )
    assert content_hashes == 1


def test_rsync_finalize_observation_is_destination_verification_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "value.txt"
    source.write_bytes(b"before")
    transport = LocalPairTransport(local, remote)
    original_transfer = transport._transfer_rsync

    def corrupt_after_rsync(*args: object, **kwargs: object) -> None:
        original_transfer(*args, **kwargs)
        destination = remote / "value.txt"
        original_stat = destination.stat()
        destination.write_bytes(b"AFTER!")
        os.utime(
            destination,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )

    monkeypatch.setattr(transport, "_transfer_rsync", corrupt_after_rsync)

    expected = fingerprint_local(local, "value.txt", with_hash=True)
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("value.txt", expected, MissingEntry("value.txt")),),
        ),
        lambda _progress: None,
    )

    assert result.completed == ("value.txt",)
    assert result.verified_fingerprints[0].content_hash == expected.content_hash
