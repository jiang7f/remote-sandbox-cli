from __future__ import annotations

import dataclasses
import os
import subprocess
from collections.abc import Iterable
from pathlib import Path

import pytest

import remote_sandbox._transport_fingerprint as fingerprint_module
from remote_sandbox._transport_fingerprint import LocalPathChanged, ProtectedLocalRoot
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry, fingerprint_local
from remote_sandbox.state import AuditSignature
from remote_sandbox.transport import (
    BatchTransport,
    LocalPairTransport,
    RsyncCapabilities,
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferResult,
)


class _RemoteFingerprinter:
    def __init__(self, responses: Iterable[dict[str, EntryFingerprint | MissingEntry]]) -> None:
        self._responses = list(responses)
        self._index = 0

    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        entries, _signatures = self.observations(paths, with_hash=True)
        return entries

    def observations(
        self,
        _paths: Iterable[str],
        *,
        with_hash: bool,
    ) -> tuple[
        dict[str, EntryFingerprint | MissingEntry],
        dict[str, AuditSignature | None],
    ]:
        response = self._responses[self._index]
        signatures = {
            path: (
                AuditSignature(path, entry.kind, 100 + self._index, 1, position + 1)
                if isinstance(entry, EntryFingerprint)
                else None
            )
            for position, (path, entry) in enumerate(response.items())
        }
        self._index += 1
        if with_hash:
            return response, signatures
        return {
            path: (
                dataclasses.replace(entry, content_hash=None)
                if isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.FILE
                else entry
            )
            for path, entry in response.items()
        }, signatures

    def audit_signatures(
        self,
        _paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]:
        response = self._responses[self._index]
        return {
            path: (
                AuditSignature(path, entry.kind, 100 + self._index, 1, position + 1)
                if isinstance(entry, EntryFingerprint)
                else None
            )
            for position, (path, entry) in enumerate(response.items())
        }


class _Runner:
    def run_workspace_python_bytes(
        self,
        _target: str,
        _root: str,
        _code: str,
        _input_data: bytes,
        _args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(["ssh"], 0, b"", b"")

    def delete_workspace_path(self, _target: str, _root: str, _path: str) -> None:
        raise AssertionError("delete is not expected")


def _recording_hash(monkeypatch: pytest.MonkeyPatch) -> list[bytes]:
    original = fingerprint_module._hash_descriptor
    observed: list[bytes] = []

    def record(descriptor: int) -> str:
        duplicate = os.dup(descriptor)
        try:
            with os.fdopen(duplicate, "rb") as handle:
                observed.append(handle.read())
        finally:
            pass
        return original(descriptor)

    monkeypatch.setattr(fingerprint_module, "_hash_descriptor", record)
    return observed


def test_descriptor_fingerprint_rejects_parent_swap_without_reading_outside(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "safe").mkdir()
    (root / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    observed = _recording_hash(monkeypatch)
    original_open = fingerprint_module._open_directory

    def open_then_swap(path: str, *, dir_fd: int | None = None) -> int:
        descriptor = original_open(path, dir_fd=dir_fd)
        if path == "safe" and (root / "safe").is_dir():
            (root / "safe").rename(root / "original-safe")
            (root / "safe").symlink_to(outside, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(fingerprint_module, "_open_directory", open_then_swap)
    with (
        ProtectedLocalRoot(root) as protected,
        pytest.raises(LocalPathChanged, match="parent changed"),
    ):
        protected.fingerprint("safe/value.txt", with_hash=True)
    assert observed == [b"inside"]


def test_transport_preflight_rejects_parent_swap_without_outside_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "safe").mkdir()
    (source / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    observed = _recording_hash(monkeypatch)
    original_open = fingerprint_module._open_directory

    def open_then_swap(path: str, *, dir_fd: int | None = None) -> int:
        descriptor = original_open(path, dir_fd=dir_fd)
        if path == "safe" and (source / "safe").is_dir():
            (source / "safe").rename(source / "original-safe")
            (source / "safe").symlink_to(outside, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(fingerprint_module, "_open_directory", open_then_swap)
    with pytest.raises(LocalPathChanged):
        LocalPairTransport(source, destination, engine="tar").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("safe/value.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert observed == [b"inside"]
    assert not (destination / "safe" / "value.txt").exists()


def test_transport_postflight_reports_parent_swap_as_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "safe").mkdir()
    (source / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    observed = _recording_hash(monkeypatch)
    original_open = fingerprint_module._open_directory
    source_parent_opens = 0

    def open_then_swap_on_postflight(path: str, *, dir_fd: int | None = None) -> int:
        nonlocal source_parent_opens
        descriptor = original_open(path, dir_fd=dir_fd)
        if path == "safe" and (source / "safe").is_dir():
            source_parent_opens += 1
            if source_parent_opens == 2:
                (source / "safe").rename(source / "original-safe")
                (source / "safe").symlink_to(outside, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(
        fingerprint_module,
        "_open_directory",
        open_then_swap_on_postflight,
    )
    progress: list[TransferResult] = []
    result = LocalPairTransport(source, destination, engine="tar").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("safe/value.txt", None, None),),
        ),
        progress.append,
    )
    assert result.completed == ()
    assert result.changed_during_transfer == ("safe/value.txt",)
    assert progress == []
    assert observed == [b"inside", b"inside"]


def test_local_pair_postflight_initial_symlink_parent_is_reported_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "safe").mkdir()
    (source / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    observed = _recording_hash(monkeypatch)
    transport = LocalPairTransport(source, destination, engine="tar")
    original = transport._transfer_tar

    def transfer_then_swap(*args: object) -> None:
        original(*args)
        (source / "safe").rename(source / "original-safe")
        (source / "safe").symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(transport, "_transfer_tar", transfer_then_swap)
    progress: list[TransferResult] = []
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("safe/value.txt", None, None),),
        ),
        progress.append,
    )
    assert result.completed == ()
    assert result.changed_during_transfer == ("safe/value.txt",)
    assert progress == []
    assert observed == [b"inside"]


def test_batch_transport_postflight_initial_symlink_parent_is_reported_changed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    outside = tmp_path / "outside"
    source_root.mkdir()
    outside.mkdir()
    (source_root / "safe").mkdir()
    (source_root / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    source = fingerprint_local(source_root, "safe/value.txt", with_hash=True)
    assert isinstance(source, EntryFingerprint)
    remote = _RemoteFingerprinter(
        [
            {"safe/value.txt": MissingEntry("safe/value.txt")},
            {"safe/value.txt": source},
        ]
    )
    observed = _recording_hash(monkeypatch)
    transport = BatchTransport(
        source_root,
        "host",
        "/remote with space",
        remote,
        runner=_Runner(),
        capabilities=RsyncCapabilities(False, False),
    )
    original = transport._transfer_tar

    def transfer_then_swap(*args: object) -> None:
        original(*args)
        (source_root / "safe").rename(source_root / "original-safe")
        (source_root / "safe").symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(transport, "_transfer_tar", transfer_then_swap)
    progress: list[TransferResult] = []
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem(
                    "safe/value.txt",
                    source,
                    MissingEntry("safe/value.txt"),
                ),
            ),
        ),
        progress.append,
    )
    assert result.completed == ()
    assert result.changed_during_transfer == ("safe/value.txt",)
    assert progress == []
    assert observed == [b"inside"]
