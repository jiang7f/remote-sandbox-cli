from __future__ import annotations

import os
from pathlib import Path

import pytest

import remote_sandbox._transport_fingerprint as fingerprint_module
from remote_sandbox._transport_fingerprint import LocalPathChanged, ProtectedLocalRoot
from remote_sandbox.transport import (
    LocalPairTransport,
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferResult,
)


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
