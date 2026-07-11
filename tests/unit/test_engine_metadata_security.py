import os
from pathlib import Path

import pytest

import remote_sandbox._transport_fingerprint as fingerprint_module
from remote_sandbox._engine_metadata import LocalMetadata
from remote_sandbox._transport_fingerprint import LocalPathChanged
from remote_sandbox.manifest import EntryFingerprint, EntryKind
from remote_sandbox.placeholder import PlaceholderMetadata, encode_placeholder
from remote_sandbox.policy import StaticPolicyEngine


def _swap_parent_after_open(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    outside: Path,
) -> list[bool]:
    original = fingerprint_module._open_directory
    swapped = [False]

    def open_then_swap(path: str, *, dir_fd: int | None = None) -> int:
        descriptor = original(path, dir_fd=dir_fd)
        if path == "safe" and not swapped[0]:
            swapped[0] = True
            (root / "safe").rename(root / "inside-safe")
            (root / "safe").symlink_to(outside, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(fingerprint_module, "_open_directory", open_then_swap)
    return swapped


@pytest.mark.parametrize("with_hash", [False, True])
def test_selective_metadata_parent_swap_never_reads_outside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_hash: bool,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    (root / "safe").mkdir(parents=True)
    outside.mkdir()
    (root / "safe" / "value.txt").write_bytes(b"inside")
    (outside / "value.txt").write_bytes(b"outside")
    observed: list[bytes] = []
    original_hash = fingerprint_module._hash_descriptor

    def record_hash(descriptor: int) -> str:
        duplicate = os.dup(descriptor)
        with os.fdopen(duplicate, "rb") as handle:
            observed.append(handle.read())
        return original_hash(descriptor)

    monkeypatch.setattr(fingerprint_module, "_hash_descriptor", record_hash)
    swapped = _swap_parent_after_open(monkeypatch, root, outside)
    metadata = LocalMetadata(root, StaticPolicyEngine())

    with pytest.raises(LocalPathChanged):
        metadata.paths(("safe/value.txt",), with_hash=with_hash, base={})

    assert swapped == [True]
    assert b"outside" not in observed
    if with_hash:
        assert observed == [b"inside"]


def test_full_audit_snapshot_parent_swap_never_reads_outside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    (root / "safe").mkdir(parents=True)
    outside.mkdir()
    (root / "safe" / "value.txt").write_bytes(b"inside")
    (outside / "value.txt").write_bytes(b"outside")
    swapped = _swap_parent_after_open(monkeypatch, root, outside)

    with pytest.raises((LocalPathChanged, NotADirectoryError, OSError)):
        LocalMetadata(root, StaticPolicyEngine()).snapshot_with_signatures({})

    assert swapped == [True]


def test_placeholder_decode_parent_swap_never_reads_outside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    (root / "safe").mkdir(parents=True)
    outside.mkdir()
    path = "safe/value.bin"
    metadata = PlaceholderMetadata(path, 100, 10, "inside-hash")
    (root / path).write_bytes(encode_placeholder(metadata))
    outside_metadata = PlaceholderMetadata(path, 100, 10, "outside-hash")
    (outside / "value.bin").write_bytes(encode_placeholder(outside_metadata))
    base = EntryFingerprint(
        path,
        EntryKind.FILE,
        100,
        10,
        0o100644,
        content_hash="inside-hash",
        is_placeholder=True,
    )
    swapped = _swap_parent_after_open(monkeypatch, root, outside)

    with pytest.raises((LocalPathChanged, NotADirectoryError, OSError)):
        LocalMetadata(root, StaticPolicyEngine()).paths(
            (path,),
            with_hash=False,
            base={path: base},
        )

    assert swapped == [True]
