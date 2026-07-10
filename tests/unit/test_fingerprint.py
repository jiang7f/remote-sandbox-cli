import hashlib
import os
import stat
from pathlib import Path

import pytest

from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    content_identity,
    fingerprint_local,
    normalize_relative_path,
    workspace_path,
)


def test_regular_file_fingerprint_hashes_only_when_requested(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    content = b"local content\n"
    (root / "notes.txt").write_bytes(content)

    quick = fingerprint_local(root, "notes.txt", with_hash=False)
    strong = fingerprint_local(root, "notes.txt", with_hash=True)

    assert isinstance(quick, EntryFingerprint)
    assert quick.path == "notes.txt"
    assert quick.kind is EntryKind.FILE
    assert quick.size == len(content)
    assert quick.mtime_ns is not None
    assert quick.mode is not None and stat.S_ISREG(quick.mode)
    assert quick.content_hash is None
    assert isinstance(strong, EntryFingerprint)
    assert strong.content_hash == hashlib.sha256(content).hexdigest()


def test_directory_fingerprint_has_no_content_fields(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "pkg").mkdir()

    entry = fingerprint_local(root, "pkg", with_hash=True)

    assert isinstance(entry, EntryFingerprint)
    assert entry.kind is EntryKind.DIR
    assert entry.size is None
    assert entry.link_target is None
    assert entry.content_hash is None


@pytest.mark.parametrize("target", ["/etc/passwd", "missing/target.txt"])
def test_symlink_fingerprint_preserves_target_without_following(
    tmp_path: Path,
    target: str,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "outside").symlink_to(target)

    entry = fingerprint_local(root, "outside", with_hash=True)

    assert isinstance(entry, EntryFingerprint)
    assert entry.kind is EntryKind.SYMLINK
    assert entry.link_target == target
    assert entry.size is None
    assert entry.content_hash == hashlib.sha256(os.fsencode(target)).hexdigest()
    assert content_identity(entry) == (EntryKind.SYMLINK, target)


def test_parent_symlink_cannot_escape_workspace_during_fingerprint(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    (root / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink parent"):
        fingerprint_local(root, "escape/secret.txt", with_hash=True)

    with pytest.raises(ValueError, match="symlink parent"):
        workspace_path(root, "escape/new.txt")


def test_missing_entry_keeps_its_normalized_path(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()

    entry = fingerprint_local(root, "pkg/../missing.txt", with_hash=True)

    assert entry == MissingEntry("missing.txt")


def test_stale_descendant_below_parent_that_became_file_is_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    descendant = root / "a" / "b" / "c"
    descendant.parent.mkdir(parents=True)
    descendant.write_text("old", encoding="utf-8")
    descendant.unlink()
    descendant.parent.rmdir()
    descendant.parent.parent.rmdir()
    (root / "a").write_text("replacement", encoding="utf-8")

    entry = fingerprint_local(root, "a/b/c", with_hash=True)

    assert entry == MissingEntry("a/b/c")


def test_backslash_absolute_path_cannot_escape_workspace() -> None:
    with pytest.raises(ValueError, match="Invalid relative path"):
        normalize_relative_path("\\etc\\passwd")


def test_content_identity_uses_hash_only_for_regular_files() -> None:
    entry = EntryFingerprint(
        "notes.txt",
        EntryKind.FILE,
        4,
        123,
        0o100644,
        content_hash="abc123",
    )

    assert content_identity(entry) == (EntryKind.FILE, "abc123")
