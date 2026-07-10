from __future__ import annotations

import hashlib
import os
import posixpath
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final


class EntryKind(StrEnum):
    FILE = "file"
    DIR = "dir"
    SYMLINK = "symlink"
    SPECIAL = "special"

    # Temporary compatibility for the legacy scanner and reconciler. New code treats these
    # entries as special files and never transfers them.
    UNSUPPORTED = "special"

    @classmethod
    def _missing_(cls, value: object) -> EntryKind | None:
        if value == "unsupported":
            return cls.SPECIAL
        return None


@dataclass(frozen=True, slots=True)
class EntryFingerprint:
    path: str
    kind: EntryKind
    size: int | None
    mtime_ns: int | None
    mode: int | None
    link_target: str | None = None
    content_hash: str | None = None
    is_placeholder: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.path, str):
            raise ValueError("fingerprint path must be a string")
        if not isinstance(self.kind, EntryKind):
            raise ValueError("fingerprint kind must be an EntryKind")
        for field, integer_value in (
            ("size", self.size),
            ("mtime_ns", self.mtime_ns),
            ("mode", self.mode),
        ):
            if integer_value is not None and type(integer_value) is not int:
                raise ValueError(f"fingerprint {field} must be an integer or None")
        if self.size is not None and self.size < 0:
            raise ValueError("fingerprint size must be non-negative")
        for field, string_value in (
            ("link_target", self.link_target),
            ("content_hash", self.content_hash),
        ):
            if string_value is not None and not isinstance(string_value, str):
                raise ValueError(f"fingerprint {field} must be a string or None")
        if type(self.is_placeholder) is not bool:
            raise ValueError("fingerprint is_placeholder must be a boolean")
        object.__setattr__(self, "path", normalize_relative_path(self.path))


@dataclass(frozen=True, slots=True)
class FileEntry:
    kind: EntryKind
    path: str
    size: int | None
    mtime: float | None
    hash: str | None
    is_placeholder: bool = False

    def __post_init__(self) -> None:
        normalized = normalize_relative_path(self.path)
        object.__setattr__(self, "path", normalized)
        if self.kind == EntryKind.FILE and self.size is None:
            raise ValueError("file entries require size")
        if self.kind == EntryKind.DIR and self.size is not None:
            raise ValueError("directory entries must not have size")
        if self.kind == EntryKind.DIR and self.hash is not None:
            raise ValueError("directory entries must not have hash")
        if self.kind == EntryKind.SPECIAL and self.hash is not None:
            raise ValueError("special entries must not have hash")


@dataclass(frozen=True, slots=True)
class MissingEntry:
    path: str | None = None

    def __post_init__(self) -> None:
        if self.path is not None:
            object.__setattr__(self, "path", normalize_relative_path(self.path))

    def __repr__(self) -> str:
        if self.path is None:
            return "MISSING"
        return f"MissingEntry(path={self.path!r})"


MISSING: Final = MissingEntry()
EntryState = FileEntry | MissingEntry


def normalize_relative_path(path: str) -> str:
    if not path or _has_control_char(path):
        raise ValueError("Invalid relative path")
    if path.startswith("/") or path.startswith("../") or path == "..":
        raise ValueError("Invalid relative path")
    normalized = posixpath.normpath(path.replace("\\", "/"))
    if (
        normalized in {"", "."}
        or normalized.startswith("/")
        or normalized.startswith("../")
        or normalized == ".."
    ):
        raise ValueError("Invalid relative path")
    return normalized


def is_missing(entry: EntryState) -> bool:
    return isinstance(entry, MissingEntry)


def workspace_path(root: Path, relative_path: str) -> Path:
    normalized = normalize_relative_path(relative_path)
    candidate = root
    for part in Path(normalized).parts[:-1]:
        candidate /= part
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            break
        if stat.S_ISLNK(mode):
            raise ValueError(f"symlink parent escapes workspace: {relative_path}")
    return root / normalized


def fingerprint_local(
    root: Path,
    relative_path: str,
    *,
    with_hash: bool,
) -> EntryFingerprint | MissingEntry:
    normalized = normalize_relative_path(relative_path)
    try:
        candidate = workspace_path(root, normalized)
    except NotADirectoryError:
        return MissingEntry(normalized)
    try:
        entry_stat = candidate.lstat()
    except (FileNotFoundError, NotADirectoryError):
        return MissingEntry(normalized)

    if stat.S_ISLNK(entry_stat.st_mode):
        target = os.readlink(candidate)
        link_digest = hashlib.sha256(os.fsencode(target)).hexdigest()
        return EntryFingerprint(
            normalized,
            EntryKind.SYMLINK,
            None,
            entry_stat.st_mtime_ns,
            entry_stat.st_mode,
            target,
            link_digest,
        )
    if stat.S_ISDIR(entry_stat.st_mode):
        return EntryFingerprint(
            normalized,
            EntryKind.DIR,
            None,
            entry_stat.st_mtime_ns,
            entry_stat.st_mode,
        )
    if stat.S_ISREG(entry_stat.st_mode):
        file_digest = _sha256_file(candidate) if with_hash else None
        return EntryFingerprint(
            normalized,
            EntryKind.FILE,
            entry_stat.st_size,
            entry_stat.st_mtime_ns,
            entry_stat.st_mode,
            content_hash=file_digest,
        )
    return EntryFingerprint(
        normalized,
        EntryKind.SPECIAL,
        None,
        entry_stat.st_mtime_ns,
        entry_stat.st_mode,
    )


def content_identity(entry: EntryFingerprint) -> tuple[object, ...]:
    if entry.kind is EntryKind.SYMLINK:
        return (entry.kind, entry.link_target)
    if entry.kind is EntryKind.FILE:
        return (entry.kind, entry.content_hash)
    return (entry.kind,)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)
