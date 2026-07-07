from __future__ import annotations

import posixpath
from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class EntryKind(StrEnum):
    FILE = "file"
    DIR = "dir"
    UNSUPPORTED = "unsupported"


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
        if self.kind == EntryKind.UNSUPPORTED and self.hash is not None:
            raise ValueError("unsupported entries must not have hash")


class MissingEntry:
    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"


MISSING: Final = MissingEntry()
EntryState = FileEntry | MissingEntry


def normalize_relative_path(path: str) -> str:
    if not path or _has_control_char(path):
        raise ValueError("Invalid relative path")
    if path.startswith("/") or path.startswith("../") or path == "..":
        raise ValueError("Invalid relative path")
    normalized = posixpath.normpath(path.replace("\\", "/"))
    if normalized in {"", "."} or normalized.startswith("../") or normalized == "..":
        raise ValueError("Invalid relative path")
    return normalized


def is_missing(entry: EntryState) -> bool:
    return entry is MISSING


def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)
