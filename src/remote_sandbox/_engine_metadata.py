from __future__ import annotations

import os
import stat
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TypeAlias

from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    fingerprint_local,
    normalize_relative_path,
    workspace_path,
)
from remote_sandbox.placeholder import decode_placeholder
from remote_sandbox.policy import PolicyEngine

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry


class LocalMetadata:
    def __init__(self, root: Path, policy: PolicyEngine) -> None:
        self.root = root
        self.policy = policy

    def paths(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
        base: Mapping[str, EntryFingerprint],
    ) -> dict[str, FingerprintState]:
        result: dict[str, FingerprintState] = {}
        for raw_path in paths:
            path = normalize_relative_path(raw_path)
            entry = fingerprint_local(self.root, path, with_hash=with_hash)
            base_entry = base.get(path)
            if (
                isinstance(entry, EntryFingerprint)
                and entry.kind is EntryKind.FILE
                and base_entry is not None
                and base_entry.is_placeholder
            ):
                entry = self._placeholder_fingerprint(path, entry, base_entry)
            result[path] = entry
        return result

    def snapshot(self, base: Mapping[str, EntryFingerprint]) -> dict[str, EntryFingerprint]:
        paths: list[str] = []

        def scan(directory: Path, prefix: str) -> None:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
            for entry in entries:
                path = entry.name if not prefix else f"{prefix}/{entry.name}"
                if self.policy.is_ignored(path):
                    continue
                paths.append(path)
                if entry.is_dir(follow_symlinks=False):
                    scan(Path(entry.path), path)

        scan(self.root, "")
        observed = self.paths(paths, with_hash=False, base=base)
        return {
            path: entry
            for path, entry in observed.items()
            if isinstance(entry, EntryFingerprint)
        }

    def _placeholder_fingerprint(
        self,
        path: str,
        physical: EntryFingerprint,
        base: EntryFingerprint,
    ) -> EntryFingerprint:
        candidate = workspace_path(self.root, path)
        try:
            metadata = decode_placeholder(candidate.read_bytes(), expected_path=path)
        except (OSError, ValueError):
            return physical
        if metadata is None:
            return physical
        return EntryFingerprint(
            path,
            EntryKind.FILE,
            metadata.size,
            metadata.mtime_ns,
            base.mode,
            content_hash=metadata.content_hash,
            is_placeholder=True,
        )


def quick_matches_base(
    current: FingerprintState,
    base: EntryFingerprint | None,
) -> bool:
    if base is None:
        return isinstance(current, MissingEntry)
    if isinstance(current, MissingEntry) or current.kind is not base.kind:
        return False
    if base.is_placeholder:
        return current == base
    if current.kind is EntryKind.FILE:
        return (
            current.size == base.size
            and current.mtime_ns == base.mtime_ns
            and current.mode == base.mode
        )
    if current.kind is EntryKind.SYMLINK:
        return current.link_target == base.link_target
    if current.kind is EntryKind.DIR:
        return stat.S_IMODE(current.mode or 0) == stat.S_IMODE(base.mode or 0)
    return current == base
