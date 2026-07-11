from __future__ import annotations

import stat
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TypeAlias

from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.placeholder import decode_placeholder
from remote_sandbox.policy import PolicyEngine
from remote_sandbox.state import AuditSignature

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
        entries, _signatures = self.observations(paths, with_hash=with_hash, base=base)
        return entries

    def observations(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
        base: Mapping[str, EntryFingerprint],
    ) -> tuple[dict[str, FingerprintState], dict[str, AuditSignature | None]]:
        with ProtectedLocalRoot(self.root) as protected:
            return self._observations_with_root(
                protected,
                paths,
                with_hash=with_hash,
                base=base,
            )

    def _observations_with_root(
        self,
        protected: ProtectedLocalRoot,
        paths: Iterable[str],
        *,
        with_hash: bool,
        base: Mapping[str, EntryFingerprint],
    ) -> tuple[dict[str, FingerprintState], dict[str, AuditSignature | None]]:
        result: dict[str, FingerprintState] = {}
        signatures: dict[str, AuditSignature | None] = {}
        for raw_path in paths:
            path = normalize_relative_path(raw_path)
            entry, signature = protected.observe(path, with_hash=with_hash)
            base_entry = base.get(path)
            if (
                isinstance(entry, EntryFingerprint)
                and entry.kind is EntryKind.FILE
                and base_entry is not None
                and base_entry.is_placeholder
            ):
                entry = self._placeholder_fingerprint(protected, path, entry, base_entry)
            result[path] = entry
            signatures[path] = signature
        return result, signatures

    def snapshot(self, base: Mapping[str, EntryFingerprint]) -> dict[str, EntryFingerprint]:
        observed, _signatures = self.snapshot_with_signatures(base)
        return observed

    def snapshot_with_signatures(
        self,
        base: Mapping[str, EntryFingerprint],
    ) -> tuple[dict[str, EntryFingerprint], dict[str, AuditSignature]]:
        with ProtectedLocalRoot(self.root) as protected:
            paths = protected.walk_paths(self.policy.is_ignored)
            observed, signatures = self._observations_with_root(
                protected,
                paths,
                with_hash=False,
                base=base,
            )
        return {
            path: entry
            for path, entry in observed.items()
            if isinstance(entry, EntryFingerprint)
        }, {path: signature for path, signature in signatures.items() if signature is not None}

    def audit_signatures(
        self,
        paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]:
        with ProtectedLocalRoot(self.root) as protected:
            return {
                path: protected.audit_signature(path)
                for path in (normalize_relative_path(raw) for raw in paths)
            }

    def _placeholder_fingerprint(
        self,
        protected: ProtectedLocalRoot,
        path: str,
        physical: EntryFingerprint,
        base: EntryFingerprint,
    ) -> EntryFingerprint:
        try:
            _observed, content = protected.read_entry(path)
            metadata = decode_placeholder(content or b"", expected_path=path)
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
