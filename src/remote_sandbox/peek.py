from __future__ import annotations

from pathlib import Path

from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.engine import RemoteReplica
from remote_sandbox.manifest import EntryFingerprint, EntryKind, normalize_relative_path
from remote_sandbox.placeholder import decode_placeholder
from remote_sandbox.state import WorkspaceStore


class PeekError(RuntimeError):
    pass


def peek_placeholder(
    *,
    local_root: Path,
    store: WorkspaceStore,
    remote: RemoteReplica,
    path: str,
    lines: int,
    tail: bool,
) -> bytes:
    if lines <= 0:
        raise PeekError("lines must be positive")
    normalized = normalize_relative_path(path)
    with ProtectedLocalRoot(local_root) as protected:
        physical, placeholder_bytes = protected.read_entry(normalized)
    if not isinstance(physical, EntryFingerprint) or physical.kind is not EntryKind.FILE:
        raise PeekError(f"not a placeholder: {normalized}")
    try:
        metadata = decode_placeholder(placeholder_bytes or b"", expected_path=normalized)
    except ValueError as exc:
        raise PeekError(str(exc)) from exc
    if metadata is None:
        raise PeekError(f"not a placeholder: {normalized}")
    base = store.get_base(normalized)
    if not isinstance(base, EntryFingerprint) or not base.is_placeholder:
        raise PeekError(f"placeholder base metadata changed: {normalized}")
    source = remote.hash_paths((normalized,))[normalized]
    if (
        not isinstance(source, EntryFingerprint)
        or source.kind is not EntryKind.FILE
        or source.content_hash != metadata.content_hash
        or source.size != metadata.size
    ):
        raise PeekError(f"remote placeholder source changed: {normalized}")
    content = remote.read_path(normalized)
    if content is None:
        raise PeekError(f"remote placeholder source is missing: {normalized}")
    split = content.splitlines(keepends=True)
    return b"".join(split[-lines:] if tail else split[:lines])
