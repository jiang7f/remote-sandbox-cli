from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.engine import RemoteReplica, SyncTransport
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.placeholder import PlaceholderMetadata, decode_placeholder
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.transport import TransferBatch, TransferDirection, TransferItem


class FetchError(RuntimeError):
    pass


ConfirmCallback = Callable[[str], bool]


def fetch_placeholders(
    *,
    local_root: Path,
    store: WorkspaceStore,
    remote: RemoteReplica,
    transport: SyncTransport,
    path: str | None,
    fetch_all: bool,
    confirm: ConfirmCallback,
) -> tuple[int, bool]:
    selected = _select_placeholders(local_root, store, path=path, fetch_all=fetch_all)
    if not selected:
        return 0, False
    if fetch_all and not confirm(_fetch_all_prompt(selected)):
        return 0, True

    remote_entries = remote.hash_paths(item.path for item in selected)
    items: list[TransferItem] = []
    for item in selected:
        source = remote_entries[item.path]
        if not isinstance(source, EntryFingerprint) or source.kind is not EntryKind.FILE:
            raise FetchError(f"remote placeholder source is not a regular file: {item.path}")
        if source.content_hash != item.metadata.content_hash or source.size != item.metadata.size:
            raise FetchError(f"remote placeholder source changed: {item.path}")
        items.append(TransferItem(item.path, source, item.physical))

    result = transport.transfer(
        TransferBatch(TransferDirection.PULL, tuple(items)),
        lambda _result: None,
    )
    expected_paths = tuple(item.path for item in selected)
    if result.completed != expected_paths:
        changed = ", ".join(result.changed_during_transfer or expected_paths)
        raise FetchError(f"placeholder source changed during fetch: {changed}")

    with ProtectedLocalRoot(local_root) as protected:
        final = {
            item.path: protected.fingerprint(item.path, with_hash=True) for item in selected
        }
    with store.transaction():
        for item in selected:
            fingerprint = final[item.path]
            if isinstance(fingerprint, MissingEntry):
                raise FetchError(f"fetched placeholder destination is missing: {item.path}")
            store.upsert_base(fingerprint)
            store.set_expected_echo("local", fingerprint)
    return len(selected), False


def fetch_all_prompt(local_root: Path, store: WorkspaceStore) -> str | None:
    selected = _select_placeholders(local_root, store, path=None, fetch_all=True)
    return _fetch_all_prompt(selected) if selected else None


class _SelectedPlaceholder:
    __slots__ = ("path", "metadata", "physical")

    def __init__(
        self,
        path: str,
        metadata: PlaceholderMetadata,
        physical: EntryFingerprint,
    ) -> None:
        self.path = path
        self.metadata = metadata
        self.physical = physical


def _select_placeholders(
    local_root: Path,
    store: WorkspaceStore,
    *,
    path: str | None,
    fetch_all: bool,
) -> list[_SelectedPlaceholder]:
    if fetch_all and path is not None:
        raise FetchError("use either a path or --all, not both")
    if not fetch_all and path is None:
        raise FetchError("fetch requires a path or --all")
    with ProtectedLocalRoot(local_root) as protected:
        candidates = (
            protected.walk_paths(lambda _path: False)
            if fetch_all
            else (normalize_relative_path(path or ""),)
        )
        selected: list[_SelectedPlaceholder] = []
        for candidate in sorted(candidates):
            physical, content = protected.read_entry(candidate)
            if not isinstance(physical, EntryFingerprint) or physical.kind is not EntryKind.FILE:
                if fetch_all:
                    continue
                raise FetchError(f"not a placeholder: {candidate}")
            try:
                metadata = decode_placeholder(content or b"", expected_path=candidate)
            except ValueError as exc:
                raise FetchError(str(exc)) from exc
            if metadata is None:
                if fetch_all:
                    continue
                raise FetchError(f"not a placeholder: {candidate}")
            base = store.get_base(candidate)
            if (
                not isinstance(base, EntryFingerprint)
                or not base.is_placeholder
                or base.content_hash != metadata.content_hash
                or base.size != metadata.size
            ):
                raise FetchError(f"placeholder base metadata changed: {candidate}")
            selected.append(_SelectedPlaceholder(candidate, metadata, physical))
    return selected


def _fetch_all_prompt(placeholders: list[_SelectedPlaceholder]) -> str:
    total = sum(item.metadata.size for item in placeholders)
    return (
        f"This will fetch {len(placeholders)} placeholder files, total {_format_size(total)}.\n"
        "Continue? [y/N] "
    )


def _format_size(size: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1000 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1000
    return f"{size} B"
