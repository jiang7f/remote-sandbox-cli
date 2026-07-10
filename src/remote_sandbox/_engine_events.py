from __future__ import annotations

from collections.abc import Iterable

from remote_sandbox.journal import EventKind, JournalEvent, coalesce_events
from remote_sandbox.state import WorkspaceStore


def dirty_sources(
    local_events: Iterable[JournalEvent],
    remote_events: Iterable[JournalEvent],
    requeued: set[str],
) -> dict[str, set[str]]:
    dirty: dict[str, set[str]] = {path: {"requeue"} for path in requeued}
    for event in coalesce_events([*local_events, *remote_events]):
        if event.kind is EventKind.RESCAN_REQUIRED:
            continue
        dirty.setdefault(event.path, set()).add(event.side)
        if event.kind is EventKind.MOVE:
            assert event.destination_path is not None
            dirty.setdefault(event.destination_path, set()).add(event.side)
    return dirty


def contains_rescan(*event_groups: Iterable[JournalEvent]) -> bool:
    return any(event.kind is EventKind.RESCAN_REQUIRED for group in event_groups for event in group)


def acknowledge_pending(
    store: WorkspaceStore,
    side: str,
    events: list[JournalEvent],
) -> None:
    if events:
        store.acknowledge(side, events[-1].sequence)
