from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import StrEnum

from remote_sandbox.manifest import normalize_relative_path


class EventKind(StrEnum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"
    MOVE = "move"
    RESCAN_REQUIRED = "rescan-required"


@dataclass(frozen=True, slots=True)
class JournalEvent:
    side: str
    sequence: int
    kind: EventKind
    path: str
    destination_path: str | None = None

    def __post_init__(self) -> None:
        if self.side not in {"local", "remote"}:
            raise ValueError(f"invalid journal side: {self.side}")
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError("event sequence must be a positive integer")
        if self.kind is EventKind.RESCAN_REQUIRED:
            if self.path != "*" or self.destination_path is not None:
                raise ValueError("rescan events must use '*' without a destination")
            return
        object.__setattr__(self, "path", normalize_relative_path(self.path))
        if self.kind is EventKind.MOVE:
            if self.destination_path is None:
                raise ValueError("move events require a destination path")
            object.__setattr__(
                self,
                "destination_path",
                normalize_relative_path(self.destination_path),
            )
        elif self.destination_path is not None:
            raise ValueError("only move events may have a destination path")


def coalesce_events(events: Iterable[JournalEvent]) -> tuple[JournalEvent, ...]:
    path_events: dict[tuple[str, str], JournalEvent] = {}
    structural_events: list[JournalEvent] = []
    for event in sorted(events, key=lambda item: (item.sequence, item.side)):
        if event.kind in {EventKind.MOVE, EventKind.RESCAN_REQUIRED}:
            structural_events.append(event)
            continue
        key = (event.side, event.path)
        previous = path_events.get(key)
        path_events[key] = _coalesce_path_event(previous, event)
    combined = [*structural_events, *path_events.values()]
    return tuple(sorted(combined, key=lambda item: (item.sequence, item.side)))


def _coalesce_path_event(
    previous: JournalEvent | None,
    current: JournalEvent,
) -> JournalEvent:
    if previous is None:
        return current
    if previous.kind is EventKind.CREATE and current.kind is EventKind.MODIFY:
        return replace(current, kind=EventKind.CREATE)
    if previous.kind is EventKind.DELETE and current.kind is EventKind.CREATE:
        return replace(current, kind=EventKind.MODIFY)
    return current
