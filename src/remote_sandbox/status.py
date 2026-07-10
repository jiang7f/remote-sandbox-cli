from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkspacePhase(StrEnum):
    STARTING = "starting"
    INITIAL_SYNCING = "initial-syncing"
    READY = "ready"
    SYNCING = "syncing"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class SyncProgress:
    stage: str
    files_done: int = 0
    files_total: int = 0
    bytes_done: int = 0
    bytes_total: int = 0
    current_path: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.stage, str) or not self.stage:
            raise ValueError("progress stage must not be empty")
        counts = (self.files_done, self.files_total, self.bytes_done, self.bytes_total)
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("progress counts must be non-negative integers")
        if self.files_total and self.files_done > self.files_total:
            raise ValueError("files_done must not exceed files_total")
        if self.bytes_total and self.bytes_done > self.bytes_total:
            raise ValueError("bytes_done must not exceed bytes_total")


@dataclass(frozen=True, slots=True)
class WorkspaceStatus:
    phase: WorkspacePhase
    progress: SyncProgress
    pending: int = 0
    conflicts: int = 0
    last_error: str | None = None
    last_sync_at: float | None = None

    def __post_init__(self) -> None:
        counts = (self.pending, self.conflicts)
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("status counts must be non-negative integers")


def format_progress(progress: SyncProgress) -> str:
    parts = [progress.stage]
    if progress.files_total:
        parts.append(f"{progress.files_done}/{progress.files_total} files")
    elif progress.files_done:
        parts.append(f"{progress.files_done} files")
    if progress.bytes_total:
        parts.append(
            f"{_format_megabytes(progress.bytes_done)}/{_format_megabytes(progress.bytes_total)} MB"
        )
    elif progress.bytes_done:
        parts.append(f"{_format_megabytes(progress.bytes_done)} MB")
    if progress.current_path:
        parts.append(progress.current_path)
    return " ".join(parts)


def _format_megabytes(byte_count: int) -> str:
    return f"{byte_count / 1_000_000:.1f}"
