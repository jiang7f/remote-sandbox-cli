from __future__ import annotations

import math

from wcwidth import wcswidth, wcwidth

from remote_sandbox.status import WorkspacePhase, WorkspaceStatus

DEFAULT_PROMPT_WIDTH = 34
_MIN_PROMPT_WIDTH = 8


class PromptRenderer:
    """Render compact and fixed-width workspace prompt fields."""

    def __init__(self, width: int = DEFAULT_PROMPT_WIDTH) -> None:
        if type(width) is not int or width < _MIN_PROMPT_WIDTH:
            raise ValueError(f"prompt width must be at least {_MIN_PROMPT_WIDTH}")
        self.width = width

    def render(self, target: str, name: str, status: WorkspaceStatus) -> str:
        """Render one locally trusted workspace status field."""
        _reject_control_characters(target, field="target")
        _reject_control_characters(name, field="name")
        suffix = _status_suffix(status)
        value = f"[{target}:{name}{suffix}]"
        fitted = _truncate_closed_bracket(value, self.width)
        if status.phase is WorkspacePhase.READY:
            return fitted
        return fitted + " " * (self.width - display_width(fitted))


class PromptRedrawController:
    """Rate-limit private Readline redraw requests."""

    REDRAW_SEQUENCE = b"\x1b[777~"

    def __init__(self, max_hz: float = 4.0) -> None:
        if not math.isfinite(max_hz) or max_hz <= 0:
            raise ValueError("max_hz must be positive and finite")
        self._interval = 1.0 / max_hz
        self._last = float("-inf")

    def request_redraw(
        self,
        now: float,
        *,
        at_prompt: bool,
        command_running: bool,
    ) -> bytes | None:
        """Return the private redraw sequence when state and rate allow it."""
        if not at_prompt or command_running or now - self._last < self._interval:
            return None
        self._last = now
        return self.REDRAW_SEQUENCE


def render_status_slot(
    target: str,
    name: str,
    status: WorkspaceStatus,
) -> str:
    """Render the default fixed-width prompt status field."""
    return PromptRenderer().render(target, name, status)


def display_width(value: str) -> int:
    """Return the terminal cell width of plain prompt text."""
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("prompt text contains a nonprintable character")
    width = wcswidth(value)
    if width < 0:
        raise ValueError("prompt text contains a nonprintable character")
    return width


def _status_suffix(status: WorkspaceStatus) -> str:
    if status.conflicts:
        return f" conflict {status.conflicts}"
    if status.phase is WorkspacePhase.DISCONNECTED:
        return " offline"
    if status.phase is WorkspacePhase.DEGRADED:
        return " degraded"
    if status.phase is WorkspacePhase.FAILED:
        return " failed"
    if status.phase is WorkspacePhase.STOPPED:
        return " stopped"
    if status.phase is WorkspacePhase.READY:
        return ""
    if status.progress.stage == "scanning":
        return " scanning"
    if status.progress.stage == "planning":
        return " planning"
    if status.progress.files_total:
        percent = status.progress.files_done * 100 // status.progress.files_total
        return f" sync {percent}%"
    if status.progress.bytes_total:
        percent = status.progress.bytes_done * 100 // status.progress.bytes_total
        return f" sync {percent}%"
    return " sync"


def _truncate_closed_bracket(value: str, width: int) -> str:
    if display_width(value) <= width:
        return value
    clusters: list[str] = []
    for cluster in _display_clusters(value[1:-1]):
        candidate = "[" + "".join((*clusters, cluster)) + "]"
        if display_width(candidate) > width:
            break
        clusters.append(cluster)
    return "[" + "".join(clusters) + "]"


def _display_clusters(value: str) -> list[str]:
    clusters: list[str] = []
    current = ""
    join_next = False
    for char in value:
        if not current:
            current = char
        elif join_next:
            current += char
            join_next = False
        elif char == "\u200d":
            current += char
            join_next = True
        elif wcwidth(char) == 0:
            current += char
        else:
            clusters.append(current)
            current = char
    if current:
        clusters.append(current)
    return clusters


def _reject_control_characters(value: str, *, field: str) -> None:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"prompt {field} contains a terminal control character")
