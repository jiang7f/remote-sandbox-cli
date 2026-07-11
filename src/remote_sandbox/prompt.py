from __future__ import annotations

import math
import unicodedata

from remote_sandbox.status import WorkspacePhase, WorkspaceStatus

DEFAULT_PROMPT_WIDTH = 34
_MIN_PROMPT_WIDTH = len("[codex:]")


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
        value = f"[codex:{target}:{name}{suffix}]"
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
    width = 0
    for char in value:
        if unicodedata.combining(char):
            continue
        width += 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1
    return width


def _status_suffix(status: WorkspaceStatus) -> str:
    if status.conflicts:
        return f" conflict {status.conflicts}"
    if status.phase in {
        WorkspacePhase.DEGRADED,
        WorkspacePhase.DISCONNECTED,
        WorkspacePhase.FAILED,
        WorkspacePhase.STOPPED,
    }:
        return " offline"
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
    available = width - 1
    current = 0
    chars: list[str] = []
    for char in value[1:-1]:
        char_width = display_width(char)
        if current + char_width > available - 1:
            break
        chars.append(char)
        current += char_width
    return "[" + "".join(chars) + "]"


def _reject_control_characters(value: str, *, field: str) -> None:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"prompt {field} contains a terminal control character")
