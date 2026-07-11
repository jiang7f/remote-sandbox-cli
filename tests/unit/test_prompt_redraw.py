from __future__ import annotations

import math

import pytest

from remote_sandbox.prompt import PromptRedrawController


def test_redraw_is_live_while_typing_but_throttled() -> None:
    controller = PromptRedrawController(max_hz=4.0)

    assert controller.request_redraw(0.00, at_prompt=True, command_running=False) == b"\x1b[777~"
    assert controller.request_redraw(0.10, at_prompt=True, command_running=False) is None
    assert controller.request_redraw(0.26, at_prompt=True, command_running=False) == b"\x1b[777~"
    assert controller.request_redraw(0.60, at_prompt=False, command_running=True) is None


def test_suppressed_redraw_does_not_consume_the_throttle_window() -> None:
    controller = PromptRedrawController(max_hz=4.0)

    assert controller.request_redraw(0.0, at_prompt=False, command_running=False) is None
    assert controller.request_redraw(0.01, at_prompt=True, command_running=False) is not None
    assert controller.request_redraw(1.0, at_prompt=True, command_running=True) is None
    assert controller.request_redraw(1.01, at_prompt=True, command_running=False) is not None


@pytest.mark.parametrize("max_hz", [0.0, -1.0, math.inf, math.nan])
def test_redraw_rate_must_be_positive_and_finite(max_hz: float) -> None:
    with pytest.raises(ValueError, match="max_hz"):
        PromptRedrawController(max_hz=max_hz)
