from __future__ import annotations

import pytest

import remote_sandbox.prompt as prompt_module
from remote_sandbox.prompt import PromptRenderer, display_width, render_status_slot
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def _status(
    phase: WorkspacePhase,
    stage: str,
    *,
    files_done: int = 0,
    files_total: int = 0,
    bytes_done: int = 0,
    bytes_total: int = 0,
    conflicts: int = 0,
) -> WorkspaceStatus:
    return WorkspaceStatus(
        phase,
        SyncProgress(
            stage,
            files_done=files_done,
            files_total=files_total,
            bytes_done=bytes_done,
            bytes_total=bytes_total,
        ),
        conflicts=conflicts,
    )


def test_all_live_status_slots_have_equal_display_width() -> None:
    renderer = PromptRenderer(width=34)
    states = [
        _status(WorkspacePhase.INITIAL_SYNCING, "scanning"),
        _status(WorkspacePhase.INITIAL_SYNCING, "planning"),
        _status(
            WorkspacePhase.INITIAL_SYNCING,
            "transferring",
            files_done=40,
            files_total=100,
        ),
        _status(WorkspacePhase.DISCONNECTED, "offline"),
    ]

    rendered = [renderer.render("ZJU_2", "dq", status) for status in states]

    assert {display_width(value) for value in rendered} == {34}
    assert "scanning" in rendered[0]
    assert "planning" in rendered[1]
    assert "sync 40%" in rendered[2]
    assert "offline" in rendered[3]


@pytest.mark.parametrize(
    ("status", "suffix"),
    [
        (_status(WorkspacePhase.INITIAL_SYNCING, "scanning"), " scanning"),
        (_status(WorkspacePhase.INITIAL_SYNCING, "planning"), " planning"),
        (
            _status(
                WorkspacePhase.SYNCING,
                "transferring",
                bytes_done=1,
                bytes_total=4,
            ),
            " sync 25%",
        ),
        (_status(WorkspacePhase.SYNCING, "transferring"), " sync"),
        (_status(WorkspacePhase.DEGRADED, "audit-requested"), " degraded"),
        (_status(WorkspacePhase.DISCONNECTED, "reconnecting"), " offline"),
        (_status(WorkspacePhase.FAILED, "failed"), " failed"),
        (_status(WorkspacePhase.STOPPED, "stopped"), " stopped"),
    ],
)
def test_status_suffix_handles_phases_and_unknown_totals(
    status: WorkspaceStatus,
    suffix: str,
) -> None:
    assert render_status_slot("target", "name", status).rstrip().endswith(f"{suffix}]")


def test_conflicts_take_priority_over_offline_and_progress() -> None:
    status = _status(
        WorkspacePhase.DISCONNECTED,
        "transferring",
        files_done=40,
        files_total=100,
        conflicts=7,
    )

    rendered = PromptRenderer().render("host", "dq", status)

    assert "conflict 7" in rendered
    assert "offline" not in rendered
    assert "sync 40%" not in rendered


def test_ready_prompt_is_compact() -> None:
    status = _status(WorkspacePhase.READY, "idle")

    assert PromptRenderer(width=34).render("ZJU_2", "dq", status) == "[ZJU_2:dq]"


def test_long_unicode_labels_truncate_to_display_width_and_close_bracket() -> None:
    rendered = PromptRenderer(width=24).render(
        "量子计算中心-long-target",
        "同步工作区-name",
        _status(WorkspacePhase.INITIAL_SYNCING, "planning"),
    )

    assert display_width(rendered) == 24
    assert rendered.endswith("]")
    assert not rendered.endswith(" ]")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("👩‍💻", 2),
        ("✈️", 2),
        ("e\u0301", 1),
        ("量", 2),
    ],
)
def test_display_width_uses_terminal_sequence_semantics(value: str, expected: int) -> None:
    assert display_width(value) == expected


def test_display_width_rejects_nonprintable_text() -> None:
    with pytest.raises(ValueError, match="nonprintable"):
        display_width("visible\x00hidden")


def test_emoji_and_combining_labels_keep_exact_fixed_width() -> None:
    rendered = PromptRenderer(width=34).render(
        "cluster-👩‍💻-✈️",
        "cafe\u0301-量子",
        _status(WorkspacePhase.INITIAL_SYNCING, "planning"),
    )

    assert display_width(rendered) == 34
    assert rendered.endswith("]") or rendered.rstrip().endswith("]")


def test_truncation_does_not_leave_a_dangling_joiner_or_variation_selector() -> None:
    rendered = PromptRenderer(width=18).render(
        "👩‍💻" * 12,
        "✈️" * 12,
        _status(WorkspacePhase.INITIAL_SYNCING, "planning"),
    )
    visible = rendered.rstrip()

    assert display_width(rendered) == 18
    assert visible.endswith("]")
    assert not visible.endswith(("\u200d]", "\ufe0f]", "\u0301]"))


def test_truncation_keeps_emoji_presentation_sequence_atomic() -> None:
    assert prompt_module._truncate_closed_bracket("[123456✈️Z]", 9) == "[123456]"


@pytest.mark.parametrize("field", ["target", "name"])
@pytest.mark.parametrize("control", ["\n", "\r", "\t", "\x1b", "\x7f"])
def test_prompt_rejects_terminal_control_characters(field: str, control: str) -> None:
    values = {"target": "host", "name": "dq"}
    values[field] += control

    with pytest.raises(ValueError, match="control"):
        PromptRenderer().render(
            values["target"],
            values["name"],
            _status(WorkspacePhase.INITIAL_SYNCING, "scanning"),
        )


@pytest.mark.parametrize("width", [0, 1, 2, 7])
def test_prompt_width_must_fit_the_fixed_prefix(width: int) -> None:
    with pytest.raises(ValueError, match="width"):
        PromptRenderer(width=width)
