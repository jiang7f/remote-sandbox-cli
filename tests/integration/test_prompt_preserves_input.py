from __future__ import annotations

from helpers.sync_harness import PromptShellHarness


def test_progress_redraw_preserves_partial_readline_buffer(
    shell_fixture: PromptShellHarness,
) -> None:
    shell_fixture.type_without_enter("python tra")

    shell_fixture.publish_progress(32)
    shell_fixture.publish_progress(40)

    assert shell_fixture.visible_input == "python tra"
    assert shell_fixture.cursor_offset == len("python tra")
    assert "sync 40%" in shell_fixture.current_prompt


def test_multiple_redraws_preserve_a_cursor_in_the_middle(
    shell_fixture: PromptShellHarness,
) -> None:
    shell_fixture.type_without_enter("python trace.py")
    shell_fixture.move_cursor_left(8)
    expected_cursor = len("python trace.py") - 8

    shell_fixture.publish_progress(8)
    shell_fixture.publish_progress(16)
    shell_fixture.publish_progress(24)

    assert shell_fixture.visible_input == "python trace.py"
    assert shell_fixture.cursor_offset == expected_cursor
    assert "sync 24%" in shell_fixture.current_prompt


def test_unchanged_status_does_not_request_another_redraw(
    shell_fixture: PromptShellHarness,
) -> None:
    shell_fixture.publish_progress(40)
    redraws = shell_fixture.redraw_count

    shell_fixture.publish_progress(40)

    assert shell_fixture.redraw_count == redraws


def test_status_waits_for_the_next_prompt_while_a_command_runs(
    shell_fixture: PromptShellHarness,
) -> None:
    shell_fixture.submit("sleep 1")
    prompt_before = shell_fixture.current_prompt

    shell_fixture.publish_progress(72)

    assert shell_fixture.current_prompt == prompt_before
    assert "sync 72%" not in shell_fixture.current_prompt
    shell_fixture.publish_prompt()
    assert "sync 72%" in shell_fixture.current_prompt
