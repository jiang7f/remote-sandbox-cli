from __future__ import annotations

import time

import pytest


@pytest.mark.e2e
def test_same_shell_and_live_prompt_preserve_partial_input(ssh_fixture) -> None:
    local = ssh_fixture.local_workspace()
    remote = ssh_fixture.remote_workspace(empty=True)
    ssh_fixture.populate_local(local, files=2_000)
    shell = ssh_fixture.enter()
    original_pid = shell.remote_shell_pid()
    started = time.monotonic()

    shell.connect(remote=remote, local=local, name="prompt")
    shell.type_without_enter("python tra")
    before = shell.cursor_offset()
    shell.wait_for_prompt_text("sync ", timeout=5.0)
    shell.wait_for_prompt_change(timeout=5.0)

    assert shell.first_sync_status_at - started < 1.0
    assert shell.remote_shell_pid() == original_pid
    assert shell.visible_input() == "python tra"
    assert shell.cursor_offset() == before


@pytest.mark.e2e
def test_foreground_program_receives_no_prompt_redraw_bytes(ssh_fixture) -> None:
    shell = ssh_fixture.bound_shell(name="foreground")
    shell.run_foreground_probe(seconds=2.0)
    shell.trigger_remote_change("during-command.txt", b"x")

    assert shell.foreground_probe_received_private_redraw() is False
    shell.wait_for_prompt_text("[codex:", timeout=5.0)


@pytest.mark.e2e
def test_cancelled_binding_keeps_browsing_shell_open(ssh_fixture) -> None:
    shell = ssh_fixture.enter()
    original_pid = shell.remote_shell_pid()

    shell.begin_connect(name="cancelled")
    shell.reject_binding()

    assert shell.remote_shell_pid() == original_pid
    assert shell.is_open()
