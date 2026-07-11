from helpers.sync_harness import FakePtyBackendHarness


def test_binding_success_reuses_the_original_pty(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    original_pid = session.remote_shell_pid

    session.type("codex-rsb connect --name dq\n")
    session.accept_binding()

    assert session.remote_shell_pid == original_pid
    assert "Shared connection" not in session.output
    assert session.prompt_mode == "managed"


def test_incomplete_remote_destination_starts_in_home_then_enters_when_ready(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()

    session.connect(direction="local-to-remote", remote_root="/work/dq")

    assert session.remote_cwd == "/home/test"
    session.publish_ready()
    assert session.remote_cwd == "/work/dq"


def test_complete_remote_source_starts_in_workspace_immediately(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()

    session.connect(direction="remote-to-local", remote_root="/work/dq")

    assert session.remote_cwd == "/work/dq"


def test_ready_does_not_change_directory_after_user_leaves_holding_directory(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    session.connect(direction="local-to-remote", remote_root="/work/dq")

    session.type("cd /tmp\n")
    session.publish_ready()

    assert session.remote_cwd == "/tmp"


def test_binding_cancellation_keeps_browsing_in_the_original_pty(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    original_pid = session.remote_shell_pid

    session.type("codex-rsb connect --name dq\n")
    session.reject_binding("Binding cancelled")

    assert session.remote_shell_pid == original_pid
    assert session.prompt_mode == "enter"
    assert "Binding cancelled" in session.output
