from __future__ import annotations

from pathlib import Path

import pytest
from helpers.sync_harness import CliHarness

from remote_sandbox.placeholder import PLACEHOLDER_MAGIC, PlaceholderMetadata, encode_placeholder


def test_remote_exit_code_wins_when_sync_followup_fails(cli_fixture: CliHarness) -> None:
    cli_fixture.remote_command_result(returncode=7)
    cli_fixture.followup_sync_fails("network down")

    result = cli_fixture.run(["run", "dq", "--", "false"])

    assert result.exit_code == 7
    assert "sync" in result.stderr
    assert "network down" in result.stderr
    assert "Traceback" not in result.stderr


def test_status_explains_foreground_reconnect_for_password_auth(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.set_workspace_state("dq", "disconnected", error="authentication required")

    result = cli_fixture.run(["status", "dq"])

    assert result.exit_code == 0
    assert "disconnected" in result.stdout
    assert "rsb reconnect dq" in result.stdout


def test_init_writes_only_user_ignore_configuration(cli_fixture: CliHarness) -> None:
    result = cli_fixture.run(["init"])

    assert result.exit_code == 0
    content = (cli_fixture.pair.local / ".rsbignore").read_text(encoding="utf-8")
    assert ".venv/" in content
    assert "__pycache__/" in content
    assert "Git metadata is always local-only" in content
    assert not (cli_fixture.pair.local / ".remote-sandbox").exists()


def test_fetch_replaces_valid_placeholder_without_in_tree_metadata(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.create_remote_placeholder("weights.bin", b"remote-weights")

    result = cli_fixture.run(["fetch", "weights.bin"])

    assert result.exit_code == 0
    assert cli_fixture.local_bytes("weights.bin") == b"remote-weights"
    assert not (cli_fixture.pair.local / ".remote-sandbox").exists()


def test_fetch_rejects_invalid_placeholder_metadata(cli_fixture: CliHarness) -> None:
    cli_fixture.create_remote_placeholder("weights.bin", b"remote-weights")
    (cli_fixture.pair.local / "weights.bin").write_bytes(PLACEHOLDER_MAGIC + b"not-json")

    result = cli_fixture.run(["fetch", "weights.bin"])

    assert result.exit_code == 2
    assert "placeholder metadata is invalid" in result.stderr


def test_fetch_rejects_placeholder_path_mismatch(cli_fixture: CliHarness) -> None:
    cli_fixture.create_remote_placeholder("weights.bin", b"remote-weights")
    (cli_fixture.pair.local / "weights.bin").write_bytes(
        encode_placeholder(
            PlaceholderMetadata(
                "other.bin",
                len(b"remote-weights"),
                1,
                "bad-hash",
            )
        )
    )

    result = cli_fixture.run(["fetch", "weights.bin"])

    assert result.exit_code == 2
    assert "placeholder path mismatch" in result.stderr


def test_fetch_rejects_symlink_parent(cli_fixture: CliHarness, tmp_path: Path) -> None:
    outside = tmp_path / "outside-placeholder"
    outside.mkdir()
    (cli_fixture.pair.local / "linked").symlink_to(outside, target_is_directory=True)

    result = cli_fixture.run(["fetch", "linked/weights.bin"])

    assert result.exit_code == 2
    assert "symlink parent" in result.stderr


def test_no_shell_connect_returns_after_initial_syncing_publication(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.block_initial_sync()

    result = cli_fixture.run(
        [
            "connect",
            "host",
            "--remote",
            "/work/dq",
            "--local",
            str(cli_fixture.pair.local),
            "--name",
            "dq",
            "--no-shell",
        ]
    )

    assert result.exit_code == 0
    assert "Connected dq" in result.stdout
    assert "initial sync continues in background" in result.stdout
    assert "rsb status dq --watch" in result.stdout
    assert cli_fixture.store.get_status().phase.value == "initial-syncing"
    assert cli_fixture.store.initial_sync_started_generation() == 1


def test_no_shell_fast_initial_sync_uses_ack_without_false_background_copy(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.complete_initial_sync_immediately()

    result = cli_fixture.run(
        [
            "connect",
            "host",
            "--remote",
            "/work/dq",
            "--local",
            str(cli_fixture.pair.local),
            "--name",
            "dq",
            "--no-shell",
        ]
    )

    assert result.exit_code == 0
    assert cli_fixture.store.initial_sync_started_generation() == 1
    assert cli_fixture.store.get_status().phase.value == "ready"
    assert "continues in background" not in result.stdout
    assert "Initial sync completed" in result.stdout


def test_reconnect_completed_workspace_does_not_wait_for_new_initial_ack(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.reconnect_existing_workspace()
    cli_fixture.services.wait_initial_sync = lambda _record, _generation: (_ for _ in ()).throw(
        AssertionError("reconnect must not wait for initial-sync acknowledgement")
    )

    result = cli_fixture.run(["reconnect", "dq", "--no-shell"])

    assert result.exit_code == 0
    assert "Workspace is ready" in result.stdout
    assert "continues in background" not in result.stdout


def test_default_error_is_one_line_and_debug_enables_traceback(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.followup_sync_fails("first line\nsecond line")
    normal = cli_fixture.run(["run", "dq", "--", "true"])
    debug = cli_fixture.run(["--debug", "run", "dq", "--", "true"])

    assert "Traceback" not in normal.stderr
    assert "first line second line" in normal.stderr
    assert "Traceback" in debug.stderr


@pytest.mark.parametrize(
    "phase",
    [
        "starting",
        "initial-syncing",
        "syncing",
        "ready",
        "degraded",
        "disconnected",
        "failed",
        "stopped",
    ],
)
def test_status_renders_every_workspace_phase(
    cli_fixture: CliHarness,
    phase: str,
) -> None:
    cli_fixture.set_workspace_state("dq", phase)

    result = cli_fixture.run(["status", "dq"])

    assert result.exit_code == 0
    assert phase in result.stdout


def test_status_watch_redraws_one_table(cli_fixture: CliHarness) -> None:
    cli_fixture.set_workspace_state("dq", "starting")
    cli_fixture.services.watch_limit = 2
    cli_fixture.services.watch_sleep = lambda _seconds: cli_fixture.set_workspace_state(
        "dq", "ready"
    )

    result = cli_fixture.run(["status", "dq", "--watch"])

    assert result.exit_code == 0
    assert "starting" in result.stdout
    assert "\x1b[H\x1b[2J" in result.stdout
    assert result.stdout.rstrip().endswith("0")
    assert "ready" in result.stdout.split("\x1b[H\x1b[2J")[-1]


def test_status_watch_ctrl_c_exits_without_traceback(cli_fixture: CliHarness) -> None:
    def interrupt(_seconds: float) -> None:
        raise KeyboardInterrupt

    cli_fixture.services.watch_sleep = interrupt

    result = cli_fixture.run(["status", "dq", "--watch"])

    assert result.exit_code == 0
    assert "Traceback" not in result.stderr
