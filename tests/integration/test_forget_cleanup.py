from __future__ import annotations

from helpers.sync_harness import CliHarness


def test_forget_keeps_local_binding_when_remote_cleanup_is_unavailable(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.remote_forget_fails("offline")

    result = cli_fixture.run(["forget", "dq"])

    assert result.exit_code == 2
    assert cli_fixture.registry_has("dq")


def test_local_only_forget_removes_local_state_and_reports_remote_residue(
    cli_fixture: CliHarness,
) -> None:
    result = cli_fixture.run(["forget", "dq", "--local-only"])

    assert result.exit_code == 0
    assert not cli_fixture.registry_has("dq")
    assert "~/.codex-remote-sandbox/workspaces/" in result.stdout
    assert cli_fixture.record.workspace_id in result.stdout


def test_normal_forget_uses_double_ended_cleanup_order(cli_fixture: CliHarness) -> None:
    result = cli_fixture.run(["forget", "dq"])

    assert result.exit_code == 0
    assert cli_fixture.cleanup_calls == [
        "stop-local-supervisor",
        "stop-remote-watcher",
        "delete-remote-workspace",
        "prune-unused-remote-agent",
        "delete-local-workspace",
        "delete-registry-record",
    ]


def test_forget_is_idempotent_after_success(cli_fixture: CliHarness) -> None:
    first = cli_fixture.run(["forget", "dq"])
    second = cli_fixture.run(["forget", "dq"])

    assert first.exit_code == 0
    assert second.exit_code == 0
    assert "already forgotten" in second.stdout


def test_forget_retries_after_registry_delete_failure(cli_fixture: CliHarness) -> None:
    cli_fixture.registry_delete_fails_once()

    first = cli_fixture.run(["forget", "dq"])
    second = cli_fixture.run(["forget", "dq"])

    assert first.exit_code == 2
    assert "registry busy" in first.stderr
    assert cli_fixture.registry_has("dq") is False
    assert second.exit_code == 0
