from __future__ import annotations

import shutil
from pathlib import Path

from helpers.sync_harness import CliHarness
from pytest import MonkeyPatch

from remote_sandbox.cli import default_cli_services, invoke_cli
from remote_sandbox.registry import (
    BindingRecord,
    delete_binding_record,
    find_binding_record,
    upsert_binding_record,
)
from remote_sandbox.workspace import workspace_paths


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


def test_local_only_forget_does_not_read_or_delete_installed_registry(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    development_home = tmp_path / "codex-home"
    installed_registry = tmp_path / "installed" / "connections.toml"
    installed = BindingRecord(
        name="installed",
        workspace_id="00000000-0000-4000-8000-000000000183",
        target="host",
        remote_path="/work/installed",
        local_path=str(tmp_path / "installed-project"),
        updated_at="2026-07-12T00:00:00Z",
    )
    upsert_binding_record(installed_registry, installed)
    installed_before = installed_registry.read_bytes()
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(development_home))
    monkeypatch.setenv("REMOTE_SANDBOX_CONNECTIONS", str(installed_registry))
    services = default_cli_services()

    result = invoke_cli(
        ["forget", "installed", "--local-only"],
        services=services,
    )

    assert result.exit_code == 0
    assert "already forgotten" in result.stdout
    assert installed_registry.read_bytes() == installed_before


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


def test_forget_does_not_delete_connection_rebound_during_cleanup(
    cli_fixture: CliHarness,
) -> None:
    original_delete_local = cli_fixture.services.delete_local_workspace
    replacement = BindingRecord(
        name=cli_fixture.record.name,
        workspace_id="00000000-0000-4000-8000-000000000099",
        target="other-host",
        remote_path="/work/replacement",
        local_path=str(cli_fixture.pair.local.parent / "replacement"),
        updated_at="2026-07-11T00:00:00Z",
    )

    def delete_local_and_rebind(record: BindingRecord) -> None:
        original_delete_local(record)
        assert delete_binding_record(
            record.name,
            cli_fixture.registry,
            workspace_id=record.workspace_id,
        )
        upsert_binding_record(cli_fixture.registry, replacement)

    cli_fixture.services.delete_local_workspace = delete_local_and_rebind

    result = cli_fixture.run(["forget", "dq", "--local-only"])

    assert result.exit_code == 2
    assert "changed during cleanup" in result.stderr
    assert find_binding_record("dq", cli_fixture.registry) == replacement


def test_local_forget_keeps_registry_until_metadata_deletion_is_verified(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(state_home))
    record = BindingRecord(
        name="dq",
        workspace_id="00000000-0000-4000-8000-000000000098",
        target="host",
        remote_path="/work/dq",
        local_path=str(tmp_path / "local"),
        updated_at="2026-07-11T00:00:00Z",
    )
    metadata_root = workspace_paths(record.workspace_id).root
    metadata_root.mkdir(parents=True)
    services = default_cli_services()
    upsert_binding_record(services.registry, record)
    services.stop_local_supervisor = lambda _record: None
    original_rmtree = shutil.rmtree
    attempts = 0

    def controlled_rmtree(path: Path, *, ignore_errors: bool = False) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if ignore_errors:
                return
            raise PermissionError("metadata is not removable")
        if attempts == 2:
            return
        original_rmtree(path)

    monkeypatch.setattr("remote_sandbox.cli.shutil.rmtree", controlled_rmtree)

    permission_failure = invoke_cli(
        ["forget", "dq", "--local-only"],
        services=services,
    )
    residual_failure = invoke_cli(
        ["forget", "dq", "--local-only"],
        services=services,
    )
    success = invoke_cli(
        ["forget", "dq", "--local-only"],
        services=services,
    )

    assert permission_failure.exit_code == 2
    assert "metadata is not removable" in permission_failure.stderr
    assert residual_failure.exit_code == 2
    assert "still exists" in residual_failure.stderr
    assert success.exit_code == 0
    assert find_binding_record("dq", services.registry) is None
