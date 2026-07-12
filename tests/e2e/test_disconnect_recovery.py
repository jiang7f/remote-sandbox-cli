from __future__ import annotations

import pytest


@pytest.mark.e2e
def test_key_connection_replays_remote_delete_after_network_recovery(ssh_fixture) -> None:
    local, remote = ssh_fixture.bound_pair(name="key-recovery", password=False)
    path = remote / "delete-me.txt"
    ssh_fixture.write_remote(path, b"x")
    ssh_fixture.wait_for_local_file(local / "delete-me.txt")

    ssh_fixture.disconnect_network()
    ssh_fixture.delete_remote(path)
    (local / "queued-local.txt").write_text("local", encoding="utf-8")
    ssh_fixture.reconnect_network()

    ssh_fixture.wait_until_missing(local / "delete-me.txt", timeout=10.0)
    ssh_fixture.wait_for_remote_file(remote / "queued-local.txt", timeout=10.0)
    assert ssh_fixture.cli("status", "key-recovery").returncode == 0


@pytest.mark.e2e
def test_password_connection_requires_foreground_reconnect_and_replays_queue(
    ssh_fixture,
) -> None:
    local, remote = ssh_fixture.bound_pair(name="password-recovery", password=True)
    ssh_fixture.expire_control_master("password-recovery")
    ssh_fixture.write_remote(remote / "queued.txt", b"queued")
    ssh_fixture.wait_for_state("password-recovery", "disconnected", timeout=10.0)

    reconnect = ssh_fixture.cli_with_password(
        "reconnect",
        "password-recovery",
        "--no-shell",
        password="test-password",
    )

    assert reconnect.returncode == 0
    ssh_fixture.wait_for_local_file(local / "queued.txt", timeout=10.0)


@pytest.mark.e2e
def test_stop_then_start_audits_changes_made_without_watchers(ssh_fixture) -> None:
    local, remote = ssh_fixture.bound_pair(name="restart-audit", password=False)
    assert ssh_fixture.cli("stop", "restart-audit").returncode == 0
    ssh_fixture.write_remote(remote / "while-stopped.txt", b"x")

    assert ssh_fixture.cli("start", "restart-audit").returncode == 0
    ssh_fixture.wait_for_local_file(local / "while-stopped.txt", timeout=10.0)


@pytest.mark.e2e
def test_local_only_forget_reports_and_leaves_remote_metadata(ssh_fixture) -> None:
    ssh_fixture.bound_pair(name="local-only", password=False)
    remote_metadata = ssh_fixture.remote_metadata_path("local-only")
    local_metadata = ssh_fixture.local_metadata_path("local-only")

    result = ssh_fixture.cli("forget", "local-only", "--local-only")

    assert result.returncode == 0
    assert f"~/.remote-sandbox/workspaces/{remote_metadata.name}" in result.stdout
    assert ssh_fixture.remote_exists(remote_metadata)
    assert not local_metadata.exists()
    assert not ssh_fixture.local_binding_exists("local-only")
