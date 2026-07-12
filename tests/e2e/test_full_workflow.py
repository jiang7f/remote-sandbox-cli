from __future__ import annotations

import time

import pytest


@pytest.mark.e2e
def test_connect_sync_run_and_forget_without_workspace_metadata(ssh_fixture) -> None:
    local_state_sentinel = ssh_fixture.create_local_state_sentinel(b"do-not-touch")
    remote_state_sentinel = ssh_fixture.create_remote_state_sentinel(b"remote")
    local = ssh_fixture.local_workspace()
    remote = ssh_fixture.remote_workspace(empty=True)
    (local / "train.py").write_text("print('ok')\n", encoding="utf-8")
    (local / ".git").mkdir()
    (local / ".git" / "index").write_bytes(b"local-only-git")

    shell = ssh_fixture.enter()
    shell.connect(remote=remote, local=local, name="dq")
    ssh_fixture.wait_for_remote_file(remote / "train.py")

    result = ssh_fixture.cli("run", "dq", "--", "python3", "train.py")
    assert result.returncode == 0
    assert result.stdout.strip() == "ok"
    failed = ssh_fixture.cli("run", "dq", "--", "sh", "-c", "exit 17")
    assert failed.returncode == 17
    assert not (local / ".remote-sandbox").exists()
    assert not ssh_fixture.remote_exists(remote / ".remote-sandbox")
    assert not ssh_fixture.remote_exists(remote / ".git")

    remote_metadata = ssh_fixture.remote_metadata_path("dq")
    assert ssh_fixture.cli("forget", "dq").returncode == 0
    assert not ssh_fixture.local_binding_exists("dq")
    assert not ssh_fixture.remote_exists(remote_metadata)
    assert local_state_sentinel.read_bytes() == b"do-not-touch"
    assert ssh_fixture.read_remote(remote_state_sentinel) == b"remote"


@pytest.mark.e2e
def test_remote_watcher_subscription_propagates_within_two_seconds(ssh_fixture) -> None:
    local, remote = ssh_fixture.bound_pair(name="remote-latency", password=False)
    remote_path = remote / "latency.txt"
    local_path = local / "latency.txt"

    created_at = time.monotonic()
    ssh_fixture.write_remote(remote_path, b"observed")
    ssh_fixture.wait_for_local_file(local_path, timeout=2.0)
    create_elapsed = time.monotonic() - created_at

    deleted_at = time.monotonic()
    ssh_fixture.delete_remote(remote_path)
    ssh_fixture.wait_until_missing(local_path, timeout=2.0)
    delete_elapsed = time.monotonic() - deleted_at

    assert create_elapsed < 2.0
    assert delete_elapsed < 2.0
