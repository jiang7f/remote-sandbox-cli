from __future__ import annotations

import pytest


@pytest.mark.e2e
def test_connect_sync_run_and_forget_without_workspace_metadata(ssh_fixture) -> None:
    production_sentinel = ssh_fixture.create_production_state_sentinel(b"do-not-touch")
    remote_production_sentinel = ssh_fixture.create_remote_production_state_sentinel(b"remote")
    local = ssh_fixture.local_workspace()
    remote = ssh_fixture.remote_workspace(empty=True)
    (local / "train.py").write_text("print('ok')\n", encoding="utf-8")
    (local / ".git").mkdir()
    (local / ".git" / "index").write_bytes(b"local-only-git")

    shell = ssh_fixture.enter()
    shell.connect(remote=remote, local=local, name="dq")
    shell.wait_for_prompt("[codex:")
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
    assert production_sentinel.read_bytes() == b"do-not-touch"
    assert ssh_fixture.read_remote(remote_production_sentinel) == b"remote"
