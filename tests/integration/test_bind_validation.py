from __future__ import annotations

from pathlib import Path

import pytest

import remote_sandbox.bind as bind_module
from remote_sandbox.bind import BindError, bind_workspace
from remote_sandbox.ssh import FakeSshRunner
from remote_sandbox.workspace import workspace_paths


class RecordingRemoteRegistration:
    def __init__(self) -> None:
        self.forgotten = False
        self.closed = False

    def forget(self) -> dict[str, object]:
        self.forgotten = True
        return {"forgotten": True}

    def close(self) -> None:
        self.closed = True


def test_two_unrelated_non_empty_trees_are_rejected_before_metadata_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(state_home))
    local = tmp_path / "local"
    local.mkdir()
    (local / "local.txt").write_text("local", encoding="utf-8")
    runner = FakeSshRunner()
    runner.mkdir_p("host", "/work/remote")
    runner.write_bytes_atomic("host", "/work/remote/remote.txt", b"remote")

    with pytest.raises(BindError, match="two non-empty"):
        bind_workspace(
            target="host",
            remote="/work/remote",
            local=local,
            runner=runner,
            connection_name="dq",
        )

    assert not state_home.exists()
    assert not any(".remote-sandbox" in path for _target, path in runner.files)
    assert not any(".remote-sandbox" in path for _target, path in runner.binary_files)
    assert not any(".remote-sandbox" in path for _target, path in runner.dirs)


def test_bind_writes_only_external_metadata_and_never_runs_legacy_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(state_home))
    local = tmp_path / "local"
    runner = FakeSshRunner()
    monkeypatch.setattr(
        bind_module,
        "_register_remote_workspace",
        lambda *args, **kwargs: None,
    )

    def fail_sync(*args: object, **kwargs: object) -> None:
        raise AssertionError("legacy sync was called")

    monkeypatch.setattr(bind_module, "SyncSession", fail_sync, raising=False)
    result = bind_workspace(
        target="host",
        remote="/work/remote",
        local=local,
        runner=runner,
        connection_name="dq",
    )

    paths = workspace_paths(result.workspace.workspace_id)
    assert paths.workspace_file.is_file()
    assert not (local / ".remote-sandbox").exists()
    assert not (local / ".codex-remote-sandbox").exists()
    assert not any("/work/remote/.remote-sandbox" in path for _target, path in runner.files)
    assert result.connection.workspace_id == result.workspace.workspace_id


@pytest.mark.parametrize("failure_boundary", ["workspace", "state", "registry"])
def test_bind_rolls_back_external_metadata_and_remote_registration(
    failure_boundary: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(state_home))
    runner = FakeSshRunner()
    registration = RecordingRemoteRegistration()
    monkeypatch.setattr(
        bind_module,
        "_register_remote_workspace",
        lambda *args, **kwargs: registration,
    )

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError(f"injected {failure_boundary} failure")

    if failure_boundary == "workspace":
        monkeypatch.setattr(bind_module, "write_workspace_spec", fail)
    elif failure_boundary == "state":
        monkeypatch.setattr(bind_module.WorkspaceStore, "open", fail)
    else:
        monkeypatch.setattr(bind_module, "register_workspace", fail)

    with pytest.raises(BindError, match=failure_boundary):
        bind_workspace(
            target="host",
            remote="/work/remote",
            local=tmp_path / "local",
            runner=runner,
            connection_name="dq",
        )

    assert registration.forgotten is True
    assert registration.closed is True
    assert not state_home.exists()
