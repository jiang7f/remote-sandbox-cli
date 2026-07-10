from pathlib import Path

import pytest

from remote_sandbox.workspace import (
    WorkspaceSpec,
    new_workspace_spec,
    read_workspace_spec,
    validate_workspace_id,
    workspace_paths,
    write_workspace_spec,
)


def test_workspace_paths_live_outside_working_tree(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(tmp_path / "home"))
    local_root = tmp_path / "project"
    local_root.mkdir()
    spec = new_workspace_spec(
        name="dq",
        target="ZJU_2",
        local_root=local_root,
        remote_root="/home/user/dq",
    )

    paths = workspace_paths(spec.workspace_id)

    assert paths.root == tmp_path / "home" / "workspaces" / spec.workspace_id
    assert paths.workspace_file == paths.root / "workspace.toml"
    assert paths.state_db == paths.root / "state.sqlite3"
    assert paths.daemon_log == paths.root / "daemon.log"
    assert not (local_root / ".remote-sandbox").exists()
    assert not (local_root / ".codex-remote-sandbox").exists()


def test_workspace_metadata_is_created_with_user_only_permissions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(tmp_path / "home"))
    local_root = tmp_path / "project"
    local_root.mkdir()
    spec = new_workspace_spec(
        name="dq",
        target="host",
        local_root=local_root,
        remote_root="/work/dq",
    )
    paths = workspace_paths(spec.workspace_id)

    write_workspace_spec(paths.workspace_file, spec)

    assert paths.root.stat().st_mode & 0o777 == 0o700
    assert paths.workspace_file.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "value",
    [
        "../escape",
        "not-a-uuid",
        "00000000-0000-4000-8000-000000000001/child",
        "00000000-0000-4000-8000-00000000000A",
        "{00000000-0000-4000-8000-000000000001}",
    ],
)
def test_workspace_id_rejects_path_and_noncanonical_values(value: str) -> None:
    with pytest.raises(ValueError):
        validate_workspace_id(value)


def test_workspace_spec_round_trips_through_atomic_toml(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(tmp_path / "home"))
    spec = WorkspaceSpec(
        schema_version=1,
        workspace_id="00000000-0000-4000-8000-000000000001",
        name='dq "quoted"',
        target="ZJU_2",
        local_root=str(tmp_path / "project with spaces"),
        remote_root="/work/back\\slash\nand-newline",
        created_at="2026-07-10T00:00:00+00:00",
    )
    workspace_file = workspace_paths(spec.workspace_id).workspace_file

    write_workspace_spec(workspace_file, spec)

    assert read_workspace_spec(workspace_file) == spec
    assert list(workspace_file.parent.glob("workspace.*.tmp")) == []


def test_read_workspace_spec_rejects_noncanonical_workspace_id(tmp_path: Path) -> None:
    workspace_file = tmp_path / "workspace.toml"
    workspace_file.write_text(
        """\
schema_version = 1
workspace_id = "00000000-0000-4000-8000-00000000000A"
name = "dq"
target = "host"
local_root = "/local/dq"
remote_root = "/remote/dq"
created_at = "2026-07-10T00:00:00+00:00"
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="canonical UUID"):
        read_workspace_spec(workspace_file)
