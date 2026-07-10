from pathlib import Path

import pytest

import remote_sandbox.registry as registry_module
from remote_sandbox.registry import (
    BindingRecord,
    RegistryError,
    current_workspace_record,
    list_binding_records,
    register_workspace,
    upsert_binding_record,
)
from remote_sandbox.workspace import WorkspaceSpec


def _workspace_spec(
    *,
    workspace_id: str,
    name: str,
    local_root: Path,
) -> WorkspaceSpec:
    return WorkspaceSpec(
        schema_version=1,
        workspace_id=workspace_id,
        name=name,
        target="host",
        local_root=str(local_root),
        remote_root=f"/remote/{name}",
        created_at="2026-07-10T00:00:00+00:00",
    )


def test_register_workspace_persists_binding_record(tmp_path: Path) -> None:
    registry = tmp_path / "connections.toml"
    local_root = tmp_path / "repo" / ".." / "repo"
    spec = _workspace_spec(
        workspace_id="00000000-0000-4000-8000-000000000001",
        name="dq",
        local_root=local_root,
    )

    record = register_workspace(spec, registry=registry)

    assert record == BindingRecord(
        name="dq",
        workspace_id=spec.workspace_id,
        target="host",
        remote_path="/remote/dq",
        local_path=str(local_root.resolve(strict=False)),
        updated_at=spec.created_at,
    )
    assert list_binding_records(registry) == [record]


def test_registry_rejects_noncanonical_workspace_id(tmp_path: Path) -> None:
    registry = tmp_path / "connections.toml"
    registry.write_text(
        """\
[[connections]]
name = "dq"
workspace_id = "00000000-0000-4000-8000-00000000000A"
target = "host"
remote_path = "/remote/dq"
local_path = "/local/dq"
updated_at = "2026-07-10T00:00:00Z"
""",
        encoding="utf-8",
    )

    with pytest.raises(RegistryError, match="canonical UUID"):
        list_binding_records(registry)


@pytest.mark.parametrize("conflict", ["name", "local_root"])
def test_register_workspace_rejects_conflicting_identity(
    conflict: str,
    tmp_path: Path,
) -> None:
    registry = tmp_path / "connections.toml"
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first = _workspace_spec(
        workspace_id="00000000-0000-4000-8000-000000000001",
        name="first",
        local_root=first_root,
    )
    register_workspace(first, registry=registry)
    second = _workspace_spec(
        workspace_id="00000000-0000-4000-8000-000000000002",
        name="first" if conflict == "name" else "second",
        local_root=first_root if conflict == "local_root" else second_root,
    )

    with pytest.raises(RegistryError, match="already exists|already registered"):
        register_workspace(second, registry=registry)


def test_current_workspace_uses_longest_registered_prefix(tmp_path: Path) -> None:
    registry = tmp_path / "connections.toml"
    outer = tmp_path / "repo"
    inner = outer / "nested"
    (inner / "src").mkdir(parents=True)
    upsert_binding_record(
        registry,
        BindingRecord(
            "outer",
            "00000000-0000-4000-8000-000000000001",
            "host",
            "/outer",
            str(outer),
            "2026-07-10T00:00:00Z",
        ),
    )
    upsert_binding_record(
        registry,
        BindingRecord(
            "inner",
            "00000000-0000-4000-8000-000000000002",
            "host",
            "/inner",
            str(inner),
            "2026-07-10T00:00:00Z",
        ),
    )

    record = current_workspace_record(registry, inner / "src")

    assert record is not None
    assert record.name == "inner"


def test_current_workspace_does_not_read_in_tree_marker(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry = tmp_path / "connections.toml"
    local_root = tmp_path / "repo"
    local_root.mkdir()
    upsert_binding_record(
        registry,
        BindingRecord(
            "dq",
            "00000000-0000-4000-8000-000000000001",
            "host",
            "/remote/dq",
            str(local_root),
            "2026-07-10T00:00:00Z",
        ),
    )

    def fail_marker_read(_local_root: Path) -> None:
        raise AssertionError("current workspace lookup read an in-tree marker")

    monkeypatch.setattr(
        registry_module,
        "read_local_marker",
        fail_marker_read,
        raising=False,
    )

    record = current_workspace_record(registry, local_root)

    assert record is not None
    assert record.name == "dq"
