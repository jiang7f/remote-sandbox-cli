import contextlib
import multiprocessing
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import BrokenBarrierError
from typing import Protocol
from unittest.mock import patch

import pytest

from remote_sandbox.registry import (
    BindingRecord,
    RegistryError,
    current_workspace_record,
    delete_binding_record,
    list_binding_records,
    register_workspace,
    registry_path,
    upsert_binding_record,
)
from remote_sandbox.workspace import WorkspaceSpec


class _ProcessBarrier(Protocol):
    def wait(self, timeout: float | None = None) -> int: ...


@contextmanager
def _synchronized_registry_read(
    registry: Path,
    barrier: _ProcessBarrier,
) -> Iterator[None]:
    original_read_text = Path.read_text

    def synchronized_read_text(
        path: Path,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        content = original_read_text(path, encoding=encoding, errors=errors)
        if path == registry:
            with contextlib.suppress(BrokenBarrierError):
                barrier.wait(timeout=0.5)
        return content

    with patch.object(Path, "read_text", synchronized_read_text):
        yield


def _synchronized_upsert(
    registry: Path,
    record: BindingRecord,
    barrier: _ProcessBarrier,
) -> None:
    with _synchronized_registry_read(registry, barrier):
        upsert_binding_record(registry, record)


def _synchronized_delete(
    registry: Path,
    record: BindingRecord,
    barrier: _ProcessBarrier,
) -> None:
    with _synchronized_registry_read(registry, barrier):
        delete_binding_record(
            record.name,
            registry,
            workspace_id=record.workspace_id,
        )


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


def _record(name: str, workspace_suffix: int, local_root: Path) -> BindingRecord:
    return BindingRecord(
        name=name,
        workspace_id=f"00000000-0000-4000-8000-{workspace_suffix:012d}",
        target="host",
        remote_path=f"/remote/{name}",
        local_path=str(local_root),
        updated_at="2026-07-10T00:00:00Z",
    )


def test_registry_path_ignores_legacy_installed_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    development_home = tmp_path / "codex-home"
    installed_registry = tmp_path / "installed" / "connections.toml"
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(development_home))
    monkeypatch.setenv("REMOTE_SANDBOX_CONNECTIONS", str(installed_registry))

    assert registry_path() == development_home / "connections.toml"


def test_registry_path_honors_development_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    development_registry = tmp_path / "development" / "connections.toml"
    monkeypatch.setenv(
        "CODEX_REMOTE_SANDBOX_CONNECTIONS",
        str(development_registry),
    )

    assert registry_path() == development_registry


def test_default_registry_reads_and_writes_ignore_installed_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    development_home = tmp_path / "codex-home"
    installed_registry = tmp_path / "installed" / "connections.toml"
    installed = _record("installed", 81, tmp_path / "installed-project")
    development = _record("development", 82, tmp_path / "development-project")
    upsert_binding_record(installed_registry, installed)
    installed_before = installed_registry.read_bytes()
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(development_home))
    monkeypatch.setenv("REMOTE_SANDBOX_CONNECTIONS", str(installed_registry))

    assert list_binding_records() == []
    upsert_binding_record(None, development)

    assert list_binding_records() == [development]
    assert installed_registry.read_bytes() == installed_before


def test_connect_registration_does_not_write_installed_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    development_home = tmp_path / "codex-home"
    installed_registry = tmp_path / "installed" / "connections.toml"
    installed = _record("installed", 83, tmp_path / "installed-project")
    upsert_binding_record(installed_registry, installed)
    installed_before = installed_registry.read_bytes()
    monkeypatch.setenv("CODEX_REMOTE_SANDBOX_HOME", str(development_home))
    monkeypatch.setenv("REMOTE_SANDBOX_CONNECTIONS", str(installed_registry))
    spec = _workspace_spec(
        workspace_id="00000000-0000-4000-8000-000000000084",
        name="development",
        local_root=tmp_path / "development-project",
    )

    record = register_workspace(spec)

    assert list_binding_records() == [record]
    assert installed_registry.read_bytes() == installed_before


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

    record = current_workspace_record(registry, local_root)

    assert record is not None
    assert record.name == "dq"


def test_concurrent_upserts_preserve_both_records(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "codex-home" / "connections.toml"
    upsert_binding_record(registry, _record("seed", 1, tmp_path / "seed"))
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    first = context.Process(
        target=_synchronized_upsert,
        args=(registry, _record("first", 2, tmp_path / "first"), barrier),
    )
    second = context.Process(
        target=_synchronized_upsert,
        args=(registry, _record("second", 3, tmp_path / "second"), barrier),
    )

    first.start()
    second.start()
    first.join(timeout=5)
    second.join(timeout=5)

    assert first.exitcode == 0
    assert second.exitcode == 0
    assert [record.name for record in list_binding_records(registry)] == [
        "first",
        "second",
        "seed",
    ]


def test_concurrent_upsert_and_delete_preserve_both_mutations(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "codex-home" / "connections.toml"
    seed = _record("seed", 1, tmp_path / "seed")
    upsert_binding_record(registry, seed)
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    insert = context.Process(
        target=_synchronized_upsert,
        args=(registry, _record("added", 2, tmp_path / "added"), barrier),
    )
    delete = context.Process(
        target=_synchronized_delete,
        args=(registry, seed, barrier),
    )

    insert.start()
    delete.start()
    insert.join(timeout=5)
    delete.join(timeout=5)

    assert insert.exitcode == 0
    assert delete.exitcode == 0
    assert [record.name for record in list_binding_records(registry)] == ["added"]


def test_delete_binding_record_does_not_delete_rebound_name(tmp_path: Path) -> None:
    registry = tmp_path / "codex-home" / "connections.toml"
    replacement = _record("dq", 2, tmp_path / "replacement")
    upsert_binding_record(registry, replacement)

    deleted = delete_binding_record(
        "dq",
        registry,
        workspace_id="00000000-0000-4000-8000-000000000001",
    )

    assert deleted is False
    assert list_binding_records(registry) == [replacement]


def test_registry_transaction_files_have_private_permissions(tmp_path: Path) -> None:
    registry = tmp_path / "codex-home" / "connections.toml"

    upsert_binding_record(registry, _record("dq", 1, tmp_path / "dq"))

    lock_path = registry.with_name(f"{registry.name}.lock")
    assert stat.S_IMODE(registry.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(registry.stat().st_mode) == 0o600
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
