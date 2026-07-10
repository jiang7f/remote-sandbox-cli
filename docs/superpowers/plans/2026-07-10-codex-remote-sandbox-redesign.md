# Codex Remote Sandbox Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an isolated `codex-rsb` development tool with out-of-tree metadata, persistent local and remote filesystem journals, fast batched synchronization, truthful lifecycle state, and a same-session remote shell with a live prompt.

**Architecture:** A local supervisor owns three-way reconciliation. A local watchdog watcher and a versioned Python 3.10-compatible remote inotify agent persist ordered events in SQLite. The supervisor consumes dirty paths, requests hashes only when needed, transfers same-direction batches through rsync with a validated tar fallback, and exposes status to the CLI and managed shell.

**Tech Stack:** Python 3.11+ locally, Python 3.10+ remote agent, SQLite WAL, watchdog, OpenSSH ControlMaster, rsync, tar, pytest, pytest-cov, strict mypy, ruff, Docker-based SSH E2E tests.

## Global Constraints

- Work only in `/Users/7f/仓库/remote-sandbox-cli_副本`.
- Preserve the existing unstaged changes in `src/remote_sandbox/cli.py`, `src/remote_sandbox/daemon.py`, and `src/remote_sandbox/ssh.py`. Do not reset or discard them.
- Fold the existing reconnect and authentication-classification behavior into Tasks 8, 13, and 16 with regression tests before staging those files.
- Stage explicit task files only. Do not use `git add .`.
- The distribution is `codex-remote-sandbox`; commands are `codex-rsb` and `codex-remote-sandbox`.
- Development state is isolated under `~/.codex-remote-sandbox/` locally and remotely.
- Runtime and SSH control files are isolated under `/tmp/codex-remote-sandbox-<uid>/`.
- Never read, modify, stop, migrate, or delete the installed `rsb` tool's state.
- Never create `.remote-sandbox` control metadata inside a local or remote workspace.
- `.git/` is a hard ignore. Git operations are local-only.
- New bindings require at least one empty side.
- The remote agent must run on Python 3.10 without third-party packages or a virtual environment.
- Remote commands must use structured arguments or stdin protocols. Do not interpolate untrusted paths into shell source.
- Symlinks are copied as links and never dereferenced for scanning or transfer.
- Ordinary sync must not hash every unchanged file.
- A conflict must preserve both versions and must never become a silent skip.
- `codex-rsb run` returns the remote command's exit code even when follow-up sync fails.
- Tracebacks are hidden unless `--debug` is present.
- Prompt refresh is capped at four updates per second and must preserve Readline input and cursor state.
- Use TDD for every production behavior. Observe each new test fail before implementing it.
- Before every task commit, run the task tests plus `uv run ruff check src tests` and `uv run mypy src`.

## Planned File Structure

### New production files

- `src/remote_sandbox/namespace.py`: isolated package, home, runtime, and SSH control names.
- `src/remote_sandbox/workspace.py`: workspace identity, metadata layouts, and serialization.
- `src/remote_sandbox/status.py`: lifecycle phases, progress, and display models.
- `src/remote_sandbox/journal.py`: local journal types and SQLite operations.
- `src/remote_sandbox/placeholder.py`: validated large-file placeholder format and identity checks.
- `src/remote_sandbox/remote_protocol.py`: typed local-side encoding and decoding for agent messages.
- `src/remote_sandbox/remote_client.py`: SSH-backed remote-agent lifecycle and event subscription.
- `src/remote_sandbox/transport.py`: rsync and tar batch transport.
- `src/remote_sandbox/engine.py`: incremental synchronization transaction coordinator.
- `src/remote_sandbox/initial_sync.py`: watcher-first initial copy and journal replay.
- `src/remote_sandbox/prompt.py`: fixed-width status rendering and redraw throttling.
- `src/remote_sandbox/remote_agent/__init__.py`: remote agent version.
- `src/remote_sandbox/remote_agent/__main__.py`: remote command dispatcher.
- `src/remote_sandbox/remote_agent/store.py`: remote registry, journal, status, and watcher pid state.
- `src/remote_sandbox/remote_agent/watcher.py`: polling and inotify watcher service.
- `src/remote_sandbox/remote_agent/inotify.py`: Linux inotify backend through `ctypes`.

### Existing production files to modify

- `pyproject.toml`: isolated distribution, entry points, test dependencies, pytest markers.
- `src/remote_sandbox/settings.py`: use isolated namespace paths.
- `src/remote_sandbox/registry.py`: remove marker dependency and use workspace IDs.
- `src/remote_sandbox/manifest.py`: support fingerprints, symlinks, and special entries.
- `src/remote_sandbox/state.py`: durable base, expected echoes, and conflicts.
- `src/remote_sandbox/policy.py`: hard Git ignore and default cache ignores.
- `src/remote_sandbox/watch.py`: emit path events instead of rescanning the whole tree.
- `src/remote_sandbox/agent.py`: build, upload, verify, and version the remote zipapp.
- `src/remote_sandbox/ssh.py`: isolated control paths, structured agent calls, and streaming processes.
- `src/remote_sandbox/reconcile.py`: dirty-path plans, hash requests, and non-destructive conflicts.
- `src/remote_sandbox/daemon.py`: supervisor lifecycle and truthful status publication.
- `src/remote_sandbox/bind.py`: out-of-tree identity and watcher-first initial sync.
- `src/remote_sandbox/shell.py`: same-session connect handshake and status-slot protocol.
- `src/remote_sandbox/cli.py`: new commands, `--debug`, and service delegation.
- `src/remote_sandbox/fetch.py`: resolve workspace from registry and state.
- `src/remote_sandbox/peek.py`: resolve remote workspace without an in-tree marker.
- `README.md`: development command and final workflow documentation.

### Legacy files to retire after callers migrate

- `src/remote_sandbox/marker.py`
- `src/remote_sandbox/lock.py`
- `src/remote_sandbox/scan.py`
- `src/remote_sandbox/sync.py`
- `src/remote_sandbox/syncsession.py`

### Test layout

- `tests/helpers/__init__.py`: marks reusable test harnesses as an importable package.
- `tests/helpers/sync_harness.py`: concrete local two-replica, engine, initial-sync, supervisor,
  shell, CLI, and performance harnesses used by later tests.
- `tests/unit/conftest.py`: exposes unit harnesses such as `supervisor_fixture`.
- `tests/integration/conftest.py`: exposes `sync_pair`, `initial_pair`, `daemon_pair`,
  `shell_fixture`, and `cli_fixture` from the shared harness module.
- `tests/unit/`: pure models, registry, journal, reconciliation, policy, status, prompt, and CLI tests.
- `tests/integration/`: real temporary replicas, SQLite, watchdog, rsync, tar, restart, and conflict tests.
- `tests/e2e/`: Docker SSH server, password and key authentication, PTY workflows, and cleanup.
- `tests/performance/`: marked benchmarks for initial throughput and incremental latency.

---

### Task 1: Isolate the Development Namespace and Establish the Test Harness

**Files:**
- Create: `src/remote_sandbox/namespace.py`
- Create: `tests/unit/test_namespace.py`
- Create: `tests/unit/test_development_entry_points.py`
- Modify: `pyproject.toml:1-64`
- Modify: `src/remote_sandbox/settings.py:1-140`

**Interfaces:**
- Produces: `DEV_NAMESPACE: ToolNamespace`
- Produces: `tool_home(env: Mapping[str, str] | None = None) -> Path`
- Produces: `runtime_dir(env: Mapping[str, str] | None = None) -> Path`
- Produces: `ssh_control_dir(env: Mapping[str, str] | None = None) -> Path`
- Produces: `program_name() -> str`

- [ ] **Step 1: Write failing namespace and entry-point tests**

```python
# tests/unit/test_namespace.py
from pathlib import Path

from remote_sandbox.namespace import DEV_NAMESPACE, runtime_dir, ssh_control_dir, tool_home


def test_development_namespace_is_fully_isolated(tmp_path: Path) -> None:
    env = {
        "HOME": str(tmp_path),
        "CODEX_REMOTE_SANDBOX_HOME": str(tmp_path / "state"),
        "CODEX_REMOTE_SANDBOX_RUNTIME_DIR": str(tmp_path / "runtime"),
    }

    assert DEV_NAMESPACE.distribution == "codex-remote-sandbox"
    assert DEV_NAMESPACE.command == "codex-rsb"
    assert tool_home(env) == tmp_path / "state"
    assert runtime_dir(env) == tmp_path / "runtime"
    assert ssh_control_dir(env) == tmp_path / "runtime" / "cm"
    assert ".remote-sandbox" not in str(tool_home(env))
```

```python
# tests/unit/test_development_entry_points.py
import tomllib
from pathlib import Path


def test_project_exposes_only_development_commands() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["name"] == "codex-remote-sandbox"
    assert project["scripts"] == {
        "codex-remote-sandbox": "remote_sandbox.cli:main",
        "codex-rsb": "remote_sandbox.cli:main",
    }
    assert "rsb" not in project["scripts"]
```

- [ ] **Step 2: Run the tests and observe the missing module or wrong command failure**

Run:

```bash
uv run pytest tests/unit/test_namespace.py tests/unit/test_development_entry_points.py -v
```

Expected: FAIL because `remote_sandbox.namespace` does not exist and the project still exposes the
production distribution and commands.

- [ ] **Step 3: Implement the isolated namespace**

```python
# src/remote_sandbox/namespace.py
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ToolNamespace:
    distribution: str
    command: str
    long_command: str
    home_dirname: str
    runtime_prefix: str


DEV_NAMESPACE = ToolNamespace(
    distribution="codex-remote-sandbox",
    command="codex-rsb",
    long_command="codex-remote-sandbox",
    home_dirname=".codex-remote-sandbox",
    runtime_prefix="codex-remote-sandbox",
)


def tool_home(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("CODEX_REMOTE_SANDBOX_HOME")
    if override:
        return Path(override).expanduser()
    return Path(values.get("HOME", str(Path.home()))) / DEV_NAMESPACE.home_dirname


def runtime_dir(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    override = values.get("CODEX_REMOTE_SANDBOX_RUNTIME_DIR")
    if override:
        return Path(override).expanduser()
    return Path("/tmp") / f"{DEV_NAMESPACE.runtime_prefix}-{os.getuid()}"


def ssh_control_dir(env: Mapping[str, str] | None = None) -> Path:
    return runtime_dir(env) / "cm"


def program_name() -> str:
    return DEV_NAMESPACE.command
```

Update `pyproject.toml` so the project and scripts are exactly:

```toml
[project]
name = "codex-remote-sandbox"

[project.scripts]
codex-remote-sandbox = "remote_sandbox.cli:main"
codex-rsb = "remote_sandbox.cli:main"
```

Add `pytest-cov` and `pytest-timeout` to the existing dev dependency group. Add:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "e2e: requires Docker and an SSH fixture",
    "performance: opt-in performance benchmark",
]
```

Make `settings.remote_sandbox_home()` delegate to `namespace.tool_home()`. Do not modify
`cli.py`, `daemon.py`, or `ssh.py` in this task because those files contain pre-existing user
changes. Tasks 8, 13, and 16 add their namespace delegation together with the relevant regression
tests before staging the complete files.

- [ ] **Step 4: Run the focused tests and static checks**

Run:

```bash
uv run pytest tests/unit/test_namespace.py tests/unit/test_development_entry_points.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: all commands PASS.

- [ ] **Step 5: Verify the installed production namespace remains untouched**

Run:

```bash
uv sync --all-groups
uv run python -c 'import shutil; assert shutil.which("codex-rsb")'
command -v rsb
```

Expected: the development executable exists inside the uv environment. The second command still
points to the existing installed `rsb` executable. Do not invoke a remote command yet because SSH
and daemon namespace isolation are completed in Tasks 8 and 13.

- [ ] **Step 6: Commit the isolated development namespace**

```bash
git add pyproject.toml uv.lock src/remote_sandbox/namespace.py src/remote_sandbox/settings.py \
  tests/unit/test_namespace.py tests/unit/test_development_entry_points.py
git commit -m "chore: isolate codex remote sandbox namespace"
```

### Task 2: Add Workspace Identity, External Metadata Paths, and Marker-free Registry Lookup

**Files:**
- Create: `src/remote_sandbox/workspace.py`
- Create: `tests/unit/test_workspace.py`
- Create: `tests/unit/test_registry_workspace_lookup.py`
- Modify: `src/remote_sandbox/registry.py:1-327`
- Modify: `src/remote_sandbox/settings.py:23-34`

**Interfaces:**
- Consumes: `tool_home()` from Task 1
- Produces: `WorkspaceSpec`
- Produces: `WorkspacePaths`
- Produces: `new_workspace_spec(...) -> WorkspaceSpec`
- Produces: `validate_workspace_id(value: str) -> str`
- Produces: `workspace_paths(workspace_id: str) -> WorkspacePaths`
- Produces: `read_workspace_spec(path: Path) -> WorkspaceSpec`
- Produces: `write_workspace_spec(path: Path, spec: WorkspaceSpec) -> None`
- Changes: `BindingRecord` remains the registry-facing type and stores `workspace_id`
- Produces: `register_workspace(spec: WorkspaceSpec, *, registry: Path | None = None) -> BindingRecord`
- Changes: `current_workspace_record(path, cwd)` uses longest canonical path prefix without marker reads

- [ ] **Step 1: Write failing workspace layout and registry tests**

```python
# tests/unit/test_workspace.py
from pathlib import Path

import pytest

from remote_sandbox.workspace import (
    new_workspace_spec,
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
    assert not (local_root / ".remote-sandbox").exists()


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


def test_workspace_id_rejects_path_and_non_uuid_values() -> None:
    with pytest.raises(ValueError):
        validate_workspace_id("../escape")
    with pytest.raises(ValueError):
        validate_workspace_id("not-a-uuid")
```

```python
# tests/unit/test_registry_workspace_lookup.py
from pathlib import Path

from remote_sandbox.registry import BindingRecord, current_workspace_record, upsert_binding_record


def test_current_workspace_uses_longest_registered_prefix(tmp_path: Path) -> None:
    registry = tmp_path / "connections.toml"
    outer = tmp_path / "repo"
    inner = outer / "nested"
    inner.mkdir(parents=True)
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

    assert current_workspace_record(registry, inner / "src").name == "inner"
```

- [ ] **Step 2: Run the tests and observe failure**

```bash
uv run pytest tests/unit/test_workspace.py tests/unit/test_registry_workspace_lookup.py -v
```

Expected: FAIL because `workspace.py` is missing and registry lookup still requires an in-tree marker.

- [ ] **Step 3: Implement workspace identity and atomic TOML persistence**

```python
# src/remote_sandbox/workspace.py
from __future__ import annotations

import os
import tempfile
import tomllib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from remote_sandbox.namespace import tool_home


SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class WorkspaceSpec:
    schema_version: int
    workspace_id: str
    name: str
    target: str
    local_root: str
    remote_root: str
    created_at: str


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    root: Path
    workspace_file: Path
    state_db: Path
    daemon_log: Path


def new_workspace_spec(
    *, name: str, target: str, local_root: Path, remote_root: str
) -> WorkspaceSpec:
    return WorkspaceSpec(
        schema_version=SCHEMA_VERSION,
        workspace_id=str(uuid.uuid4()),
        name=name,
        target=target,
        local_root=str(local_root.expanduser().resolve()),
        remote_root=remote_root,
        created_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )


def workspace_paths(workspace_id: str) -> WorkspacePaths:
    safe_id = validate_workspace_id(workspace_id)
    root = tool_home() / "workspaces" / safe_id
    return WorkspacePaths(root, root / "workspace.toml", root / "state.sqlite3", root / "daemon.log")


def validate_workspace_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise ValueError("invalid workspace id") from exc
    canonical = str(parsed)
    if value != canonical:
        raise ValueError("workspace id must use canonical UUID form")
    return canonical


def write_workspace_spec(path: Path, spec: WorkspaceSpec) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    content = _to_toml(spec)
    fd, name = tempfile.mkstemp(prefix="workspace.", suffix=".tmp", dir=path.parent, text=True)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.chmod(0o600)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def read_workspace_spec(path: Path) -> WorkspaceSpec:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return WorkspaceSpec(
        schema_version=int(data["schema_version"]),
        workspace_id=str(data["workspace_id"]),
        name=str(data["name"]),
        target=str(data["target"]),
        local_root=str(data["local_root"]),
        remote_root=str(data["remote_root"]),
        created_at=str(data["created_at"]),
    )


def _to_toml(spec: WorkspaceSpec) -> str:
    values = {
        "workspace_id": spec.workspace_id,
        "name": spec.name,
        "target": spec.target,
        "local_root": spec.local_root,
        "remote_root": spec.remote_root,
        "created_at": spec.created_at,
    }
    lines = [f"schema_version = {spec.schema_version}"]
    lines.extend(f'{key} = "{value.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"' for key, value in values.items())
    return "\n".join(lines) + "\n"
```

Remove `read_local_marker` and `record_binding_from_marker` from `registry.py`. Implement
`register_workspace()` directly from `WorkspaceSpec`. Resolve each registered local root with
`Path(...).expanduser().resolve(strict=False)` and select the deepest root that contains `cwd`.
Keep duplicate-name and duplicate-local-root protection.

- [ ] **Step 4: Run focused tests and round-trip coverage**

Add a round-trip assertion to `test_workspace.py`, then run:

```bash
uv run pytest tests/unit/test_workspace.py tests/unit/test_registry_workspace_lookup.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit workspace identity and registry lookup**

```bash
git add src/remote_sandbox/workspace.py src/remote_sandbox/registry.py \
  src/remote_sandbox/settings.py tests/unit/test_workspace.py \
  tests/unit/test_registry_workspace_lookup.py
git commit -m "feat: add external workspace identity"
```

### Task 3: Define Entry Fingerprints, Symlink Semantics, and Default Ignore Policy

**Files:**
- Create: `src/remote_sandbox/placeholder.py`
- Create: `tests/unit/test_fingerprint.py`
- Create: `tests/unit/test_placeholder.py`
- Create: `tests/unit/test_policy_defaults.py`
- Modify: `src/remote_sandbox/manifest.py:1-64`
- Modify: `src/remote_sandbox/policy.py:1-193`

**Interfaces:**
- Produces: `EntryKind.FILE`, `DIR`, `SYMLINK`, `SPECIAL`
- Produces: `EntryFingerprint`
- Produces: `workspace_path(root: Path, relative_path: str) -> Path`
- Produces: `fingerprint_local(root: Path, relative_path: str, *, with_hash: bool) -> EntryFingerprint | MissingEntry`
- Produces: `content_identity(entry: EntryFingerprint) -> tuple[object, ...]`
- Produces: `PlaceholderMetadata(path: str, size: int, mtime_ns: int, content_hash: str)`
- Produces: `encode_placeholder(metadata: PlaceholderMetadata) -> bytes`
- Produces: `decode_placeholder(data: bytes, *, expected_path: str) -> PlaceholderMetadata | None`
- Changes: `.git/` is a hard ignore and cannot be re-enabled

- [ ] **Step 1: Write failing fingerprint and policy tests**

```python
# tests/unit/test_fingerprint.py
from pathlib import Path

import pytest

from remote_sandbox.manifest import EntryFingerprint, EntryKind, fingerprint_local


def test_symlink_fingerprint_preserves_target_without_following(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "outside").symlink_to("/etc/passwd")

    entry = fingerprint_local(root, "outside", with_hash=True)

    assert isinstance(entry, EntryFingerprint)
    assert entry.kind is EntryKind.SYMLINK
    assert entry.link_target == "/etc/passwd"
    assert entry.content_hash is not None


def test_parent_symlink_cannot_escape_workspace_during_fingerprint(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    (root / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink parent"):
        fingerprint_local(root, "escape/secret.txt", with_hash=True)
```

```python
# tests/unit/test_placeholder.py
import pytest

from remote_sandbox.placeholder import PlaceholderMetadata, decode_placeholder, encode_placeholder


def test_placeholder_round_trip_requires_the_expected_path() -> None:
    metadata = PlaceholderMetadata("weights.bin", 50_000_000, 123, "abc123")
    encoded = encode_placeholder(metadata)
    assert decode_placeholder(encoded, expected_path="weights.bin") == metadata
    with pytest.raises(ValueError, match="path mismatch"):
        decode_placeholder(encoded, expected_path="other.bin")


def test_ordinary_small_text_is_not_a_placeholder() -> None:
    assert decode_placeholder(b"ordinary content\n", expected_path="notes.txt") is None
```

```python
# tests/unit/test_policy_defaults.py
from remote_sandbox.policy import StaticPolicyEngine


def test_git_and_portability_caches_are_ignored_by_default() -> None:
    policy = StaticPolicyEngine()

    assert policy.is_ignored(".git/index")
    assert policy.is_ignored(".venv/bin/python")
    assert policy.is_ignored("pkg/__pycache__/module.pyc")
    assert policy.is_ignored("node_modules/pkg/index.js")


def test_git_cannot_be_reenabled() -> None:
    policy = StaticPolicyEngine.from_lines(["[sync]", ".git/**"])
    assert policy.is_ignored(".git/index")


def test_environment_cache_can_be_explicitly_reenabled() -> None:
    policy = StaticPolicyEngine.from_lines(["[sync]", ".venv/**"])
    assert not policy.is_ignored(".venv/bin/python")
```

- [ ] **Step 2: Run the tests and observe unsupported-symlink or policy failure**

```bash
uv run pytest tests/unit/test_fingerprint.py tests/unit/test_placeholder.py \
  tests/unit/test_policy_defaults.py -v
```

Expected: FAIL because fingerprints and the standalone placeholder codec do not exist, symlinks
are unsupported, and `.git` is not a hard default.

- [ ] **Step 3: Implement fingerprint models and local stat reads**

```python
# replace the entry model portion of src/remote_sandbox/manifest.py
from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class EntryKind(StrEnum):
    FILE = "file"
    DIR = "dir"
    SYMLINK = "symlink"
    SPECIAL = "special"


@dataclass(frozen=True, slots=True)
class EntryFingerprint:
    path: str
    kind: EntryKind
    size: int | None
    mtime_ns: int | None
    mode: int | None
    link_target: str | None = None
    content_hash: str | None = None
    is_placeholder: bool = False


@dataclass(frozen=True, slots=True)
class MissingEntry:
    path: str


def fingerprint_local(
    root: Path, relative_path: str, *, with_hash: bool
) -> EntryFingerprint | MissingEntry:
    normalized = normalize_relative_path(relative_path)
    candidate = workspace_path(root, normalized)
    try:
        entry_stat = candidate.lstat()
    except FileNotFoundError:
        return MissingEntry(normalized)
    if candidate.is_symlink():
        target = os.readlink(candidate)
        digest = hashlib.sha256(target.encode("utf-8", errors="surrogateescape")).hexdigest()
        return EntryFingerprint(
            normalized,
            EntryKind.SYMLINK,
            None,
            entry_stat.st_mtime_ns,
            entry_stat.st_mode,
            target,
            digest,
        )
    if candidate.is_dir():
        return EntryFingerprint(
            normalized, EntryKind.DIR, None, entry_stat.st_mtime_ns, entry_stat.st_mode
        )
    if candidate.is_file():
        digest = _sha256_file(candidate) if with_hash else None
        return EntryFingerprint(
            normalized,
            EntryKind.FILE,
            entry_stat.st_size,
            entry_stat.st_mtime_ns,
            entry_stat.st_mode,
            content_hash=digest,
        )
    return EntryFingerprint(
        normalized, EntryKind.SPECIAL, None, entry_stat.st_mtime_ns, entry_stat.st_mode
    )


def workspace_path(root: Path, relative_path: str) -> Path:
    normalized = normalize_relative_path(relative_path)
    candidate = root
    parts = Path(normalized).parts
    for part in parts[:-1]:
        candidate /= part
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            break
        if stat.S_ISLNK(mode):
            raise ValueError(f"symlink parent escapes workspace: {relative_path}")
    return root / normalized


def content_identity(entry: EntryFingerprint) -> tuple[object, ...]:
    if entry.kind is EntryKind.SYMLINK:
        return (entry.kind, entry.link_target)
    if entry.kind is EntryKind.FILE:
        return (entry.kind, entry.content_hash)
    return (entry.kind,)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
```

In `policy.py`, make `.git/` and `.git/**` hard ignores. Add default directory patterns for the
confirmed environment and cache list. Keep user overrides for portability caches, but apply hard
Git and control-metadata checks before user rules.

Move the existing validated placeholder codec out of `scan.py` into `placeholder.py`. The format
must include a fixed magic header, schema version, path, size, nanosecond mtime, and content hash.
`decode_placeholder()` returns `None` only when the magic header is absent. Once the magic header
is present, malformed metadata raises `ValueError` instead of being treated as ordinary content.

- [ ] **Step 4: Run tests and static checks**

```bash
uv run pytest tests/unit/test_fingerprint.py tests/unit/test_placeholder.py \
  tests/unit/test_policy_defaults.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit fingerprint and policy semantics**

```bash
git add src/remote_sandbox/manifest.py src/remote_sandbox/placeholder.py \
  src/remote_sandbox/policy.py tests/unit/test_fingerprint.py \
  tests/unit/test_placeholder.py tests/unit/test_policy_defaults.py
git commit -m "feat: add safe entry fingerprints"
```

### Task 4: Build Durable Status, Base State, Conflicts, and Local Event Journal

**Files:**
- Create: `src/remote_sandbox/status.py`
- Create: `src/remote_sandbox/journal.py`
- Create: `tests/unit/test_status_store.py`
- Create: `tests/unit/test_journal.py`
- Create: `tests/unit/test_conflict_store.py`
- Modify: `src/remote_sandbox/state.py:1-122`

**Interfaces:**
- Consumes: `EntryFingerprint` from Task 3
- Produces: `WorkspacePhase`
- Produces: `SyncProgress`
- Produces: `WorkspaceStatus`
- Produces: `format_progress(progress: SyncProgress) -> str`
- Produces: `EventKind`
- Produces: `JournalEvent`
- Produces: `coalesce_events(events: Iterable[JournalEvent]) -> tuple[JournalEvent, ...]`
- Produces: `WorkspaceStore.open(path: Path) -> WorkspaceStore`
- Produces: `WorkspaceStore.append_event(...) -> JournalEvent`
- Produces: `WorkspaceStore.pending_events(side, after_sequence) -> list[JournalEvent]`
- Produces: `WorkspaceStore.acknowledge(side, through_sequence) -> None`
- Produces: `WorkspaceStore.set_status(status) -> None`
- Produces: `WorkspaceStore.create_conflict(...) -> ConflictRecord`

- [ ] **Step 1: Write failing status, journal, and conflict tests**

```python
# tests/unit/test_journal.py
from pathlib import Path

from remote_sandbox.journal import EventKind, JournalEvent, coalesce_events
from remote_sandbox.state import WorkspaceStore


def test_events_are_ordered_and_acknowledged_transactionally(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        first = store.append_event("local", EventKind.MODIFY, "a.py")
        second = store.append_event("local", EventKind.DELETE, "b.py")
        assert [event.sequence for event in store.pending_events("local", 0)] == [
            first.sequence,
            second.sequence,
        ]
        store.acknowledge("local", first.sequence)
        assert [event.path for event in store.pending_events("local", first.sequence)] == ["b.py"]


def test_coalescing_preserves_move_delete_and_overflow_meaning() -> None:
    events = [
        JournalEvent("local", 1, EventKind.MODIFY, "a.py"),
        JournalEvent("local", 2, EventKind.MODIFY, "a.py"),
        JournalEvent("local", 3, EventKind.MOVE, "old.py", "new.py"),
        JournalEvent("local", 4, EventKind.DELETE, "a.py"),
        JournalEvent("local", 5, EventKind.RESCAN_REQUIRED, "*"),
    ]
    coalesced = coalesce_events(events)
    assert [(event.kind, event.path, event.destination_path) for event in coalesced] == [
        (EventKind.MOVE, "old.py", "new.py"),
        (EventKind.DELETE, "a.py", None),
        (EventKind.RESCAN_REQUIRED, "*", None),
    ]
```

```python
# tests/unit/test_status_store.py
from pathlib import Path

from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus, format_progress


def test_starting_status_is_durable_before_sync(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    status = WorkspaceStatus(WorkspacePhase.STARTING, SyncProgress("starting"))
    with WorkspaceStore.open(db) as store:
        store.set_status(status)
    with WorkspaceStore.open(db) as store:
        assert store.get_status().phase is WorkspacePhase.STARTING


def test_scanning_progress_is_informative_before_a_total_exists() -> None:
    progress = SyncProgress("scanning", files_done=1_843, bytes_done=31_000_000)
    assert format_progress(progress) == "scanning 1843 files 31.0 MB"
```

```python
# tests/unit/test_conflict_store.py
from pathlib import Path

from remote_sandbox.state import WorkspaceStore


def test_conflict_keeps_both_versions_and_remains_unresolved(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        record = store.create_conflict(
            path="model.py",
            reason="both-modified",
            local_blob=b"local\n",
            remote_blob=b"remote\n",
        )
        assert record.resolved_at is None
        assert store.get_conflict(record.conflict_id).local_blob == b"local\n"
        assert store.get_conflict(record.conflict_id).remote_blob == b"remote\n"
```

- [ ] **Step 2: Run tests and observe missing store APIs**

```bash
uv run pytest tests/unit/test_status_store.py tests/unit/test_journal.py \
  tests/unit/test_conflict_store.py -v
```

Expected: FAIL because status, journal, and conflict APIs do not exist.

- [ ] **Step 3: Define lifecycle and journal models**

```python
# src/remote_sandbox/status.py
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkspacePhase(StrEnum):
    STARTING = "starting"
    INITIAL_SYNCING = "initial-syncing"
    READY = "ready"
    SYNCING = "syncing"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class SyncProgress:
    stage: str
    files_done: int = 0
    files_total: int = 0
    bytes_done: int = 0
    bytes_total: int = 0
    current_path: str | None = None


@dataclass(frozen=True, slots=True)
class WorkspaceStatus:
    phase: WorkspacePhase
    progress: SyncProgress
    pending: int = 0
    conflicts: int = 0
    last_error: str | None = None
    last_sync_at: float | None = None
```

```python
# src/remote_sandbox/journal.py
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class EventKind(StrEnum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"
    MOVE = "move"
    RESCAN_REQUIRED = "rescan-required"


@dataclass(frozen=True, slots=True)
class JournalEvent:
    side: str
    sequence: int
    kind: EventKind
    path: str
    destination_path: str | None = None
```

Expand `state.py` into `WorkspaceStore` with SQLite WAL, `PRAGMA foreign_keys=ON`,
`PRAGMA busy_timeout=5000`, schema migrations, and explicit transaction methods. Tables must
include `base_entries`, `events`, `watermarks`, `workspace_status`, `expected_echoes`, and
`conflicts`. Serialize fingerprints and progress as validated JSON. Do not commit after each row;
commit one reconciliation transaction.

- [ ] **Step 4: Run tests and inspect the SQLite schema**

```bash
uv run pytest tests/unit/test_status_store.py tests/unit/test_journal.py \
  tests/unit/test_conflict_store.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS. Reopening the database preserves status, events, and conflicts.

- [ ] **Step 5: Commit durable workspace state**

```bash
git add src/remote_sandbox/status.py src/remote_sandbox/journal.py \
  src/remote_sandbox/state.py tests/unit/test_status_store.py tests/unit/test_journal.py \
  tests/unit/test_conflict_store.py
git commit -m "feat: add durable workspace journal"
```

### Task 5: Replace Full-tree Local Scans with Path Event Watchers

**Files:**
- Create: `tests/unit/test_local_watcher.py`
- Create: `tests/integration/test_local_watcher_journal.py`
- Modify: `src/remote_sandbox/watch.py:1-225`

**Interfaces:**
- Consumes: `EventKind` and `JournalEvent` from Task 4
- Produces: `LocalEventWatcher`
- Produces: `create_local_watcher(root, policy, on_event) -> LocalEventWatcher`
- Callback: `on_event(kind: EventKind, path: str, destination_path: str | None) -> None`

- [ ] **Step 1: Write failing event mapping tests**

```python
# tests/unit/test_local_watcher.py
from pathlib import Path

from remote_sandbox.journal import EventKind
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.watch import map_watchdog_event


class Event:
    event_type = "moved"
    src_path = "/workspace/old.py"
    dest_path = "/workspace/new.py"
    is_directory = False


def test_watchdog_move_becomes_one_relative_move_event() -> None:
    mapped = map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), Event())
    assert mapped == (EventKind.MOVE, "old.py", "new.py")


def test_ignored_paths_do_not_emit_events() -> None:
    event = Event()
    event.event_type = "modified"
    event.src_path = "/workspace/.git/index"
    assert map_watchdog_event(Path("/workspace"), StaticPolicyEngine(), event) is None
```

```python
# tests/integration/test_local_watcher_journal.py
import time
from pathlib import Path

from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.watch import create_local_watcher


def test_local_file_creation_emits_one_relative_path(tmp_path: Path) -> None:
    events: list[tuple[object, str, str | None]] = []
    watcher = create_local_watcher(
        tmp_path,
        StaticPolicyEngine(),
        lambda kind, path, destination: events.append((kind, path, destination)),
    )
    watcher.start()
    try:
        (tmp_path / "hello.py").write_text("print('hello')\n", encoding="utf-8")
        deadline = time.monotonic() + 3
        while not events and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        watcher.stop()
    assert any(path == "hello.py" for _kind, path, _destination in events)
```

- [ ] **Step 2: Run tests and observe the old detector API failure**

```bash
uv run pytest tests/unit/test_local_watcher.py \
  tests/integration/test_local_watcher_journal.py -v
```

Expected: FAIL because `map_watchdog_event` and the path-event callback API do not exist.

- [ ] **Step 3: Implement direct watchdog event emission**

Replace full-tree `LocalChangeDetector` rescans with event mapping:

```python
def map_watchdog_event(
    root: Path,
    policy: PolicyEngine,
    event: object,
) -> tuple[EventKind, str, str | None] | None:
    event_type = str(getattr(event, "event_type"))
    src = _relative_event_path(root, str(getattr(event, "src_path")))
    if src is None or policy.is_ignored(src):
        return None
    if event_type == "moved":
        destination = _relative_event_path(root, str(getattr(event, "dest_path")))
        if destination is None or policy.is_ignored(destination):
            return (EventKind.DELETE, src, None)
        return (EventKind.MOVE, src, destination)
    kind = {
        "created": EventKind.CREATE,
        "modified": EventKind.MODIFY,
        "deleted": EventKind.DELETE,
    }.get(event_type)
    return None if kind is None else (kind, src, None)


def _relative_event_path(root: Path, value: str) -> str | None:
    try:
        relative = Path(value).relative_to(root).as_posix()
        return normalize_relative_path(relative)
    except ValueError:
        return None
```

`WatchdogLocalWatcher` must debounce duplicate events by `(kind, path, destination)` for a short
window without converting rename into unrelated delete and create operations. The polling fallback
may compare metadata snapshots, but it must emit the changed paths rather than one global poke.

- [ ] **Step 4: Run watcher tests and static checks**

```bash
uv run pytest tests/unit/test_local_watcher.py \
  tests/integration/test_local_watcher_journal.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit local path-event watching**

```bash
git add src/remote_sandbox/watch.py tests/unit/test_local_watcher.py \
  tests/integration/test_local_watcher_journal.py
git commit -m "feat: emit local filesystem events"
```

### Task 6: Package and Bootstrap a Versioned Python 3.10 Remote Agent

**Files:**
- Create: `src/remote_sandbox/remote_protocol.py`
- Create: `src/remote_sandbox/remote_agent/__init__.py`
- Create: `src/remote_sandbox/remote_agent/__main__.py`
- Create: `tests/unit/test_remote_protocol.py`
- Create: `tests/integration/test_agent_zipapp.py`
- Modify: `src/remote_sandbox/agent.py:1-117`
- Modify: `src/remote_sandbox/ssh.py:28-68,577-720`

**Interfaces:**
- Consumes: development namespace from Task 1
- Produces: `AGENT_VERSION`
- Produces: `AgentRequest` and `AgentResponse`
- Produces: `AgentInstall(version: str, remote_path: str, sha256: str)`
- Produces: `encode_request(request) -> bytes`
- Produces: `decode_response(data) -> AgentResponse`
- Produces: `build_agent_zipapp(destination: Path) -> Path`
- Produces: `RemoteAgentManager.ensure(target: str) -> AgentInstall`
- Extends: `SshRunner.run_agent(target, agent_path, request_bytes) -> bytes`

- [ ] **Step 1: Write failing protocol and zipapp tests**

```python
# tests/unit/test_remote_protocol.py
from remote_sandbox.remote_protocol import AgentRequest, decode_request, encode_request


def test_agent_protocol_round_trips_unicode_paths_without_shell_quoting() -> None:
    request = AgentRequest("register", {"workspace_id": "w1", "root": "/home/u/算法测试"})
    assert decode_request(encode_request(request)) == request
```

```python
# tests/integration/test_agent_zipapp.py
import subprocess
from pathlib import Path

from remote_sandbox.agent import build_agent_zipapp


def test_agent_zipapp_self_check(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    result = subprocess.run(
        ["python3", str(archive), "self-check"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.startswith("codex-remote-sandbox-agent ")
```

- [ ] **Step 2: Run tests and observe missing protocol/package failure**

```bash
uv run pytest tests/unit/test_remote_protocol.py tests/integration/test_agent_zipapp.py -v
```

Expected: FAIL because the remote package and structured protocol do not exist.

- [ ] **Step 3: Implement newline-delimited JSON protocol and zipapp build**

```python
# src/remote_sandbox/remote_protocol.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AgentRequest:
    command: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AgentResponse:
    ok: bool
    payload: dict[str, Any]
    error: str | None = None


def encode_request(request: AgentRequest) -> bytes:
    return json.dumps(
        {"command": request.command, "payload": request.payload},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8") + b"\n"


def decode_request(data: bytes) -> AgentRequest:
    raw = json.loads(data.decode("utf-8"))
    return AgentRequest(str(raw["command"]), dict(raw["payload"]))


def decode_response(data: bytes) -> AgentResponse:
    raw = json.loads(data.decode("utf-8"))
    return AgentResponse(bool(raw["ok"]), dict(raw.get("payload", {})), raw.get("error"))
```

```python
# src/remote_sandbox/remote_agent/__init__.py
AGENT_VERSION = "0.2.0-dev"
```

```python
# add to src/remote_sandbox/agent.py
@dataclass(frozen=True, slots=True)
class AgentInstall:
    version: str
    remote_path: str
    sha256: str
```

```python
# minimal command dispatcher in src/remote_sandbox/remote_agent/__main__.py
from __future__ import annotations

import json
import sys

from remote_agent import AGENT_VERSION


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        print("codex-remote-sandbox-agent " + AGENT_VERSION)
        return 0
    request = json.loads(sys.stdin.buffer.readline().decode("utf-8"))
    print(json.dumps({"ok": False, "payload": {}, "error": "unsupported command: " + str(request.get("command"))}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

`build_agent_zipapp()` must copy the `remote_agent` package into a temporary staging directory,
name the package `remote_agent` inside the archive, and call `zipapp.create_archive()` with
`interpreter="/usr/bin/env python3"`. `RemoteAgentManager.ensure()` uploads to
`~/.codex-remote-sandbox/agents/<version>/agent.pyz`, writes atomically, and verifies `self-check`.

Add an SSH runner method that passes the encoded request on stdin and never embeds payload fields
in shell source.

- [ ] **Step 4: Verify Python 3.10 compatibility and agent self-check**

Run:

```bash
uv run pytest tests/unit/test_remote_protocol.py tests/integration/test_agent_zipapp.py -v
python3 -m compileall -q src/remote_sandbox/remote_agent
uv run ruff check src tests
uv run mypy src
```

When `python3.10` is available, also run:

```bash
uv run python -c 'from pathlib import Path; from remote_sandbox.agent import build_agent_zipapp; build_agent_zipapp(Path("/tmp/codex-agent-test.pyz"))'
if command -v python3.10 >/dev/null 2>&1; then
  python3.10 /tmp/codex-agent-test.pyz self-check
fi
```

Expected: the archive is built before it is invoked. All available checks PASS, and the zipapp
uses no Python 3.11-only syntax. A machine without `python3.10` skips only that final interpreter
check; Linux CI in Task 17 must run it against Python 3.10.

- [ ] **Step 5: Commit the versioned remote agent bootstrap**

```bash
git add src/remote_sandbox/remote_protocol.py src/remote_sandbox/remote_agent \
  src/remote_sandbox/agent.py src/remote_sandbox/ssh.py tests/unit/test_remote_protocol.py \
  tests/integration/test_agent_zipapp.py
git commit -m "feat: bootstrap versioned remote agent"
```

### Task 7: Implement the Remote Journal, Polling Watcher, and Linux Inotify Backend

**Files:**
- Create: `src/remote_sandbox/remote_agent/store.py`
- Create: `src/remote_sandbox/remote_agent/watcher.py`
- Create: `src/remote_sandbox/remote_agent/inotify.py`
- Create: `tests/unit/test_remote_agent_store.py`
- Create: `tests/unit/test_inotify_parser.py`
- Create: `tests/integration/test_remote_agent_polling.py`
- Modify: `src/remote_sandbox/remote_agent/__main__.py`

**Interfaces:**
- Consumes: structured stdin protocol from Task 6
- Produces: remote commands `register`, `start`, `stop`, `status`, `events`, `ack`, `snapshot`, `forget`
- Produces: `RemoteStore`
- Produces: `PollingWatcher`
- Produces: `InotifyBackend`
- Produces: `WatcherService`

- [ ] **Step 1: Write failing remote store and watcher tests**

```python
# tests/unit/test_remote_agent_store.py
from pathlib import Path

from remote_sandbox.remote_agent.store import RemoteStore


def test_remote_events_survive_store_reopen(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with RemoteStore(db) as store:
        event = store.append_event("modify", "train.py", None)
    with RemoteStore(db) as store:
        assert store.events_after(0)[0].sequence == event.sequence
        store.acknowledge(event.sequence)
        assert store.acknowledged_sequence() == event.sequence
```

```python
# tests/unit/test_inotify_parser.py
import struct

from remote_sandbox.remote_agent.inotify import IN_Q_OVERFLOW, parse_inotify_buffer


def test_inotify_overflow_becomes_rescan_required() -> None:
    raw = struct.pack("iIII", -1, IN_Q_OVERFLOW, 0, 0)
    events = parse_inotify_buffer(raw)
    assert events[0].overflow is True
```

```python
# tests/integration/test_remote_agent_polling.py
import platform
import threading
import time
from pathlib import Path

import pytest

from remote_sandbox.remote_agent.store import RemoteStore
from remote_sandbox.remote_agent.watcher import PollingWatcher, WatcherService


def test_polling_watcher_records_a_delete(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    file = root / "x.txt"
    file.write_text("x", encoding="utf-8")
    store = RemoteStore(tmp_path / "state.sqlite3")
    watcher = PollingWatcher(root, store, interval=0.05)
    thread = threading.Thread(target=watcher.run, daemon=True)
    thread.start()
    try:
        time.sleep(0.1)
        file.unlink()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any(e.kind == "delete" for e in store.events_after(0)):
            time.sleep(0.05)
    finally:
        watcher.stop()
        thread.join(timeout=2)
        store.close()


@pytest.mark.skipif(platform.system() != "Linux", reason="inotify is Linux-only")
def test_linux_service_selects_inotify_and_watches_new_directories(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = RemoteStore(tmp_path / "state.sqlite3")
    service = WatcherService(root, store, poll_interval=0.05)
    thread = threading.Thread(target=service.run, daemon=True)
    thread.start()
    try:
        assert service.backend_name == "inotify"
        (root / "new").mkdir()
        (root / "new" / "file.txt").write_text("x", encoding="utf-8")
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any(
            event.path == "new/file.txt" for event in store.events_after(0)
        ):
            time.sleep(0.05)
        assert any(event.path == "new/file.txt" for event in store.events_after(0))
    finally:
        service.stop()
        thread.join(timeout=2)
        store.close()
```

- [ ] **Step 2: Run tests and observe missing remote watcher APIs**

```bash
uv run pytest tests/unit/test_remote_agent_store.py tests/unit/test_inotify_parser.py \
  tests/integration/test_remote_agent_polling.py -v
```

Expected: FAIL because remote store and watcher modules are missing.

- [ ] **Step 3: Implement the remote store and watcher backends**

`RemoteStore` must use SQLite WAL and tables for `workspace`, `events`, `watermark`, `watcher`, and
`remote_index`. It must validate that registered roots are absolute canonical directories and are
not `/` or the remote home directory.

Implement the inotify parser with the Linux record layout:

```python
_HEADER = struct.Struct("iIII")


@dataclass(frozen=True)
class InotifyEvent:
    watch_descriptor: int
    mask: int
    cookie: int
    name: str
    overflow: bool


def parse_inotify_buffer(data: bytes) -> list[InotifyEvent]:
    offset = 0
    parsed: list[InotifyEvent] = []
    while offset + _HEADER.size <= len(data):
        descriptor, mask, cookie, name_length = _HEADER.unpack_from(data, offset)
        offset += _HEADER.size
        raw_name = data[offset : offset + name_length]
        offset += name_length
        name = raw_name.split(b"\0", 1)[0].decode("utf-8", errors="surrogateescape")
        parsed.append(InotifyEvent(descriptor, mask, cookie, name, bool(mask & IN_Q_OVERFLOW)))
    if offset != len(data):
        raise ValueError("truncated inotify buffer")
    return parsed
```

`InotifyBackend` must call libc through `ctypes`, recursively register directories, add watches for
new directories, pair moves by cookie, and emit `rescan-required` on overflow. `WatcherService`
selects inotify on Linux and polling elsewhere. `start` launches a detached watcher with an early
pid record. `events` streams newline-delimited JSON after a sequence. `forget` refuses to remove
state while the watcher is running.

- [ ] **Step 4: Run remote-agent tests and static checks**

```bash
uv run pytest tests/unit/test_remote_agent_store.py tests/unit/test_inotify_parser.py \
  tests/integration/test_remote_agent_polling.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS. The Linux-only test proves inotify is selected and a newly created directory is
watched without a polling restart.

- [ ] **Step 5: Commit remote journaling and watchers**

```bash
git add src/remote_sandbox/remote_agent tests/unit/test_remote_agent_store.py \
  tests/unit/test_inotify_parser.py tests/integration/test_remote_agent_polling.py
git commit -m "feat: add persistent remote watcher"
```

### Task 8: Add the SSH Remote Client, Event Subscription, and Authentication Regression Coverage

**Files:**
- Create: `src/remote_sandbox/remote_client.py`
- Create: `tests/unit/test_remote_client.py`
- Create: `tests/unit/test_ssh_connection_classification.py`
- Modify: `src/remote_sandbox/ssh.py:28-68,386-775`
- Modify: `src/remote_sandbox/agent.py`

**Interfaces:**
- Consumes: agent manager and protocol from Tasks 6 and 7
- Produces: `RemoteWorkspaceClient`
- Produces: `RemoteEventSubscription`
- Produces: `RemoteSnapshot`
- Produces: `RemoteWorkspaceClient.subscribe(after_sequence) -> RemoteEventSubscription`
- Preserves: `clear_master(target) -> None`
- Preserves: `probe_connection(target) -> Literal["ok", "auth", "network"]`

- [ ] **Step 1: Write failing remote-client and existing-auth regression tests**

```python
# tests/unit/test_ssh_connection_classification.py
from remote_sandbox.ssh import _classify_ssh_failure


def test_permission_denied_is_classified_as_authentication() -> None:
    assert _classify_ssh_failure("Permission denied (publickey,password).") == "auth"


def test_timeout_is_classified_as_network() -> None:
    assert _classify_ssh_failure("Connection timed out") == "network"
```

```python
# tests/unit/test_remote_client.py
from remote_sandbox.journal import EventKind
from remote_sandbox.remote_client import parse_event_line


def test_remote_event_line_decodes_sequence_and_unicode_path() -> None:
    event = parse_event_line(b'{"sequence":7,"kind":"delete","path":"算法.py","destination_path":null}\n')
    assert event.sequence == 7
    assert event.kind is EventKind.DELETE
    assert event.path == "算法.py"
```

- [ ] **Step 2: Run tests and verify the remote client is missing while auth behavior stays explicit**

```bash
uv run pytest tests/unit/test_remote_client.py \
  tests/unit/test_ssh_connection_classification.py -v
```

Expected: remote-client test FAILS. Authentication tests must either PASS from the existing unstaged
changes or fail with a precise missing-classification reason. Do not remove that behavior.

- [ ] **Step 3: Implement the SSH-backed client and subscription**

```python
# src/remote_sandbox/remote_client.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import BinaryIO, Iterator

from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import EntryFingerprint, MissingEntry


@dataclass(frozen=True, slots=True)
class RemoteSnapshot:
    entries: dict[str, EntryFingerprint | MissingEntry]
    latest_sequence: int


class RemoteEventSubscription:
    def __init__(self, stream: BinaryIO, process: object) -> None:
        self._stream = stream
        self._process = process

    def __iter__(self) -> Iterator[JournalEvent]:
        for line in self._stream:
            yield parse_event_line(line)

    def close(self) -> None:
        terminate = getattr(self._process, "terminate", None)
        if callable(terminate):
            terminate()


def parse_event_line(line: bytes) -> JournalEvent:
    raw = json.loads(line.decode("utf-8"))
    return JournalEvent(
        side="remote",
        sequence=int(raw["sequence"]),
        kind=EventKind(str(raw["kind"])),
        path=str(raw["path"]),
        destination_path=None if raw.get("destination_path") is None else str(raw["destination_path"]),
    )
```

`RemoteWorkspaceClient` must expose `register`, `start_watcher`, `stop_watcher`, `status`,
`snapshot`, `hash_paths`, `subscribe`, `acknowledge`, and `forget`. Subscription uses a long-lived
`ssh ... python3 agent.pyz events` process and restarts from the last acknowledged sequence.

Extend `SubprocessSshRunner` with a structured foreground call and a streaming `Popen` call. Keep
the existing `clear_master`, `probe_connection`, exponential reconnect inputs, and authentication
classification. Use the isolated ControlPath from Task 1.

- [ ] **Step 4: Run focused tests and static checks**

```bash
uv run pytest tests/unit/test_remote_client.py \
  tests/unit/test_ssh_connection_classification.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit the remote client and preserved connection behavior**

```bash
git add src/remote_sandbox/remote_client.py src/remote_sandbox/ssh.py \
  src/remote_sandbox/agent.py tests/unit/test_remote_client.py \
  tests/unit/test_ssh_connection_classification.py
git commit -m "feat: subscribe to remote workspace events"
```

### Task 9: Refactor Reconciliation Around Dirty Paths, Hash Requests, and Explicit Conflicts

**Files:**
- Create: `tests/unit/test_reconcile_incremental.py`
- Create: `tests/unit/test_reconcile_conflicts.py`
- Modify: `src/remote_sandbox/reconcile.py:1-286`
- Modify: `src/remote_sandbox/state.py`

**Interfaces:**
- Consumes: `EntryFingerprint`, `MissingEntry`, `WorkspaceStore`
- Produces: `ActionType`
- Produces: `HashRequest`
- Produces: `PlanWarning`
- Produces: `SyncAction`
- Produces: `SyncPlan(hash_requests, actions, conflicts, warnings)`
- Produces: `build_incremental_plan(base, local, remote, dirty_paths, policy) -> SyncPlan`

- [ ] **Step 1: Write failing one-sided, hash-request, and conflict tests**

```python
# tests/unit/test_reconcile_incremental.py
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.reconcile import ActionType, build_incremental_plan


def file(path: str, digest: str | None) -> EntryFingerprint:
    return EntryFingerprint(path, EntryKind.FILE, 4, 1, 0o100644, content_hash=digest)


def test_only_local_change_pushes_remote() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": file("a.py", "new")},
        remote={"a.py": file("a.py", "old")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [action.type for action in plan.actions] == [ActionType.PUSH]


def test_ambiguous_changed_file_requests_hash_before_decision() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": file("a.py", None)},
        remote={"a.py": file("a.py", "old")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [(item.side, item.path) for item in plan.hash_requests] == [("local", "a.py")]


def test_both_sides_reaching_the_same_content_updates_only_the_base() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": file("a.py", "same")},
        remote={"a.py": file("a.py", "same")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [action.type for action in plan.actions] == [ActionType.UPDATE_BASE]
    assert plan.conflicts == ()
```

```python
# tests/unit/test_reconcile_conflicts.py
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.reconcile import build_incremental_plan

from tests.unit.test_reconcile_incremental import file


def test_both_modified_preserves_a_conflict_instead_of_selecting_a_winner() -> None:
    plan = build_incremental_plan(
        base={"model.py": file("model.py", "base")},
        local={"model.py": file("model.py", "local")},
        remote={"model.py": file("model.py", "remote")},
        dirty_paths={"model.py"},
        policy=StaticPolicyEngine(),
    )
    assert not plan.actions
    assert plan.conflicts[0].path == "model.py"
    assert plan.conflicts[0].reason == "both-modified"


def test_deleted_placeholder_conflicts_with_unchanged_remote_source() -> None:
    placeholder = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        50_000_000,
        1,
        0o100644,
        content_hash="remote",
        is_placeholder=True,
    )
    remote = file("weights.bin", "remote")
    plan = build_incremental_plan(
        base={"weights.bin": placeholder},
        local={"weights.bin": MissingEntry("weights.bin")},
        remote={"weights.bin": remote},
        dirty_paths={"weights.bin"},
        policy=StaticPolicyEngine(),
    )
    assert plan.actions == ()
    assert plan.conflicts[0].reason == "placeholder-changed"


def test_special_entry_warns_without_blocking_unrelated_paths() -> None:
    special = EntryFingerprint("socket", EntryKind.SPECIAL, None, 1, 0o140777)
    plan = build_incremental_plan(
        base={},
        local={"socket": special},
        remote={"socket": MissingEntry("socket")},
        dirty_paths={"socket"},
        policy=StaticPolicyEngine(),
    )
    assert plan.actions == ()
    assert [(warning.path, warning.reason) for warning in plan.warnings] == [
        ("socket", "special-entry-not-transferred")
    ]
```

- [ ] **Step 2: Run tests and observe mismatch with the full-manifest planner**

```bash
uv run pytest tests/unit/test_reconcile_incremental.py \
  tests/unit/test_reconcile_conflicts.py -v
```

Expected: FAIL because the current planner has no dirty-path or hash-request model.

- [ ] **Step 3: Implement immutable incremental planning models**

```python
class ActionType(StrEnum):
    PUSH = "push"
    PULL = "pull"
    DELETE_LOCAL = "delete-local"
    DELETE_REMOTE = "delete-remote"
    UPDATE_BASE = "update-base"


@dataclass(frozen=True, slots=True)
class HashRequest:
    side: str
    path: str


@dataclass(frozen=True, slots=True)
class ConflictDecision:
    path: str
    reason: str
    local: EntryFingerprint | MissingEntry
    remote: EntryFingerprint | MissingEntry


@dataclass(frozen=True, slots=True)
class PlanWarning:
    path: str
    reason: str


@dataclass(frozen=True, slots=True)
class SyncAction:
    type: ActionType
    path: str
    expected_local: EntryFingerprint | MissingEntry
    expected_remote: EntryFingerprint | MissingEntry
    base_after: EntryFingerprint | MissingEntry


@dataclass(frozen=True, slots=True)
class SyncPlan:
    hash_requests: tuple[HashRequest, ...]
    actions: tuple[SyncAction, ...]
    conflicts: tuple[ConflictDecision, ...]
    warnings: tuple[PlanWarning, ...]
```

`build_incremental_plan()` must iterate only `dirty_paths`, respect ignore policy, request hashes
before deciding ambiguous regular files, treat symlink target text as content identity, propagate
one-sided deletes, and produce conflict records for both-modified, delete-versus-modify, and kind
divergence. Edited or deleted placeholders produce `placeholder-changed` conflicts. Special entries
produce `PlanWarning` values and no transfer action. Remove the current `CONFLICT` and `NEEDS_HASH`
actions so they cannot be silently skipped by an executor.

- [ ] **Step 4: Run all reconciliation tests**

```bash
uv run pytest tests/unit/test_reconcile_incremental.py \
  tests/unit/test_reconcile_conflicts.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit incremental reconciliation**

```bash
git add src/remote_sandbox/reconcile.py src/remote_sandbox/state.py \
  tests/unit/test_reconcile_incremental.py tests/unit/test_reconcile_conflicts.py
git commit -m "feat: plan incremental sync conflicts"
```

### Task 10: Implement Verified Batched rsync Transport with a Safe tar Fallback

**Files:**
- Create: `src/remote_sandbox/transport.py`
- Create: `tests/unit/test_transport_commands.py`
- Create: `tests/integration/test_transport_rsync.py`
- Create: `tests/integration/test_transport_tar.py`
- Modify: `src/remote_sandbox/ssh.py`

**Interfaces:**
- Consumes: `EntryFingerprint` and `SyncAction`
- Produces: `TransferDirection`
- Produces: `TransferItem`
- Produces: `TransferBatch`
- Produces: `TransferResult`
- Produces: `RsyncCapabilities(protect_args: bool, secluded_args: bool)`
- Produces: `RsyncPathUnsupported`
- Produces: `build_rsync_argv(..., capabilities: RsyncCapabilities) -> list[str]`
- Produces: `validate_tar_member(name: str) -> str`
- Produces: `BatchTransport.transfer(batch, on_progress) -> TransferResult`
- Produces: `BatchTransport.delete_remote(paths) -> None`
- Produces: `BatchTransport.delete_local(paths) -> None`

- [ ] **Step 1: Write failing command construction and real transfer tests**

```python
# tests/unit/test_transport_commands.py
from pathlib import Path

import pytest

from remote_sandbox.transport import (
    RsyncCapabilities,
    TransferBatch,
    TransferDirection,
    TransferItem,
    RsyncPathUnsupported,
    build_rsync_argv,
    validate_tar_member,
)


def test_rsync_uses_one_files_from_session_and_preserves_links(tmp_path: Path) -> None:
    batch = TransferBatch(
        TransferDirection.PUSH,
        (TransferItem("a.py", None, None), TransferItem("dir/link", None, None)),
    )
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "host",
        "/remote",
        capabilities=RsyncCapabilities(protect_args=True, secluded_args=False),
    )
    joined = " ".join(argv)
    assert "--archive" in argv
    assert "--links" in argv
    assert "--files-from=-" in argv
    assert joined.count("ssh") <= 1


def test_optional_argument_protection_flag_is_omitted_when_unsupported(tmp_path: Path) -> None:
    batch = TransferBatch(TransferDirection.PUSH, (TransferItem("a.py", None, None),))
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "host",
        "/remote",
        capabilities=RsyncCapabilities(protect_args=False, secluded_args=False),
    )
    assert "--protect-args" not in argv
    assert "--secluded-args" not in argv


def test_unsafe_remote_root_falls_back_when_argument_protection_is_unavailable(
    tmp_path: Path,
) -> None:
    batch = TransferBatch(TransferDirection.PUSH, (TransferItem("a.py", None, None),))
    with pytest.raises(RsyncPathUnsupported):
        build_rsync_argv(
            batch,
            tmp_path,
            "host",
            "/remote/with space",
            capabilities=RsyncCapabilities(protect_args=False, secluded_args=False),
        )


@pytest.mark.parametrize("name", ["../escape", "/absolute", "a/../../escape", "line\nbreak"])
def test_tar_member_validation_rejects_escape_and_control_characters(name: str) -> None:
    with pytest.raises(ValueError):
        validate_tar_member(name)


def test_tar_member_validation_accepts_unicode_spaces_and_leading_dash() -> None:
    assert validate_tar_member("-实验 data/file.txt") == "-实验 data/file.txt"
```

```python
# tests/integration/test_transport_rsync.py
from pathlib import Path

from remote_sandbox.transport import LocalPairTransport, TransferBatch, TransferDirection, TransferItem


def test_batch_push_copies_multiple_files_and_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    (source / "b.txt").write_text("b", encoding="utf-8")
    (source / "link").symlink_to("a.txt")
    transport = LocalPairTransport(source, destination, engine="rsync")
    transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            tuple(TransferItem(path, None, None) for path in ("a.txt", "b.txt", "link")),
        ),
        lambda _progress: None,
    )
    assert (destination / "a.txt").read_text(encoding="utf-8") == "a"
    assert (destination / "link").readlink() == Path("a.txt")
```

```python
# tests/integration/test_transport_tar.py
from pathlib import Path

from remote_sandbox.transport import LocalPairTransport, TransferBatch, TransferDirection, TransferItem


def test_tar_fallback_copies_multiple_files_and_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested" / "b.txt").write_text("b", encoding="utf-8")
    (source / "link").symlink_to("a.txt")
    transport = LocalPairTransport(source, destination, engine="tar")
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            tuple(TransferItem(path, None, None) for path in ("a.txt", "nested/b.txt", "link")),
        ),
        lambda _progress: None,
    )
    assert result.changed_during_transfer == ()
    assert (destination / "nested" / "b.txt").read_text(encoding="utf-8") == "b"
    assert (destination / "link").readlink() == Path("a.txt")
```

- [ ] **Step 2: Run tests and observe the missing batch transport**

```bash
uv run pytest tests/unit/test_transport_commands.py \
  tests/integration/test_transport_rsync.py tests/integration/test_transport_tar.py -v
```

Expected: FAIL because `transport.py` and `LocalPairTransport` do not exist.

- [ ] **Step 3: Implement validated transfer batches**

```python
class TransferDirection(StrEnum):
    PUSH = "push"
    PULL = "pull"


@dataclass(frozen=True, slots=True)
class TransferItem:
    path: str
    expected_source: EntryFingerprint | MissingEntry | None
    expected_destination: EntryFingerprint | MissingEntry | None


@dataclass(frozen=True, slots=True)
class TransferBatch:
    direction: TransferDirection
    items: tuple[TransferItem, ...]


@dataclass(frozen=True, slots=True)
class TransferResult:
    completed: tuple[str, ...]
    changed_during_transfer: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RsyncCapabilities:
    protect_args: bool
    secluded_args: bool
```

`build_rsync_argv()` must use the portable local feature intersection:

```python
argv = [
    "rsync",
    "--archive",
    "--links",
    "--times",
    "--itemize-changes",
    "--files-from=-",
]
if capabilities.protect_args:
    argv.append("--protect-args")
elif capabilities.secluded_args:
    argv.append("--secluded-args")
argv.extend((source, destination))
return argv
```

Do not add unsupported flags blindly. When neither optional argument-protection flag is supported,
omit both. Capability probing must never select one unsupported flag merely because the other is
unavailable. In that mode, use rsync only when the validated target and remote root contain the
strict safe subset `[A-Za-z0-9_./@+-]`; otherwise raise `RsyncPathUnsupported` and let
`BatchTransport` use the structured tar fallback.
Feed validated newline-delimited paths through stdin because local openrsync lacks `--from0`.
Reject control-character paths before transport. For tar fallback, create and extract only validated
relative paths, reject absolute or `..` members, and use temporary destinations plus atomic rename.
Resolve every source and destination through `manifest.workspace_path()` so a parent symlink cannot
redirect reads or writes outside a registered root.

Before transfer, compare each item with its expected source and destination fingerprints. After
transfer, fingerprint the destination. Return changed paths without updating base. Never mark an
entire batch successful because the process returned zero if an expected fingerprint changed.

- [ ] **Step 4: Run transport tests and compare both engines**

```bash
uv run pytest tests/unit/test_transport_commands.py \
  tests/integration/test_transport_rsync.py tests/integration/test_transport_tar.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS for rsync and tar implementations.

- [ ] **Step 5: Commit verified batch transport**

```bash
git add src/remote_sandbox/transport.py src/remote_sandbox/ssh.py \
  tests/unit/test_transport_commands.py tests/integration/test_transport_rsync.py \
  tests/integration/test_transport_tar.py
git commit -m "feat: add verified batch transport"
```

### Task 11: Build the Incremental Sync Engine, Echo Suppression, and Audit Recovery

**Files:**
- Create: `src/remote_sandbox/engine.py`
- Create: `tests/helpers/__init__.py`
- Create: `tests/helpers/sync_harness.py`
- Create: `tests/unit/conftest.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/unit/test_engine_transaction.py`
- Create: `tests/integration/test_incremental_sync.py`
- Create: `tests/integration/test_audit_recovery.py`
- Modify: `src/remote_sandbox/state.py`

**Interfaces:**
- Consumes: local and remote journals, reconciler, transport, policy, workspace store
- Produces: `SyncEngine`
- Produces: `SyncEngine.run_once(reason: str) -> EngineResult`
- Produces: `SyncEngine.audit() -> EngineResult`
- Produces: `SyncEngine.seed_base_from_current_replicas() -> None` for initial-sync and integration setup
- Produces: `SyncEngine.seed_base_from_transfer(batch, completed_paths) -> None`
- Produces: `SyncEngine.requeue_paths(paths, reason) -> None`
- Produces: `SyncEngine.apply_initial_placeholders(placeholders) -> None`
- Produces: `EngineResult`
- Test fixture: `engine_fixture -> EngineHarness`
- Test fixture: `sync_pair -> SyncPair`

- [ ] **Step 1: Write failing transaction and echo tests**

```python
# tests/helpers/sync_harness.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.engine import SyncEngine
from remote_sandbox.journal import EventKind
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.state import WorkspaceStore


@dataclass(slots=True)
class EngineHarness:
    local: Path
    remote: Path
    store: WorkspaceStore
    transport: ControllableLocalPairTransport
    remote_client: LocalReplicaClient
    engine: SyncEngine

    def append_local_modify(self, path: str, content: bytes) -> None:
        destination = self.local / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.store.append_event("local", EventKind.MODIFY, path)

    def append_remote_event_for_current_fingerprint(self, path: str) -> None:
        self.remote_client.append_event(EventKind.MODIFY, path)


@dataclass(slots=True)
class SyncPair:
    local: Path
    remote: Path
    store: WorkspaceStore
    remote_client: LocalReplicaClient
    transport: ControllableLocalPairTransport
    engine: SyncEngine

    def append_local_modify(self, path: str, content: bytes) -> None:
        destination = self.local / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.store.append_event("local", EventKind.MODIFY, path)

    def append_remote_delete(self, path: str) -> None:
        (self.remote / path).unlink()
        self.remote_client.append_event(EventKind.DELETE, path)


def make_engine_harness(tmp_path: Path) -> EngineHarness:
    pair = make_sync_pair(tmp_path)
    return EngineHarness(
        pair.local,
        pair.remote,
        pair.store,
        pair.transport,
        pair.remote_client,
        pair.engine,
    )


def make_sync_pair(tmp_path: Path) -> SyncPair:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    store = WorkspaceStore.open(tmp_path / "state.sqlite3")
    remote_client = LocalReplicaClient(remote, tmp_path / "remote-state.sqlite3")
    transport = ControllableLocalPairTransport(local, remote)
    engine = SyncEngine(
        store=store,
        local_root=local,
        remote=remote_client,
        transport=transport,
        policy=StaticPolicyEngine(),
    )
    return SyncPair(local, remote, store, remote_client, transport, engine)
```

In the same helper file, implement `LocalReplicaClient` as an in-process implementation of the
`RemoteWorkspaceClient` interface backed by the remote directory and a `RemoteStore`.
`ControllableLocalPairTransport` must implement `BatchTransport`, perform real local filesystem
copies, count transfer calls, and expose `change_source_before_commit(path)` by mutating the source
after preflight verification but before destination commit. These helpers are test code only and
must not be imported by production modules.

```python
# tests/unit/conftest.py
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.helpers.sync_harness import EngineHarness, make_engine_harness


@pytest.fixture
def engine_fixture(tmp_path: Path) -> Iterator[EngineHarness]:
    harness = make_engine_harness(tmp_path)
    yield harness
    harness.store.close()
    harness.remote_client.close()
```

```python
# tests/integration/conftest.py
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.helpers.sync_harness import SyncPair, make_sync_pair


@pytest.fixture
def sync_pair(tmp_path: Path) -> Iterator[SyncPair]:
    pair = make_sync_pair(tmp_path)
    yield pair
    pair.store.close()
    pair.remote_client.close()
```

```python
# tests/unit/test_engine_transaction.py
from tests.helpers.sync_harness import EngineHarness


def test_engine_does_not_ack_event_when_transfer_changes_midflight(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.transport.change_source_before_commit("a.py")
    engine_fixture.append_local_modify("a.py", b"new")
    result = engine_fixture.engine.run_once("watcher")
    assert result.requeued == ("a.py",)
    assert engine_fixture.store.acknowledged_sequence("local") == 0


def test_expected_destination_event_is_acknowledged_as_echo(
    engine_fixture: EngineHarness,
) -> None:
    engine_fixture.append_local_modify("a.py", b"new")
    first = engine_fixture.engine.run_once("watcher")
    assert first.completed == ("a.py",)
    engine_fixture.append_remote_event_for_current_fingerprint("a.py")
    second = engine_fixture.engine.run_once("remote-watch")
    assert second.transferred == ()
    assert second.echoes == ("a.py",)
```

```python
# tests/integration/test_incremental_sync.py
from tests.helpers.sync_harness import SyncPair


def test_local_modify_and_remote_delete_are_reconciled_incrementally(sync_pair: SyncPair) -> None:
    (sync_pair.local / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.remote / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.local / "remote.txt").write_text("delete", encoding="utf-8")
    (sync_pair.remote / "remote.txt").write_text("delete", encoding="utf-8")
    sync_pair.engine.seed_base_from_current_replicas()

    sync_pair.append_local_modify("local.txt", b"new")
    sync_pair.append_remote_delete("remote.txt")
    result = sync_pair.engine.run_once("integration")

    assert set(result.completed) == {"local.txt", "remote.txt"}
    assert (sync_pair.remote / "local.txt").read_bytes() == b"new"
    assert not (sync_pair.local / "remote.txt").exists()
    assert sync_pair.transport.transfer_calls == 1
```

```python
# tests/integration/test_audit_recovery.py
from tests.helpers.sync_harness import SyncPair


def test_audit_finds_change_when_watcher_event_was_lost(sync_pair: SyncPair) -> None:
    (sync_pair.remote / "lost.txt").write_text("remote", encoding="utf-8")
    result = sync_pair.engine.audit()
    assert "lost.txt" in result.completed
    assert (sync_pair.local / "lost.txt").read_text(encoding="utf-8") == "remote"
```

- [ ] **Step 2: Run tests and observe missing engine behavior**

```bash
uv run pytest tests/unit/test_engine_transaction.py \
  tests/integration/test_incremental_sync.py tests/integration/test_audit_recovery.py -v
```

Expected: FAIL because `SyncEngine` does not exist.

- [ ] **Step 3: Implement one transactional sync cycle**

```python
@dataclass(frozen=True, slots=True)
class EngineResult:
    transferred: tuple[str, ...] = ()
    completed: tuple[str, ...] = ()
    requeued: tuple[str, ...] = ()
    echoes: tuple[str, ...] = ()
    conflict_ids: tuple[str, ...] = ()


class SyncEngine:
    def run_once(self, reason: str) -> EngineResult:
        local_events = self.store.pending_events("local", self.store.acknowledged_sequence("local"))
        remote_events = self.remote.events_after(self.store.acknowledged_sequence("remote"))
        dirty = coalesce_dirty_paths(local_events, remote_events)
        if not dirty:
            return EngineResult()
        local = self.local_metadata.snapshot(dirty, with_hash=False)
        remote = self.remote.snapshot(dirty, with_hash=False)
        plan = build_incremental_plan(self.store.list_base(), local, remote.entries, dirty, self.policy)
        plan = self._satisfy_hash_requests(plan, local, remote.entries)
        return self._execute_and_commit(plan, local_events, remote_events, reason)
```

`_execute_and_commit()` must:

1. Persist conflicts before acknowledging their triggering sequences.
2. Group transfer actions by direction.
3. Register expected destination fingerprints before watchers can report echoes.
4. Execute verified transport batches.
5. Requeue changed-during-transfer paths.
6. Update base, expected echoes, conflict count, and journal watermarks in one SQLite transaction.
7. Acknowledge remote sequence through the remote client only after the local transaction commits.

`audit()` scans path metadata on both sides, compares it with stored fingerprints, writes synthetic
events for drift, and calls `run_once("audit")`. It hashes only ambiguous candidates.

- [ ] **Step 4: Run engine, audit, and existing reconciliation tests**

```bash
uv run pytest tests/unit/test_engine_transaction.py tests/unit/test_reconcile_incremental.py \
  tests/unit/test_reconcile_conflicts.py tests/integration/test_incremental_sync.py \
  tests/integration/test_audit_recovery.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit the incremental engine**

```bash
git add src/remote_sandbox/engine.py src/remote_sandbox/state.py \
  tests/helpers/__init__.py tests/helpers/sync_harness.py tests/unit/conftest.py \
  tests/integration/conftest.py tests/unit/test_engine_transaction.py \
  tests/integration/test_incremental_sync.py tests/integration/test_audit_recovery.py
git commit -m "feat: add incremental sync engine"
```

### Task 12: Implement Watcher-first Initial Sync and Live Progress

**Files:**
- Create: `src/remote_sandbox/initial_sync.py`
- Create: `tests/integration/test_bind_validation.py`
- Create: `tests/integration/test_initial_sync.py`
- Create: `tests/integration/test_initial_sync_concurrent_changes.py`
- Modify: `tests/helpers/sync_harness.py`
- Modify: `tests/integration/conftest.py`
- Modify: `src/remote_sandbox/bind.py:1-343`
- Modify: `src/remote_sandbox/status.py`

**Interfaces:**
- Consumes: `SyncEngine`, `BatchTransport`, local watcher, remote client, workspace store
- Produces: `InitialDirection`
- Produces: `InitialSyncPlan(transfer_batch, placeholders)`
- Produces: `InitialSyncCoordinator`
- Produces: `InitialSyncCoordinator.run() -> InitialSyncResult`
- Produces: progress stages `scanning`, `planning`, `transferring`, `replaying`, `ready`
- Test fixture: `initial_pair -> InitialPairHarness`

- [ ] **Step 1: Write failing initial-copy and concurrent-change tests**

```python
# tests/integration/test_initial_sync.py
from remote_sandbox.placeholder import decode_placeholder
from tests.helpers.sync_harness import InitialPairHarness


def test_remote_source_bulk_sync_starts_watchers_before_copy(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")
    result = initial_pair.coordinator.run()
    assert result.direction.value == "remote-to-local"
    assert initial_pair.local_watcher.started_before_transfer is True
    assert initial_pair.remote_watcher.started_before_transfer is True
    assert (initial_pair.local / "a.txt").read_text(encoding="utf-8") == "a"
    assert initial_pair.store.get_status().phase.value == "ready"


def test_local_source_bulk_sync_uses_the_same_immediate_coordinator(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.local / "local.txt").write_text("local", encoding="utf-8")
    result = initial_pair.coordinator.run()
    assert result.direction.value == "local-to-remote"
    assert (initial_pair.remote / "local.txt").read_text(encoding="utf-8") == "local"


def test_both_empty_reaches_ready_without_a_transfer(initial_pair: InitialPairHarness) -> None:
    result = initial_pair.coordinator.run()
    assert result.direction.value == "empty"
    assert initial_pair.transport.transfer_calls == 0
    assert initial_pair.store.get_status().phase.value == "ready"


def test_large_remote_file_becomes_a_validated_local_placeholder(
    initial_pair: InitialPairHarness,
) -> None:
    initial_pair.set_placeholder_limit(4)
    (initial_pair.remote / "weights.bin").write_bytes(b"0123456789")
    initial_pair.coordinator.run()
    metadata = decode_placeholder(
        (initial_pair.local / "weights.bin").read_bytes(),
        expected_path="weights.bin",
    )
    assert metadata is not None
    assert metadata.size == 10
    assert (initial_pair.remote / "weights.bin").read_bytes() == b"0123456789"
```

```python
# tests/integration/test_bind_validation.py
from pathlib import Path

import pytest

from remote_sandbox.bind import BindError, bind_workspace
from remote_sandbox.ssh import FakeSshRunner


def test_two_unrelated_non_empty_trees_are_rejected_before_metadata_commit(
    monkeypatch,
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
    remote_paths = [path for _target, path in [*runner.files, *runner.binary_files, *runner.dirs]]
    assert not any(".codex-remote-sandbox" in path for path in remote_paths)
```

```python
# tests/integration/test_initial_sync_concurrent_changes.py
from tests.helpers.sync_harness import InitialPairHarness


def test_change_created_during_bulk_copy_is_replayed(
    initial_pair: InitialPairHarness,
) -> None:
    initial_pair.transport.on_first_progress = lambda: (
        initial_pair.remote / "late.txt"
    ).write_text("late", encoding="utf-8")
    initial_pair.coordinator.run()
    assert (initial_pair.local / "late.txt").read_text(encoding="utf-8") == "late"
```

Before running these tests, append the exact initial-sync harness to
`tests/helpers/sync_harness.py`:

```python
@dataclass(slots=True)
class InitialPairHarness:
    local: Path
    remote: Path
    store: WorkspaceStore
    local_watcher: RecordingWatcher
    remote_watcher: RecordingWatcher
    transport: ControllableLocalPairTransport
    coordinator: InitialSyncCoordinator

    def set_placeholder_limit(self, value: int) -> None:
        self.coordinator.placeholder_limit = value


def make_initial_pair(tmp_path: Path) -> InitialPairHarness:
    pair = make_sync_pair(tmp_path)
    order = OperationOrder()
    local_watcher = RecordingWatcher(order, "local-watcher")
    remote_watcher = RecordingWatcher(order, "remote-watcher")
    pair.transport.operation_order = order
    coordinator = InitialSyncCoordinator(
        store=pair.store,
        local_root=pair.local,
        remote=pair.remote_client,
        transport=pair.transport,
        engine=pair.engine,
        start_local_watcher=local_watcher.start,
        start_remote_watcher=remote_watcher.start,
    )
    return InitialPairHarness(
        pair.local,
        pair.remote,
        pair.store,
        local_watcher,
        remote_watcher,
        pair.transport,
        coordinator,
    )
```

`OperationOrder` records monotonically increasing call numbers. `RecordingWatcher.start()` stores
its call number and returns the journal sequence present at that moment.
`ControllableLocalPairTransport.transfer()` stores its own call number, and each watcher's
`started_before_transfer` property compares those values. This makes watcher-first ordering an
observable assertion rather than a manually assigned Boolean.

Append the fixture to `tests/integration/conftest.py`:

```python
from tests.helpers.sync_harness import InitialPairHarness, make_initial_pair


@pytest.fixture
def initial_pair(tmp_path: Path) -> Iterator[InitialPairHarness]:
    pair = make_initial_pair(tmp_path)
    yield pair
    pair.store.close()
    pair.coordinator.remote.close()
```

- [ ] **Step 2: Run tests and observe the old foreground-plus-startup sync behavior**

```bash
uv run pytest tests/integration/test_bind_validation.py \
  tests/integration/test_initial_sync.py \
  tests/integration/test_initial_sync_concurrent_changes.py -v
```

Expected: FAIL because initial sync is still performed in `bind_workspace` and repeated by daemon startup.

- [ ] **Step 3: Implement one owner for initial synchronization**

```python
class InitialDirection(StrEnum):
    LOCAL_TO_REMOTE = "local-to-remote"
    REMOTE_TO_LOCAL = "remote-to-local"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class InitialSyncResult:
    direction: InitialDirection
    files: int
    bytes: int
    placeholders: int


class InitialSyncCoordinator:
    def run(self) -> InitialSyncResult:
        self.store.set_status(WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("scanning")))
        local_start = self.start_local_watcher()
        remote_start = self.remote.start_watcher()
        local_snapshot = self.local_metadata.full_snapshot(
            with_hash=False,
            on_progress=self._on_scan_progress,
        )
        remote_snapshot = self.remote.full_snapshot(
            with_hash=False,
            on_progress=self._on_scan_progress,
        )
        self.store.set_status(
            WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("planning"))
        )
        direction = choose_initial_direction(local_snapshot, remote_snapshot, self.policy)
        plan = build_initial_plan(
            direction,
            local_snapshot,
            remote_snapshot,
            placeholder_limit=self.placeholder_limit,
        )
        batch = plan.transfer_batch
        transfer_result = (
            self.transport.transfer(batch, self._on_progress)
            if batch.items
            else TransferResult((), ())
        )
        self.engine.seed_base_from_transfer(batch, transfer_result.completed)
        self.engine.apply_initial_placeholders(plan.placeholders)
        self.engine.requeue_paths(
            transfer_result.changed_during_transfer,
            reason="changed-during-initial-transfer",
        )
        self.engine.replay_until_quiet(local_start, remote_start, quiet_seconds=0.5)
        self.store.set_status(WorkspaceStatus(WorkspacePhase.READY, SyncProgress("ready")))
        return InitialSyncResult(
            direction,
            len(batch.items),
            sum(item_size(item) for item in batch.items),
            len(plan.placeholders),
        )
```

Move direction confirmation and metadata creation into `bind.py`, but remove its direct
`SyncSession.sync_once()` call. The daemon/supervisor owns the coordinator. Both-non-empty remains a
hard error. Both-empty transitions to ready without transfer. Progress is persisted before each
stage and updated from scan and transport callbacks. Scan callbacks report entries and bytes seen
at least every 250 ms even before totals are known. Transfer callbacks report file and byte totals,
current relative path, and elapsed time without hashing unchanged content.

For remote-to-local initial sync, regular files above the configured placeholder limit are removed
from the transfer batch. Request strong hashes only for those selected large files, write validated
placeholder content atomically on the local side, register its expected watcher echo, and store the
remote file fingerprint as an unresolved placeholder base. Local-to-remote initial sync never
uploads placeholder text as ordinary file content.

- [ ] **Step 4: Run initial-sync and engine regression tests**

```bash
uv run pytest tests/integration/test_bind_validation.py \
  tests/integration/test_initial_sync.py \
  tests/integration/test_initial_sync_concurrent_changes.py \
  tests/integration/test_incremental_sync.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS. Instrumented tests show exactly one initial bulk transfer.

- [ ] **Step 5: Commit watcher-first initial sync**

```bash
git add src/remote_sandbox/initial_sync.py src/remote_sandbox/bind.py \
  src/remote_sandbox/status.py tests/helpers/sync_harness.py \
  tests/integration/conftest.py tests/integration/test_bind_validation.py \
  tests/integration/test_initial_sync.py \
  tests/integration/test_initial_sync_concurrent_changes.py
git commit -m "feat: add watcher-first initial sync"
```

### Task 13: Replace the Daemon with a Truthful Workspace Supervisor

**Files:**
- Create: `tests/unit/test_daemon_lifecycle.py`
- Create: `tests/unit/test_daemon_reconnect.py`
- Create: `tests/unit/test_daemon_failure_modes.py`
- Create: `tests/integration/test_daemon_restart.py`
- Modify: `tests/helpers/sync_harness.py`
- Modify: `tests/unit/conftest.py`
- Modify: `tests/integration/conftest.py`
- Modify: `src/remote_sandbox/daemon.py:1-590`
- Modify: `src/remote_sandbox/workspace.py`
- Modify: `src/remote_sandbox/remote_client.py`

**Interfaces:**
- Consumes: workspace store, initial coordinator, incremental engine, remote subscription
- Produces: `WorkspaceSupervisor`
- Produces: `SupervisorClient`
- Produces: control requests `status`, `sync`, `stop`
- Preserves: auth versus network failure classification from the existing unstaged daemon changes
- Test fixture: `supervisor_fixture -> SupervisorHarness`
- Test fixture: `daemon_pair -> DaemonPairHarness`

- [ ] **Step 1: Write failing lifecycle, reconnect, and restart tests**

```python
# tests/unit/test_daemon_lifecycle.py
from remote_sandbox.status import WorkspacePhase
from tests.helpers.sync_harness import SupervisorHarness


def test_supervisor_publishes_starting_before_initial_sync(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()
    status = supervisor_fixture.store.get_status()
    assert status.phase is WorkspacePhase.STARTING
    assert supervisor_fixture.client.status().running is True
```

```python
# tests/unit/test_daemon_reconnect.py
from remote_sandbox.status import WorkspacePhase
from tests.helpers.sync_harness import SupervisorHarness


def test_password_auth_failure_becomes_disconnected(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_auth_failure()
    supervisor_fixture.supervisor.handle_subscription_failure()
    assert supervisor_fixture.store.get_status().phase is WorkspacePhase.DISCONNECTED


def test_network_failure_retries_without_requesting_password(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_network_failure()
    delay = supervisor_fixture.supervisor.handle_subscription_failure()
    assert delay == 2.0
    assert supervisor_fixture.store.get_status().phase is WorkspacePhase.DISCONNECTED
```

```python
# tests/integration/test_daemon_restart.py
from tests.helpers.sync_harness import DaemonPairHarness


def test_restart_replays_unacknowledged_events(daemon_pair: DaemonPairHarness) -> None:
    daemon_pair.append_remote_change("after-crash.txt", b"x")
    daemon_pair.kill_local_daemon()
    daemon_pair.start_local_daemon()
    daemon_pair.wait_until_ready()
    assert (daemon_pair.local / "after-crash.txt").read_bytes() == b"x"
```

```python
# tests/unit/test_daemon_failure_modes.py
from remote_sandbox.status import WorkspacePhase
from tests.helpers.sync_harness import SupervisorHarness


def test_remote_watcher_crash_becomes_degraded_and_requests_audit(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_watcher_crash()
    supervisor_fixture.supervisor.handle_subscription_failure()
    status = supervisor_fixture.store.get_status()
    assert status.phase is WorkspacePhase.DEGRADED
    assert supervisor_fixture.supervisor.audit_requested is True


def test_live_pid_without_control_socket_is_never_reported_stopped(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.publish_live_pid_without_socket()
    status = supervisor_fixture.client.status()
    assert status.phase in {WorkspacePhase.STARTING, WorkspacePhase.DEGRADED}
```

Append the following harness contracts to `tests/helpers/sync_harness.py`:

```python
@dataclass(slots=True)
class SupervisorHarness:
    store: WorkspaceStore
    remote: ControllableRemoteClient
    initial_sync: BlockingInitialSync
    supervisor: WorkspaceSupervisor
    client: SupervisorClient
    thread: Thread | None = None

    def start_in_thread(self) -> None:
        self.thread = Thread(target=self.supervisor.run, daemon=True)
        self.thread.start()
        self.client.wait_until_running(timeout=2.0)

    def close(self) -> None:
        self.client.stop()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
        self.store.close()


@dataclass(slots=True)
class DaemonPairHarness:
    local: Path
    remote: Path
    supervisor: WorkspaceSupervisor
    client: SupervisorClient
    remote_client: LocalReplicaClient

    def append_remote_change(self, path: str, content: bytes) -> None:
        destination = self.remote / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        self.remote_client.append_event(EventKind.MODIFY, path)

    def kill_local_daemon(self) -> None:
        self.client.kill_for_test()

    def start_local_daemon(self) -> None:
        self.client.start()

    def wait_until_ready(self) -> None:
        self.client.wait_for_phase(WorkspacePhase.READY, timeout=5.0)
```

`make_supervisor_harness()` must use `BlockingInitialSync`, whose `run()` blocks on an event before
scanning. `ControllableRemoteClient.raise_auth_failure()` and `raise_network_failure()` configure
the next subscription attempt to raise the same typed exceptions used by production SSH code.
`make_daemon_pair()` uses the real SQLite store and worker thread, with only SSH replaced by the
in-process `LocalReplicaClient`.

Append fixtures to their conftest files:

```python
# tests/unit/conftest.py
@pytest.fixture
def supervisor_fixture(tmp_path: Path) -> Iterator[SupervisorHarness]:
    harness = make_supervisor_harness(tmp_path)
    yield harness
    harness.close()
```

```python
# tests/integration/conftest.py
@pytest.fixture
def daemon_pair(tmp_path: Path) -> Iterator[DaemonPairHarness]:
    harness = make_daemon_pair(tmp_path)
    yield harness
    harness.client.stop()
    harness.remote_client.close()
```

- [ ] **Step 2: Run tests and observe late publication or missing supervisor behavior**

```bash
uv run pytest tests/unit/test_daemon_lifecycle.py tests/unit/test_daemon_reconnect.py \
  tests/unit/test_daemon_failure_modes.py tests/integration/test_daemon_restart.py -v
```

Expected: FAIL because the current daemon publishes pid/socket after startup sync and has no journal replay supervisor.

- [ ] **Step 3: Implement supervisor lifecycle and durable status**

```python
class WorkspaceSupervisor:
    def run(self) -> None:
        self._acquire_single_instance_lock()
        self.store.set_status(WorkspaceStatus(WorkspacePhase.STARTING, SyncProgress("starting")))
        self._write_pidfile()
        self.control_server.start()
        try:
            self.remote.ensure_agent()
            if not self.store.initial_sync_completed():
                self.initial_sync.run()
            self._start_subscription()
            self._worker_loop()
        except AuthenticationRequired as exc:
            self.store.set_status(
                WorkspaceStatus(WorkspacePhase.DISCONNECTED, SyncProgress("offline"), last_error=str(exc))
            )
            self._retry_loop(requires_foreground_auth=True)
        except Exception as exc:
            self.store.set_status(
                WorkspaceStatus(WorkspacePhase.FAILED, SyncProgress("failed"), last_error=str(exc))
            )
            raise
        finally:
            self.control_server.stop()
            self._remove_pidfile()
            self._release_lock()
```

Write pid and open the control socket before any remote call. `daemon_status()` must combine durable
status, pid liveness, and socket response. A live process with an unavailable socket is `starting`
or `degraded`, never `stopped`. A dead pid with stale state is `failed`. Use rotated logs.

Configure `logging.handlers.RotatingFileHandler` at each workspace `daemon.log` with
`maxBytes=5 * 1024 * 1024`, `backupCount=3`, UTF-8 encoding, and user-only file permissions.

On subscription failure, call the existing `clear_master()` and `probe_connection()`. Network and
key-auth failures retry with bounded exponential backoff. Password auth remains disconnected until
`codex-rsb reconnect` establishes a foreground master and sends a resume control request.

- [ ] **Step 4: Run lifecycle and existing connection tests**

```bash
uv run pytest tests/unit/test_daemon_lifecycle.py tests/unit/test_daemon_reconnect.py \
  tests/unit/test_daemon_failure_modes.py tests/unit/test_ssh_connection_classification.py \
  tests/integration/test_daemon_restart.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit the workspace supervisor**

```bash
git add src/remote_sandbox/daemon.py src/remote_sandbox/workspace.py \
  src/remote_sandbox/remote_client.py tests/helpers/sync_harness.py \
  tests/unit/conftest.py tests/integration/conftest.py tests/unit/test_daemon_lifecycle.py \
  tests/unit/test_daemon_reconnect.py tests/unit/test_daemon_failure_modes.py \
  tests/integration/test_daemon_restart.py
git commit -m "feat: add truthful workspace supervisor"
```

### Task 14: Keep `enter` and `connect` in One SSH PTY

**Files:**
- Create: `tests/unit/test_shell_connect_handshake.py`
- Create: `tests/integration/test_same_session_shell.py`
- Modify: `tests/helpers/sync_harness.py`
- Modify: `tests/integration/conftest.py`
- Modify: `src/remote_sandbox/shell.py:1-553`
- Modify: `src/remote_sandbox/cli.py:444-482`

**Interfaces:**
- Consumes: local binding service and workspace status
- Produces: `ConnectRequestEvent`
- Produces: `ConnectResponse`
- Produces: `ManagedShellSession`
- Changes: `enter_shell_loop(...)` accepts `on_connect_request` and does not return merely to bind
- Test fixture: `fake_pty_backend -> FakePtyBackendHarness`

- [ ] **Step 1: Write failing same-session protocol tests**

```python
# tests/unit/test_shell_connect_handshake.py
from remote_sandbox.shell import ConnectResponse, build_enter_remote_shell_command


def test_connect_request_does_not_emit_exit_or_close_session() -> None:
    command = build_enter_remote_shell_command("host", "~", nonce="abc")
    script = command[-1]
    assert "connect-request" in script
    assert "exit 0" not in script
    assert "read -r __codex_response" in script


def test_success_response_activates_managed_prompt() -> None:
    response = ConnectResponse(ok=True, workspace_id="w1", name="dq", remote_root="/work/dq")
    assert response.encode().startswith("ok\t")
```

```python
# tests/integration/test_same_session_shell.py
from tests.helpers.sync_harness import FakePtyBackendHarness


def test_binding_success_reuses_the_original_pty(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    original_pid = session.remote_shell_pid
    session.type("codex-rsb connect --name dq\n")
    session.accept_binding()
    assert session.remote_shell_pid == original_pid
    assert "Shared connection" not in session.output
    assert session.prompt_mode == "managed"


def test_incomplete_remote_destination_starts_in_home_then_enters_when_ready(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    session.connect(direction="local-to-remote", remote_root="/work/dq")
    assert session.remote_cwd == "/home/test"
    session.publish_ready()
    assert session.remote_cwd == "/work/dq"


def test_complete_remote_source_starts_in_workspace_immediately(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    session.connect(direction="remote-to-local", remote_root="/work/dq")
    assert session.remote_cwd == "/work/dq"


def test_ready_does_not_change_directory_after_user_leaves_holding_directory(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    session.connect(direction="local-to-remote", remote_root="/work/dq")
    session.type("cd /tmp\n")
    session.publish_ready()
    assert session.remote_cwd == "/tmp"
```

Add a deterministic PTY protocol harness to `tests/helpers/sync_harness.py`:

```python
@dataclass(slots=True)
class FakeManagedPtySession:
    remote_shell_pid: int
    output: str
    prompt_mode: str
    _session: ManagedShellSession

    def type(self, text: str) -> None:
        self._session.feed_user_input(text.encode())

    def accept_binding(self) -> None:
        self._session.handle_connect_response(
            ConnectResponse(ok=True, workspace_id="w1", name="dq", remote_root="/work/dq")
        )
        self.output = self._session.captured_output()
        self.prompt_mode = self._session.prompt_mode

    def connect(self, *, direction: str, remote_root: str) -> None:
        self._session.activate_workspace(
            ConnectResponse(ok=True, workspace_id="w1", name="dq", remote_root=remote_root),
            direction=direction,
        )

    def publish_ready(self) -> None:
        self._session.publish_ready()

    @property
    def remote_cwd(self) -> str:
        return self._session.remote_cwd


@dataclass(slots=True)
class FakePtyBackendHarness:
    def open_enter_shell(self) -> FakeManagedPtySession:
        session = ManagedShellSession.for_test(remote_shell_pid=4242)
        return FakeManagedPtySession(4242, "", "enter", session)
```

Append the fixture to `tests/integration/conftest.py`:

```python
@pytest.fixture
def fake_pty_backend() -> FakePtyBackendHarness:
    return FakePtyBackendHarness()
```

- [ ] **Step 2: Run tests and observe the intentional `exit 0` failure**

```bash
uv run pytest tests/unit/test_shell_connect_handshake.py \
  tests/integration/test_same_session_shell.py -v
```

Expected: FAIL because the injected function exits the browsing shell.

- [ ] **Step 3: Implement a bidirectional connect handshake**

```python
@dataclass(frozen=True, slots=True)
class ConnectResponse:
    ok: bool
    workspace_id: str | None = None
    name: str | None = None
    remote_root: str | None = None
    error: str | None = None

    def encode(self) -> str:
        if self.ok:
            return "\t".join(("ok", self.workspace_id or "", self.name or "", self.remote_root or ""))
        return "\t".join(("error", self.error or "binding failed"))
```

The remote `codex-rsb` shell function must:

1. Parse local, remote, and name arguments.
2. Emit the nonce-authenticated connect request marker.
3. Disable terminal echo for the response read.
4. `read -r __codex_response` from the PTY.
5. Restore echo in a trap-safe block.
6. On `ok`, export workspace fields, change prompt mode, and return without exiting.
7. On `error`, print one line and return to browsing prompt.

The local PTY backend temporarily routes stdin to local confirmation while the remote function is
waiting. It runs the binding service, starts the supervisor, sends `ConnectResponse.encode()` to the
existing PTY, and resumes raw passthrough. The success response is sent as soon as metadata is
committed and the supervisor has published its pid, control socket, and `initial-syncing` state. It
must not wait for the initial transfer to finish. Print one `Connected <name>: ...` binding line,
then keep progress only in the dynamically redrawn prompt field. Cancellation and binding failure
do not close SSH.

- [ ] **Step 4: Run handshake and shell parser regression tests**

```bash
uv run pytest tests/unit/test_shell_connect_handshake.py tests/integration/test_same_session_shell.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit same-session shell binding**

```bash
git add src/remote_sandbox/shell.py src/remote_sandbox/cli.py \
  tests/helpers/sync_harness.py tests/integration/conftest.py \
  tests/unit/test_shell_connect_handshake.py tests/integration/test_same_session_shell.py
git commit -m "feat: keep connect in one ssh shell"
```

### Task 15: Add the Live Fixed-width Prompt and Readline-safe Redraw

**Files:**
- Create: `src/remote_sandbox/prompt.py`
- Create: `tests/unit/test_prompt_renderer.py`
- Create: `tests/unit/test_prompt_redraw.py`
- Create: `tests/integration/test_prompt_preserves_input.py`
- Modify: `tests/helpers/sync_harness.py`
- Modify: `tests/integration/conftest.py`
- Modify: `src/remote_sandbox/shell.py`

**Interfaces:**
- Consumes: `WorkspaceStatus`
- Produces: `PromptRenderer(width: int)`
- Produces: `PromptRedrawController(max_hz: float = 4.0)`
- Produces: `render_status_slot(target, name, status) -> str`
- Produces: `request_redraw(now: float, at_prompt: bool, command_running: bool) -> bytes | None`
- Test fixture: `shell_fixture -> PromptShellHarness`

- [ ] **Step 1: Write failing fixed-width and throttling tests**

```python
# tests/unit/test_prompt_renderer.py
from remote_sandbox.prompt import PromptRenderer
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def test_all_live_status_slots_have_equal_display_width() -> None:
    renderer = PromptRenderer(width=34)
    states = [
        WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("scanning")),
        WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("planning")),
        WorkspaceStatus(
            WorkspacePhase.INITIAL_SYNCING,
            SyncProgress("transferring", files_done=40, files_total=100),
        ),
        WorkspaceStatus(WorkspacePhase.DISCONNECTED, SyncProgress("offline")),
    ]
    rendered = [renderer.render("ZJU_2", "dq", status) for status in states]
    assert {len(value) for value in rendered} == {34}
    assert "planning" in rendered[1]
    assert "sync 40%" in rendered[2]
```

```python
# tests/unit/test_prompt_redraw.py
from remote_sandbox.prompt import PromptRedrawController


def test_redraw_is_live_while_typing_but_throttled() -> None:
    controller = PromptRedrawController(max_hz=4.0)
    assert controller.request_redraw(0.00, at_prompt=True, command_running=False) is not None
    assert controller.request_redraw(0.10, at_prompt=True, command_running=False) is None
    assert controller.request_redraw(0.26, at_prompt=True, command_running=False) is not None
    assert controller.request_redraw(0.60, at_prompt=False, command_running=True) is None
```

```python
# tests/integration/test_prompt_preserves_input.py
from tests.helpers.sync_harness import PromptShellHarness


def test_progress_redraw_preserves_partial_readline_buffer(
    shell_fixture: PromptShellHarness,
) -> None:
    shell_fixture.type_without_enter("python tra")
    shell_fixture.publish_progress(32)
    shell_fixture.publish_progress(40)
    assert shell_fixture.visible_input == "python tra"
    assert shell_fixture.cursor_offset == len("python tra")
    assert "sync 40%" in shell_fixture.current_prompt
```

Append a prompt-aware shell harness to `tests/helpers/sync_harness.py`:

```python
@dataclass(slots=True)
class PromptShellHarness:
    session: ManagedShellSession

    def type_without_enter(self, text: str) -> None:
        self.session.feed_user_input(text.encode("utf-8"))

    def publish_progress(self, percent: int) -> None:
        self.session.publish_status(
            WorkspaceStatus(
                WorkspacePhase.INITIAL_SYNCING,
                SyncProgress("transferring", files_done=percent, files_total=100),
            ),
            now=percent / 100.0,
        )

    @property
    def visible_input(self) -> str:
        return self.session.readline_buffer

    @property
    def cursor_offset(self) -> int:
        return self.session.readline_cursor

    @property
    def current_prompt(self) -> str:
        return self.session.rendered_prompt
```

`make_prompt_shell_harness()` creates a managed test session already positioned at a Bash prompt
with target `ZJU_2`, name `dq`, and a fake Readline endpoint that applies the private redraw binding.
Append the fixture to `tests/integration/conftest.py`:

```python
@pytest.fixture
def shell_fixture() -> PromptShellHarness:
    return make_prompt_shell_harness()
```

- [ ] **Step 2: Run tests and observe missing renderer/redraw behavior**

```bash
uv run pytest tests/unit/test_prompt_renderer.py tests/unit/test_prompt_redraw.py \
  tests/integration/test_prompt_preserves_input.py -v
```

Expected: FAIL because prompt rendering is static.

- [ ] **Step 3: Implement status rendering and private Readline redraw**

```python
class PromptRenderer:
    def __init__(self, width: int = 34) -> None:
        self.width = width

    def render(self, target: str, name: str, status: WorkspaceStatus) -> str:
        suffix = _status_suffix(status)
        value = f"[codex:{target}:{name}{suffix}]"
        if len(value) > self.width:
            value = value[: self.width - 2] + "]"
        return value.ljust(self.width)


def _status_suffix(status: WorkspaceStatus) -> str:
    if status.conflicts:
        return f" conflict {status.conflicts}"
    if status.phase is WorkspacePhase.DISCONNECTED:
        return " offline"
    if status.progress.stage == "scanning":
        return " scanning"
    if status.progress.stage == "planning":
        return " planning"
    if status.progress.files_total:
        percent = int(status.progress.files_done * 100 / status.progress.files_total)
        return f" sync {percent}%"
    return ""


class PromptRedrawController:
    REDRAW_SEQUENCE = b"\x1b[777~"

    def __init__(self, max_hz: float = 4.0) -> None:
        self._interval = 1.0 / max_hz
        self._last = float("-inf")

    def request_redraw(self, now: float, *, at_prompt: bool, command_running: bool) -> bytes | None:
        if not at_prompt or command_running or now - self._last < self._interval:
            return None
        self._last = now
        return self.REDRAW_SEQUENCE
```

The injected Bash rc binds the private escape sequence to `redraw-current-line` in the active
Readline keymaps. The prompt contains a sentinel with the same display width. The local parser
replaces only that sentinel. PTY state markers distinguish prompt input from a foreground command.
On transition to ready, the current line may remain padded; the next normal prompt switches to the
compact `[codex:target:name]` form.

- [ ] **Step 4: Run prompt and same-session shell tests**

```bash
uv run pytest tests/unit/test_prompt_renderer.py tests/unit/test_prompt_redraw.py \
  tests/integration/test_prompt_preserves_input.py tests/integration/test_same_session_shell.py -v
uv run ruff check src tests
uv run mypy src
```

Expected: PASS.

- [ ] **Step 5: Commit dynamic prompt rendering**

```bash
git add src/remote_sandbox/prompt.py src/remote_sandbox/shell.py \
  tests/helpers/sync_harness.py tests/integration/conftest.py \
  tests/unit/test_prompt_renderer.py tests/unit/test_prompt_redraw.py \
  tests/integration/test_prompt_preserves_input.py
git commit -m "feat: render live sync prompt"
```

### Task 16: Wire CLI Commands, Conflict Resolution, Run Semantics, and Double-ended Forget

**Files:**
- Create: `tests/unit/test_cli_commands.py`
- Create: `tests/integration/test_conflict_resolution.py`
- Create: `tests/integration/test_forget_cleanup.py`
- Create: `tests/integration/test_run_exit_status.py`
- Modify: `src/remote_sandbox/cli.py:1-870`
- Modify: `src/remote_sandbox/fetch.py:1-199`
- Modify: `src/remote_sandbox/peek.py:1-75`
- Modify: `src/remote_sandbox/registry.py`
- Modify: `src/remote_sandbox/policy.py`

**Interfaces:**
- Consumes: supervisor client, workspace registry, workspace store, remote client
- Produces: `CliServices`
- Produces: `CapturedCliResult(exit_code: int, stdout: str, stderr: str)`
- Produces: `invoke_cli(argv: list[str], *, services: CliServices) -> CapturedCliResult`
- Produces CLI commands: `init`, `status --watch`, `conflicts`, `resolve`, `forget --local-only`
- Preserves CLI commands: `list`, `set placeholder-limit`, `enter`, `connect`, `reconnect`,
  `start`, `stop`, `shell`, `run`, `fetch`, `peek`

- [ ] **Step 1: Write failing parser and command semantic tests**

```python
# tests/unit/test_cli_commands.py
from remote_sandbox.cli import build_parser


def test_parser_exposes_confirmed_commands_and_debug_flag() -> None:
    parser = build_parser()
    assert parser.parse_args(["--debug", "status", "dq", "--watch"]).debug is True
    assert parser.parse_args(["conflicts", "dq"]).command == "conflicts"
    resolved = parser.parse_args(["resolve", "model.py", "--use-local"])
    assert resolved.use_local is True
    forgotten = parser.parse_args(["forget", "dq", "--local-only"])
    assert forgotten.local_only is True
    no_shell = parser.parse_args(
        ["connect", "host", "--remote", "/work/dq", "--name", "dq", "--no-shell"]
    )
    assert no_shell.no_shell is True
```

```python
# tests/integration/test_run_exit_status.py
from remote_sandbox.status import WorkspacePhase
from tests.helpers.sync_harness import CliHarness


def test_remote_exit_code_wins_when_sync_followup_fails(cli_fixture: CliHarness) -> None:
    cli_fixture.remote_command_result(returncode=7)
    cli_fixture.followup_sync_fails("network down")
    result = cli_fixture.run(["run", "dq", "--", "false"])
    assert result.exit_code == 7
    assert "sync" in result.stderr
    assert "Traceback" not in result.stderr


def test_status_explains_foreground_reconnect_for_password_auth(cli_fixture: CliHarness) -> None:
    cli_fixture.set_workspace_state("dq", "disconnected", error="authentication required")
    result = cli_fixture.run(["status", "dq"])
    assert result.exit_code == 0
    assert "disconnected" in result.stdout
    assert "codex-rsb reconnect dq" in result.stdout


def test_init_writes_only_user_ignore_configuration(cli_fixture: CliHarness) -> None:
    result = cli_fixture.run(["init"])
    assert result.exit_code == 0
    content = (cli_fixture.pair.local / ".rsbignore").read_text(encoding="utf-8")
    assert ".venv/" in content
    assert "__pycache__/" in content
    assert "Git metadata is always local-only" in content
    assert not (cli_fixture.pair.local / ".remote-sandbox").exists()


def test_fetch_replaces_valid_placeholder_without_using_in_tree_metadata(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.create_remote_placeholder("weights.bin", b"remote-weights")
    result = cli_fixture.run(["fetch", "weights.bin"])
    assert result.exit_code == 0
    assert cli_fixture.local_bytes("weights.bin") == b"remote-weights"
    assert not (cli_fixture.pair.local / ".remote-sandbox").exists()


def test_no_shell_connect_returns_after_supervisor_start_while_sync_continues(
    cli_fixture: CliHarness,
) -> None:
    cli_fixture.block_initial_sync()
    result = cli_fixture.run(
        ["connect", "host", "--remote", "/work/dq", "--name", "dq", "--no-shell"]
    )
    assert result.exit_code == 0
    assert "Connected dq" in result.stdout
    assert "initial sync continues in background" in result.stdout
    assert "codex-rsb status dq --watch" in result.stdout
    assert cli_fixture.store.get_status().phase is WorkspacePhase.INITIAL_SYNCING
```

```python
# tests/integration/test_conflict_resolution.py
from tests.helpers.sync_harness import CliHarness


def test_use_local_resolution_transfers_selected_version_and_closes_conflict(
    cli_fixture: CliHarness,
) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )
    result = cli_fixture.run(["resolve", "model.py", "--use-local"])
    assert result.exit_code == 0
    assert cli_fixture.remote_bytes("model.py") == b"local\n"
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None


def test_use_remote_resolution_transfers_selected_version_and_closes_conflict(
    cli_fixture: CliHarness,
) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )
    result = cli_fixture.run(["resolve", "model.py", "--use-remote"])
    assert result.exit_code == 0
    assert cli_fixture.local_bytes("model.py") == b"remote\n"
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None
```

```python
# tests/integration/test_forget_cleanup.py
from tests.helpers.sync_harness import CliHarness


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
```

Extend `tests/helpers/sync_harness.py` with `CliHarness`. It must construct command service
dependencies directly instead of spawning the installed executable:

```python
@dataclass(slots=True)
class CliHarness:
    pair: SyncPair
    store: WorkspaceStore
    registry: Path
    services: CliServices

    def run(self, argv: list[str]) -> CapturedCliResult:
        return invoke_cli(argv, services=self.services)

    def create_conflict(
        self,
        *,
        path: str,
        base: bytes,
        local: bytes,
        remote: bytes,
    ) -> ConflictRecord:
        self.pair.seed_file(path, base)
        (self.pair.local / path).write_bytes(local)
        (self.pair.remote / path).write_bytes(remote)
        return self.store.create_conflict(
            path=path,
            reason="both-modified",
            local_blob=local,
            remote_blob=remote,
        )

    def local_bytes(self, path: str) -> bytes:
        return (self.pair.local / path).read_bytes()

    def remote_bytes(self, path: str) -> bytes:
        return (self.pair.remote / path).read_bytes()
```

The same harness provides `remote_command_result`, `followup_sync_fails`, `registry_has`, and
`remote_forget_fails` by configuring `CliServices` fakes. `set_workspace_state()` persists the
requested `WorkspaceStatus` in the harness store. `create_remote_placeholder()` writes remote
content, writes matching local placeholder bytes through `encode_placeholder()`, and stores the
placeholder base fingerprint. `block_initial_sync()` configures the supervisor fake to publish
`initial-syncing` and then wait on a test event after the CLI has returned. Add this fixture to
`tests/integration/conftest.py`:

```python
from tests.helpers.sync_harness import CliHarness, make_cli_harness


@pytest.fixture
def cli_fixture(tmp_path: Path) -> Iterator[CliHarness]:
    harness = make_cli_harness(tmp_path)
    yield harness
    harness.store.close()
    harness.pair.remote_client.close()
```

- [ ] **Step 2: Run tests and observe missing commands or old marker lookups**

```bash
uv run pytest tests/unit/test_cli_commands.py tests/integration/test_conflict_resolution.py \
  tests/integration/test_forget_cleanup.py tests/integration/test_run_exit_status.py -v
```

Expected: FAIL because commands and external workspace resolution are incomplete.

- [ ] **Step 3: Implement command delegation and concise error handling**

Add top-level `--debug`. Build parsers for:

```python
status.add_argument("name", nargs="?")
status.add_argument("--watch", action="store_true")
forget.add_argument("--local-only", action="store_true")
resolve.add_argument("path")
winner = resolve.add_mutually_exclusive_group(required=True)
winner.add_argument("--use-local", action="store_true")
winner.add_argument("--use-remote", action="store_true")
```

Move command bodies to small service functions that accept registry, store, remote client, and
supervisor dependencies. `status --watch` redraws one table. `init` writes the confirmed defaults.
`conflicts` lists unresolved records. `resolve` verifies the selected source fingerprint, transfers
it, updates base, marks the conflict resolved, and requeues watcher echoes.

`run` starts or resumes the supervisor, executes the remote command, prints output, asks the daemon
for a fresh sync, and returns the command result regardless of sync outcome. `fetch` and `peek` use
the current registry record and external state instead of `read_local_marker`. They import
`decode_placeholder` from `placeholder.py`, never from the legacy `scan.py`.
`fetch` delegates replacement and base update to the supervisor's serialized engine transaction,
so neither command imports the legacy workspace `lock.py`.

Preserve `connect --no-shell`, but change it to return immediately after the supervisor publishes
`initial-syncing`. It prints `Connected <name>`, says that initial sync continues in the background,
and names `codex-rsb status <name> --watch`; it never waits for the initial transfer.

Normal `forget` follows local-supervisor-stop, remote-watcher-stop, remote-workspace-delete,
unused-remote-agent-prune, local-workspace-delete, registry-delete order. Each completed step is
idempotent so a retry resumes cleanup safely. `--local-only` skips remote calls and prints the
exact remote workspace metadata path.

Wrap `main()` errors in one-line messages. Use `traceback.print_exc()` only when `args.debug` is true.

- [ ] **Step 4: Run CLI, conflict, cleanup, and run tests**

```bash
uv run pytest tests/unit/test_cli_commands.py tests/integration/test_conflict_resolution.py \
  tests/integration/test_forget_cleanup.py tests/integration/test_run_exit_status.py -v
uv run codex-rsb --help | head -n 2
uv run ruff check src tests
uv run mypy src
```

Expected: tests and static checks PASS, and help begins with `usage: codex-rsb`.

- [ ] **Step 5: Commit the complete CLI service surface**

```bash
git add src/remote_sandbox/cli.py src/remote_sandbox/fetch.py src/remote_sandbox/peek.py \
  src/remote_sandbox/registry.py src/remote_sandbox/policy.py \
  tests/helpers/sync_harness.py tests/integration/conftest.py \
  tests/unit/test_cli_commands.py tests/integration/test_conflict_resolution.py \
  tests/integration/test_forget_cleanup.py tests/integration/test_run_exit_status.py
git commit -m "feat: expose codex workspace commands"
```

### Task 17: Add SSH E2E, Security, Performance, CI, Documentation, and Retire Legacy Sync Modules

**Files:**
- Create: `tests/e2e/Dockerfile`
- Create: `tests/e2e/sshd_config`
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/test_full_workflow.py`
- Create: `tests/e2e/test_disconnect_recovery.py`
- Create: `tests/e2e/test_prompt_pty.py`
- Create: `tests/unit/test_path_security.py`
- Create: `tests/unit/test_terminal_marker_security.py`
- Create: `tests/performance/conftest.py`
- Create: `tests/performance/test_sync_performance.py`
- Create: `.github/workflows/test.yml`
- Modify: `README.md`
- Modify: `pyproject.toml`
- Delete after caller verification: `src/remote_sandbox/marker.py`
- Delete after caller verification: `src/remote_sandbox/lock.py`
- Delete after caller verification: `src/remote_sandbox/scan.py`
- Delete after caller verification: `src/remote_sandbox/sync.py`
- Delete after caller verification: `src/remote_sandbox/syncsession.py`

**Interfaces:**
- Consumes: all production interfaces from Tasks 1-16
- Produces: automated release gates and final development documentation

- [ ] **Step 1: Write failing path and terminal-marker security tests**

```python
# tests/unit/test_path_security.py
import pytest

from remote_sandbox.manifest import normalize_relative_path


@pytest.mark.parametrize("path", ["../escape", "/absolute", "a/../../escape", "line\nbreak"])
def test_unsafe_relative_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        normalize_relative_path(path)
```

```python
# tests/unit/test_terminal_marker_security.py
from remote_sandbox.shell import ShellOutputParser


def test_connect_marker_with_wrong_nonce_is_printed_as_plain_output() -> None:
    parser = ShellOutputParser("expected")
    data = b"\x1b]777;codex-remote-sandbox;connect-request;forged;b64:e30=\x07"
    events = parser.feed(data)
    assert len(events) == 1
    assert getattr(events[0], "data") == data
```

- [ ] **Step 2: Create the Docker SSH fixture and failing full-workflow E2E test**

Use Ubuntu 22.04 so the remote-agent gate actually runs on Python 3.10:

```dockerfile
# tests/e2e/Dockerfile
FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openssh-server python3 rsync tar ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /run/sshd /home/test/.ssh && \
    useradd --create-home --shell /bin/bash test && \
    echo 'test:test-password' | chpasswd && \
    chown -R test:test /home/test && \
    chmod 700 /home/test/.ssh

COPY sshd_config /etc/ssh/sshd_config

EXPOSE 2222
CMD ["/usr/sbin/sshd", "-D", "-e", "-f", "/etc/ssh/sshd_config"]
```

```text
# tests/e2e/sshd_config
Port 2222
ListenAddress 0.0.0.0
PasswordAuthentication yes
PubkeyAuthentication yes
PermitRootLogin no
UsePAM no
AllowUsers test
PidFile /run/sshd.pid
Subsystem sftp internal-sftp
```

`tests/e2e/conftest.py` owns the container lifecycle. Define the fixture contract exactly:

```python
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(slots=True)
class SshFixture:
    container_id: str
    host: str
    port: int
    key_file: Path
    state_home: Path


@pytest.fixture
def ssh_fixture(tmp_path: Path) -> Iterator[SshFixture]:
    fixture = start_ssh_fixture(tmp_path)
    try:
        yield fixture
    finally:
        fixture.close()
```

Implement these exact methods on `SshFixture`: `local_workspace() -> Path`,
`remote_workspace(empty: bool) -> Path`, `enter(password: bool = False) -> PtyShell`,
`cli(*argv: str) -> CompletedProcess[str]`, `disconnect_network() -> None`,
`reconnect_network() -> None`, `wait_for_remote_file(path, timeout=5.0) -> None`,
`wait_for_local_file(path, timeout=5.0) -> None`, `remote_exists(path) -> bool`, and `close() -> None`.
Every subprocess call uses an argument list and `check=` explicitly. `start_ssh_fixture()` generates
a temporary Ed25519 client key, builds the Dockerfile, publishes container port 2222 to a random
loopback port, installs the public key into `/home/test/.ssh/authorized_keys`, waits for
`python3 --version` to report 3.10, and sets `CODEX_REMOTE_SANDBOX_HOME` and
`CODEX_REMOTE_SANDBOX_RUNTIME_DIR` below `tmp_path`. It never reads the user's production SSH keys
or `~/.remote-sandbox` state.

```python
# tests/e2e/test_full_workflow.py
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
    assert not (local / ".remote-sandbox").exists()
    assert not ssh_fixture.remote_exists(remote / ".remote-sandbox")
    assert not ssh_fixture.remote_exists(remote / ".git")
    assert ssh_fixture.cli("forget", "dq").returncode == 0
    assert production_sentinel.read_bytes() == b"do-not-touch"
    assert ssh_fixture.read_remote(remote_production_sentinel) == b"remote"
```

Run once and confirm it fails before implementing fixture gaps:

```bash
uv run pytest tests/e2e/test_full_workflow.py -m e2e -v
```

- [ ] **Step 3: Complete E2E disconnect, password reconnect, and prompt tests**

Add these concrete disconnect tests:

```python
# tests/e2e/test_disconnect_recovery.py
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
def test_password_connection_requires_foreground_reconnect_and_replays_queue(ssh_fixture) -> None:
    local, remote = ssh_fixture.bound_pair(name="password-recovery", password=True)
    ssh_fixture.expire_control_master("password-recovery")
    ssh_fixture.write_remote(remote / "queued.txt", b"queued")
    ssh_fixture.wait_for_state("password-recovery", "disconnected", timeout=10.0)
    reconnect = ssh_fixture.cli_with_password(
        "reconnect",
        "password-recovery",
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
    result = ssh_fixture.cli("forget", "local-only", "--local-only")
    assert result.returncode == 0
    assert str(remote_metadata) in result.stdout
    assert ssh_fixture.remote_exists(remote_metadata)
    assert not ssh_fixture.local_binding_exists("local-only")
```

Add these PTY tests:

```python
# tests/e2e/test_prompt_pty.py
import time

import pytest


@pytest.mark.e2e
def test_same_shell_and_live_prompt_preserve_partial_input(ssh_fixture) -> None:
    local = ssh_fixture.local_workspace()
    remote = ssh_fixture.remote_workspace(empty=True)
    ssh_fixture.populate_local(local, files=2_000)
    shell = ssh_fixture.enter()
    original_pid = shell.remote_shell_pid()
    started = time.monotonic()
    shell.connect(remote=remote, local=local, name="prompt")
    shell.type_without_enter("python tra")
    before = shell.cursor_offset()
    shell.wait_for_prompt_text("sync ", timeout=5.0)
    assert shell.first_sync_status_at - started < 1.0
    shell.wait_for_prompt_change(timeout=5.0)
    assert shell.remote_shell_pid() == original_pid
    assert shell.visible_input() == "python tra"
    assert shell.cursor_offset() == before


@pytest.mark.e2e
def test_foreground_program_receives_no_prompt_redraw_bytes(ssh_fixture) -> None:
    shell = ssh_fixture.bound_shell(name="foreground")
    shell.run_foreground_probe(seconds=2.0)
    shell.trigger_remote_change("during-command.txt", b"x")
    assert shell.foreground_probe_received_private_redraw() is False
    shell.wait_for_prompt_text("[codex:", timeout=5.0)


@pytest.mark.e2e
def test_cancelled_binding_keeps_browsing_shell_open(ssh_fixture) -> None:
    shell = ssh_fixture.enter()
    original_pid = shell.remote_shell_pid()
    shell.begin_connect(name="cancelled")
    shell.reject_binding()
    assert shell.remote_shell_pid() == original_pid
    assert shell.is_open()
```

Extend `SshFixture` and `PtyShell` only with the exact helper methods used in these tests. Normal
forget and `--local-only` cleanup remain asserted in `test_full_workflow.py` and
`test_disconnect_recovery.py`; inspect both local and remote metadata paths as well as registry
contents.

Run:

```bash
uv run pytest tests/e2e -m e2e -v --timeout=120
```

Expected: PASS.

- [ ] **Step 4: Add measurable performance gates**

```python
# tests/performance/test_sync_performance.py
import time

import pytest


@pytest.mark.performance
def test_small_remote_delete_reaches_local_within_two_seconds(performance_pair) -> None:
    path = performance_pair.remote / "delete-me.txt"
    path.write_text("x", encoding="utf-8")
    performance_pair.wait_until_synced(path)
    started = time.monotonic()
    path.unlink()
    performance_pair.wait_until_missing(performance_pair.local / "delete-me.txt")
    assert time.monotonic() - started < 2.0


@pytest.mark.performance
def test_noop_cycle_hashes_no_unchanged_files(performance_pair) -> None:
    performance_pair.populate(5_000)
    performance_pair.initial_sync()
    performance_pair.hash_counter.reset()
    performance_pair.engine.run_once("noop")
    assert performance_pair.hash_counter.count == 0


@pytest.mark.performance
def test_initial_batch_is_close_to_direct_rsync_and_uses_one_session(performance_pair) -> None:
    performance_pair.populate(5_000)
    direct_seconds = performance_pair.measure_direct_rsync()
    codex_seconds = performance_pair.measure_initial_sync()
    assert codex_seconds <= max(direct_seconds * 1.5, direct_seconds + 1.0)
    assert performance_pair.transport.ssh_process_count == 1
```

Create the fixture in `tests/performance/conftest.py` so the benchmark does not depend on E2E SSH:

```python
from pathlib import Path

import pytest

from tests.helpers.sync_harness import PerformancePair, make_performance_pair


@pytest.fixture
def performance_pair(tmp_path: Path) -> PerformancePair:
    return make_performance_pair(tmp_path)
```

`PerformancePair` extends `SyncPair` with a counting hash provider and deadline-based
`wait_until_synced`, `wait_until_missing`, `populate`, and `initial_sync` methods. The wait methods
poll observable filesystem state at 10 ms intervals and raise an assertion containing the last
workspace status when their deadline expires. `populate(5_000)` writes deterministic 128-byte
files so CI results are comparable.

Add a benchmark that compares initial transfer throughput with direct rsync and fails if the
batched implementation falls back to one SSH process per file. Keep extended benchmarks opt-in;
run the three 5,000-file smoke benchmarks above in CI.

- [ ] **Step 5: Add CI and coverage gates**

`.github/workflows/test.yml` must run on macOS and Linux for Python 3.11, 3.12, and 3.13. Linux CI
also builds the SSH E2E container. Commands:

```bash
uv sync --all-groups
uv run ruff check src tests
uv run mypy src
uv run pytest tests/unit tests/integration --cov=remote_sandbox --cov-report=term-missing --cov-fail-under=85
uv run pytest tests/e2e -m e2e -v --timeout=120
```

Expected: every job PASS.

- [ ] **Step 6: Remove legacy modules only after proving no callers remain**

Run:

```bash
rg -n "remote_sandbox\.(marker|lock|scan|sync|syncsession)|from remote_sandbox import (marker|lock|scan|sync|syncsession)" src tests
```

Expected: no production or test imports. Then delete the five legacy modules and run the full suite.

- [ ] **Step 7: Update documentation and run final local verification**

README must document only `codex-rsb` for the development build, both external metadata layouts,
same-session connect, live prompt states, default ignores, conflicts, reconnect, status watch,
double-ended forget, and the eventual rename-back procedure. It must state that `.git` is not
synchronized.

Run:

```bash
uv run ruff check src tests
uv run mypy src
uv run pytest tests/unit tests/integration -v \
  --cov=remote_sandbox --cov-report=term-missing --cov-fail-under=85
uv run pytest tests/e2e -m e2e -v --timeout=120
git diff --check
```

Expected: all verification commands PASS and no workspace contains control metadata.

- [ ] **Step 8: Perform manual ZJU_2 acceptance after automated gates pass**

Use only the development command and disposable directories:

```bash
uv run codex-rsb enter ZJU_2
uv run codex-rsb status dq --watch
uv run codex-rsb reconnect dq
uv run codex-rsb forget dq
```

Verify:

- The existing `rsb` executable and its state remain untouched.
- `codex-rsb connect` keeps the same SSH shell.
- The prompt updates dynamically while text is partially entered.
- Initial progress appears within one second.
- A local edit and remote delete each reach the other side within the target latency.
- Network interruption preserves queued local and remote events.
- Both local and remote project directories remain free of control metadata.
- Forget removes both development metadata trees without deleting project files.

- [ ] **Step 9: Commit E2E gates, cleanup, and documentation**

```bash
git add .github/workflows/test.yml README.md pyproject.toml uv.lock \
  tests/e2e/Dockerfile tests/e2e/sshd_config tests/e2e/conftest.py \
  tests/e2e/test_full_workflow.py tests/e2e/test_disconnect_recovery.py \
  tests/e2e/test_prompt_pty.py tests/unit/test_path_security.py \
  tests/unit/test_terminal_marker_security.py tests/performance/conftest.py \
  tests/performance/test_sync_performance.py tests/helpers/sync_harness.py
git add -u src/remote_sandbox/marker.py src/remote_sandbox/lock.py \
  src/remote_sandbox/scan.py src/remote_sandbox/sync.py \
  src/remote_sandbox/syncsession.py
git commit -m "test: verify codex remote sandbox workflow"
```

## Final Verification Checklist

- [ ] `git status --short` contains no unexpected generated files.
- [ ] Existing pre-redesign user behavior in authentication classification has regression coverage.
- [ ] `uv run ruff check src tests` passes.
- [ ] `uv run mypy src` passes.
- [ ] Unit and integration coverage is at least 85 percent.
- [ ] Docker SSH E2E passes for key and password authentication.
- [ ] Performance smoke tests meet the one-second progress and two-second small-change targets.
- [ ] No normal no-op cycle hashes unchanged file contents.
- [ ] No in-tree `.remote-sandbox` control directory is created.
- [ ] `codex-rsb run` preserves the remote command exit code.
- [ ] Conflicts preserve both versions and remain visible until resolved.
- [ ] `codex-rsb forget` cleans both sides, while `--local-only` is explicit.
- [ ] Production `rsb` state, processes, sockets, and installation are untouched.
