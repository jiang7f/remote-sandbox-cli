from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path

from remote_sandbox.shell import (
    build_enter_remote_shell_command,
    build_managed_remote_shell_command,
)
from remote_sandbox.ssh import build_remote_shell_command


def test_quality_job_has_explicit_non_skippable_release_checks() -> None:
    workflow = Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    expected = {
        "Check generated shell syntax": "test_generated_shell_syntax",
        "Check Python 3.10 packaged agent": "test_agent_zipapp_runs_on_python_310",
        "Check legacy imports": "test_no_legacy_module_imports",
        "Check in-tree metadata": "test_bind_writes_only_external_metadata",
        "Check generated artifacts": "test_no_tracked_generated_artifacts",
        "Check git diff": "git diff --check",
    }
    for name, command_fragment in expected.items():
        match = re.search(
            rf"(?ms)^      - name: {re.escape(name)}\n(?P<body>.*?)(?=^      - |^  [a-z])",
            workflow,
        )
        assert match is not None, name
        body = match.group("body")
        assert "if:" not in body
        assert command_fragment in body


def test_generated_shell_syntax() -> None:
    commands = (
        build_remote_shell_command("host", "/work/project"),
        build_managed_remote_shell_command("host", "/work/project", nonce="test-nonce"),
        build_enter_remote_shell_command("host", "/work/project", nonce="test-nonce"),
    )
    generated: list[tuple[str, str]] = []
    for index, argv in enumerate(commands):
        remote = shlex.split(argv[-1])
        assert remote[:2] == ["sh", "-c"]
        script = remote[2]
        generated.append((f"remote-{index}", script))
        marker = "    cat <<'EOF'\n"
        if marker in script:
            _prefix, remainder = script.split(marker, 1)
            rcfile, _suffix = remainder.split("EOF\n", 1)
            generated.append((f"rcfile-{index}", rcfile))
    failures: dict[str, str] = {}
    for name, script in generated:
        result = subprocess.run(
            ["bash", "-n"],
            input=script,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            failures[name] = result.stderr
    assert failures == {}


def test_no_legacy_module_imports() -> None:
    legacy = {"marker", "lock", "scan", "sync", "syncsession"}
    matches: list[str] = []
    patterns = (
        re.compile(r"\b(?:from|import)\s+remote_sandbox\.([a-z_][a-z0-9_]*)"),
        re.compile(r"\bfrom\s+remote_sandbox\s+import\s+([^\n]+)"),
    )
    for root in (Path("src"), Path("tests")):
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            for match in patterns[0].finditer(source):
                if match.group(1) in legacy:
                    matches.append(f"{path}:{match.group(0)}")
            for match in patterns[1].finditer(source):
                imported = {name.strip().split()[0] for name in match.group(1).split(",")}
                if imported & legacy:
                    matches.append(f"{path}:{match.group(0)}")
    assert matches == []


def test_no_tracked_generated_artifacts() -> None:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    tracked = [Path(raw.decode()) for raw in result.stdout.split(b"\0") if raw]
    generated_names = {
        ".coverage",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "build",
        "dist",
        "htmlcov",
    }
    generated = [
        path
        for path in tracked
        if any(part in generated_names for part in path.parts)
        or path.suffix in {".pyc", ".pyo"}
        or path.name.endswith(".egg-info")
    ]
    assert generated == []


def test_readme_advertises_only_codex_rsb_command() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    assert readme.startswith("# codex-remote-sandbox\n")
    assert "长命令是 `codex-remote-sandbox`" not in readme
    command_lines = [
        line.strip()
        for line in readme.splitlines()
        if line.strip().startswith("uv run")
    ]
    assert command_lines
    assert all(
        "uv run codex-rsb" in line or not line.startswith("uv run codex-")
        for line in command_lines
    )
