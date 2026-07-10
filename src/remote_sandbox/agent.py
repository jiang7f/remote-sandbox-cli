from __future__ import annotations

import hashlib
import os
import posixpath
import shutil
import tempfile
import zipapp
from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.marker import METADATA_DIR
from remote_sandbox.namespace import DEV_NAMESPACE
from remote_sandbox.remote_agent import AGENT_VERSION
from remote_sandbox.ssh import SshRunner

LEGACY_AGENT_VERSION = "0.1.0"
AGENT_FILE = "agent.py"

_ARCHIVE_MTIME = 315532800
_ARCHIVE_MAIN = """from remote_agent.__main__ import main
import sys

raise SystemExit(main(sys.argv[1:]))
"""

AGENT_SOURCE = f'''# remote-sandbox remote agent
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

VERSION = "{LEGACY_AGENT_VERSION}"


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def should_ignore(path: str) -> bool:
    return path == ".remote-sandbox" or path.startswith(".remote-sandbox/")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest() -> dict[str, object]:
    root = workspace_root()
    entries: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if should_ignore(rel):
            continue
        if path.is_symlink():
            entries.append({{
                "kind": "unsupported",
                "path": rel,
                "size": None,
                "mtime": None,
                "hash": None,
                "is_placeholder": False,
            }})
            continue
        stat = path.stat()
        if path.is_dir():
            entries.append({{
                "kind": "dir",
                "path": rel,
                "size": None,
                "mtime": stat.st_mtime,
                "hash": None,
                "is_placeholder": False,
            }})
        elif path.is_file():
            entries.append({{
                "kind": "file",
                "path": rel,
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "hash": sha256_file(path),
                "is_placeholder": False,
            }})
    return {{"entries": entries}}


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        print("remote-sandbox-agent " + VERSION)
        return 0
    if argv == ["manifest"]:
        print(json.dumps(manifest(), separators=(",", ":")))
        return 0
    print("unknown command", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
'''


@dataclass(frozen=True, slots=True)
class AgentBootstrapResult:
    path: str
    version: str


@dataclass(frozen=True, slots=True)
class AgentInstall:
    version: str
    remote_path: str
    sha256: str


def build_agent_zipapp(destination: Path) -> Path:
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    package_source = Path(__file__).with_name("remote_agent")

    with tempfile.TemporaryDirectory(prefix="codex-remote-agent-") as temporary:
        staging = Path(temporary) / "staging"
        package_destination = staging / "remote_agent"
        shutil.copytree(
            package_source,
            package_destination,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        (staging / "__main__.py").write_text(_ARCHIVE_MAIN, encoding="utf-8")
        for path in sorted(staging.rglob("*")):
            path.chmod(0o755 if path.is_dir() else 0o644)
            os.utime(path, (_ARCHIVE_MTIME, _ARCHIVE_MTIME))

        temporary_archive = Path(temporary) / "agent.pyz"
        zipapp.create_archive(
            staging,
            target=temporary_archive,
            interpreter="/usr/bin/env python3",
            compressed=True,
        )
        os.replace(temporary_archive, destination)
    return destination


class RemoteAgentManager:
    def __init__(self, runner: SshRunner) -> None:
        self._runner = runner

    def ensure(self, target: str) -> AgentInstall:
        with tempfile.TemporaryDirectory(prefix="codex-remote-agent-build-") as temporary:
            archive = build_agent_zipapp(Path(temporary) / "agent.pyz")
            content = archive.read_bytes()

        digest = hashlib.sha256(content).hexdigest()
        remote_path = posixpath.join(
            "~",
            DEV_NAMESPACE.home_dirname,
            "agents",
            AGENT_VERSION,
            "agent.pyz",
        )
        self._runner.write_bytes_atomic(target, remote_path, content)
        output = self._runner.run_python_file(target, remote_path, ("self-check",))
        expected = f"codex-remote-sandbox-agent {AGENT_VERSION} {digest}"
        if output.strip() != expected:
            raise RuntimeError(
                f"Remote agent self-check failed: expected {expected!r}, got {output.strip()!r}"
            )
        return AgentInstall(version=AGENT_VERSION, remote_path=remote_path, sha256=digest)


def remote_agent_dir(remote_root: str) -> str:
    return posixpath.join(remote_root.rstrip("/") or "/", METADATA_DIR, "agent")


def remote_agent_path(remote_root: str) -> str:
    return posixpath.join(remote_agent_dir(remote_root), AGENT_FILE)


def bootstrap_agent(runner: SshRunner, target: str, remote_root: str) -> AgentBootstrapResult:
    agent_dir = remote_agent_dir(remote_root)
    agent_path = remote_agent_path(remote_root)
    runner.mkdir_p(target, agent_dir)
    runner.write_text_atomic(target, agent_path, AGENT_SOURCE)
    output = runner.run_python_file(target, agent_path, ("self-check",))
    expected = f"remote-sandbox-agent {LEGACY_AGENT_VERSION}"
    if output.strip() != expected:
        raise RuntimeError(f"Remote agent self-check failed: {output.strip()}")
    return AgentBootstrapResult(path=agent_path, version=LEGACY_AGENT_VERSION)
