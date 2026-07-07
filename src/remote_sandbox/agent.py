from __future__ import annotations

import posixpath
from dataclasses import dataclass

from remote_sandbox.marker import METADATA_DIR
from remote_sandbox.ssh import SshRunner

AGENT_VERSION = "0.1.0"
AGENT_FILE = "agent.py"

AGENT_SOURCE = f'''# remote-sandbox remote agent
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

VERSION = "{AGENT_VERSION}"


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
    expected = f"remote-sandbox-agent {AGENT_VERSION}"
    if output.strip() != expected:
        raise RuntimeError(f"Remote agent self-check failed: {output.strip()}")
    return AgentBootstrapResult(path=agent_path, version=AGENT_VERSION)
