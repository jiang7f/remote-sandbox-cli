from __future__ import annotations

import posixpath
from dataclasses import dataclass

from remote_sandbox.marker import METADATA_DIR
from remote_sandbox.ssh import SshRunner

AGENT_VERSION = "0.2.0"
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


def cache_path() -> Path:
    return Path(__file__).resolve().parent / "hashcache.json"


def should_ignore(path: str) -> bool:
    return path == ".remote-sandbox" or path.startswith(".remote-sandbox/")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_cache() -> dict:
    try:
        with cache_path().open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return {{}}
    return data if isinstance(data, dict) else {{}}


def save_cache(cache: dict) -> None:
    tmp = cache_path().with_name("hashcache.json.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(cache, handle, separators=(",", ":"))
        tmp.replace(cache_path())
    except OSError:
        pass


def manifest() -> dict:
    root = workspace_root()
    cache = load_cache()
    new_cache = {{}}
    entries = []
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
            size = stat.st_size
            mtime_ns = stat.st_mtime_ns
            cached = cache.get(rel)
            # Trust the cached hash when size + mtime are unchanged, so a manifest scan does
            # not re-read the whole tree on every sync (the remote-side analogue of git's
            # index). A changed file falls through to a fresh hash.
            if (
                isinstance(cached, list)
                and len(cached) == 3
                and cached[0] == size
                and cached[1] == mtime_ns
            ):
                digest = cached[2]
            else:
                digest = sha256_file(path)
            new_cache[rel] = [size, mtime_ns, digest]
            entries.append({{
                "kind": "file",
                "path": rel,
                "size": size,
                "mtime": stat.st_mtime,
                "hash": digest,
                "is_placeholder": False,
            }})
    save_cache(new_cache)
    return {{"entries": entries}}


def main(argv: list) -> int:
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
