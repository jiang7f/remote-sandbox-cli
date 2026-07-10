from __future__ import annotations

import posixpath
from dataclasses import dataclass

from remote_sandbox.marker import remote_meta_dir
from remote_sandbox.ssh import SshRunner

AGENT_VERSION = "0.4.0"
AGENT_FILE = "agent.py"

AGENT_SOURCE = f'''# remote-sandbox remote agent
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
import sys

VERSION = "{AGENT_VERSION}"


def resolve_root(value: str) -> Path:
    if value == "~" or value.startswith("~/"):
        return Path(value).expanduser()
    return Path(value)


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


def manifest(root: Path) -> dict:
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


def snapshot(root: Path) -> dict:
    """Cheap stat-only signature of every tracked file/dir: path -> (mtime_ns, size).

    No hashing — this is the fast poll used by watch mode to detect adds/edits/deletes.
    """
    snap = {{}}
    for path in root.rglob("*"):
        rel = path.relative_to(root).as_posix()
        if should_ignore(rel):
            continue
        try:
            if path.is_symlink():
                snap[rel] = (0, -1)
            elif path.is_dir():
                snap[rel] = (0, -2)
            elif path.is_file():
                st = path.stat()
                snap[rel] = (st.st_mtime_ns, st.st_size)
        except OSError:
            continue
    return snap


def watch(root: Path, interval: float) -> int:
    """Emit one line per detected change until stdin closes or we are killed.

    Pure stdlib, stat-only polling (no hashing) so it is cheap even on a big tree. Each
    changed/added/removed path is printed as a single line and flushed immediately; the
    local daemon reads these and syncs just what moved, instead of scanning the whole remote
    tree on a timer. The exact line content is only a hint — the daemon reconciles by
    manifest — so we simply print the path.
    """
    prev = snapshot(root)
    # Announce readiness so the local side knows the watcher is live.
    print("__rsb_watch_ready__", flush=True)
    while True:
        time.sleep(interval)
        cur = snapshot(root)
        if cur != prev:
            changed = set(cur) ^ set(prev)
            changed |= {{k for k in (set(cur) & set(prev)) if cur[k] != prev[k]}}
            for rel in sorted(changed):
                print(rel, flush=True)
            prev = cur


def _extract_root(argv: list) -> tuple[str, list]:
    root = "."
    rest = []
    i = 0
    while i < len(argv):
        if argv[i] == "--root" and i + 1 < len(argv):
            root = argv[i + 1]
            i += 2
        else:
            rest.append(argv[i])
            i += 1
    return root, rest


def main(argv: list) -> int:
    root_arg, rest = _extract_root(argv)
    root = resolve_root(root_arg)
    if rest == ["self-check"]:
        print("remote-sandbox-agent " + VERSION)
        return 0
    if rest == ["manifest"]:
        print(json.dumps(manifest(root), separators=(",", ":")))
        return 0
    if rest and rest[0] == "watch":
        interval = 1.0
        if len(rest) == 3 and rest[1] == "--interval":
            try:
                interval = max(0.2, float(rest[2]))
            except ValueError:
                interval = 1.0
        try:
            return watch(root, interval)
        except KeyboardInterrupt:
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
    # Out-of-tree home dir for this workspace's agent + hashcache + marker.
    return remote_meta_dir(remote_root)


def remote_agent_path(remote_root: str) -> str:
    return posixpath.join(remote_agent_dir(remote_root), AGENT_FILE)


def bootstrap_agent(runner: SshRunner, target: str, remote_root: str) -> AgentBootstrapResult:
    agent_dir = remote_agent_dir(remote_root)
    agent_path = remote_agent_path(remote_root)
    runner.mkdir_p(target, agent_dir)
    runner.write_text_atomic(target, agent_path, AGENT_SOURCE)
    # Pass --root so the agent, which no longer lives under the workspace tree, knows which
    # directory to scan.
    output = runner.run_python_file(target, agent_path, ("--root", remote_root, "self-check"))
    expected = f"remote-sandbox-agent {AGENT_VERSION}"
    if output.strip() != expected:
        raise RuntimeError(f"Remote agent self-check failed: {output.strip()}")
    return AgentBootstrapResult(path=agent_path, version=AGENT_VERSION)
