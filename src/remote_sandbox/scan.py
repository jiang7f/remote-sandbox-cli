from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from remote_sandbox.agent import remote_agent_path
from remote_sandbox.manifest import EntryKind, FileEntry, normalize_relative_path
from remote_sandbox.policy import PolicyEngine
from remote_sandbox.ssh import SshRunner

PLACEHOLDER_HEADER = b"REMOTE-SANDBOX PLACEHOLDER\n"


def scan_local_manifest(
    root: Path,
    policy: PolicyEngine,
    *,
    hash_cache: dict[str, tuple[int, int, str]] | None = None,
) -> dict[str, FileEntry]:
    """Build the local manifest, reusing cached hashes for unchanged files.

    When ``hash_cache`` (path -> (size, mtime_ns, hash)) is supplied it is used and updated
    in place: a file whose ``(size, mtime_ns)`` matches its cache entry is NOT re-read, and
    entries for vanished paths are pruned. This is what makes a no-op sync near-instant
    instead of re-hashing the whole tree every cycle.
    """
    root = root.expanduser().resolve()
    entries: dict[str, FileEntry] = {}
    seen: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath)
        rel_dir = current_dir.relative_to(root).as_posix()
        if rel_dir == ".":
            rel_dir = ""
        kept_dirs: list[str] = []
        for dirname in sorted(dirnames):
            path = current_dir / dirname
            rel_path = f"{rel_dir}/{dirname}" if rel_dir else dirname
            if policy.is_ignored(rel_path):
                continue
            if path.is_symlink():
                entries[rel_path] = FileEntry(
                    kind=EntryKind.UNSUPPORTED,
                    path=rel_path,
                    size=None,
                    mtime=None,
                    hash=None,
                )
                continue
            kept_dirs.append(dirname)
            stat = path.stat()
            entries[rel_path] = FileEntry(
                kind=EntryKind.DIR,
                path=rel_path,
                size=None,
                mtime=stat.st_mtime,
                hash=None,
            )
        dirnames[:] = kept_dirs
        for filename in sorted(filenames):
            path = current_dir / filename
            rel_path = f"{rel_dir}/{filename}" if rel_dir else filename
            if policy.is_ignored(rel_path):
                continue
            if path.is_symlink():
                entries[rel_path] = FileEntry(
                    kind=EntryKind.UNSUPPORTED,
                    path=rel_path,
                    size=None,
                    mtime=None,
                    hash=None,
                )
                continue
            stat = path.stat()
            if path.is_file():
                try:
                    placeholder = read_placeholder_entry(
                        path,
                        expected_path=rel_path,
                        raise_on_path_mismatch=True,
                        raise_on_invalid_placeholder=True,
                    )
                except ValueError:
                    entries[rel_path] = FileEntry(
                        kind=EntryKind.UNSUPPORTED,
                        path=rel_path,
                        size=None,
                        mtime=None,
                        hash=None,
                    )
                    continue
                if placeholder is not None:
                    entries[rel_path] = placeholder
                    continue
                digest = _cached_or_hash(path, stat, rel_path, hash_cache)
                seen.add(rel_path)
                entries[rel_path] = FileEntry(
                    kind=EntryKind.FILE,
                    path=rel_path,
                    size=stat.st_size,
                    mtime=stat.st_mtime,
                    hash=digest,
                )
    if hash_cache is not None:
        for stale in set(hash_cache) - seen:
            del hash_cache[stale]
    return entries


def _cached_or_hash(
    path: Path,
    stat: os.stat_result,
    rel_path: str,
    hash_cache: dict[str, tuple[int, int, str]] | None,
) -> str:
    size = stat.st_size
    mtime_ns = stat.st_mtime_ns
    if hash_cache is not None:
        cached = hash_cache.get(rel_path)
        if cached is not None and cached[0] == size and cached[1] == mtime_ns:
            return cached[2]
    digest = _sha256_file(path)
    if hash_cache is not None:
        hash_cache[rel_path] = (size, mtime_ns, digest)
    return digest


def scan_remote_manifest(
    runner: SshRunner,
    target: str,
    remote_root: str,
) -> dict[str, FileEntry]:
    output = runner.run_python_file(
        target, remote_agent_path(remote_root), ("--root", remote_root, "manifest")
    )
    raw = json.loads(output)
    if not isinstance(raw, dict):
        raise ValueError("Invalid remote manifest")
    entries = raw.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Invalid remote manifest")
    parsed = [_entry_from_json(item) for item in entries]
    return {entry.path: entry for entry in parsed}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_placeholder_entry(
    path: Path,
    *,
    expected_path: str | None = None,
    raise_on_path_mismatch: bool = False,
    raise_on_invalid_placeholder: bool = False,
) -> FileEntry | None:
    try:
        with path.open("rb") as handle:
            prefix = handle.read(4096)
            if not prefix.startswith(PLACEHOLDER_HEADER):
                return None
            rest = handle.read(64 * 1024)
    except OSError:
        return None
    raw_content = prefix + rest
    try:
        content = raw_content.decode("utf-8")
    except UnicodeDecodeError:
        return _invalid_placeholder(raise_on_invalid_placeholder, "invalid encoding")
    lines = content.splitlines()
    if not lines or lines[0] != "REMOTE-SANDBOX PLACEHOLDER":
        return _invalid_placeholder(raise_on_invalid_placeholder, "invalid header")
    values: dict[str, str] = {}
    for line in lines[1:]:
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        values[key] = value
    rel_path = values.get("path")
    size_raw = values.get("bytes")
    mtime_raw = values.get("mtime")
    hash_value = values.get("hash")
    if rel_path is None or size_raw is None or hash_value is None:
        return _invalid_placeholder(raise_on_invalid_placeholder, "missing metadata")
    try:
        normalized_rel_path = normalize_relative_path(rel_path)
    except ValueError:
        return _invalid_placeholder(raise_on_invalid_placeholder, "invalid path")
    if expected_path is not None:
        normalized_expected = normalize_relative_path(expected_path)
        if normalized_rel_path != normalized_expected:
            if raise_on_path_mismatch:
                raise ValueError(
                    "placeholder path mismatch: "
                    f"file is {normalized_expected}, metadata says {normalized_rel_path}"
                )
            return None
    try:
        size = int(size_raw)
    except ValueError:
        return _invalid_placeholder(raise_on_invalid_placeholder, "invalid size")
    mtime = _parse_placeholder_mtime(mtime_raw)
    return FileEntry(
        kind=EntryKind.FILE,
        path=normalized_rel_path,
        size=size,
        mtime=mtime,
        hash=hash_value,
        is_placeholder=True,
    )


def _invalid_placeholder(should_raise: bool, reason: str) -> FileEntry | None:
    if should_raise:
        raise ValueError(f"placeholder metadata is invalid: {reason}")
    return None


def _parse_placeholder_mtime(value: str | None) -> float | None:
    if value is None or value == "unknown":
        return None
    from datetime import datetime

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _entry_from_json(item: Any) -> FileEntry:
    if not isinstance(item, dict):
        raise ValueError("Invalid remote manifest entry")
    kind = EntryKind(str(item["kind"]))
    size = item.get("size")
    if size is not None and not isinstance(size, int):
        raise ValueError("Invalid remote manifest entry size")
    mtime = item.get("mtime")
    if mtime is not None:
        mtime = float(mtime)
    hash_value = item.get("hash")
    if hash_value is not None and not isinstance(hash_value, str):
        raise ValueError("Invalid remote manifest entry hash")
    path = item.get("path")
    if not isinstance(path, str):
        raise ValueError("Invalid remote manifest entry path")
    return FileEntry(
        kind=kind,
        path=normalize_relative_path(path),
        size=size,
        mtime=mtime,
        hash=hash_value,
        is_placeholder=bool(item.get("is_placeholder", False)),
    )
