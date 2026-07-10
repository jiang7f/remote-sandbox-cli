from __future__ import annotations

import contextlib
import hashlib
import json
import os
import posixpath
import secrets
import shlex
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from remote_sandbox.marker import (
    METADATA_DIR,
    WORKSPACE_FILE,
    WorkspaceMarker,
    marker_from_toml,
    marker_to_toml,
    remote_meta_dir,
)


class SshError(RuntimeError):
    pass


class SshRunner(Protocol):
    def exists(self, target: str, path: str) -> bool: ...

    def is_dir(self, target: str, path: str) -> bool: ...

    def is_symlink(self, target: str, path: str) -> bool: ...

    def listdir(self, target: str, path: str) -> list[str]: ...

    def mkdir_p(self, target: str, path: str) -> None: ...

    def read_text(self, target: str, path: str) -> str: ...

    def write_text_atomic(self, target: str, path: str, content: str) -> None: ...

    def read_bytes(self, target: str, path: str) -> bytes: ...

    def read_head(self, target: str, path: str, lines: int) -> bytes: ...

    def read_tail(self, target: str, path: str, lines: int) -> bytes: ...

    def write_bytes_atomic(self, target: str, path: str, content: bytes) -> None: ...

    def delete_path(self, target: str, path: str) -> None: ...

    def remove_metadata_tree(self, target: str, path: str) -> None: ...

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str: ...

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult: ...

    def clear_master(self, target: str) -> None: ...

    def push_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None: ...

    def pull_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None: ...

    def interactive_shell(
        self,
        target: str,
        cwd: str,
        on_barrier: Callable[[int], None] | None = None,
    ) -> int: ...


def validate_target(target: str) -> str:
    if not target or target.startswith("-") or _has_control_char(target):
        raise ValueError("Invalid SSH target")
    return target


def validate_remote_path(path: str) -> str:
    if not path or _has_control_char(path):
        raise ValueError("Invalid remote path")
    normalized_input = path.replace("\\", "/")
    parts = normalized_input.split("/")
    if ".." in parts:
        raise ValueError("Invalid remote path")
    if normalized_input == "~":
        return "~"
    if normalized_input.startswith("~/"):
        suffix = posixpath.normpath(normalized_input.removeprefix("~/"))
        if suffix in {"", "."} or suffix.startswith("../") or suffix == "..":
            raise ValueError("Invalid remote path")
        return f"~/{suffix}"
    if normalized_input.startswith("/"):
        if normalized_input.startswith("//"):
            raise ValueError("Invalid remote path")
        normalized = posixpath.normpath(normalized_input)
        if normalized != "/" and normalized.startswith("//"):
            raise ValueError("Invalid remote path")
        if not normalized.startswith("/"):
            raise ValueError("Invalid remote path")
        return normalized
    raise ValueError("Invalid remote path")


def remote_marker_path(remote_root: str) -> str:
    # Out-of-tree home dir for this workspace (mirrors the agent location).
    return posixpath.join(remote_meta_dir(remote_root), WORKSPACE_FILE)


def legacy_remote_marker_path(remote_root: str) -> str:
    base = remote_root.rstrip("/") or "/"
    return posixpath.join(base, METADATA_DIR, WORKSPACE_FILE)


def _control_dir() -> str:
    """Directory holding SSH ControlMaster sockets (kept short for the sun_path limit)."""
    base = os.environ.get("REMOTE_SANDBOX_CONTROL_DIR") or f"/tmp/remote-sandbox-{os.getuid()}/cm"
    os.makedirs(base, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(base, 0o700)
    return base


def ssh_control_opts() -> list[str]:
    """SSH options that share one authenticated master connection per target.

    The first connection authenticates (a password is typed once); ``ControlPersist``
    keeps the master socket alive so every later connection — including the background
    daemon — reuses it without prompting. ``%C`` derives a unique socket per target, so
    the same option list works for every call site.
    """
    return [
        "-o",
        "ControlMaster=auto",
        "-o",
        f"ControlPath={_control_dir()}/%C",
        "-o",
        "ControlPersist=10m",
    ]


def build_remote_shell_command(target: str, cwd: str) -> list[str]:
    validate_target(target)
    validate_remote_path(cwd)
    script = (
        "p=$1\n"
        'case "$p" in\n'
        '  "~") p=$HOME ;;\n'
        '  "~/"*) p=$HOME/${p#"~/"} ;;\n'
        "esac\n"
        'cd -- "$p" || exit\n'
        'exec "${SHELL:-/bin/sh}" -i\n'
    )
    remote_command = f"sh -c {shlex.quote(script)} sh {shlex.quote(cwd)}"
    return ["ssh", *ssh_control_opts(), "-tt", target, remote_command]


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass
class FakeSshRunner:
    files: dict[tuple[str, str], str] = field(default_factory=dict)
    binary_files: dict[tuple[str, str], bytes] = field(default_factory=dict)
    dirs: set[tuple[str, str]] = field(default_factory=set)
    symlinks: set[tuple[str, str]] = field(default_factory=set)
    shell_calls: list[tuple[str, str]] = field(default_factory=list)
    shell_barrier_callbacks: list[Callable[[int], None] | None] = field(default_factory=list)
    python_file_calls: list[tuple[str, str, tuple[str, ...]]] = field(default_factory=list)
    command_calls: list[tuple[str, str, tuple[str, ...]]] = field(default_factory=list)
    command_result: CommandResult = field(default_factory=lambda: CommandResult(0, "", ""))
    fail_on: set[tuple[str, str]] = field(default_factory=set)
    fail_operations: set[str] = field(default_factory=set)

    def exists(self, target: str, path: str) -> bool:
        normalized = _normalize_remote_path(path)
        return (
            (target, normalized) in self.files
            or (target, normalized) in self.binary_files
            or (target, normalized) in self.dirs
        )

    def is_dir(self, target: str, path: str) -> bool:
        return (target, _normalize_remote_path(path)) in self.dirs

    def is_symlink(self, target: str, path: str) -> bool:
        return (target, _normalize_remote_path(path)) in self.symlinks

    def listdir(self, target: str, path: str) -> list[str]:
        root = _normalize_remote_path(path)
        root_prefix = root.rstrip("/") + "/"
        names: set[str] = set()
        for item_target, item_path in [*self.files.keys(), *self.binary_files.keys(), *self.dirs]:
            if item_target != target or item_path == root:
                continue
            if item_path.startswith(root_prefix):
                suffix = item_path[len(root_prefix) :]
                first = suffix.split("/", 1)[0]
                if first:
                    names.add(first)
        return sorted(names)

    def mkdir_p(self, target: str, path: str) -> None:
        self._maybe_fail("mkdir_p", path)
        current = ""
        for part in _normalize_remote_path(path).strip("/").split("/"):
            current = f"{current}/{part}" if current else f"/{part}"
            self.dirs.add((target, current))

    def read_text(self, target: str, path: str) -> str:
        self._maybe_fail("read_text", path)
        normalized = _normalize_remote_path(path)
        if (target, normalized) in self.binary_files:
            return self.binary_files[(target, normalized)].decode("utf-8")
        try:
            return self.files[(target, normalized)]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc

    def write_text_atomic(self, target: str, path: str, content: str) -> None:
        self._maybe_fail("write_text_atomic", path)
        normalized = _normalize_remote_path(path)
        parent = posixpath.dirname(normalized)
        self.mkdir_p(target, parent)
        self.files[(target, normalized)] = content
        self.binary_files.pop((target, normalized), None)

    def read_bytes(self, target: str, path: str) -> bytes:
        self._maybe_fail("read_bytes", path)
        normalized = _normalize_remote_path(path)
        if (target, normalized) in self.binary_files:
            return self.binary_files[(target, normalized)]
        try:
            return self.files[(target, normalized)].encode("utf-8")
        except KeyError as exc:
            raise FileNotFoundError(path) from exc

    def read_head(self, target: str, path: str, lines: int) -> bytes:
        content = self.read_bytes(target, path)
        return b"".join(content.splitlines(keepends=True)[:lines])

    def read_tail(self, target: str, path: str, lines: int) -> bytes:
        content = self.read_bytes(target, path)
        return b"".join(content.splitlines(keepends=True)[-lines:])

    def write_bytes_atomic(self, target: str, path: str, content: bytes) -> None:
        self._maybe_fail("write_bytes_atomic", path)
        normalized = _normalize_remote_path(path)
        parent = posixpath.dirname(normalized)
        self.mkdir_p(target, parent)
        self.binary_files[(target, normalized)] = content
        try:
            self.files[(target, normalized)] = content.decode("utf-8")
        except UnicodeDecodeError:
            self.files.pop((target, normalized), None)

    def delete_path(self, target: str, path: str) -> None:
        self._maybe_fail("delete_path", path)
        normalized = _normalize_remote_path(path)
        if (target, normalized) in self.dirs:
            prefix = normalized.rstrip("/") + "/"
            has_child = any(
                item_target == target and item_path.startswith(prefix)
                for item_target, item_path in [
                    *self.files.keys(),
                    *self.binary_files.keys(),
                    *self.dirs,
                ]
            )
            if has_child:
                raise SshError(f"remote directory not empty: {path}")
            self.dirs.discard((target, normalized))
            return
        self.files.pop((target, normalized), None)
        self.binary_files.pop((target, normalized), None)

    def remove_metadata_tree(self, target: str, path: str) -> None:
        _require_metadata_tree(path)
        normalized = _normalize_remote_path(path)
        prefix = normalized.rstrip("/") + "/"
        for key in [*self.files.keys(), *self.binary_files.keys()]:
            t, p = key
            if t == target and (p == normalized or p.startswith(prefix)):
                self.files.pop(key, None)
                self.binary_files.pop(key, None)
        self.dirs = {
            (t, p)
            for (t, p) in self.dirs
            if not (t == target and (p == normalized or p.startswith(prefix)))
        }

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str:
        self._maybe_fail("run_python_file", path)
        normalized = _normalize_remote_path(path)
        if (target, normalized) not in self.files:
            raise FileNotFoundError(path)
        self.python_file_calls.append((target, normalized, args))
        root, rest = _fake_extract_root(args)
        if rest == ("self-check",):
            return f"{_fake_agent_selfcheck(self.files.get((target, normalized), ''))}\n"
        if rest == ("manifest",):
            return self._manifest_json(target, _normalize_remote_path(root))
        return "ok\n"

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult:
        self._maybe_fail("run_command", cwd)
        self.command_calls.append((target, _normalize_remote_path(cwd), argv))
        return self.command_result

    def clear_master(self, target: str) -> None:
        del target

    def push_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None:
        from pathlib import Path

        base = Path(local_root).expanduser()
        for rel in paths:
            data = (base / rel).read_bytes()
            remote_path = posixpath.join(remote_root.rstrip("/") or "/", rel)
            self.write_bytes_atomic(target, remote_path, data)

    def pull_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None:
        from pathlib import Path

        base = Path(local_root).expanduser()
        for rel in paths:
            data = self.read_bytes(target, posixpath.join(remote_root.rstrip("/") or "/", rel))
            dest = base / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)

    def interactive_shell(
        self,
        target: str,
        cwd: str,
        on_barrier: Callable[[int], None] | None = None,
    ) -> int:
        self.shell_calls.append((target, cwd))
        self.shell_barrier_callbacks.append(on_barrier)
        return 0

    def read_marker(self, target: str, remote_root: str) -> WorkspaceMarker:
        return marker_from_toml(self.read_text(target, remote_marker_path(remote_root)))

    def write_marker(
        self,
        target: str,
        remote_root: str,
        marker: WorkspaceMarker,
    ) -> None:
        self.write_text_atomic(target, remote_marker_path(remote_root), marker_to_toml(marker))

    def remove_marker(self, target: str, remote_root: str) -> None:
        self.files.pop((target, _normalize_remote_path(remote_marker_path(remote_root))), None)

    def _maybe_fail(self, operation: str, path: str) -> None:
        key = (operation, _normalize_remote_path(path))
        if operation in self.fail_operations or key in self.fail_on:
            raise SshError(f"Injected failure for {operation} {path}")

    def _manifest_json(self, target: str, root: str) -> str:
        root_prefix = root.rstrip("/") + "/"
        paths: set[str] = set()
        entries: list[dict[str, object]] = []
        for item_target, item_path in sorted(self.dirs):
            if item_target != target or item_path == root or not item_path.startswith(root_prefix):
                continue
            rel = item_path[len(root_prefix) :]
            if _fake_manifest_ignored(rel):
                continue
            paths.add(rel)
            entries.append(
                {
                    "kind": "dir",
                    "path": rel,
                    "size": None,
                    "mtime": None,
                    "hash": None,
                    "is_placeholder": False,
                }
            )
        merged_files: dict[tuple[str, str], bytes] = {}
        for key, content in self.files.items():
            merged_files[key] = content.encode("utf-8")
        merged_files.update(self.binary_files)
        for (item_target, item_path), file_content in sorted(merged_files.items()):
            if item_target != target or not item_path.startswith(root_prefix):
                continue
            rel = item_path[len(root_prefix) :]
            if _fake_manifest_ignored(rel):
                continue
            parent = posixpath.dirname(rel)
            while parent and parent not in paths:
                paths.add(parent)
                entries.append(
                    {
                        "kind": "dir",
                        "path": parent,
                        "size": None,
                        "mtime": None,
                        "hash": None,
                        "is_placeholder": False,
                    }
                )
                parent = posixpath.dirname(parent)
            entries.append(
                {
                    "kind": "file",
                    "path": rel,
                    "size": len(file_content),
                    "mtime": None,
                    "hash": hashlib.sha256(file_content).hexdigest(),
                    "is_placeholder": False,
                }
            )
        entries.sort(key=lambda item: str(item["path"]))
        return json.dumps({"entries": entries}, separators=(",", ":")) + "\n"


class SubprocessSshRunner:
    timeout_s = 30.0

    def ensure_master(self, target: str) -> None:
        """Ensure a shared SSH master connection exists, authenticating once if needed.

        Reuses a live master when present; otherwise opens one interactively (inheriting
        this process's TTY so a password can be typed a single time). Batch calls and the
        background daemon then multiplex over it without further prompts.
        """
        validate_target(target)
        check = subprocess.run(
            ["ssh", *ssh_control_opts(), "-O", "check", target],
            check=False,
            capture_output=True,
            text=True,
            timeout=self.timeout_s,
        )
        if check.returncode == 0:
            return
        established = subprocess.run(
            ["ssh", *ssh_control_opts(), "-o", "ConnectTimeout=10", target, "true"],
            check=False,
        )
        if established.returncode != 0:
            raise SshError(
                f"could not open an SSH connection to {target}. "
                "If this host needs a password, run the command from an interactive "
                "terminal; otherwise configure an SSH key."
            )

    def clear_master(self, target: str) -> None:
        """Best-effort drop of a dead/stale ControlMaster so the next call re-dials."""
        with contextlib.suppress(Exception):
            subprocess.run(
                ["ssh", *ssh_control_opts(), "-O", "exit", target],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )

    # Batch transfer time budget scales with volume; a big initial sync can legitimately
    # run for many minutes, unlike the fixed 30 s used for small metadata ops.
    batch_timeout_s = 3600.0

    def push_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None:
        """Upload many files in ONE ssh connection via a tar stream.

        Replaces N per-file round trips (each its own ssh + `cat`) with a single
        `tar -cf - -T - | ssh ... tar -xf -`, the reason bulk/initial sync now approaches
        git/rsync speed. tar is universally present, so this keeps the ssh+python3-only
        promise. Paths are workspace-relative and fed NUL-delimited so odd names are safe.
        """
        if not paths:
            return
        validate_target(target)
        validate_remote_path(remote_root)
        local_base = os.path.abspath(os.path.expanduser(local_root))
        name_blob = b"".join(p.encode("utf-8") + b"\0" for p in paths)
        # The caller passes only regular-file paths (dirs are created separately), so no
        # --no-recursion is needed. Flags are kept to the portable GNU/bsd-tar intersection
        # so the LOCAL tar works whether it is GNU tar (Linux) or bsdtar (macOS).
        remote_script = self._remote_path_function() + (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'mkdir -p -- "$p"\n'
            'tar -C "$p" -xf -\n'
        )
        remote_cmd = " ".join(
            ["sh", "-c", shlex.quote(remote_script), "sh", shlex.quote(remote_root)]
        )
        tar = subprocess.Popen(
            ["tar", "-C", local_base, "--null", "-T", "-", "-cf", "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_tar_env(),
        )
        ssh = subprocess.Popen(
            self._ssh_exec(target, remote_cmd),
            stdin=tar.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._drive_tar_pipeline(tar, ssh, name_blob, direction="push")

    def pull_files(
        self, target: str, local_root: str, remote_root: str, paths: list[str]
    ) -> None:
        """Download many files in ONE ssh connection via a tar stream (see push_files)."""
        if not paths:
            return
        validate_target(target)
        validate_remote_path(remote_root)
        local_base = os.path.abspath(os.path.expanduser(local_root))
        os.makedirs(local_base, exist_ok=True)
        name_blob = b"".join(p.encode("utf-8") + b"\0" for p in paths)
        remote_script = self._remote_path_function() + (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'cd -- "$p" || exit 2\n'
            "tar --null -T - -cf -\n"
        )
        remote_cmd = " ".join(
            ["sh", "-c", shlex.quote(remote_script), "sh", shlex.quote(remote_root)]
        )
        ssh = subprocess.Popen(
            self._ssh_exec(target, remote_cmd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        tar = subprocess.Popen(
            ["tar", "-C", local_base, "-xf", "-"],
            stdin=ssh.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_tar_env(),
        )
        self._drive_tar_pipeline(ssh, tar, name_blob, direction="pull")

    def _drive_tar_pipeline(
        self,
        producer: subprocess.Popen[bytes],
        consumer: subprocess.Popen[bytes],
        name_blob: bytes,
        *,
        direction: str,
    ) -> None:
        # The consumer reads directly from the producer's stdout (wired at Popen time); close
        # our copy so EOF propagates. The producer takes the NUL-delimited file list on its
        # stdin (local tar for push, remote ssh for pull). stderr is drained in threads to
        # avoid a full-pipe deadlock on a chatty error.
        if producer.stdout is not None:
            producer.stdout.close()
        errs: dict[str, bytes] = {}

        def _drain(name: str, proc: subprocess.Popen[bytes]) -> None:
            if proc.stderr is not None:
                errs[name] = proc.stderr.read()

        threads = [
            threading.Thread(target=_drain, args=("producer", producer)),
            threading.Thread(target=_drain, args=("consumer", consumer)),
        ]
        for thread in threads:
            thread.start()
        if producer.stdin is not None:
            with contextlib.suppress(BrokenPipeError):
                producer.stdin.write(name_blob)
                producer.stdin.close()
        producer.wait(timeout=self.batch_timeout_s)
        consumer.wait(timeout=self.batch_timeout_s)
        for thread in threads:
            thread.join(timeout=5.0)
        if producer.returncode not in (0, None) or consumer.returncode not in (0, None):
            detail = (
                (errs.get("consumer") or errs.get("producer") or b"")
                .decode("utf-8", "replace")
                .strip()
            )
            raise SshError(f"batch {direction} failed: {detail or 'tar/ssh error'}")

    def exists(self, target: str, path: str) -> bool:
        result = self._run_test(target, path, "-e")
        return result.returncode == 0

    def is_dir(self, target: str, path: str) -> bool:
        result = self._run_test(target, path, "-d")
        return result.returncode == 0

    def is_symlink(self, target: str, path: str) -> bool:
        result = self._run_test(target, path, "-L")
        return result.returncode == 0

    def listdir(self, target: str, path: str) -> list[str]:
        script = self._listdir_script(use_find=True)
        result = self._run_script(target, script, [path], capture=True)
        if result.returncode != 0:
            raise SshError(result.stderr.strip() or "remote listdir failed")
        return [line for line in result.stdout.splitlines() if line]

    def _listdir_script(self, *, use_find: bool) -> str:
        find_part = (
            'find "$p" -mindepth 1 -maxdepth 1 -printf "%f\\n" 2>/dev/null || '
            if use_find
            else ""
        )
        fallback = (
            '(cd "$p" && for f in .* *; do '
            'case "$f" in "."|"..") continue ;; esac; '
            '[ -e "$f" ] || continue; '
            'printf "%s\\n" "$f"; '
            "done)\n"
        )
        return (
            "p=$(remote_sandbox_path \"$1\") || exit 2\n"
            'if [ ! -d "$p" ]; then exit 0; fi\n'
            f"{find_part}{fallback}"
        )

    def mkdir_p(self, target: str, path: str) -> None:
        script = 'p=$(remote_sandbox_path "$1") || exit 2\nmkdir -p -- "$p"\n'
        self._check(self._run_script(target, script, [path], capture=True), "remote mkdir failed")

    def read_text(self, target: str, path: str) -> str:
        script = 'p=$(remote_sandbox_path "$1") || exit 2\ncat -- "$p"\n'
        result = self._run_script(target, script, [path], capture=True)
        if result.returncode != 0:
            raise FileNotFoundError(path)
        return result.stdout

    def write_text_atomic(self, target: str, path: str, content: str) -> None:
        script = (
            "set -e\n"
            "umask 077\n"
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'dir=$(dirname -- "$p")\n'
            'mkdir -p -- "$dir"\n'
            'tmp=$(mktemp "$dir/.tmp.XXXXXX.remote-sandbox") || exit 2\n'
            'trap \'rm -f -- "$tmp"\' EXIT HUP INT TERM\n'
            'cat > "$tmp"\n'
            'mv -f -- "$tmp" "$p"\n'
            "trap - EXIT HUP INT TERM\n"
        )
        result = self._run_script(target, script, [path], input_text=content, capture=True)
        self._check(result, "remote write failed")

    def read_bytes(self, target: str, path: str) -> bytes:
        script = 'p=$(remote_sandbox_path "$1") || exit 2\ncat -- "$p"\n'
        result = self._run_script_bytes(target, script, [path], capture=True)
        if result.returncode != 0:
            raise FileNotFoundError(path)
        return result.stdout

    def read_head(self, target: str, path: str, lines: int) -> bytes:
        return self._read_lines(target, path, lines, tail=False)

    def read_tail(self, target: str, path: str, lines: int) -> bytes:
        return self._read_lines(target, path, lines, tail=True)

    def _read_lines(self, target: str, path: str, lines: int, *, tail: bool) -> bytes:
        if lines <= 0:
            raise ValueError("lines must be positive")
        tool = "tail" if tail else "head"
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            "n=$2\n"
            f'{tool} -n "$n" -- "$p"\n'
        )
        result = self._run_script_bytes(
            target,
            script,
            [path, str(lines)],
            capture=True,
            path_arg_count=1,
        )
        if result.returncode != 0:
            raise FileNotFoundError(path)
        return result.stdout

    def write_bytes_atomic(self, target: str, path: str, content: bytes) -> None:
        script = (
            "set -e\n"
            "umask 077\n"
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'dir=$(dirname -- "$p")\n'
            'mkdir -p -- "$dir"\n'
            'tmp=$(mktemp "$dir/.tmp.XXXXXX.remote-sandbox") || exit 2\n'
            'trap \'rm -f -- "$tmp"\' EXIT HUP INT TERM\n'
            'cat > "$tmp"\n'
            'mv -f -- "$tmp" "$p"\n'
            "trap - EXIT HUP INT TERM\n"
        )
        result = self._run_script_bytes(target, script, [path], input_bytes=content, capture=True)
        self._check_bytes(result, "remote write failed")

    def delete_path(self, target: str, path: str) -> None:
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'if [ -d "$p" ] && [ ! -L "$p" ]; then\n'
            '  rmdir -- "$p"\n'
            'else\n'
            '  rm -f -- "$p"\n'
            'fi\n'
        )
        result = self._run_script(target, script, [path], capture=True)
        self._check(result, "remote delete failed")

    def remove_metadata_tree(self, target: str, path: str) -> None:
        # Guarded recursive delete: refuse anything whose final component is not
        # `.remote-sandbox`, so a bug can never `rm -rf` a project directory. Also require the
        # directory to exist and not be a symlink.
        _require_metadata_tree(path)
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'case "$p" in\n'
            '  */.remote-sandbox|*/.remote-sandbox/*) ;;\n'
            '  *) echo "refusing to remove non-metadata path" >&2; exit 3 ;;\n'
            'esac\n'
            'if [ -L "$p" ]; then rm -f -- "$p"; exit 0; fi\n'
            'if [ -d "$p" ]; then rm -rf -- "$p"; fi\n'
        )
        result = self._run_script(target, script, [path], capture=True)
        self._check(result, "remote metadata cleanup failed")

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str:
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            "shift\n"
            'python3 "$p" "$@"\n'
        )
        result = self._run_script(
            target,
            script,
            [path, *args],
            capture=True,
            path_arg_count=1,
        )
        self._check(result, "remote python failed")
        return result.stdout

    def spawn_remote_watch(
        self, target: str, agent_path: str, remote_root: str, interval: float
    ) -> subprocess.Popen[str]:
        """Start the resident remote watcher and return the live process (text stdout).

        Runs `python3 <agent> --root <root> watch --interval N` over the shared master; each
        line on stdout is one changed path. The caller reads it in a thread and closes/kills
        the process to stop. Uses -T (no PTY) so EOF/kill cleanly ends the remote python.
        """
        validate_target(target)
        validate_remote_path(agent_path)
        validate_remote_path(remote_root)
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            'exec python3 -u "$p" --root "$2" watch --interval "$3"\n'
        )
        full_script = self._remote_path_function() + script
        remote_command = " ".join(
            [
                "sh",
                "-c",
                shlex.quote(full_script),
                "sh",
                shlex.quote(agent_path),
                shlex.quote(remote_root),
                shlex.quote(str(interval)),
            ]
        )
        return subprocess.Popen(
            [*self._ssh_batch_args(), "-T", target, remote_command],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult:
        if not argv:
            raise SshError("remote command is empty")
        script = (
            'p=$(remote_sandbox_path "$1") || exit 2\n'
            "shift\n"
            'cd -- "$p" || exit\n'
            '"$@"\n'
        )
        result = self._run_script(
            target,
            script,
            [cwd, *argv],
            capture=True,
            path_arg_count=1,
        )
        return CommandResult(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def interactive_shell(
        self,
        target: str,
        cwd: str,
        on_barrier: Callable[[int], None] | None = None,
    ) -> int:
        if not os.isatty(0) or not os.isatty(1):
            raise SshError(
                "interactive shell requires a TTY; use --no-shell in non-interactive runs"
            )
        from remote_sandbox.shell import managed_shell_loop

        return managed_shell_loop(
            target,
            cwd,
            nonce=secrets.token_hex(8),
            on_barrier=on_barrier or (lambda _status: None),
        )

    def _run_test(self, target: str, path: str, test_op: str) -> subprocess.CompletedProcess[str]:
        script = f'p=$(remote_sandbox_path "$1") || exit 2\n[ {test_op} "$p" ]\n'
        return self._run_script(target, script, [path], capture=True)

    def _run_script(
        self,
        target: str,
        script: str,
        args: list[str],
        *,
        input_text: str | None = None,
        capture: bool = False,
        path_arg_count: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        validate_target(target)
        checked_args = args if path_arg_count is None else args[:path_arg_count]
        for arg in checked_args:
            validate_remote_path(arg)
        full_script = self._remote_path_function() + script
        remote_command = " ".join(
            ["sh", "-c", shlex.quote(full_script), "sh", *(shlex.quote(arg) for arg in args)]
        )
        return subprocess.run(
            [*self._ssh_batch_args(), target, remote_command],
            check=False,
            text=True,
            input=input_text,
            capture_output=capture,
            timeout=self.timeout_s,
        )

    def _run_script_bytes(
        self,
        target: str,
        script: str,
        args: list[str],
        *,
        input_bytes: bytes | None = None,
        capture: bool = False,
        path_arg_count: int | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        validate_target(target)
        checked_args = args if path_arg_count is None else args[:path_arg_count]
        for arg in checked_args:
            validate_remote_path(arg)
        full_script = self._remote_path_function() + script
        remote_command = " ".join(
            ["sh", "-c", shlex.quote(full_script), "sh", *(shlex.quote(arg) for arg in args)]
        )
        return subprocess.run(
            [*self._ssh_batch_args(), target, remote_command],
            check=False,
            input=input_bytes,
            capture_output=capture,
            timeout=self.timeout_s,
        )

    @staticmethod
    def _check(result: subprocess.CompletedProcess[str], message: str) -> None:
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise SshError(f"{message}: {detail}" if detail else message)

    @staticmethod
    def _check_bytes(result: subprocess.CompletedProcess[bytes], message: str) -> None:
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
            raise SshError(f"{message}: {detail}" if detail else message)

    @staticmethod
    def _remote_path_function() -> str:
        return _REMOTE_PATH_FUNC

    def _ssh_exec(self, target: str, remote_cmd: str) -> list[str]:
        """Full argv to run one remote command over the shared master. Seam for tests."""
        return [*self._ssh_batch_args(), target, remote_cmd]

    @staticmethod
    def _ssh_batch_args() -> list[str]:
        return [
            "ssh",
            *ssh_control_opts(),
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
        ]


_REMOTE_PATH_FUNC = (
    "remote_sandbox_path() {\n"
    "  p=$1\n"
    '  case "$p" in\n'
    '    "") return 2 ;;\n'
    '    "~") printf "%s\\n" "$HOME" ;;\n'
    '    "~/"*) printf "%s/%s\\n" "$HOME" "${p#\\~/}" ;;\n'
    '    /*) printf "%s\\n" "$p" ;;\n'
    "    *) return 2 ;;\n"
    "  esac\n"
    "}\n"
)


def _normalize_remote_path(path: str) -> str:
    validated = validate_remote_path(path)
    if validated == "~":
        return "/home/fake"
    if validated.startswith("~/"):
        return posixpath.normpath(posixpath.join("/home/fake", validated[2:]))
    return posixpath.normpath(validated)


def _fake_extract_root(args: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
    """Mirror the real agent's --root parsing so the fake uses the same arg convention."""
    root = "."
    rest: list[str] = []
    i = 0
    items = list(args)
    while i < len(items):
        if items[i] == "--root" and i + 1 < len(items):
            root = items[i + 1]
            i += 2
        else:
            rest.append(items[i])
            i += 1
    return root, tuple(rest)


def _require_metadata_tree(path: str) -> None:
    """Fail unless `path` sits inside the `.remote-sandbox` namespace — a recursive-delete guard.

    The recursive remove is only ever meant for a workspace's own metadata: the legacy in-tree
    `<remote>/.remote-sandbox`, or the out-of-tree home dir `~/.remote-sandbox/workspaces/…`.
    Requiring `.remote-sandbox` to be one of the path segments — plus the remote-side shell
    check — makes an accidental `rm -rf` of a project directory impossible.
    """
    validate_remote_path(path)
    segments = [seg for seg in path.replace("\\", "/").split("/") if seg not in ("", ".", "~")]
    if METADATA_DIR not in segments:
        raise SshError(f"refusing to recursively remove non-metadata path: {path}")


def _tar_env() -> dict[str, str]:
    """Env for the local tar so macOS bsdtar does not emit AppleDouble `._*` sidecars.

    Without COPYFILE_DISABLE, bsdtar packs a `._name` companion (xattrs/resource fork) for
    every file; extracted on the Linux remote those become stray `._*` files that clutter
    the tree. GNU tar ignores the variable, so it is safe to set unconditionally.
    """
    return {**os.environ, "COPYFILE_DISABLE": "1"}


def _fake_agent_selfcheck(source: str) -> str:
    """Echo the agent's own VERSION so the fake never drifts from AGENT_VERSION."""
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("VERSION ="):
            version = stripped.split("=", 1)[1].strip().strip('"').strip("'")
            return f"remote-sandbox-agent {version}"
    return "remote-sandbox-agent unknown"


def _fake_manifest_ignored(path: str) -> bool:
    return path == ".remote-sandbox" or path.startswith(".remote-sandbox/")


def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)
