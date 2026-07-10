from __future__ import annotations

import contextlib
import hashlib
import json
import os
import posixpath
import secrets
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol

from remote_sandbox.marker import (
    METADATA_DIR,
    WORKSPACE_FILE,
    WorkspaceMarker,
    marker_from_toml,
    marker_to_toml,
)
from remote_sandbox.namespace import ssh_control_dir


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

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str: ...

    def run_python_file_bytes(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]: ...

    def stream_python_file(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.Popen[bytes]: ...

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult: ...

    def clear_master(self, target: str) -> None: ...

    def probe_connection(self, target: str) -> Literal["ok", "auth", "network"]: ...

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
    base = remote_root.rstrip("/") or "/"
    return posixpath.join(base, METADATA_DIR, WORKSPACE_FILE)


def _control_dir() -> str:
    """Directory holding SSH ControlMaster sockets (kept short for the sun_path limit)."""
    base = str(ssh_control_dir())
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
    probe_result: Literal["ok", "auth", "network"] = "ok"

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

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str:
        self._maybe_fail("run_python_file", path)
        normalized = _normalize_remote_path(path)
        if (target, normalized) not in self.files:
            raise FileNotFoundError(path)
        self.python_file_calls.append((target, normalized, args))
        if args == ("self-check",):
            return "remote-sandbox-agent 0.1.0\n"
        if args == ("manifest",):
            return self._manifest_json(target, normalized)
        return "ok\n"

    def run_command(self, target: str, cwd: str, argv: tuple[str, ...]) -> CommandResult:
        self._maybe_fail("run_command", cwd)
        self.command_calls.append((target, _normalize_remote_path(cwd), argv))
        return self.command_result

    def clear_master(self, target: str) -> None:
        del target

    def probe_connection(self, target: str) -> Literal["ok", "auth", "network"]:
        del target
        return self.probe_result

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

    def _manifest_json(self, target: str, agent_path: str) -> str:
        root = _workspace_root_from_agent_path(agent_path)
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

    def probe_connection(self, target: str) -> Literal["ok", "auth", "network"]:
        """Classify why the daemon can't reach the host, without ever prompting.

        Returns ``"ok"`` (reachable — a key host also re-establishes the master here),
        ``"auth"`` (reachable but needs a password/key the background process can't
        supply — the user must re-authenticate), or ``"network"`` (unreachable /
        transient — it will self-heal). Never raises.
        """
        try:
            result = subprocess.run(
                ["ssh", *ssh_control_opts(), "-o", "BatchMode=yes", "-o",
                 "ConnectTimeout=8", target, "true"],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
            )
        except Exception:
            return "network"
        if result.returncode == 0:
            return "ok"
        return _classify_ssh_failure(result.stderr)

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

    def run_python_file_bytes(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            self._python_file_command(target, path, args),
            check=False,
            input=input_data,
            capture_output=True,
            timeout=self.timeout_s,
        )

    def stream_python_file(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.Popen[bytes]:
        process = subprocess.Popen(
            self._python_file_command(target, path, args),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdin is None or process.stdout is None or process.stderr is None:
            process.terminate()
            raise SshError("remote streaming process did not create pipes")
        try:
            process.stdin.write(input_data)
            process.stdin.close()
        except BaseException:
            process.terminate()
            raise
        return process

    def _python_file_command(
        self,
        target: str,
        path: str,
        args: tuple[str, ...],
    ) -> list[str]:
        validate_target(target)
        validate_remote_path(path)
        if any(_has_control_char(arg) for arg in args):
            raise ValueError("Invalid remote argument")
        script = (
            self._remote_path_function()
            + 'p=$(remote_sandbox_path "$1") || exit 2\n'
            + "shift\n"
            + 'exec python3 "$p" "$@"\n'
        )
        remote_command = " ".join(
            ["sh", "-c", shlex.quote(script), "sh", shlex.quote(path)]
            + [shlex.quote(arg) for arg in args]
        )
        return [*self._ssh_batch_args(), target, remote_command]

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


def _workspace_root_from_agent_path(agent_path: str) -> str:
    suffix = "/.remote-sandbox/agent/agent.py"
    if not agent_path.endswith(suffix):
        raise SshError(f"Invalid agent path: {agent_path}")
    root = agent_path[: -len(suffix)]
    return root or "/"


def _fake_manifest_ignored(path: str) -> bool:
    return path == ".remote-sandbox" or path.startswith(".remote-sandbox/")


def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


_AUTH_FAILURE_MARKERS = (
    "permission denied",
    "authentication failed",
    "too many authentication failures",
    "no more authentication methods",
)


def _classify_ssh_failure(stderr: str) -> Literal["auth", "network"]:
    """Map a failed BatchMode ssh's stderr to "auth" (needs the user) or "network"."""
    lowered = stderr.lower()
    if any(marker in lowered for marker in _AUTH_FAILURE_MARKERS):
        return "auth"
    return "network"
