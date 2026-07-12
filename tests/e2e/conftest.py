from __future__ import annotations

import base64
import fcntl
import os
import pty
import re
import select
import shlex
import shutil
import signal
import subprocess
import sys
import termios
import time
import tomllib
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType

import pytest

from remote_sandbox.namespace import ssh_control_dir

_ROOT = Path(__file__).resolve().parents[2]
_E2E_DIR = Path(__file__).resolve().parent
_PASSWORD = "test-password"
_PROMPT = b":enter]"


@dataclass(frozen=True, slots=True)
class TerminalState:
    visible_input: str
    cursor_offset: int
    remote_shell_pid: int


class PtyShell:
    """Small deadline-based PTY driver for one real `rsb enter` session."""

    def __init__(self, fixture: SshFixture, *, password: bool = False) -> None:
        self._fixture = fixture
        self._output = bytearray()
        self._foreground_start = 0
        self._foreground_result: bytes | None = None
        self.first_sync_status_at = float("inf")
        target = fixture.password_host if password else fixture.host
        self._pid, self._fd = _spawn_pty(
            [str(fixture.cli_executable), "enter", target],
            env=fixture.env,
            cwd=fixture.local_workspace(),
        )
        if password:
            self._wait_for(b"password:", timeout=10.0)
            self._send((_PASSWORD + "\n").encode())
        self._wait_for(_PROMPT, timeout=15.0)
        self._install_terminal_probe()

    def __enter__(self) -> PtyShell:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.close()

    def connect(self, *, remote: Path, local: Path, name: str) -> None:
        command = shlex.join(
            (
                "rsb",
                "connect",
                "--remote",
                str(remote),
                "--local",
                str(local),
                "--name",
                name,
            )
        )
        started = time.monotonic()
        output_start = len(self._output)
        self._send(command.encode() + b"\n")
        self._wait_for(b"[y/N]", timeout=20.0, start=output_start)
        output_start = len(self._output)
        self._send(b"y\n")
        self._wait_for(f"[{self._fixture.host}:{name}".encode(), timeout=20.0, start=output_start)
        if self.first_sync_status_at == float("inf"):
            self.first_sync_status_at = time.monotonic()
        if self.first_sync_status_at < started:
            raise AssertionError("initial sync status timestamp preceded connect")

    def begin_connect(self, *, name: str) -> None:
        remote = self._fixture.remote_workspace(empty=False)
        local = self._fixture.local_workspace()
        command = shlex.join(
            (
                "rsb",
                "connect",
                "--remote",
                str(remote),
                "--local",
                str(local),
                "--name",
                name,
            )
        )
        output_start = len(self._output)
        self._send(command.encode() + b"\n")
        self._wait_for(b"[y/N]", timeout=10.0, start=output_start)

    def reject_binding(self) -> None:
        output_start = len(self._output)
        self._send(b"n\n")
        self._wait_for(_PROMPT, timeout=10.0, start=output_start)

    def wait_for_prompt(
        self,
        text: str,
        timeout: float = 10.0,
        *,
        start: int | None = None,
    ) -> None:
        self._wait_for(
            text.encode(),
            timeout=timeout,
            start=len(self._output) if start is None else start,
        )

    def wait_for_prompt_text(
        self,
        text: str,
        timeout: float = 10.0,
        *,
        start: int | None = None,
    ) -> None:
        self._wait_for(
            text.encode(),
            timeout=timeout,
            start=len(self._output) if start is None else start,
        )

    def wait_for_prompt_change(self, timeout: float = 10.0, *, start: int | None = None) -> None:
        output_start = len(self._output) if start is None else start
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._read_available(deadline)
            current = bytes(self._output[output_start:])
            if b"planning" in current or b"sync " in current or b"]" in current:
                return
        self._fail("prompt did not change", start=output_start)

    def output_position(self) -> int:
        return len(self._output)

    def type_without_enter(self, text: str) -> None:
        self._send(text.encode())

    def visible_input(self) -> str:
        return self.terminal_state().visible_input

    def cursor_offset(self) -> int:
        return self.terminal_state().cursor_offset

    def remote_shell_pid(self) -> int:
        return self.terminal_state().remote_shell_pid

    def terminal_state(self) -> TerminalState:
        start = len(self._output)
        marker = b"__E2E_TERMINAL__"
        self._send(b"\x18\x0f")
        self._wait_for(marker, timeout=5.0, start=start)
        match = re.search(
            marker + rb"([A-Za-z0-9+/=]*):(\d+):(\d+)",
            bytes(self._output[start:]),
        )
        if match is None:
            self._fail("terminal state probe was not printed", start=start)
        try:
            visible = base64.b64decode(match.group(1), validate=True).decode("utf-8")
        except (UnicodeDecodeError, ValueError) as exc:
            raise AssertionError("terminal state probe contained invalid input") from exc
        return TerminalState(visible, int(match.group(2)), int(match.group(3)))

    def run_foreground_probe(self, seconds: float) -> None:
        self._foreground_start = len(self._output)
        code = (
            "import os,select,time; print('__E2E_FOREGROUND_'+'READY__',flush=True); "
            "end=time.monotonic()+float(os.environ['D']); data=b''; "
            "[(lambda r: None)(select.select([0],[],[],max(0,end-time.monotonic())))]; "
            "r,_,_=select.select([0],[],[],0); data=os.read(0,4096) if r else b''; "
            "print('__E2E_FOREGROUND__'+data.hex())"
        )
        command = f"D={shlex.quote(str(seconds))} python3 -c {shlex.quote(code)}"
        self._send(command.encode() + b"\n")
        self._wait_for(
            b"__E2E_FOREGROUND_READY__",
            timeout=5.0,
            start=self._foreground_start,
        )

    def trigger_remote_change(self, path: str, content: bytes) -> None:
        remote = self._fixture.remote_for_shell(self)
        self._fixture.write_remote(remote / path, content)

    def foreground_probe_received_private_redraw(self) -> bool:
        self._wait_for(b"__E2E_FOREGROUND__", timeout=10.0, start=self._foreground_start)
        self._wait_for(b"[", timeout=10.0, start=self._foreground_start)
        match = re.search(
            rb"__E2E_FOREGROUND__([0-9a-f]*)",
            bytes(self._output[self._foreground_start :]),
        )
        if match is None:
            self._fail("foreground probe result was not printed", start=self._foreground_start)
        self._foreground_result = bytes.fromhex(match.group(1).decode())
        return b"\x1b[777~" in self._foreground_result

    def is_open(self) -> bool:
        pid, _status = os.waitpid(self._pid, os.WNOHANG)
        return pid == 0

    def close(self) -> None:
        if self._fd < 0:
            return
        with _suppress_os_error():
            self._send(b"\x03exit\n")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            pid, _status = os.waitpid(self._pid, os.WNOHANG)
            if pid:
                break
            time.sleep(0.02)
        else:
            with _suppress_os_error():
                os.kill(self._pid, signal.SIGTERM)
            with _suppress_os_error():
                os.waitpid(self._pid, 0)
        with _suppress_os_error():
            os.close(self._fd)
        self._fd = -1

    def _send(self, data: bytes) -> None:
        os.write(self._fd, data)

    def _install_terminal_probe(self) -> None:
        binding = (
            '"\\C-x\\C-o":'
            '__rsb_e2e_line=$(printf %s "$READLINE_LINE" | base64 | tr -d "\\n"); '
            'printf "\\n__E2E_TERMINAL__%s:%s:%s\\n" '
            '"$__rsb_e2e_line" "$READLINE_POINT" "$$"'
        )
        start = len(self._output)
        self._send(f"bind -x {shlex.quote(binding)}\n".encode())
        self._wait_for(_PROMPT, timeout=5.0, start=start)

    def _wait_for(
        self,
        expected: bytes,
        *,
        timeout: float,
        start: int = 0,
    ) -> None:
        deadline = time.monotonic() + timeout
        while expected not in self._output[start:] and time.monotonic() < deadline:
            self._read_available(deadline)
        if expected not in self._output[start:]:
            self._fail(f"PTY did not emit {expected!r}", start=start)

    def _read_available(self, deadline: float) -> None:
        timeout = max(0.0, min(0.1, deadline - time.monotonic()))
        ready, _, _ = select.select([self._fd], [], [], timeout)
        if not ready:
            return
        try:
            data = os.read(self._fd, 65536)
        except OSError:
            data = b""
        if data:
            self._output.extend(data)
            if self.first_sync_status_at == float("inf") and any(
                marker in data for marker in (b"scanning", b"planning", b"sync ")
            ):
                self.first_sync_status_at = time.monotonic()

    def _fail(self, message: str, *, start: int = 0) -> None:
        tail = bytes(self._output[start:])[-4000:].decode("utf-8", errors="replace")
        raise AssertionError(f"{message}. Last PTY output:\n{tail}")


@dataclass(slots=True)
class SshFixture:
    container_id: str
    image: str
    host: str
    password_host: str
    port: int
    key_file: Path
    state_home: Path
    runtime_dir: Path
    home: Path
    env: dict[str, str]
    cli_executable: Path
    _tmp_path: Path
    _shells: list[PtyShell] = field(default_factory=list)
    _shell_remotes: dict[int, Path] = field(default_factory=dict)
    _local_count: int = 0
    _remote_count: int = 0
    _closed: bool = False

    def local_workspace(self) -> Path:
        self._local_count += 1
        path = self._tmp_path / "local" / f"workspace-{self._local_count}"
        path.mkdir(parents=True)
        return path

    def remote_workspace(self, *, empty: bool) -> Path:
        self._remote_count += 1
        path = Path(f"/home/test/workspaces/workspace-{self._remote_count}")
        self._docker_exec("mkdir", "-p", str(path))
        self._docker_exec("chown", "-R", "test:test", str(path))
        if not empty:
            self.write_remote(path / "existing.txt", b"existing")
        return path

    def enter(self, *, password: bool = False) -> PtyShell:
        shell = PtyShell(self, password=password)
        self._shells.append(shell)
        return shell

    def cli(self, *argv: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.cli_executable), *argv],
            check=False,
            cwd=self._tmp_path,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=120.0,
        )

    def cli_with_password(
        self,
        *argv: str,
        password: str,
    ) -> subprocess.CompletedProcess[str]:
        return _run_pty_command(
            [str(self.cli_executable), *argv],
            env=self.env,
            cwd=self._tmp_path,
            password=password,
            timeout=120.0,
        )

    def disconnect_network(self) -> None:
        self._docker_exec(
            "iptables",
            "-I",
            "INPUT",
            "1",
            "-p",
            "tcp",
            "--dport",
            "2222",
            "-j",
            "REJECT",
        )

    def reconnect_network(self) -> None:
        self._docker_exec(
            "iptables",
            "-D",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "2222",
            "-j",
            "REJECT",
        )
        _wait_for_ssh_python(self)

    def wait_for_remote_file(self, path: Path, timeout: float = 5.0) -> None:
        self._wait_until(lambda: self.remote_exists(path), f"remote file {path}", timeout)

    def wait_for_local_file(self, path: Path, timeout: float = 5.0) -> None:
        self._wait_until(path.is_file, f"local file {path}", timeout)

    def wait_until_missing(self, path: Path, timeout: float = 5.0) -> None:
        if str(path).startswith("/home/test/"):
            def predicate() -> bool:
                return not self.remote_exists(path)
        else:
            def predicate() -> bool:
                return not path.exists()
        self._wait_until(predicate, f"missing path {path}", timeout)

    def wait_for_state(self, name: str, state: str, timeout: float = 10.0) -> None:
        last = ""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.cli("status", name)
            last = result.stdout + result.stderr
            if state in last:
                return
            time.sleep(0.05)
        raise AssertionError(f"workspace {name} did not reach {state}. Last status:\n{last}")

    def remote_exists(self, path: Path) -> bool:
        result = self._docker_exec("test", "-e", str(path), check=False)
        return result.returncode == 0

    def read_remote(self, path: Path) -> bytes:
        result = self._docker_exec_bytes("cat", str(path))
        return result.stdout

    def write_remote(self, path: Path, content: bytes) -> None:
        code = (
            "from pathlib import Path; import sys; p=Path(sys.argv[1]); "
            "p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(sys.stdin.buffer.read())"
        )
        self._docker_exec_bytes("python3", "-c", code, str(path), input_data=content)
        self._docker_exec("chown", "-R", "test:test", str(path.parent))

    def delete_remote(self, path: Path) -> None:
        self._docker_exec("rm", "-f", "--", str(path))

    def bound_pair(self, *, name: str, password: bool) -> tuple[Path, Path]:
        local = self.local_workspace()
        remote = self.remote_workspace(empty=True)
        command = (
            "connect",
            self.password_host if password else self.host,
            "--remote",
            str(remote),
            "--local",
            str(local),
            "--name",
            name,
            "--no-shell",
            "--yes",
        )
        result = (
            self.cli_with_password(*command, password=_PASSWORD)
            if password
            else self.cli(*command)
        )
        assert result.returncode == 0, result.stdout + result.stderr
        self.wait_for_state(name, "ready", timeout=20.0)
        return local, remote

    def bound_shell(self, *, name: str) -> PtyShell:
        local = self.local_workspace()
        remote = self.remote_workspace(empty=True)
        shell = self.enter()
        shell.connect(remote=remote, local=local, name=name)
        self._shell_remotes[id(shell)] = remote
        return shell

    def remote_for_shell(self, shell: PtyShell) -> Path:
        try:
            return self._shell_remotes[id(shell)]
        except KeyError as exc:
            raise AssertionError("PTY shell has no bound remote workspace") from exc

    def populate_local(self, local: Path, *, files: int) -> None:
        for index in range(files):
            path = local / f"files/{index // 100:03d}/file-{index:05d}.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((f"{index:012d}".encode() * 11)[:128])

    def create_local_state_sentinel(self, content: bytes) -> Path:
        path = self.state_home / "sentinel"
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
        return path

    def create_remote_state_sentinel(self, content: bytes) -> Path:
        path = Path("/home/test/.remote-sandbox/sentinel")
        self.write_remote(path, content)
        return path

    def remote_metadata_path(self, name: str) -> Path:
        workspace_id = self._workspace_id(name)
        return Path(f"/home/test/.remote-sandbox/workspaces/{workspace_id}")

    def local_metadata_path(self, name: str) -> Path:
        return self.state_home / "workspaces" / self._workspace_id(name)

    def local_binding_exists(self, name: str) -> bool:
        return any(record.get("name") == name for record in self._binding_records())

    def expire_control_master(self, name: str) -> None:
        record = next(record for record in self._binding_records() if record.get("name") == name)
        target = str(record["target"])
        subprocess.run(
            [
                "ssh",
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPath={ssh_control_dir(self.env)}/%C",
                "-O",
                "exit",
                target,
            ],
            check=False,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=20.0,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[str] = []
        for shell in reversed(self._shells):
            _record_cleanup_failure(failures, "close PTY shell", shell.close)
        records: list[dict[str, object]] = []
        try:
            records = self._binding_records()
        except Exception as exc:
            failures.append(f"read binding records: {exc}")
        process_ids = _fixture_process_ids(self.state_home, self.runtime_dir)
        for record in records:
            name = record.get("name")
            target = record.get("target")
            if isinstance(name, str):
                try:
                    result = (
                        self.cli_with_password("forget", name, password=_PASSWORD)
                        if target == self.password_host
                        else self.cli("forget", name)
                    )
                    if result.returncode != 0:
                        raise AssertionError(result.stdout + result.stderr)
                except Exception as exc:
                    try:
                        local_result = self.cli("forget", name, "--local-only")
                        if local_result.returncode != 0:
                            raise AssertionError(local_result.stdout + local_result.stderr)
                    except Exception as local_exc:
                        failures.append(f"forget {name}: {exc}")
                        failures.append(f"local-only forget {name}: {local_exc}")
            if isinstance(target, str):
                _record_cleanup_failure(
                    failures,
                    f"close SSH ControlMaster for {target}",
                    lambda value=target: _close_control_master(self, value),
                )
        _record_cleanup_failure(
            failures,
            "terminate local fixture processes",
            lambda: _terminate_processes(process_ids),
        )
        for path, label in (
            (self.key_file, "private key"),
            (self.key_file.with_suffix(".pub"), "public key"),
            (self.state_home, "state directory"),
            (self.runtime_dir, "runtime directory"),
            (self.home, "fixture home"),
        ):
            _record_cleanup_failure(
                failures,
                f"remove {label}",
                lambda value=path: _remove_path(value),
            )
        _record_cleanup_failure(
            failures,
            "remove Docker container",
            lambda: _run_cleanup_command(["docker", "rm", "-f", self.container_id], 30.0),
        )
        _record_cleanup_failure(
            failures,
            "remove Docker image",
            lambda: _run_cleanup_command(["docker", "image", "rm", "-f", self.image], 30.0),
        )
        container = _inspect_cleanup_resource(
            failures,
            "Docker container",
            ["docker", "container", "inspect", self.container_id],
        )
        image = _inspect_cleanup_resource(
            failures,
            "Docker image",
            ["docker", "image", "inspect", self.image],
        )
        if container.returncode == 0 or image.returncode == 0:
            failures.append("Docker E2E fixture left container or image residue")
        for path, label in (
            (self.key_file, "private key"),
            (self.key_file.with_suffix(".pub"), "public key"),
            (self.state_home, "state directory"),
            (self.runtime_dir, "runtime directory"),
            (self.home, "fixture home"),
        ):
            if path.exists():
                failures.append(f"fixture left {label} residue at {path}")
        surviving = tuple(pid for pid in process_ids if _process_exists(pid))
        if surviving:
            failures.append(f"fixture left local processes running: {surviving}")
        if failures:
            raise AssertionError("E2E cleanup failures\n" + "\n".join(failures))

    def _workspace_id(self, name: str) -> str:
        for record in self._binding_records():
            if record.get("name") == name:
                value = record.get("workspace_id")
                if isinstance(value, str):
                    return value
        raise AssertionError(f"no binding named {name}")

    def _binding_records(self) -> list[dict[str, object]]:
        registry = self.state_home / "connections.toml"
        if not registry.exists():
            return []
        data = tomllib.loads(registry.read_text(encoding="utf-8"))
        records = data.get("connections", [])
        if not isinstance(records, list):
            raise AssertionError("E2E registry connections are malformed")
        return [record for record in records if isinstance(record, dict)]

    def _wait_until(self, predicate: Callable[[], bool], label: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        last_status = ""
        while time.monotonic() < deadline:
            if predicate():
                return
            status = self.cli("status")
            last_status = status.stdout + status.stderr
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {label}. Last status:\n{last_status}")

    def _docker_exec(
        self,
        *argv: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "exec", self.container_id, *argv],
            check=check,
            capture_output=True,
            text=True,
            timeout=30.0,
        )

    def _docker_exec_bytes(
        self,
        *argv: str,
        input_data: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["docker", "exec", "-i", self.container_id, *argv],
            check=True,
            input=input_data,
            capture_output=True,
            timeout=30.0,
        )


def _write_isolated_ssh_wrapper(wrapper: Path, executable: Path, config: Path) -> None:
    wrapper.parent.mkdir(parents=True, mode=0o700)
    wrapper.write_text(
        "#!/bin/sh\n"
        f"exec {shlex.quote(str(executable))} -F {shlex.quote(str(config))} "
        '-o IdentityAgent=none "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o700)


def _isolated_ssh_environment(base: Mapping[str, str], wrapper: Path) -> dict[str, str]:
    env = dict(base)
    inherited_path = env.get("PATH", "")
    env["PATH"] = str(wrapper.parent) + (os.pathsep + inherited_path if inherited_path else "")
    env["RSYNC_RSH"] = str(wrapper)
    return env


def _record_cleanup_failure(
    failures: list[str],
    label: str,
    operation: Callable[[], object],
) -> None:
    try:
        operation()
    except Exception as exc:
        failures.append(f"{label}: {exc}")


def _close_control_master(fixture: SshFixture, target: str) -> None:
    result = subprocess.run(
        [
            "ssh",
            "-o",
            "ControlMaster=auto",
            "-o",
            f"ControlPath={ssh_control_dir(fixture.env)}/%C",
            "-O",
            "exit",
            target,
        ],
        check=False,
        env=fixture.env,
        capture_output=True,
        text=True,
        timeout=20.0,
    )
    if result.returncode not in {0, 255}:
        raise AssertionError(result.stderr or result.stdout or "ControlMaster exit failed")


def _fixture_process_ids(state_home: Path, runtime_dir: Path) -> tuple[int, ...]:
    process_ids: set[int] = set()
    for pidfile in (state_home / "workspaces").glob("*/daemon.pid"):
        try:
            pid = int(pidfile.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            continue
        if pid > 1 and pid != os.getpid():
            process_ids.add(pid)
    result = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        check=False,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    if result.returncode == 0:
        marker = str(runtime_dir)
        for line in result.stdout.splitlines():
            if marker not in line:
                continue
            raw_pid, _separator, _command = line.strip().partition(" ")
            try:
                pid = int(raw_pid)
            except ValueError:
                continue
            if pid > 1 and pid != os.getpid():
                process_ids.add(pid)
    return tuple(sorted(process_ids))


def _process_exists(pid: int) -> bool:
    try:
        waited, _status = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        waited = 0
    if waited == pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_processes(process_ids: tuple[int, ...]) -> None:
    for pid in process_ids:
        if _process_exists(pid):
            os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and any(_process_exists(pid) for pid in process_ids):
        time.sleep(0.02)
    for pid in process_ids:
        if _process_exists(pid):
            os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and any(_process_exists(pid) for pid in process_ids):
        time.sleep(0.02)
    surviving = tuple(pid for pid in process_ids if _process_exists(pid))
    if surviving:
        raise AssertionError(f"local fixture processes did not stop: {surviving}")


def _remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _run_cleanup_command(argv: list[str], timeout: float) -> None:
    result = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr or result.stdout or f"cleanup command failed: {argv}")


def _inspect_cleanup_resource(
    failures: list[str],
    label: str,
    argv: list[str],
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
    except Exception as exc:
        failures.append(f"inspect {label}: {exc}")
        return subprocess.CompletedProcess(argv, 0, "", str(exc))


def _cleanup_failed_ssh_fixture_startup(
    *,
    docker: str,
    image: str,
    container_id: str,
    paths: tuple[Path, ...],
) -> list[BaseException]:
    failures: list[BaseException] = []

    def record(operation: Callable[[], object]) -> None:
        try:
            operation()
        except BaseException as exc:
            failures.append(exc)

    for path in paths:
        record(lambda value=path: _remove_path(value))
    if container_id:
        record(
            lambda: _run_cleanup_command(
                [docker, "rm", "-f", container_id],
                30.0,
            )
        )
    record(
        lambda: _run_cleanup_command(
            [docker, "image", "rm", "-f", image],
            30.0,
        )
    )
    return failures


def start_ssh_fixture(tmp_path: Path) -> SshFixture:
    docker = shutil.which("docker")
    if docker is None:
        if os.environ.get("RSB_E2E_REQUIRED") == "1":
            raise RuntimeError("Docker is required because RSB_E2E_REQUIRED=1")
        pytest.skip("Docker is unavailable. SSH E2E requires the disposable Ubuntu fixture")
    ssh_executable = shutil.which("ssh")
    if ssh_executable is None:
        raise RuntimeError("OpenSSH is required for SSH E2E")
    image = f"rsb-e2e:{uuid.uuid4().hex}"
    key_file = tmp_path / "client-key"
    home = tmp_path / "home"
    state_home = tmp_path / "rsb-state"
    runtime = tmp_path / "rsb-runtime"
    container_id = ""
    try:
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key_file)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30.0,
        )
        key_file.chmod(0o600)
        key_file.with_suffix(".pub").chmod(0o644)
        subprocess.run(
            [
                docker,
                "build",
                "-t",
                image,
                "-f",
                str(_E2E_DIR / "Dockerfile"),
                str(_E2E_DIR),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=300.0,
        )
        run = subprocess.run(
            [
                docker,
                "run",
                "-d",
                "--rm",
                "--cap-add",
                "NET_ADMIN",
                "-p",
                "127.0.0.1::2222",
                "-v",
                f"{key_file.with_suffix('.pub')}:/fixture/client.pub:ro",
                image,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30.0,
        )
        container_id = run.stdout.strip()
        port_result = subprocess.run(
            [docker, "port", container_id, "2222/tcp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        port = int(port_result.stdout.strip().rsplit(":", 1)[1])
        ssh_dir = home / ".ssh"
        ssh_dir.mkdir(parents=True, mode=0o700)
        config = ssh_dir / "config"
        config.write_text(
            _ssh_config(port=port, key_file=key_file),
            encoding="utf-8",
        )
        config.chmod(0o600)
        ssh_wrapper = home / "isolated-bin" / "ssh"
        _write_isolated_ssh_wrapper(ssh_wrapper, Path(ssh_executable), config)
        env = _isolated_ssh_environment(
            {
                **os.environ,
                "HOME": str(home),
                "REMOTE_SANDBOX_HOME": str(state_home),
                "REMOTE_SANDBOX_RUNTIME_DIR": str(runtime),
                "REMOTE_SANDBOX_CONTROL_DIR": str(runtime / "control"),
            },
            ssh_wrapper,
        )
        executable = Path(sys.executable).with_name("rsb")
        if not executable.is_file():
            raise RuntimeError(f"rsb CLI is unavailable at {executable}")
        fixture = SshFixture(
            container_id=container_id,
            image=image,
            host="rsb-e2e-key",
            password_host="rsb-e2e-password",
            port=port,
            key_file=key_file,
            state_home=state_home,
            runtime_dir=runtime,
            home=home,
            env=env,
            cli_executable=executable,
            _tmp_path=tmp_path,
        )
        _wait_for_ssh_python(fixture)
        return fixture
    except BaseException as startup_failure:
        cleanup_failures = _cleanup_failed_ssh_fixture_startup(
            docker=docker,
            image=image,
            container_id=container_id,
            paths=(
                key_file,
                key_file.with_suffix(".pub"),
                state_home,
                runtime,
                home,
            ),
        )
        if cleanup_failures:
            raise BaseExceptionGroup(
                "SSH E2E fixture startup and cleanup failed",
                [startup_failure, *cleanup_failures],
            ) from None
        raise


@pytest.fixture
def ssh_fixture(tmp_path: Path) -> Iterator[SshFixture]:
    fixture = start_ssh_fixture(tmp_path)
    try:
        yield fixture
    finally:
        fixture.close()


def _wait_for_ssh_python(fixture: SshFixture) -> None:
    deadline = time.monotonic() + 30.0
    last = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["ssh", fixture.host, "python3", "--version"],
            check=False,
            env=fixture.env,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        last = result.stdout + result.stderr
        if result.returncode == 0 and re.search(r"Python 3\.10(?:\.|\s|$)", last):
            return
        time.sleep(0.1)
    raise AssertionError(f"Ubuntu fixture did not expose Python 3.10. Last result:\n{last}")


def _ssh_config(*, port: int, key_file: Path) -> str:
    common = (
        "  HostName 127.0.0.1\n"
        "  User test\n"
        f"  Port {port}\n"
        "  StrictHostKeyChecking no\n"
        "  UserKnownHostsFile /dev/null\n"
        "  GlobalKnownHostsFile /dev/null\n"
        "  LogLevel ERROR\n"
        "  ConnectTimeout 5\n"
        "  IdentityAgent none\n"
        "  IdentitiesOnly yes\n"
    )
    return (
        "Host rsb-e2e-key\n"
        + common
        + f"  IdentityFile {key_file}\n"
        + "Host rsb-e2e-password\n"
        + common
        + "  IdentityFile none\n"
        + "  PubkeyAuthentication no\n"
        + "  PreferredAuthentications password\n"
    )


def _spawn_pty(
    argv: list[str],
    *,
    env: Mapping[str, str],
    cwd: Path,
) -> tuple[int, int]:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, b"\x28\x00\x78\x00\x00\x00\x00\x00")
    pid = os.fork()
    if pid == 0:
        os.close(master)
        os.login_tty(slave)
        os.chdir(cwd)
        os.execve(argv[0], argv, dict(env))
    os.close(slave)
    return pid, master


def _run_pty_command(
    argv: list[str],
    *,
    env: Mapping[str, str],
    cwd: Path,
    password: str,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    pid, fd = _spawn_pty(argv, env=env, cwd=cwd)
    output = bytearray()
    sent_password = False
    deadline = time.monotonic() + timeout
    status = 1
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([fd], [], [], 0.1)
            if ready:
                try:
                    data = os.read(fd, 65536)
                except OSError:
                    data = b""
                output.extend(data)
                if not sent_password and b"password:" in output.lower():
                    os.write(fd, password.encode() + b"\n")
                    sent_password = True
            waited, raw = os.waitpid(pid, os.WNOHANG)
            if waited:
                status = os.waitstatus_to_exitcode(raw)
                break
        else:
            os.kill(pid, signal.SIGTERM)
            os.waitpid(pid, 0)
            raise subprocess.TimeoutExpired(argv, timeout, output=bytes(output))
    finally:
        with _suppress_os_error():
            os.close(fd)
    text = output.decode("utf-8", errors="replace").replace("\r", "")
    return subprocess.CompletedProcess(argv, status, text, "")


class _suppress_os_error:
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc, traceback
        return exc_type is not None and issubclass(exc_type, OSError)
