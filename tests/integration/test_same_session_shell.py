import fcntl
import os
import pty
import select
import shlex
import signal
import struct
import sys
import termios
import time
import traceback
from contextlib import suppress
from pathlib import Path

import pytest
from helpers.sync_harness import FakePtyBackendHarness

import remote_sandbox.shell as shell_module
from remote_sandbox.shell import ConnectRequestEvent, ConnectResponse


def test_binding_success_reuses_the_original_pty(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    original_pid = session.remote_shell_pid

    session.type("rsb connect --name dq\n")
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


def test_symmetric_connect_enters_incomplete_remote_destination_immediately(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()

    session.connect(
        direction="local-to-remote",
        remote_root="/work/dq",
        enter_immediately=True,
    )

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


def test_binding_cancellation_keeps_browsing_in_the_original_pty(
    fake_pty_backend: FakePtyBackendHarness,
) -> None:
    session = fake_pty_backend.open_enter_shell()
    original_pid = session.remote_shell_pid

    session.type("rsb connect --name dq\n")
    session.reject_binding("Binding cancelled")

    assert session.remote_shell_pid == original_pid
    assert session.prompt_mode == "enter"
    assert "Binding cancelled" in session.output


@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork\\(\\) may lead to "
    "deadlocks in the child\\.:DeprecationWarning"
)
@pytest.mark.timeout(15)
def test_real_pty_ready_preserves_partial_readline_buffer_and_shell_process(
    tmp_path: Path,
) -> None:
    nonce = "realpty14"
    workspace = tmp_path / "remote workspace"
    workspace.mkdir()
    ready_signal = tmp_path / "ready"
    stop_signal = tmp_path / "stop"
    ready_log = tmp_path / "ready.log"
    probe_log = tmp_path / "probe.log"
    rcfile = tmp_path / "bashrc"
    rc_script = _enter_rcfile(nonce).replace(
        "__rsb_publish_ready() {",
        "__rsb_publish_ready_impl() {",
        1,
    )
    rcfile.write_text(
        "RSB_DISPLAY_LABEL=host\n"
        f"__rsb_nonce={shlex.quote(nonce)}\n{rc_script}\n"
        "__rsb_publish_ready() {\n"
        f"  printf 'ready\\n' >> {shlex.quote(str(ready_log))}\n"
        "  __rsb_publish_ready_impl\n"
        "}\n",
        encoding="utf-8",
    )
    frontend_master, frontend_slave = _open_frontend_pty()
    pid = os.fork()
    if pid == 0:
        os.close(frontend_master)
        os.login_tty(frontend_slave)
        sys.stdin = os.fdopen(os.dup(0), "r", encoding="utf-8")
        sys.stdout = os.fdopen(os.dup(1), "w", encoding="utf-8")

        def connect(event: ConnectRequestEvent) -> ConnectResponse:
            assert event.remote == str(workspace)

            def ready_probe() -> str:
                with probe_log.open("a", encoding="utf-8") as handle:
                    handle.write(f"{time.monotonic()}\n")
                if stop_signal.exists():
                    return "stop"
                if ready_signal.exists():
                    return "ready"
                return "pending"

            return ConnectResponse(
                ok=True,
                workspace_id="w1",
                name="dq",
                remote_root=str(workspace),
                direction="local-to-remote",
                ready_probe=ready_probe,
            )

        try:
            status = shell_module._pty_enter_shell_backend(
                ["bash", "--noprofile", "--rcfile", str(rcfile), "-i"],
                nonce,
                connect,
            )
        except BaseException:
            traceback.print_exc()
            status = 1
        os._exit(status)

    os.close(frontend_slave)
    output = bytearray()
    child_reaped = False
    try:
        _read_pty_until(frontend_master, output, b":enter]", timeout=3.0)
        command_start = len(output)
        os.write(frontend_master, b"printf 'PID:%s\\n' \"$$\"\n")
        _read_command_until_prompt(
            frontend_master,
            output,
            b"\r\nPID:",
            start=command_start,
            prompt=b"[host:enter]",
        )
        _connect_remote(frontend_master, output, workspace)
        _wait_for_line_count(probe_log, 2, timeout=2.0)
        probe_times = _float_lines(probe_log)
        assert all(
            later - earlier >= 0.24
            for earlier, later in zip(probe_times, probe_times[1:], strict=False)
        )

        command_start = len(output)
        partial = b"printf 'BUFFER:%s:%s:%s\\n' \"$$\" \"$PWD\" XZ"
        os.write(frontend_master, partial)
        os.write(frontend_master, b"\x02")
        ready_signal.touch()
        _wait_for_line_count(ready_log, 1, timeout=2.0)
        probes_after_ready = _line_count(probe_log)
        time.sleep(0.55)
        assert _line_count(probe_log) == probes_after_ready
        os.write(frontend_master, b"Y\n")
        _read_command_until_prompt(
            frontend_master,
            output,
            b"\r\nBUFFER:",
            start=command_start,
        )

        text = output.decode("utf-8", errors="replace").replace("\r", "")
        pid_line = next(line for line in text.splitlines() if line.startswith("PID:"))
        buffer_line = next(line for line in text.splitlines() if line.startswith("BUFFER:"))
        original_pid = pid_line.removeprefix("PID:")
        buffer_pid, buffer_cwd, inserted = buffer_line.removeprefix("BUFFER:").split(
            ":", 2
        )

        assert buffer_pid == original_pid
        assert buffer_cwd == str(workspace)
        assert inserted == "XYZ"

        ready_signal.unlink()
        _connect_remote(frontend_master, output, workspace)
        command_start = len(output)
        os.write(frontend_master, b"cd /tmp\n")
        _read_pty_until(
            frontend_master,
            output,
            b"[host:dq",
            timeout=3.0,
            start=command_start,
        )
        ready_signal.touch()
        _wait_for_line_count(ready_log, 2, timeout=2.0)
        command_start = len(output)
        os.write(frontend_master, b"printf 'LEFT:%s\\n' \"$PWD\"\n")
        _read_command_until_prompt(
            frontend_master,
            output,
            b"\r\nLEFT:/tmp",
            start=command_start,
        )
        time.sleep(0.35)
        assert _line_count(ready_log) == 2

        ready_signal.unlink()
        _connect_remote(frontend_master, output, workspace)
        command_start = len(output)
        os.write(
            frontend_master,
            b"sleep 0.8; printf 'FOREGROUND:%s\\n' \"$PWD\"\n",
        )
        time.sleep(0.1)
        ready_signal.touch()
        time.sleep(0.35)
        assert _line_count(ready_log) == 2
        _read_command_until_prompt(
            frontend_master,
            output,
            b"\r\nFOREGROUND:",
            start=command_start,
            timeout=2.0,
        )
        _wait_for_line_count(ready_log, 3, timeout=2.0)
        command_start = len(output)
        os.write(frontend_master, b"printf 'AFTER:%s\\n' \"$PWD\"\n")
        _read_command_until_prompt(
            frontend_master,
            output,
            f"\r\nAFTER:{workspace}".encode(),
            start=command_start,
        )

        ready_signal.unlink()
        _connect_remote(frontend_master, output, workspace)
        stop_probe_start = _line_count(probe_log)
        _wait_for_line_count(probe_log, stop_probe_start + 2, timeout=2.0)
        stop_signal.touch()
        time.sleep(0.35)
        probes_after_stop = _line_count(probe_log)
        time.sleep(0.55)
        assert _line_count(probe_log) == probes_after_stop
        assert _line_count(ready_log) == 3

        stop_signal.unlink()
        _connect_remote(frontend_master, output, workspace)
        exit_probe_start = _line_count(probe_log)
        _wait_for_line_count(probe_log, exit_probe_start + 1, timeout=2.0)
        exit_output_start = len(output)
        os.write(frontend_master, b"exit\n")
        _wait_for_child_exit(
            pid,
            timeout=2.0,
            output=output,
            output_start=exit_output_start,
            frontend_fd=frontend_master,
        )
        child_reaped = True
        probes_after_exit = _line_count(probe_log)
        time.sleep(0.35)
        assert _line_count(probe_log) == probes_after_exit

        text = output.decode("utf-8", errors="replace").replace("\r", "")
        foreground_line = next(
            line for line in text.splitlines() if line.startswith("FOREGROUND:")
        )
        assert foreground_line.removeprefix("FOREGROUND:") == str(Path.home())
        assert "ok\tw1\tdq" not in text
        assert "cd --" not in text
        assert "__rsb_publish_ready" not in text
        assert "bash_execute_unix_command" not in text
    finally:
        if not child_reaped:
            with suppress(OSError):
                os.write(frontend_master, b"\nexit\n")
            _terminate_child(pid)
        os.close(frontend_master)


@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork\\(\\) may lead to "
    "deadlocks in the child\\.:DeprecationWarning"
)
@pytest.mark.timeout(8)
def test_blocked_ready_probe_does_not_block_pty_passthrough(tmp_path: Path) -> None:
    nonce = "nonblocking14"
    workspace = tmp_path / "remote"
    workspace.mkdir()
    probe_started = tmp_path / "probe-started"
    release_probe = tmp_path / "release-probe"
    rcfile = tmp_path / "bashrc"
    rcfile.write_text(
        "RSB_DISPLAY_LABEL=host\n"
        f"__rsb_nonce={shlex.quote(nonce)}\n{_enter_rcfile(nonce)}\n",
        encoding="utf-8",
    )
    frontend_master, frontend_slave = _open_frontend_pty()
    pid = os.fork()
    if pid == 0:
        os.close(frontend_master)
        os.login_tty(frontend_slave)
        sys.stdin = os.fdopen(os.dup(0), "r", encoding="utf-8")
        sys.stdout = os.fdopen(os.dup(1), "w", encoding="utf-8")
        probe_calls = 0

        def connect(_event: ConnectRequestEvent) -> ConnectResponse:
            def ready_probe() -> str:
                nonlocal probe_calls
                probe_calls += 1
                if probe_calls == 1:
                    return "pending"
                probe_started.touch()
                deadline = time.monotonic() + 2.0
                while not release_probe.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                return "pending"

            return ConnectResponse(
                ok=True,
                workspace_id="w1",
                name="dq",
                remote_root=str(workspace),
                direction="local-to-remote",
                ready_probe=ready_probe,
            )

        try:
            status = shell_module._pty_enter_shell_backend(
                ["bash", "--noprofile", "--rcfile", str(rcfile), "-i"],
                nonce,
                connect,
            )
        except BaseException:
            traceback.print_exc()
            status = 1
        os._exit(status)

    os.close(frontend_slave)
    output = bytearray()
    try:
        _read_pty_until(frontend_master, output, b":enter]", timeout=2.0)
        _connect_remote(frontend_master, output, workspace)
        _wait_for_path(probe_started, timeout=1.0)
        command_start = len(output)
        os.write(frontend_master, b"printf 'FLOW:%s\\n' \"$$\"\n")
        _read_pty_until(
            frontend_master,
            output,
            b"\r\nFLOW:",
            timeout=0.5,
            start=command_start,
        )
    finally:
        release_probe.touch()
        with suppress(OSError):
            os.write(frontend_master, b"\nexit\n")
        _terminate_child(pid)
        os.close(frontend_master)


@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork\\(\\) may lead to "
    "deadlocks in the child\\.:DeprecationWarning"
)
@pytest.mark.parametrize("old_result", ["pending", "ready"])
@pytest.mark.timeout(8)
def test_successful_reconnect_invalidates_in_flight_ready_probe(
    tmp_path: Path,
    old_result: str,
) -> None:
    nonce = f"stale14-{old_result}"
    workspace = tmp_path / "remote"
    workspace.mkdir()
    probe_started = tmp_path / "probe-started"
    release_probe = tmp_path / "release-probe"
    probe_log = tmp_path / "probe.log"
    ready_log = tmp_path / "ready.log"
    rcfile = tmp_path / "bashrc"
    rc_script = _enter_rcfile(nonce).replace(
        "__rsb_publish_ready() {",
        "__rsb_publish_ready_impl() {",
        1,
    )
    rcfile.write_text(
        "RSB_DISPLAY_LABEL=host\n"
        f"__rsb_nonce={shlex.quote(nonce)}\n{rc_script}\n"
        "__rsb_publish_ready() {\n"
        f"  printf 'ready\\n' >> {shlex.quote(str(ready_log))}\n"
        "  __rsb_publish_ready_impl\n"
        "}\n",
        encoding="utf-8",
    )
    frontend_master, frontend_slave = _open_frontend_pty()
    pid = os.fork()
    if pid == 0:
        os.close(frontend_master)
        os.login_tty(frontend_slave)
        sys.stdin = os.fdopen(os.dup(0), "r", encoding="utf-8")
        sys.stdout = os.fdopen(os.dup(1), "w", encoding="utf-8")
        connect_calls = 0

        def connect(_event: ConnectRequestEvent) -> ConnectResponse:
            nonlocal connect_calls
            connect_calls += 1
            if connect_calls == 1:

                def old_probe() -> str:
                    with probe_log.open("a", encoding="utf-8") as handle:
                        handle.write("old\n")
                    probe_started.touch()
                    deadline = time.monotonic() + 2.0
                    while not release_probe.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    return old_result

                return ConnectResponse(
                    ok=True,
                    workspace_id="old",
                    name="dq",
                    remote_root=str(workspace),
                    direction="local-to-remote",
                    ready_probe=old_probe,
                )
            return ConnectResponse(
                ok=True,
                workspace_id="new",
                name="next",
                remote_root=str(workspace),
                direction="remote-to-local",
            )

        try:
            status = shell_module._pty_enter_shell_backend(
                ["bash", "--noprofile", "--rcfile", str(rcfile), "-i"],
                nonce,
                connect,
            )
        except BaseException:
            traceback.print_exc()
            status = 1
        os._exit(status)

    os.close(frontend_slave)
    output = bytearray()
    try:
        _read_pty_until(frontend_master, output, b":enter]", timeout=2.0)
        _connect_remote(frontend_master, output, workspace)
        _wait_for_path(probe_started, timeout=1.0)
        _connect_remote(frontend_master, output, workspace, name="next")
        release_probe.touch()
        time.sleep(0.65)
        assert _line_count(probe_log) == 1
        assert _line_count(ready_log) == 0

        command_start = len(output)
        os.write(frontend_master, b"printf 'NEW:%s\\n' \"$PWD\"\n")
        _read_command_until_prompt(
            frontend_master,
            output,
            f"\r\nNEW:{workspace}".encode(),
            start=command_start,
            prompt=b"[host:next",
        )
        text = output.decode("utf-8", errors="replace")
        assert "bash_execute_unix_command" not in text
    finally:
        release_probe.touch()
        with suppress(OSError):
            os.write(frontend_master, b"\nexit\n")
        _terminate_child(pid)
        os.close(frontend_master)


def _enter_rcfile(nonce: str) -> str:
    remote_command = shell_module.build_enter_remote_shell_command(
        "host",
        "~",
        nonce=nonce,
    )[-1]
    outer_script = shlex.split(remote_command)[2]
    return outer_script.split("cat <<'EOF'\n", 1)[1].split("\nEOF\n", 1)[0]


def _connect_remote(
    fd: int,
    output: bytearray,
    workspace: Path,
    *,
    name: str = "dq",
) -> None:
    output_start = len(output)
    command_marker = b"rsb connect --remote "
    os.write(
        fd,
        f"rsb connect --remote {shlex.quote(str(workspace))}\n".encode(),
    )
    command_at = _read_pty_until(
        fd,
        output,
        command_marker,
        timeout=3.0,
        start=output_start,
    )
    _read_pty_until(
        fd,
        output,
        f"\x1b[01;36m[host:{name}".encode(),
        timeout=3.0,
        start=command_at,
    )


def _read_command_until_prompt(
    fd: int,
    output: bytearray,
    expected: bytes,
    *,
    start: int,
    timeout: float = 3.0,
    prompt: bytes = b"[host:dq",
) -> None:
    expected_at = _read_pty_until(fd, output, expected, timeout=timeout, start=start)
    _read_pty_until(fd, output, prompt, timeout=timeout, start=expected_at)


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _float_lines(path: Path) -> list[float]:
    return [float(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _wait_for_line_count(path: Path, expected: int, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while _line_count(path) < expected and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _line_count(path) >= expected


def _wait_for_path(path: Path, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()


def _read_pty_until(
    fd: int,
    output: bytearray,
    expected: bytes,
    *,
    timeout: float,
    start: int = 0,
) -> int:
    deadline = time.monotonic() + timeout
    match_end = _terminal_match_end(output, expected, start=start)
    while match_end is None and time.monotonic() < deadline:
        readable, _, _ = select.select([fd], [], [], 0.05)
        if not readable:
            continue
        try:
            chunk = os.read(fd, 4096)
        except OSError:
            break
        if not chunk:
            break
        output.extend(chunk)
        match_end = _terminal_match_end(output, expected, start=start)
    normalized = _normalize_terminal_output(bytes(output[start:]))
    normalized_expected = _normalize_terminal_output(expected)
    assert normalized_expected in normalized, normalized.decode("utf-8", errors="replace")
    assert match_end is not None
    return match_end


def _normalize_terminal_output(value: bytes) -> bytes:
    return value.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def _terminal_match_end(output: bytearray, expected: bytes, *, start: int) -> int | None:
    normalized = _normalize_terminal_output(expected)
    variants = {
        normalized,
        normalized.replace(b"\n", b"\r"),
        normalized.replace(b"\n", b"\r\n"),
    }
    matches = [
        (index, variant)
        for variant in variants
        if (index := output.find(variant, start)) >= 0
    ]
    if not matches:
        return None
    index, variant = min(matches, key=lambda match: match[0])
    return index + len(variant)


def _terminate_child(pid: int) -> None:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        waited, _status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return
        time.sleep(0.01)
    os.kill(pid, signal.SIGTERM)
    os.waitpid(pid, 0)


def _open_frontend_pty() -> tuple[int, int]:
    master, slave = pty.openpty()
    fcntl.ioctl(
        slave,
        termios.TIOCSWINSZ,
        struct.pack("HHHH", 40, 160, 0, 0),
    )
    return master, slave


def _wait_for_child_exit(
    pid: int,
    *,
    timeout: float,
    output: bytearray,
    output_start: int,
    frontend_fd: int,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readable, _, _ = select.select([frontend_fd], [], [], 0.01)
        if readable:
            with suppress(OSError):
                output.extend(os.read(frontend_fd, 4096))
        waited, _status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return
    tail = output[output_start:].decode("utf-8", errors="replace")
    raise AssertionError(f"PTY backend did not exit; tail={tail!r}")
