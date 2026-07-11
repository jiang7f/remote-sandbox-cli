from __future__ import annotations

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

import remote_sandbox.shell as shell_module
from remote_sandbox.shell import ConnectRequestEvent, ConnectResponse
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork\\(\\) may lead to "
    "deadlocks in the child\\.:DeprecationWarning"
)
@pytest.mark.timeout(12)
def test_live_prompt_redraw_is_readline_safe_in_a_real_bash_pty(tmp_path: Path) -> None:
    nonce = "realpty15"
    workspace = tmp_path / "remote"
    workspace.mkdir()
    status_file = tmp_path / "status"
    status_file.write_text("scanning", encoding="utf-8")
    probe_log = tmp_path / "probe.log"
    rcfile = tmp_path / "bashrc"
    rcfile.write_text(
        "CODEX_RSB_DISPLAY_LABEL=ZJU_2\n"
        f"__codex_nonce={shlex.quote(nonce)}\n{_enter_rcfile(nonce)}\n",
        encoding="utf-8",
    )
    frontend_master, frontend_slave = pty.openpty()
    fcntl.ioctl(
        frontend_slave,
        termios.TIOCSWINSZ,
        struct.pack("HHHH", 40, 160, 0, 0),
    )
    pid = os.fork()
    if pid == 0:
        os.close(frontend_master)
        os.login_tty(frontend_slave)
        sys.stdin = os.fdopen(os.dup(0), "r", encoding="utf-8")
        sys.stdout = os.fdopen(os.dup(1), "w", encoding="utf-8")

        def connect(_event: ConnectRequestEvent) -> ConnectResponse:
            def status_probe() -> WorkspaceStatus:
                value = status_file.read_text(encoding="utf-8").strip()
                with probe_log.open("a", encoding="utf-8") as handle:
                    handle.write(f"{time.monotonic()} {value}\n")
                if value == "scanning":
                    return WorkspaceStatus(
                        WorkspacePhase.INITIAL_SYNCING,
                        SyncProgress("scanning"),
                    )
                if value == "planning":
                    return WorkspaceStatus(
                        WorkspacePhase.INITIAL_SYNCING,
                        SyncProgress("planning"),
                    )
                if value == "offline":
                    return WorkspaceStatus(
                        WorkspacePhase.DISCONNECTED,
                        SyncProgress("offline"),
                    )
                if value == "degraded":
                    return WorkspaceStatus(
                        WorkspacePhase.DEGRADED,
                        SyncProgress("audit-requested"),
                    )
                if value == "conflict":
                    return WorkspaceStatus(
                        WorkspacePhase.DEGRADED,
                        SyncProgress("audit-requested"),
                        conflicts=3,
                    )
                if value == "ready":
                    return WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle"))
                percent = int(value)
                return WorkspaceStatus(
                    WorkspacePhase.INITIAL_SYNCING,
                    SyncProgress(
                        "transferring",
                        files_done=percent,
                        files_total=100,
                    ),
                )

            return ConnectResponse(
                ok=True,
                workspace_id="00000000-0000-4000-8000-000000000015",
                name="dq",
                remote_root=str(workspace),
                direction="remote-to-local",
                status_probe=status_probe,
            )

        try:
            result = shell_module._pty_enter_shell_backend(
                ["bash", "--noprofile", "--rcfile", str(rcfile), "-i"],
                nonce,
                connect,
                target="ZJU_2",
            )
        except BaseException:
            traceback.print_exc()
            result = 1
        os._exit(result)

    os.close(frontend_slave)
    output = bytearray()
    try:
        _read_until(frontend_master, output, b":enter]", timeout=2.0)
        os.write(
            frontend_master,
            f"codex-rsb connect --remote {shlex.quote(str(workspace))}\n".encode(),
        )
        _read_until(frontend_master, output, b"[codex:ZJU_2:dq scanning]", timeout=3.0)

        pid_start = len(output)
        os.write(frontend_master, b"printf 'PID:%s\\n' \"$$\"\n")
        _read_until(frontend_master, output, b"\r\nPID:", timeout=2.0, start=pid_start)
        shell_pid = _value_after(output, b"\r\nPID:", start=pid_start)

        typing_start = len(output)
        os.write(frontend_master, b"printf 'CURSOR:%s\\n' AD")
        os.write(frontend_master, b"\x02")
        status_file.write_text("32", encoding="utf-8")
        _read_until(frontend_master, output, b"sync 32%", timeout=2.0, start=typing_start)
        status_file.write_text("40", encoding="utf-8")
        _read_until(frontend_master, output, b"sync 40%", timeout=2.0, start=typing_start)
        os.write(frontend_master, b"M\n")
        _read_until(frontend_master, output, b"\r\nCURSOR:AMD", timeout=2.0, start=typing_start)
        typing_output = bytes(output[typing_start:])
        assert typing_output.count(b"\r\n[codex:") == 0
        assert b"CURSOR:AMD" in typing_output
        assert b"CURSOR:AAMD" not in typing_output

        status_file.write_text("planning", encoding="utf-8")
        _read_until(frontend_master, output, b"planning", timeout=2.0)
        calls_before_command = _line_count(probe_log)
        foreground_start = len(output)
        os.write(frontend_master, b"sleep 0.8; printf 'FOREGROUND\\n'\n")
        time.sleep(0.12)
        status_file.write_text("offline", encoding="utf-8")
        time.sleep(0.35)
        _drain(frontend_master, output)
        assert b"offline" not in output[foreground_start:]
        assert _line_count(probe_log) == calls_before_command
        _read_until(frontend_master, output, b"\r\nFOREGROUND", timeout=2.0, start=foreground_start)
        _read_until(frontend_master, output, b"offline", timeout=2.0, start=foreground_start)

        status_file.write_text("ready", encoding="utf-8")
        time.sleep(0.35)
        os.write(frontend_master, b"\n")
        compact_start = len(output)
        _read_until(
            frontend_master,
            output,
            b"[codex:ZJU_2:dq]",
            timeout=2.0,
            start=compact_start,
        )
        pid_check_start = len(output)
        os.write(frontend_master, b"printf 'PID2:%s\\n' \"$$\"\n")
        _read_until(frontend_master, output, b"\r\nPID2:", timeout=2.0, start=pid_check_start)
        assert _value_after(output, b"\r\nPID2:", start=pid_check_start) == shell_pid

        status_file.write_text("offline", encoding="utf-8")
        offline_start = len(output)
        _read_until(
            frontend_master,
            output,
            b"[codex:ZJU_2:dq offline]",
            timeout=2.0,
            start=offline_start,
        )
        assert b"\r\n[codex:" not in output[offline_start:]
        status_file.write_text("conflict", encoding="utf-8")
        _read_until(frontend_master, output, b"conflict 3", timeout=2.0, start=offline_start)
        status_file.write_text("degraded", encoding="utf-8")
        _read_until(frontend_master, output, b"offline", timeout=2.0, start=len(output))

        probe_times = [
            float(line.split()[0])
            for line in probe_log.read_text(encoding="utf-8").splitlines()
        ]
        assert all(
            later - earlier >= 0.20
            for earlier, later in zip(probe_times, probe_times[1:], strict=False)
        )
        text = output.decode("utf-8", errors="replace")
        assert "codex-rsb;prompt" not in text
        assert "codex-slot:" not in text
        assert "connect-request" not in text
        assert "ok\t00000000" not in text
    finally:
        with suppress(OSError):
            os.write(frontend_master, b"\nexit\n")
        _terminate_child(pid)
        os.close(frontend_master)


def _enter_rcfile(nonce: str) -> str:
    command = shell_module.build_enter_remote_shell_command("ZJU_2", "~", nonce=nonce)[-1]
    outer_script = shlex.split(command)[2]
    return outer_script.split("cat <<'EOF'\n", 1)[1].split("\nEOF\n", 1)[0]


def _read_until(
    fd: int,
    output: bytearray,
    expected: bytes,
    *,
    timeout: float,
    start: int = 0,
) -> None:
    deadline = time.monotonic() + timeout
    while expected not in output[start:] and time.monotonic() < deadline:
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
    assert expected in output[start:], output[start:].decode("utf-8", errors="replace")


def _drain(fd: int, output: bytearray) -> None:
    while select.select([fd], [], [], 0.01)[0]:
        with suppress(OSError):
            output.extend(os.read(fd, 4096))


def _value_after(output: bytearray, marker: bytes, *, start: int) -> bytes:
    begin = output.index(marker, start) + len(marker)
    end = output.index(b"\r\n", begin)
    return bytes(output[begin:end])


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _terminate_child(pid: int) -> None:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        waited, _status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return
        time.sleep(0.01)
    os.kill(pid, signal.SIGTERM)
    os.waitpid(pid, 0)
