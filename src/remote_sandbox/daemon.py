from __future__ import annotations

import contextlib
import fcntl
import hashlib
import logging
import os
import socket
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import BinaryIO

from remote_sandbox.marker import METADATA_DIR, read_local_marker
from remote_sandbox.ssh import SshRunner, SubprocessSshRunner
from remote_sandbox.syncsession import SyncSession
from remote_sandbox.watch import LocalChangeDetector, create_local_watcher

DAEMON_PID_FILE = "daemon.pid"
DAEMON_LOCK_FILE = "daemon.lock"
DAEMON_LOG_FILE = "daemon.log"

# AF_UNIX path limit is ~104 bytes on macOS, ~108 on Linux; leave margin.
_SOCK_PATH_MAX = 100
_READY_TIMEOUT_S = 60.0
_STOP_TIMEOUT_S = 10.0
_CONTROL_REQUEST_TIMEOUT_S = 2.0
_CONTROL_MAX_LINE_BYTES = 64 * 1024


class DaemonError(RuntimeError):
    pass


@dataclass(frozen=True)
class DaemonStatus:
    running: bool
    pid: int | None


class StopResult(Enum):
    NOT_RUNNING = "not_running"
    STOPPED = "stopped"
    TIMEOUT = "timeout"


def meta_dir(local_root: Path) -> Path:
    return local_root / METADATA_DIR


def pidfile_path(local_root: Path) -> Path:
    return meta_dir(local_root) / DAEMON_PID_FILE


def daemon_lock_path(local_root: Path) -> Path:
    return meta_dir(local_root) / DAEMON_LOCK_FILE


def logfile_path(local_root: Path) -> Path:
    return meta_dir(local_root) / DAEMON_LOG_FILE


def socket_path(local_root: Path) -> Path:
    """Short control-socket path keyed by the resolved workspace path."""
    resolved = local_root.expanduser().resolve()
    digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:16]
    return _runtime_dir() / f"{digest}.sock"


def _runtime_dir() -> Path:
    # AF_UNIX paths are short-capped. macOS $TMPDIR is long, so fall back to /tmp.
    override = os.environ.get("REMOTE_SANDBOX_RUNTIME_DIR")
    if override:
        runtime = Path(override)
    elif os.environ.get("XDG_RUNTIME_DIR"):
        runtime = Path(os.environ["XDG_RUNTIME_DIR"]) / "remote-sandbox"
    else:
        runtime = Path("/tmp") / f"remote-sandbox-{os.getuid()}"
    runtime.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        runtime.chmod(0o700)
    return runtime


class _ControlServer:
    def __init__(
        self,
        local_root: Path,
        *,
        on_poke: Callable[[str], None],
        on_stop: Callable[[], None],
        status: Callable[[], str],
        log: logging.Logger,
    ) -> None:
        self._path = socket_path(local_root)
        self._on_poke = on_poke
        self._on_stop = on_stop
        self._status = status
        self._log = log
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        path = str(self._path)
        if len(path.encode()) > _SOCK_PATH_MAX:
            raise DaemonError(
                f"daemon socket path too long ({len(path)} bytes): {path}. "
                "Set REMOTE_SANDBOX_RUNTIME_DIR to a shorter directory."
            )
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        sock.listen(8)
        sock.settimeout(0.5)
        self._sock = sock
        self._thread = threading.Thread(target=self._serve, name="rsb-daemon-control", daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        assert self._sock is not None
        while True:
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with conn:
                try:
                    conn.settimeout(_CONTROL_REQUEST_TIMEOUT_S)
                    self._handle(conn)
                except (TimeoutError, ValueError) as exc:
                    self._log.warning("invalid control request: %s", exc)
                except Exception as exc:  # pragma: no cover - defensive
                    self._log.exception("control request failed: %s", exc)

    def _handle(self, conn: socket.socket) -> None:
        request = _recv_line(conn).strip()
        verb, _, arg = request.partition(" ")
        if verb == "poke":
            self._on_poke(arg.strip() or "unknown")
            _send_line(conn, "ok queued")
        elif verb == "status":
            _send_line(conn, f"ok {self._status()}")
        elif verb == "stop":
            _send_line(conn, "ok stopping")
            self._on_stop()
        else:
            _send_line(conn, f"error unknown command: {verb}")

    def stop(self) -> None:
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()


class Daemon:
    """Per-workspace sync daemon core. `run()` blocks until stopped."""

    def __init__(
        self,
        local_root: Path,
        *,
        runner: SshRunner | None = None,
        on_ready: Callable[[], None] | None = None,
    ) -> None:
        self._local_root = local_root.expanduser().resolve()
        self._runner = runner or SubprocessSshRunner()
        self._on_ready = on_ready
        self._stop_event = threading.Event()
        self._sync_requested = threading.Event()
        self._sync_count = 0
        self._last_error: str | None = None
        self._last_success: float | None = None
        self._log = logging.getLogger(f"remote_sandbox.daemon.{self._local_root.name}")

    def run(self) -> None:
        marker = read_local_marker(self._local_root)
        if marker is None:
            raise DaemonError(f"not a bound workspace: {self._local_root}")
        try:
            lock_handle = _acquire_daemon_lock(self._local_root)
        except BlockingIOError as exc:
            raise DaemonError(f"a daemon is already running for {self._local_root}") from exc

        session = SyncSession(
            local_root=self._local_root,
            runner=self._runner,
            target=marker.binding.target,
            remote=marker.binding.remote_path,
        )
        server = _ControlServer(
            self._local_root,
            on_poke=lambda _source: self._sync_requested.set(),
            on_stop=self.stop,
            status=self._status_text,
            log=self._log,
        )
        watcher = None
        try:
            self._sync_strict(session, "startup")
            watcher = create_local_watcher(
                detector=LocalChangeDetector(self._local_root, session.current_policy),
                on_change=lambda: self._sync_requested.set(),
            )
            watcher.start()
            server.start()
            _write_pidfile(self._local_root, os.getpid())
            if self._on_ready is not None:
                self._on_ready()
            self._log.info("daemon ready for %s", self._local_root)
            self._worker_loop(session)
        finally:
            if watcher is not None:
                watcher.stop()
            _remove_pidfile(self._local_root)
            _release_daemon_lock(lock_handle)
            server.stop()
            self._log.info("daemon stopped for %s", self._local_root)

    def stop(self) -> None:
        self._stop_event.set()
        self._sync_requested.set()

    def _worker_loop(self, session: SyncSession) -> None:
        while not self._stop_event.is_set():
            self._sync_requested.wait(timeout=30.0)
            if self._stop_event.is_set():
                break
            requested = self._sync_requested.is_set()
            self._sync_requested.clear()
            self._safe_sync(session, "poke" if requested else "poll")

    def _sync_strict(self, session: SyncSession, source: str) -> None:
        session.sync_once()
        self._record_sync_success(source)

    def _safe_sync(self, session: SyncSession, source: str) -> None:
        try:
            session.sync_once()
            self._record_sync_success(source)
        except Exception as exc:  # keep the daemon alive across transient sync errors
            self._last_error = str(exc)
            self._log.warning("sync failed (%s): %s", source, exc)

    def _record_sync_success(self, source: str) -> None:
        del source
        self._sync_count += 1
        self._last_success = time.time()
        self._last_error = None

    def _status_text(self) -> str:
        last_success = "none" if self._last_success is None else f"{self._last_success:.0f}"
        last_error = self._last_error or "none"
        return (
            f"pid={os.getpid()} syncs={self._sync_count} "
            f"last_success={last_success} last_error={last_error}"
        )


# --------------------------------------------------------------------------- #
# Client-side helpers.
# --------------------------------------------------------------------------- #


def daemon_status(local_root: Path) -> DaemonStatus:
    local_root = local_root.expanduser().resolve()
    reply = _request(local_root, "status")
    if reply is None:
        pid = _read_pidfile(local_root)
        if socket_path(local_root).exists() and pid is not None and _process_exists(pid):
            return DaemonStatus(running=True, pid=pid)
        _cleanup_stale_runtime_files(local_root)
        return DaemonStatus(running=False, pid=None)
    pid = _parse_status_pid(reply)
    if pid is None:
        return DaemonStatus(running=False, pid=None)
    return DaemonStatus(running=True, pid=pid)


def poke_daemon(local_root: Path, source: str = "cli") -> bool:
    return _request(local_root, f"poke {source}") is not None


def stop_daemon(local_root: Path) -> bool:
    return stop_daemon_result(local_root) == StopResult.STOPPED


def stop_daemon_result(local_root: Path) -> StopResult:
    local_root = local_root.expanduser().resolve()
    if not daemon_status(local_root).running:
        return StopResult.NOT_RUNNING
    if _request(local_root, "stop") is None:
        return StopResult.TIMEOUT
    if _wait_until_stopped(local_root):
        return StopResult.STOPPED
    return StopResult.TIMEOUT


def ensure_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    local_root = local_root.expanduser().resolve()
    status = daemon_status(local_root)
    if status.running:
        return status
    return start_daemon(local_root, runner=runner)


def start_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    """Double-fork a detached daemon process; wait until it is ready."""
    local_root = local_root.expanduser().resolve()
    if read_local_marker(local_root) is None:
        raise DaemonError(f"not a bound workspace: {local_root}")
    existing = daemon_status(local_root)
    if existing.running:
        return existing

    meta_dir(local_root).mkdir(parents=True, exist_ok=True)
    pid = os.fork()
    if pid > 0:
        os.waitpid(pid, 0)  # reap the intermediate child
        if not _wait_until_running(local_root):
            raise DaemonError(f"daemon failed to start; see {logfile_path(local_root)}")
        return daemon_status(local_root)

    os.setsid()
    pid2 = os.fork()
    if pid2 > 0:
        os._exit(0)

    _configure_daemon_logging(local_root)
    _detach_standard_streams(local_root)
    try:
        Daemon(local_root, runner=runner).run()
    except Exception:  # pragma: no cover - exercised only in a real fork
        logging.getLogger("remote_sandbox.daemon").exception("daemon crashed")
        os._exit(1)
    os._exit(0)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #


def _request(local_root: Path, message: str) -> str | None:
    path = str(socket_path(local_root.expanduser().resolve()))
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(_STOP_TIMEOUT_S)
            sock.connect(path)
            _send_line(sock, message)
            reply = _recv_line(sock).strip()
    except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError):
        return None
    if reply.startswith("error"):
        return None
    return reply


def _wait_until_running(local_root: Path, timeout: float = _READY_TIMEOUT_S) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if daemon_status(local_root).running:
            return True
        time.sleep(0.05)
    return False


def _wait_until_stopped(local_root: Path, timeout: float = _STOP_TIMEOUT_S) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not socket_path(local_root).exists() and _read_pidfile(local_root) is None:
            return _daemon_lock_is_free(local_root)
        time.sleep(0.05)
    return False


def _parse_status_pid(reply: str) -> int | None:
    # reply: "ok pid=123 syncs=..."
    for part in reply.split():
        if part.startswith("pid="):
            try:
                return int(part.removeprefix("pid="))
            except ValueError:
                return None
    return None


def _cleanup_stale_runtime_files(local_root: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        socket_path(local_root).unlink()
    _remove_pidfile(local_root)


def _read_pidfile(local_root: Path) -> int | None:
    try:
        return int(pidfile_path(local_root).read_text(encoding="utf-8").strip())
    except (FileNotFoundError, OSError, ValueError):
        return None


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _daemon_lock_is_free(local_root: Path) -> bool:
    path = daemon_lock_path(local_root)
    try:
        handle = path.open("a+b")
    except OSError:
        return False
    with handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return True


def _write_pidfile(local_root: Path, pid: int) -> None:
    path = pidfile_path(local_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="daemon.", suffix=".pid.tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{pid}\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()


def _remove_pidfile(local_root: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        pidfile_path(local_root).unlink()


def _acquire_daemon_lock(local_root: Path) -> BinaryIO:
    path = daemon_lock_path(local_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise
    return handle


def _release_daemon_lock(handle: BinaryIO) -> None:
    with handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _configure_daemon_logging(local_root: Path) -> None:
    handler = logging.FileHandler(logfile_path(local_root), encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger("remote_sandbox.daemon")
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def _detach_standard_streams(local_root: Path) -> None:
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    log_fd = os.open(logfile_path(local_root), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    if devnull > 2:
        os.close(devnull)
    if log_fd > 2:
        os.close(log_fd)


def _recv_line(sock: socket.socket) -> str:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > _CONTROL_MAX_LINE_BYTES:
            raise ValueError("control request is too large")
        if b"\n" in chunk:
            break
    return b"".join(chunks).split(b"\n", 1)[0].decode("utf-8", errors="replace")


def _send_line(sock: socket.socket, message: str) -> None:
    sock.sendall(message.encode("utf-8") + b"\n")
