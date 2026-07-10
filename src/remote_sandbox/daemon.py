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
from enum import Enum, StrEnum
from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote, unquote

from remote_sandbox.agent import remote_agent_path
from remote_sandbox.marker import local_meta_dir, migrate_local_metadata, read_local_marker
from remote_sandbox.ssh import SshRunner, SubprocessSshRunner
from remote_sandbox.sync import SyncCancelled
from remote_sandbox.syncsession import SyncProgress, SyncSession
from remote_sandbox.watch import LocalChangeDetector, create_local_watcher

DAEMON_PID_FILE = "daemon.pid"
DAEMON_LOCK_FILE = "daemon.lock"
DAEMON_LOG_FILE = "daemon.log"

# AF_UNIX path limit is ~104 bytes on macOS, ~108 on Linux; leave margin.
_SOCK_PATH_MAX = 100
# The daemon now publishes its pidfile/socket *before* the first sync, so becoming
# discoverable takes milliseconds — no need to wait a whole sync's worth of time.
_READY_TIMEOUT_S = 15.0
_STOP_TIMEOUT_S = 10.0
_CONTROL_REQUEST_TIMEOUT_S = 2.0
_CONTROL_MAX_LINE_BYTES = 64 * 1024


class DaemonError(RuntimeError):
    pass


class DaemonPhase(StrEnum):
    """Lifecycle of a running daemon, reported over the control socket.

    ``starting``        process is up, pidfile/socket published, first sync not begun.
    ``initial-syncing`` the very first (bootstrap) sync is in flight.
    ``syncing``         a later sync cycle is in flight.
    ``ready``           idle and healthy; last sync succeeded.
    ``degraded``        the last sync failed but the daemon is retrying.
    ``failed``          startup failed fatally (surfaced in the log; process exits).
    """

    STARTING = "starting"
    INITIAL_SYNCING = "initial-syncing"
    SYNCING = "syncing"
    READY = "ready"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True)
class DaemonStatus:
    running: bool
    pid: int | None
    phase: DaemonPhase | None = None
    consecutive_failures: int = 0
    last_error: str | None = None
    sync_count: int = 0
    sync_phase: str | None = None
    files_total: int | None = None
    files_done: int | None = None
    bytes_total: int | None = None
    bytes_done: int | None = None
    current_path: str | None = None


class StopResult(Enum):
    NOT_RUNNING = "not_running"
    STOPPED = "stopped"
    TIMEOUT = "timeout"


def meta_dir(local_root: Path) -> Path:
    return local_meta_dir(local_root)


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


class _RemoteWatcher:
    """Runs the resident remote agent `watch` and pokes the daemon on each remote change.

    A background thread spawns `ssh … agent.py watch` and reads its stdout; every line is a
    changed remote path, which triggers a sync. On disconnect it re-dials with backoff. This
    is what makes a file deleted on the server sync within ~1 s instead of up to 30 s, and it
    replaces per-timer full-tree remote scans as the primary change signal.
    """

    def __init__(
        self,
        *,
        runner: SshRunner,
        target: str,
        remote: str,
        on_change: Callable[[], None],
        stop_event: threading.Event,
        log: logging.Logger,
    ) -> None:
        self._runner = runner
        self._target = target
        self._remote = remote
        self._on_change = on_change
        self._stop_event = stop_event
        self._log = log
        self._thread: threading.Thread | None = None
        self._proc: object | None = None

    def start(self) -> None:
        spawn = getattr(self._runner, "spawn_remote_watch", None)
        if spawn is None:  # e.g. a fake runner without streaming support
            self._log.info("remote watcher unavailable for this runner; relying on poll")
            return
        self._thread = threading.Thread(
            target=self._run, name="rsb-remote-watch", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        agent_path = remote_agent_path(self._remote)
        failures = 0
        while not self._stop_event.is_set():
            proc = None
            try:
                proc = self._runner.spawn_remote_watch(  # type: ignore[attr-defined]
                    self._target, agent_path, self._remote, 1.0
                )
                self._proc = proc
                failures = 0
                assert proc.stdout is not None
                for line in proc.stdout:
                    if self._stop_event.is_set():
                        break
                    line = line.strip()
                    if not line or line == "__rsb_watch_ready__":
                        continue
                    # Any remote change → ask the worker to sync now.
                    self._on_change()
            except Exception as exc:  # keep trying across transient ssh/agent errors
                self._log.warning("remote watcher error: %s", exc)
            finally:
                if proc is not None:
                    with contextlib.suppress(Exception):
                        proc.terminate()
            if self._stop_event.is_set():
                break
            failures += 1
            # Back off on repeated failures (dead host, missing python3) up to ~30 s.
            self._stop_event.wait(min(2.0 * failures, 30.0))

    def stop(self) -> None:
        proc = self._proc
        if proc is not None:
            with contextlib.suppress(Exception):
                proc.terminate()  # type: ignore[attr-defined]
        if self._thread is not None:
            self._thread.join(timeout=3.0)



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
        self._consecutive_failures = 0
        self._target: str | None = None
        # Guards the mutable status fields read by the control thread while the worker
        # thread mutates them. A plain lock is enough — updates are tiny.
        self._status_lock = threading.Lock()
        self._phase = DaemonPhase.STARTING
        self._progress: SyncProgress | None = None
        self._log = logging.getLogger(f"remote_sandbox.daemon.{self._local_root.name}")

    def run(self) -> None:
        marker = read_local_marker(self._local_root)
        if marker is None:
            raise DaemonError(f"not a bound workspace: {self._local_root}")
        self._target = marker.binding.target
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
        remote_watcher = None
        try:
            # Publish discoverability FIRST: pidfile, control socket, and watcher come up
            # in milliseconds so `rsb status` and the connect UX see the daemon (and its
            # phase) immediately, instead of the process looking "stopped" for the entire
            # duration of a large first sync.
            watcher = create_local_watcher(
                detector=LocalChangeDetector(self._local_root, session.current_policy),
                on_change=lambda: self._sync_requested.set(),
            )
            watcher.start()
            server.start()
            _write_pidfile(self._local_root, os.getpid())
            if self._on_ready is not None:
                self._on_ready()
            self._log.info("daemon up for %s; starting initial sync", self._local_root)
            # The bootstrap sync now runs INSIDE the published daemon, reported as
            # `initial-syncing`. `bind` no longer performs its own blocking sync, so this
            # is the single source of the first transfer (no duplicate pass).
            self._run_sync(session, "startup", initial=True)
            # Start the remote-side watcher only after the first sync (the agent is
            # bootstrapped by then). It pokes _sync_requested when the remote tree changes,
            # so a file deleted/edited on the server syncs in ~1 s instead of waiting for the
            # 30 s poll. The poll stays as a safety net if the watcher can't run.
            remote_watcher = _RemoteWatcher(
                runner=self._runner,
                target=marker.binding.target,
                remote=marker.binding.remote_path,
                on_change=lambda: self._sync_requested.set(),
                stop_event=self._stop_event,
                log=self._log,
            )
            remote_watcher.start()
            self._worker_loop(session)
        finally:
            if remote_watcher is not None:
                remote_watcher.stop()
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
            self._sync_requested.wait(timeout=self._next_wait())
            if self._stop_event.is_set():
                break
            requested = self._sync_requested.is_set()
            self._sync_requested.clear()
            self._run_sync(session, "poke" if requested else "poll", initial=False)

    def _next_wait(self) -> float:
        """Poll interval: 30 s when healthy, exponential backoff while failing.

        A transient wobble retries in ~2 s instead of waiting the full poll; a longer
        outage keeps retrying every ≤30 s until the connection comes back.
        """
        if self._consecutive_failures == 0:
            return 30.0
        return float(min(2.0 * 2 ** (self._consecutive_failures - 1), 30.0))

    def _run_sync(self, session: SyncSession, source: str, *, initial: bool) -> None:
        """One sync cycle, keeping the daemon alive across transient errors.

        Sets phase to initial-syncing/syncing for the duration, then ready or degraded.
        Never raises: a failed sync leaves the daemon degraded and retrying.
        """
        self._set_phase(DaemonPhase.INITIAL_SYNCING if initial else DaemonPhase.SYNCING)
        try:
            session.sync_once(
                on_progress=self._on_progress,
                cancel=self._stop_event.is_set,
            )
        except SyncCancelled:
            # A stop was requested mid-sync; the transfer was killed. Exit quietly — not a
            # failure, don't mark degraded. The worker loop sees _stop_event and shuts down.
            self._log.info("sync cancelled (%s) for shutdown", source)
            with self._status_lock:
                self._progress = None
            return
        except Exception as exc:  # keep the daemon alive across transient sync errors
            if self._stop_event.is_set():
                # Shutting down; treat any error during teardown as a clean stop.
                with self._status_lock:
                    self._progress = None
                return
            with self._status_lock:
                self._last_error = str(exc)
                self._consecutive_failures += 1
                self._phase = DaemonPhase.DEGRADED
                self._progress = None
            self._log.warning(
                "sync failed (%s, attempt %d): %s", source, self._consecutive_failures, exc
            )
            # A dead/stale SSH master (e.g. after the laptop slept) keeps failing; drop it
            # so the next retry re-dials. Key auth reconnects automatically; password auth
            # cannot from a background process and stays degraded until a foreground command
            # re-establishes the shared master.
            if self._target is not None:
                self._runner.clear_master(self._target)
            return
        with self._status_lock:
            self._sync_count += 1
            self._last_success = time.time()
            self._last_error = None
            self._consecutive_failures = 0
            self._phase = DaemonPhase.READY
            self._progress = None

    def _on_progress(self, progress: SyncProgress) -> None:
        with self._status_lock:
            self._progress = progress

    def _set_phase(self, phase: DaemonPhase) -> None:
        with self._status_lock:
            self._phase = phase

    def _status_text(self) -> str:
        with self._status_lock:
            phase = self._phase
            sync_count = self._sync_count
            fails = self._consecutive_failures
            last_success = self._last_success
            last_error = self._last_error
            progress = self._progress
        last_success_text = "none" if last_success is None else f"{last_success:.0f}"
        parts = [
            f"pid={os.getpid()}",
            f"phase={phase.value}",
            f"syncs={sync_count}",
            f"fails={fails}",
            f"last_success={last_success_text}",
        ]
        if progress is not None:
            parts.append(f"sync_phase={progress.phase}")
            parts.extend(
                [
                    f"files_total={progress.files_total}",
                    f"files_done={progress.files_done}",
                    f"bytes_total={progress.bytes_total}",
                    f"bytes_done={progress.bytes_done}",
                ]
            )
            if progress.current_path:
                parts.append(f"current_path={_encode_status_value(progress.current_path)}")
        parts.append(f"last_error={_encode_status_value(last_error) if last_error else 'none'}")
        return " ".join(parts)


# --------------------------------------------------------------------------- #
# Client-side helpers.
# --------------------------------------------------------------------------- #


def daemon_status(local_root: Path) -> DaemonStatus:
    local_root = local_root.expanduser().resolve()
    reply = _request(local_root, "status")
    if reply is None:
        pid = _read_pidfile(local_root)
        if socket_path(local_root).exists() and pid is not None and _process_exists(pid):
            # Pidfile + socket exist but the control thread did not answer in time (a long
            # sync can momentarily starve it). Treat as running-but-unknown-phase rather
            # than lying that it is stopped.
            return DaemonStatus(running=True, pid=pid, phase=DaemonPhase.STARTING)
        _cleanup_stale_runtime_files(local_root)
        return DaemonStatus(running=False, pid=None)
    pid = _parse_status_pid(reply)
    if pid is None:
        return DaemonStatus(running=False, pid=None)
    return DaemonStatus(
        running=True,
        pid=pid,
        phase=_parse_status_phase(reply),
        consecutive_failures=_parse_status_int(reply, "fails", default=0) or 0,
        last_error=_parse_status_last_error(reply),
        sync_count=_parse_status_int(reply, "syncs", default=0) or 0,
        sync_phase=_parse_status_field(reply, "sync_phase"),
        files_total=_parse_status_int(reply, "files_total", default=None),
        files_done=_parse_status_int(reply, "files_done", default=None),
        bytes_total=_parse_status_int(reply, "bytes_total", default=None),
        bytes_done=_parse_status_int(reply, "bytes_done", default=None),
        current_path=_parse_status_field(reply, "current_path"),
    )


def poke_daemon(local_root: Path, source: str = "cli") -> bool:
    return _request(local_root, f"poke {source}") is not None


def poke_and_wait_for_sync(
    local_root: Path,
    source: str = "cli",
    *,
    timeout: float = 120.0,
) -> bool:
    """Ask the daemon to sync now and wait for one *fresh* sync cycle to complete.

    Used after `rsb run` so a caller (e.g. an AI) sees the command's output files land
    locally before continuing — WITHOUT the CLI opening its own competing SyncSession and
    fighting the daemon for the workspace lock (the old cause of a spurious traceback).

    Returns True if a new sync completed (sync_count advanced past the value observed when
    we poked), False if the daemon is not running or the wait timed out. Never raises: a
    sync hiccup must not change the command's own exit code.
    """
    local_root = local_root.expanduser().resolve()
    try:
        before = daemon_status(local_root)
    except Exception:
        return False
    if not before.running:
        return False
    baseline = before.sync_count
    if not poke_daemon(local_root, source):
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = daemon_status(local_root)
        except Exception:
            return False
        if not status.running:
            return False
        # A completed cycle bumps sync_count and returns to ready/degraded (not mid-sync).
        if status.sync_count > baseline and status.phase in {
            DaemonPhase.READY,
            DaemonPhase.DEGRADED,
        }:
            return status.phase is DaemonPhase.READY
        time.sleep(0.1)
    return False


def stop_daemon(local_root: Path) -> bool:
    return stop_daemon_result(local_root) == StopResult.STOPPED


def stop_daemon_result(local_root: Path) -> StopResult:
    local_root = local_root.expanduser().resolve()
    status = daemon_status(local_root)
    if not status.running:
        return StopResult.NOT_RUNNING
    # Ask nicely: the daemon cancels any in-flight sync (killing the tar) and shuts down.
    if _request(local_root, "stop") is not None and _wait_until_stopped(local_root):
        return StopResult.STOPPED
    # Guaranteed stop: if graceful shutdown did not complete in time (e.g. a wedged transfer),
    # signal the process directly so `rsb stop`/`forget` can never be permanently stuck.
    pid = status.pid if status.pid is not None else _read_pidfile(local_root)
    if pid is not None and _terminate_process(pid):
        return StopResult.STOPPED
    return StopResult.TIMEOUT


def _terminate_process(pid: int, *, timeout: float = 6.0) -> bool:
    """SIGTERM then SIGKILL a daemon pid; return True once it is gone."""
    import signal

    if not _process_exists(pid):
        return True
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            return True
        time.sleep(0.1)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            return True
        time.sleep(0.1)
    return not _process_exists(pid)


def ensure_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    local_root = local_root.expanduser().resolve()
    status = daemon_status(local_root)
    if status.running:
        return status
    return start_daemon(local_root, runner=runner)


def start_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    """Double-fork a detached daemon process; wait until it is ready."""
    local_root = local_root.expanduser().resolve()
    # Relocate any legacy in-tree .remote-sandbox into the out-of-tree home dir before the
    # daemon opens its state/lock there, so an upgraded binding keeps its sync base.
    migrate_local_metadata(local_root)
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
    # reply: "ok pid=123 phase=ready syncs=..."
    return _parse_status_int(reply, "pid", default=None)


def _encode_status_value(value: str) -> str:
    """Encode a free-form value (path, error) into one whitespace-free status token."""
    return quote(value, safe="")


def _decode_status_value(value: str) -> str:
    return unquote(value)


def _status_fields(reply: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in reply.split():
        key, sep, value = part.partition("=")
        if sep:
            fields[key] = value
    return fields


def _parse_status_field(reply: str, key: str) -> str | None:
    raw = _status_fields(reply).get(key)
    if raw is None:
        return None
    return _decode_status_value(raw)


def _parse_status_int(reply: str, key: str, *, default: int | None) -> int | None:
    raw = _status_fields(reply).get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_status_phase(reply: str) -> DaemonPhase | None:
    raw = _status_fields(reply).get("phase")
    if raw is None:
        return None
    try:
        return DaemonPhase(raw)
    except ValueError:
        return None


def _parse_status_last_error(reply: str) -> str | None:
    raw = _status_fields(reply).get("last_error")
    if raw is None or raw == "none":
        return None
    return _decode_status_value(raw)


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
