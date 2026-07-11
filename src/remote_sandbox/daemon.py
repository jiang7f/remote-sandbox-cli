from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import logging
import os
import queue
import socket
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import BinaryIO, Literal, Protocol, cast

from remote_sandbox.agent import RemoteAgentManager
from remote_sandbox.conflicts import resolve_conflict_transaction
from remote_sandbox.engine import SyncEngine
from remote_sandbox.fetch import fetch_placeholders
from remote_sandbox.initial_sync import InitialSyncCoordinator
from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.namespace import runtime_dir
from remote_sandbox.policy import POLICY_FILE_NAME, StaticPolicyEngine
from remote_sandbox.registry import BindingRecord, list_binding_records
from remote_sandbox.remote_client import RemoteEventSubscription, RemoteWorkspaceClient
from remote_sandbox.settings import load_settings
from remote_sandbox.ssh import SshRunner, SubprocessSshRunner
from remote_sandbox.state import WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import BatchTransport
from remote_sandbox.watch import LocalEventWatcher, create_local_watcher
from remote_sandbox.workspace import read_workspace_spec, workspace_paths

DAEMON_PID_FILE = "daemon.pid"
DAEMON_LOCK_FILE = "daemon.lock"
DAEMON_LOG_FILE = "daemon.log"

_SOCK_PATH_MAX = 100
_READY_TIMEOUT_S = 60.0
_STOP_TIMEOUT_S = 10.0
_CONTROL_REQUEST_TIMEOUT_S = 2.0
_CONTROL_MAX_LINE_BYTES = 64 * 1024
_MAX_BACKOFF_S = 30.0
_MUTATION_TIMEOUT_S = 3600.0


class DaemonError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DaemonStatus:
    running: bool
    pid: int | None
    consecutive_failures: int = 0
    last_error: str | None = None
    conn_state: str = "ok"
    phase: WorkspacePhase = WorkspacePhase.STOPPED
    workspace_status: WorkspaceStatus | None = None


class StopResult(Enum):
    NOT_RUNNING = "not_running"
    STOPPED = "stopped"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class SupervisorRuntime:
    workspace_id: str
    metadata_root: Path
    runtime_root: Path

    @property
    def state_db(self) -> Path:
        return self.metadata_root / "state.sqlite3"

    @property
    def logfile(self) -> Path:
        return self.metadata_root / DAEMON_LOG_FILE

    @property
    def pidfile(self) -> Path:
        return self.metadata_root / DAEMON_PID_FILE

    @property
    def lockfile(self) -> Path:
        return self.metadata_root / DAEMON_LOCK_FILE

    @property
    def socket(self) -> Path:
        return self.runtime_root / f"{self.workspace_id}.sock"


class _InitialSync(Protocol):
    def run(self) -> object: ...


class _IncrementalEngine(Protocol):
    def run_once(self, reason: str) -> object: ...


class _SupervisorRemote(Protocol):
    def ensure_agent(self) -> None: ...

    def start_watcher(self) -> object: ...

    def subscribe(self, after_sequence: int) -> Iterable[JournalEvent]: ...

    def clear_master(self) -> None: ...

    def probe_connection(self) -> Literal["ok", "auth", "network"]: ...

    def close(self) -> None: ...


@dataclass(slots=True)
class _ProductionComponents:
    remote: _SupervisorRemote
    engine: _IncrementalEngine
    initial_sync: _InitialSync
    local_watcher: LocalEventWatcher
    mutation_handler: Callable[[str, dict[str, object]], dict[str, object]]


@dataclass(slots=True)
class _MutationRequest:
    kind: str
    payload: dict[str, object]
    completed: threading.Event
    result: dict[str, object] | None = None
    error: BaseException | None = None


class _SupervisorControlServer:
    def __init__(
        self,
        runtime: SupervisorRuntime,
        *,
        on_sync: Callable[[], None],
        on_resume: Callable[[], None],
        on_stop: Callable[[], None],
        on_mutation: Callable[[str, dict[str, object]], dict[str, object]],
        status: Callable[[], DaemonStatus],
        log: logging.Logger,
    ) -> None:
        self._runtime = runtime
        self._on_sync = on_sync
        self._on_resume = on_resume
        self._on_stop = on_stop
        self._on_mutation = on_mutation
        self._status = status
        self._log = log
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        path = str(self._runtime.socket)
        if len(path.encode()) > _SOCK_PATH_MAX:
            raise DaemonError(f"daemon socket path too long ({len(path)} bytes): {path}")
        self._runtime.runtime_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._runtime.runtime_root.chmod(0o700)
        self._runtime.socket.unlink(missing_ok=True)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        self._runtime.socket.chmod(0o600)
        sock.listen(8)
        sock.settimeout(0.5)
        self._sock = sock
        self._thread = threading.Thread(
            target=self._serve,
            name="codex-rsb-supervisor-control",
            daemon=True,
        )
        self._thread.start()

    def _serve(self) -> None:
        assert self._sock is not None
        while True:
            try:
                connection, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with connection:
                try:
                    connection.settimeout(_CONTROL_REQUEST_TIMEOUT_S)
                    request = _recv_line(connection).strip()
                    verb = request.partition(" ")[0]
                    if verb == "status":
                        payload = _daemon_status_payload(self._status())
                        _send_line(connection, json.dumps(payload, separators=(",", ":")))
                    elif verb in {"sync", "poke"}:
                        self._on_sync()
                        _send_line(connection, "ok")
                    elif verb == "resume":
                        self._on_resume()
                        _send_line(connection, "ok")
                    elif verb == "stop":
                        _send_line(connection, "ok")
                        self._on_stop()
                    elif verb == "mutate":
                        raw_payload = request.partition(" ")[2]
                        decoded = json.loads(raw_payload)
                        if not isinstance(decoded, dict):
                            raise ValueError("mutation request must be an object")
                        kind = decoded.get("kind")
                        mutation_payload: object = decoded.get("payload")
                        if not isinstance(kind, str) or not isinstance(mutation_payload, dict):
                            raise ValueError("mutation request is malformed")
                        try:
                            result = self._on_mutation(kind, mutation_payload)
                        except Exception as exc:
                            response = {"ok": False, "error": " ".join(str(exc).split())}
                        else:
                            response = {"ok": True, "result": result}
                        _send_line(connection, json.dumps(response, separators=(",", ":")))
                    else:
                        _send_line(connection, "error")
                except Exception as exc:  # pragma: no cover - defensive boundary
                    self._log.warning("supervisor control request failed: %s", exc)

    def stop(self) -> None:
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._runtime.socket.unlink(missing_ok=True)


class WorkspaceSupervisor:
    def __init__(
        self,
        runtime: SupervisorRuntime,
        *,
        store: WorkspaceStore,
        initial_sync: _InitialSync | None = None,
        remote: _SupervisorRemote | None = None,
        engine: _IncrementalEngine | None = None,
        local_watcher: LocalEventWatcher | None = None,
        component_factory: Callable[[threading.Event], _ProductionComponents] | None = None,
        mutation_handler: Callable[[str, dict[str, object]], dict[str, object]] | None = None,
        close_store: bool = False,
    ) -> None:
        self.runtime = runtime
        self.store = store
        self.initial_sync = initial_sync
        self.remote = remote
        self.engine = engine
        self.local_watcher = local_watcher
        self._component_factory = component_factory
        self._mutation_handler = mutation_handler
        self._mutations: queue.Queue[_MutationRequest] = queue.Queue()
        self._close_store = close_store
        self._stop_event = threading.Event()
        self._sync_requested = threading.Event()
        self._resume_requested = threading.Event()
        self._consecutive_failures = 0
        self._last_error: str | None = None
        self._connection_state = "ok"
        self._requires_foreground_auth = False
        self.audit_requested = False
        self._subscription: Iterable[JournalEvent] | None = None
        self._subscription_thread: threading.Thread | None = None
        self._subscription_failure: BaseException | None = None
        self._failure_lock = threading.Lock()
        self._log = logging.getLogger(f"remote_sandbox.daemon.{runtime.workspace_id}")
        self._control = _SupervisorControlServer(
            runtime,
            on_sync=self.request_sync,
            on_resume=self.resume,
            on_stop=self.stop,
            on_mutation=self.request_mutation,
            status=self._status,
            log=self._log,
        )

    @classmethod
    def for_test(
        cls,
        *,
        workspace_id: str,
        metadata_root: Path,
        store: WorkspaceStore,
        initial_sync: _InitialSync,
        remote: _SupervisorRemote | None = None,
        engine: _IncrementalEngine | None = None,
    ) -> WorkspaceSupervisor:
        runtime_key = hashlib.sha256(str(metadata_root).encode()).hexdigest()[:12]
        return cls(
            SupervisorRuntime(
                workspace_id,
                metadata_root,
                Path("/tmp") / f"codex-rsb-test-{runtime_key}",
            ),
            store=store,
            initial_sync=initial_sync,
            remote=remote,
            engine=engine,
        )

    def run(self) -> None:
        try:
            lock_handle = self._acquire_lock()
        except BlockingIOError as exc:
            raise DaemonError("a workspace supervisor is already running") from exc
        failed = False
        try:
            self.store.set_status(
                WorkspaceStatus(WorkspacePhase.STARTING, SyncProgress("starting"))
            )
            self._write_pidfile()
            self._control.start()
            self._load_components()
            assert self.initial_sync is not None
            if not self._initialize_workspace():
                return
            self._record_success()
            self._start_subscription()
            self._worker_loop()
        except BaseException as exc:
            failed = True
            self._last_error = str(exc)
            self.store.set_status(
                WorkspaceStatus(
                    WorkspacePhase.FAILED,
                    SyncProgress("failed"),
                    last_error=self._last_error,
                )
            )
            raise
        finally:
            self._stop_subscription()
            if self.local_watcher is not None:
                self.local_watcher.stop()
            if self.remote is not None:
                self.remote.close()
            self._control.stop()
            self.runtime.pidfile.unlink(missing_ok=True)
            _release_daemon_lock(lock_handle)
            if not failed:
                self.store.set_status(
                    WorkspaceStatus(WorkspacePhase.STOPPED, SyncProgress("stopped"))
                )
            if self._close_store:
                self.store.close()

    def stop(self) -> None:
        self._stop_event.set()
        self._sync_requested.set()
        self._stop_subscription()

    def request_sync(self) -> None:
        self._sync_requested.set()

    def request_mutation(
        self,
        kind: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        if not kind or self._mutation_handler is None:
            raise DaemonError("supervisor mutation is unavailable")
        request = _MutationRequest(kind, payload, threading.Event())
        self._mutations.put(request)
        self._sync_requested.set()
        if not request.completed.wait(timeout=_MUTATION_TIMEOUT_S):
            raise DaemonError(f"supervisor mutation timed out: {kind}")
        if request.error is not None:
            raise DaemonError(str(request.error)) from request.error
        return request.result or {}

    def resume(self) -> None:
        self._requires_foreground_auth = False
        self._resume_requested.set()
        self._sync_requested.set()

    def handle_subscription_failure(self, error: BaseException) -> float | None:
        self._last_error = str(error)
        self._consecutive_failures += 1
        probe: Literal["ok", "auth", "network"] = "network"
        if self.remote is not None:
            self.remote.clear_master()
            try:
                probe = self.remote.probe_connection()
            except Exception:
                probe = "network"
        if probe == "auth":
            self._requires_foreground_auth = True
            self._connection_state = "disconnected"
            phase = WorkspacePhase.DISCONNECTED
            stage = "offline"
            delay = None
        elif probe == "ok":
            self._requires_foreground_auth = False
            self._connection_state = "degraded"
            self._request_audit()
            phase = WorkspacePhase.DEGRADED
            stage = "audit-requested"
            delay = self._backoff_delay()
        else:
            self._requires_foreground_auth = False
            self._connection_state = "reconnecting"
            phase = WorkspacePhase.DISCONNECTED
            stage = "reconnecting"
            delay = self._backoff_delay()
        current = self.store.get_status()
        self.store.set_status(
            WorkspaceStatus(
                phase,
                SyncProgress(stage),
                pending=current.pending,
                conflicts=current.conflicts,
                last_error=self._last_error,
                last_sync_at=current.last_sync_at,
            )
        )
        return delay

    def _load_components(self) -> None:
        if self._component_factory is None:
            return
        components = self._component_factory(self._sync_requested)
        self.remote = components.remote
        self.engine = components.engine
        self.initial_sync = components.initial_sync
        self.local_watcher = components.local_watcher
        self._mutation_handler = components.mutation_handler

    def _initialize_workspace(self) -> bool:
        while not self._stop_event.is_set():
            try:
                if self.remote is not None:
                    self.remote.ensure_agent()
                if self.store.initial_sync_completed():
                    self._start_watchers_for_restart()
                    self._request_audit()
                    self._run_engine("restart")
                else:
                    assert self.initial_sync is not None
                    self.initial_sync.run()
                return True
            except Exception as exc:
                delay = self.handle_subscription_failure(exc)
                if delay is not None:
                    if self._stop_event.wait(delay):
                        return False
                    continue
                while self._requires_foreground_auth and not self._stop_event.is_set():
                    self._resume_requested.wait(timeout=0.5)
                    if self._resume_requested.is_set():
                        self._resume_requested.clear()
                        self._requires_foreground_auth = False
        return False

    def _start_watchers_for_restart(self) -> None:
        if self.local_watcher is not None:
            self.local_watcher.start()
        if self.remote is not None:
            self.remote.start_watcher()

    def _worker_loop(self) -> None:
        retry_at: float | None = None
        while not self._stop_event.is_set():
            timeout = 30.0
            if retry_at is not None:
                timeout = max(0.0, retry_at - time.monotonic())
            self._sync_requested.wait(timeout=timeout)
            self._sync_requested.clear()
            if self._stop_event.is_set():
                return
            failure = self._take_subscription_failure()
            if failure is not None:
                delay = self.handle_subscription_failure(failure)
                retry_at = None if delay is None else time.monotonic() + delay
                continue
            self._run_pending_mutations()
            if self._requires_foreground_auth and not self._resume_requested.is_set():
                continue
            self._resume_requested.clear()
            if retry_at is not None and time.monotonic() < retry_at:
                continue
            try:
                if self._connection_state == "degraded" and self.remote is not None:
                    self.remote.start_watcher()
                self._run_engine("event")
                self._record_success()
                retry_at = None
                if self._subscription_thread is None or not self._subscription_thread.is_alive():
                    self._start_subscription()
            except Exception as exc:
                delay = self.handle_subscription_failure(exc)
                retry_at = None if delay is None else time.monotonic() + delay

    def _run_pending_mutations(self) -> None:
        while True:
            try:
                request = self._mutations.get_nowait()
            except queue.Empty:
                return
            try:
                assert self._mutation_handler is not None
                request.result = self._mutation_handler(request.kind, request.payload)
            except BaseException as exc:
                request.error = exc
            finally:
                request.completed.set()

    def _start_subscription(self) -> None:
        if self.remote is None:
            return
        if self._subscription_thread is not None and self._subscription_thread.is_alive():
            return
        after = self.store.acknowledged_sequence("remote")
        self._subscription = self.remote.subscribe(after)
        self._subscription_thread = threading.Thread(
            target=self._consume_subscription,
            name="codex-rsb-remote-events",
            daemon=True,
        )
        self._subscription_thread.start()

    def _consume_subscription(self) -> None:
        assert self._subscription is not None
        try:
            for event in self._subscription:
                if self._stop_event.is_set():
                    return
                self.store.record_events((event,))
                self._sync_requested.set()
        except BaseException as exc:
            if not self._stop_event.is_set():
                with self._failure_lock:
                    self._subscription_failure = exc
                self._sync_requested.set()

    def _take_subscription_failure(self) -> BaseException | None:
        with self._failure_lock:
            failure = self._subscription_failure
            self._subscription_failure = None
            return failure

    def _stop_subscription(self) -> None:
        subscription = self._subscription
        if isinstance(subscription, RemoteEventSubscription):
            subscription.close()
        self._subscription = None
        thread = self._subscription_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        self._subscription_thread = None

    def _run_engine(self, reason: str) -> None:
        if self.engine is not None:
            self.engine.run_once(reason)

    def _request_audit(self) -> None:
        self.audit_requested = True
        self.store.append_event("local", EventKind.RESCAN_REQUIRED, "*")

    def _record_success(self) -> None:
        self._consecutive_failures = 0
        self._last_error = None
        self._connection_state = "ok"
        self._requires_foreground_auth = False
        self.audit_requested = False
        status = self.store.get_status()
        if status.phase not in {WorkspacePhase.READY, WorkspacePhase.DEGRADED}:
            self.store.set_status(
                WorkspaceStatus(
                    WorkspacePhase.READY,
                    SyncProgress("idle"),
                    pending=status.pending,
                    conflicts=status.conflicts,
                    last_sync_at=time.time(),
                )
            )

    def _backoff_delay(self) -> float:
        return float(min(2.0 * 2 ** (self._consecutive_failures - 1), _MAX_BACKOFF_S))

    def _status(self) -> DaemonStatus:
        status = self.store.get_status()
        return DaemonStatus(
            running=True,
            pid=os.getpid(),
            consecutive_failures=self._consecutive_failures,
            last_error=status.last_error,
            conn_state=self._connection_state,
            phase=status.phase,
            workspace_status=status,
        )

    def _acquire_lock(self) -> BinaryIO:
        self.runtime.metadata_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.runtime.metadata_root.chmod(0o700)
        handle = self.runtime.lockfile.open("a+b")
        self.runtime.lockfile.chmod(0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            handle.close()
            raise
        return handle

    def _write_pidfile(self) -> None:
        fd, temporary_name = tempfile.mkstemp(
            prefix="daemon.",
            suffix=".pid.tmp",
            dir=self.runtime.metadata_root,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(f"{os.getpid()}\n")
                handle.flush()
                os.fsync(handle.fileno())
            temporary.chmod(0o600)
            os.replace(temporary, self.runtime.pidfile)
        finally:
            temporary.unlink(missing_ok=True)


class SupervisorClient:
    def __init__(self, runtime: SupervisorRuntime) -> None:
        self.runtime = runtime

    def status(self) -> DaemonStatus:
        reply = self._request("status")
        if reply is not None:
            return _daemon_status_from_payload(json.loads(reply))
        durable = _read_durable_status(self.runtime)
        pid = _read_runtime_pidfile(self.runtime)
        if pid is not None and _process_exists(pid):
            phase = (
                durable.phase
                if durable.phase is WorkspacePhase.STARTING
                else WorkspacePhase.DEGRADED
            )
            return DaemonStatus(
                running=True,
                pid=pid,
                consecutive_failures=0,
                last_error=durable.last_error,
                conn_state=phase.value,
                phase=phase,
                workspace_status=durable,
            )
        if pid is not None or durable.phase is not WorkspacePhase.STOPPED:
            return DaemonStatus(
                running=False,
                pid=pid,
                last_error=durable.last_error or "supervisor process is not running",
                conn_state="failed",
                phase=WorkspacePhase.FAILED,
                workspace_status=durable,
            )
        return DaemonStatus(
            False,
            None,
            phase=WorkspacePhase.STOPPED,
            workspace_status=durable,
        )

    def control_status(self) -> DaemonStatus:
        reply = self._request("status")
        if reply is None:
            raise DaemonError("supervisor control endpoint is unresponsive")
        return _daemon_status_from_payload(json.loads(reply))

    def wait_until_running(self, timeout: float) -> DaemonStatus:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            reply = self._request("status")
            if reply is not None:
                return _daemon_status_from_payload(json.loads(reply))
            time.sleep(0.01)
        raise DaemonError("supervisor did not publish its control endpoint")

    def wait_for_phase(self, phase: WorkspacePhase, timeout: float) -> DaemonStatus:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.status()
            if status.phase is phase:
                return status
            time.sleep(0.01)
        raise DaemonError(f"supervisor did not reach {phase.value}")

    def stop(self) -> bool:
        return self._request("stop") is not None

    def sync(self) -> bool:
        return self._request("sync") is not None

    def resume(self) -> bool:
        return self._request("resume") is not None

    def mutate(self, kind: str, payload: dict[str, object]) -> dict[str, object]:
        if not isinstance(kind, str) or not kind:
            raise ValueError("mutation kind must not be empty")
        request = json.dumps(
            {"kind": kind, "payload": payload},
            separators=(",", ":"),
        )
        reply = self._request(f"mutate {request}", timeout=_MUTATION_TIMEOUT_S)
        if reply is None:
            raise DaemonError("supervisor mutation endpoint is unresponsive")
        decoded = json.loads(reply)
        if not isinstance(decoded, dict) or type(decoded.get("ok")) is not bool:
            raise DaemonError("supervisor mutation response is malformed")
        if not decoded["ok"]:
            error = decoded.get("error")
            raise DaemonError(error if isinstance(error, str) else "supervisor mutation failed")
        result = decoded.get("result")
        if not isinstance(result, dict):
            raise DaemonError("supervisor mutation result is malformed")
        return cast(dict[str, object], result)

    def _request(self, message: str, *, timeout: float = _CONTROL_REQUEST_TIMEOUT_S) -> str | None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect(str(self.runtime.socket))
                _send_line(sock, message)
                reply = _recv_line(sock).strip()
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError):
            return None
        return None if reply == "error" else reply


def meta_dir(local_root: Path) -> Path:
    return _runtime_for_local(local_root).metadata_root


def pidfile_path(local_root: Path) -> Path:
    return _runtime_for_local(local_root).pidfile


def daemon_lock_path(local_root: Path) -> Path:
    return _runtime_for_local(local_root).lockfile


def logfile_path(local_root: Path) -> Path:
    return _runtime_for_local(local_root).logfile


def socket_path(local_root: Path) -> Path:
    return _runtime_for_local(local_root).socket


def daemon_status(local_root: Path) -> DaemonStatus:
    try:
        return SupervisorClient(_runtime_for_local(local_root)).status()
    except DaemonError:
        return DaemonStatus(False, None, phase=WorkspacePhase.STOPPED)


def daemon_control_status(local_root: Path) -> DaemonStatus:
    return SupervisorClient(_runtime_for_local(local_root)).control_status()


def daemon_workspace_status(workspace_id: str) -> WorkspaceStatus:
    """Read full status from the workspace supervisor without scanning the registry."""
    paths = workspace_paths(workspace_id)
    runtime = SupervisorRuntime(
        workspace_id,
        paths.root,
        runtime_dir() / "supervisors",
    )
    client = SupervisorClient(runtime)
    used_fallback = False
    try:
        daemon = client.control_status()
    except DaemonError:
        used_fallback = True
        daemon = client.status()
    if used_fallback:
        return _project_workspace_status(daemon)
    if daemon.workspace_status is None:
        raise DaemonError("supervisor did not publish workspace status")
    return daemon.workspace_status


def _project_workspace_status(daemon: DaemonStatus) -> WorkspaceStatus:
    durable = daemon.workspace_status
    if durable is None:
        return WorkspaceStatus(
            daemon.phase,
            SyncProgress(daemon.phase.value),
            last_error=daemon.last_error,
        )
    return WorkspaceStatus(
        daemon.phase,
        durable.progress,
        pending=durable.pending,
        conflicts=durable.conflicts,
        last_error=daemon.last_error or durable.last_error,
        last_sync_at=durable.last_sync_at,
    )


def wait_for_daemon_control(local_root: Path, timeout: float) -> DaemonStatus:
    client = SupervisorClient(_runtime_for_local(local_root))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = client.control_status()
        except DaemonError:
            time.sleep(0.01)
            continue
        if status.phase is not WorkspacePhase.STARTING:
            return status
        time.sleep(0.01)
    raise DaemonError("supervisor control endpoint is unresponsive")


def poke_daemon(local_root: Path, source: str = "cli") -> bool:
    del source
    return SupervisorClient(_runtime_for_local(local_root)).sync()


def stop_daemon(local_root: Path) -> bool:
    return stop_daemon_result(local_root) is StopResult.STOPPED


def stop_daemon_result(local_root: Path) -> StopResult:
    runtime = _runtime_for_local(local_root)
    client = SupervisorClient(runtime)
    if not client.status().running:
        return StopResult.NOT_RUNNING
    if not client.stop():
        return StopResult.TIMEOUT
    return StopResult.STOPPED if _wait_until_stopped(runtime) else StopResult.TIMEOUT


def ensure_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    runtime = _runtime_for_local(local_root)
    client = SupervisorClient(runtime)
    status = client.status()
    if status.running:
        if status.conn_state == "disconnected":
            client.resume()
            return client.status()
        return status
    return start_daemon(local_root, runner=runner)


def start_daemon(local_root: Path, *, runner: SshRunner | None = None) -> DaemonStatus:
    local_root = local_root.expanduser().resolve()
    runtime = _runtime_for_local(local_root)
    client = SupervisorClient(runtime)
    existing = client.status()
    if existing.running:
        return existing
    runtime.metadata_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    pid = os.fork()
    if pid > 0:
        os.waitpid(pid, 0)
        return client.wait_until_running(_READY_TIMEOUT_S)
    os.setsid()
    pid2 = os.fork()
    if pid2 > 0:
        os._exit(0)
    os.umask(0o077)
    _configure_daemon_logging(runtime.logfile)
    _detach_standard_streams(runtime.logfile)
    try:
        _build_supervisor(local_root, runtime, runner=runner).run()
    except Exception:  # pragma: no cover - real detached process boundary
        logging.getLogger("remote_sandbox.daemon").exception("workspace supervisor crashed")
        os._exit(1)
    os._exit(0)


def _build_supervisor(
    local_root: Path,
    runtime: SupervisorRuntime,
    *,
    runner: SshRunner | None,
) -> WorkspaceSupervisor:
    record = _record_for_local(local_root)
    spec = read_workspace_spec(workspace_paths(record.workspace_id).workspace_file)
    store = WorkspaceStore.open(runtime.state_db)
    ssh_runner = runner or SubprocessSshRunner()

    def build(sync_requested: threading.Event) -> _ProductionComponents:
        remote = RemoteWorkspaceClient(
            cast(object, ssh_runner),  # type: ignore[arg-type]
            target=spec.target,
            workspace_id=spec.workspace_id,
            agent_manager=RemoteAgentManager(ssh_runner),
        )
        policy = StaticPolicyEngine.from_file(
            local_root / POLICY_FILE_NAME,
            large_file_threshold=load_settings().placeholder_limit,
        )
        transport = BatchTransport(
            local_root,
            spec.target,
            spec.remote_root,
            remote,
            runner=ssh_runner,
        )
        engine = SyncEngine(
            store=store,
            local_root=local_root,
            remote=remote,
            transport=transport,
            policy=policy,
        )

        def local_event(kind: EventKind, path: str, destination: str | None) -> None:
            store.append_event("local", kind, path, destination)
            sync_requested.set()

        watcher = create_local_watcher(local_root, policy, local_event)

        def start_local_watcher() -> int:
            watcher.start()
            return store.latest_sequence("local")

        initial = InitialSyncCoordinator(
            store=store,
            local_root=local_root,
            remote=remote,
            transport=transport,
            engine=engine,
            start_local_watcher=start_local_watcher,
            placeholder_limit=load_settings().placeholder_limit,
        )

        def mutate(kind: str, payload: dict[str, object]) -> dict[str, object]:
            if kind == "resolve":
                if set(payload) != {"path", "use_local"}:
                    raise ValueError("resolve mutation payload is malformed")
                path = payload["path"]
                use_local = payload["use_local"]
                if not isinstance(path, str) or type(use_local) is not bool:
                    raise ValueError("resolve mutation payload is malformed")
                resolved = resolve_conflict_transaction(
                    store=store,
                    local_root=local_root,
                    remote=remote,
                    transport=transport,
                    path=path,
                    use_local=use_local,
                )
                return {"path": resolved.path, "conflict_id": resolved.conflict_id}
            if kind == "fetch":
                if set(payload) != {"path", "fetch_all"}:
                    raise ValueError("fetch mutation payload is malformed")
                path = payload["path"]
                fetch_all = payload["fetch_all"]
                if path is not None and not isinstance(path, str):
                    raise ValueError("fetch mutation payload is malformed")
                if type(fetch_all) is not bool:
                    raise ValueError("fetch mutation payload is malformed")
                count, cancelled = fetch_placeholders(
                    local_root=local_root,
                    store=store,
                    remote=remote,
                    transport=transport,
                    path=path,
                    fetch_all=fetch_all,
                    confirm=lambda _prompt: True,
                )
                return {"count": count, "cancelled": cancelled}
            raise ValueError(f"unsupported supervisor mutation: {kind}")

        return _ProductionComponents(remote, engine, initial, watcher, mutate)

    return WorkspaceSupervisor(
        runtime,
        store=store,
        component_factory=build,
        close_store=True,
    )


def _record_for_local(local_root: Path) -> BindingRecord:
    resolved = local_root.expanduser().resolve(strict=False)
    for record in list_binding_records():
        if Path(record.local_path).expanduser().resolve(strict=False) == resolved:
            return record
    raise DaemonError(f"not a bound workspace: {resolved}")


def _runtime_for_local(local_root: Path) -> SupervisorRuntime:
    record = _record_for_local(local_root)
    paths = workspace_paths(record.workspace_id)
    return SupervisorRuntime(
        record.workspace_id,
        paths.root,
        runtime_dir() / "supervisors",
    )


def _configure_daemon_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    log_path.parent.chmod(0o700)
    fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.close(fd)
    log_path.chmod(0o600)
    handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger = logging.getLogger("remote_sandbox.daemon")
    logger.setLevel(logging.INFO)
    for existing in tuple(logger.handlers):
        logger.removeHandler(existing)
        existing.close()
    logger.addHandler(handler)


def _detach_standard_streams(log_path: Path) -> None:
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    if devnull > 2:
        os.close(devnull)
    if log_fd > 2:
        os.close(log_fd)


def _wait_until_stopped(runtime: SupervisorRuntime) -> bool:
    deadline = time.monotonic() + _STOP_TIMEOUT_S
    while time.monotonic() < deadline:
        if (
            not runtime.socket.exists()
            and not runtime.pidfile.exists()
            and _daemon_lock_is_free(runtime.lockfile)
        ):
            return True
        time.sleep(0.05)
    return False


def _daemon_lock_is_free(path: Path) -> bool:
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


def _release_daemon_lock(handle: BinaryIO) -> None:
    with handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _daemon_status_payload(status: DaemonStatus) -> dict[str, object]:
    return {
        "running": status.running,
        "pid": status.pid,
        "consecutive_failures": status.consecutive_failures,
        "last_error": status.last_error,
        "conn_state": status.conn_state,
        "phase": status.phase.value,
        "workspace_status": (
            _workspace_status_payload(status.workspace_status)
            if status.workspace_status is not None
            else None
        ),
    }


def _daemon_status_from_payload(payload: object) -> DaemonStatus:
    if not isinstance(payload, dict):
        raise DaemonError("malformed supervisor status")
    running = payload.get("running")
    pid = payload.get("pid")
    failures = payload.get("consecutive_failures")
    last_error = payload.get("last_error")
    connection_state = payload.get("conn_state")
    phase = payload.get("phase")
    workspace_status_payload = payload.get("workspace_status")
    if (
        type(running) is not bool
        or (pid is not None and type(pid) is not int)
        or type(failures) is not int
        or (last_error is not None and not isinstance(last_error, str))
        or not isinstance(connection_state, str)
        or not isinstance(phase, str)
    ):
        raise DaemonError("malformed supervisor status")
    try:
        parsed_phase = WorkspacePhase(phase)
        workspace_status = (
            _workspace_status_from_payload(workspace_status_payload)
            if workspace_status_payload is not None
            else None
        )
    except (TypeError, ValueError) as exc:
        raise DaemonError("malformed supervisor status") from exc
    return DaemonStatus(
        running,
        pid,
        failures,
        last_error,
        connection_state,
        parsed_phase,
        workspace_status,
    )


def _workspace_status_payload(status: WorkspaceStatus) -> dict[str, object]:
    progress = status.progress
    return {
        "phase": status.phase.value,
        "progress": {
            "stage": progress.stage,
            "files_done": progress.files_done,
            "files_total": progress.files_total,
            "bytes_done": progress.bytes_done,
            "bytes_total": progress.bytes_total,
            "current_path": progress.current_path,
            "elapsed_seconds": progress.elapsed_seconds,
        },
        "pending": status.pending,
        "conflicts": status.conflicts,
        "last_error": status.last_error,
        "last_sync_at": status.last_sync_at,
    }


def _workspace_status_from_payload(payload: object) -> WorkspaceStatus:
    if not isinstance(payload, dict):
        raise DaemonError("malformed workspace status")
    progress = payload.get("progress")
    if not isinstance(progress, dict):
        raise DaemonError("malformed workspace status")
    try:
        return WorkspaceStatus(
            WorkspacePhase(payload["phase"]),
            SyncProgress(
                stage=progress["stage"],
                files_done=progress["files_done"],
                files_total=progress["files_total"],
                bytes_done=progress["bytes_done"],
                bytes_total=progress["bytes_total"],
                current_path=progress["current_path"],
                elapsed_seconds=progress["elapsed_seconds"],
            ),
            pending=payload["pending"],
            conflicts=payload["conflicts"],
            last_error=payload["last_error"],
            last_sync_at=payload["last_sync_at"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DaemonError("malformed workspace status") from exc


def _read_runtime_pidfile(runtime: SupervisorRuntime) -> int | None:
    try:
        return int(runtime.pidfile.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, OSError, ValueError):
        return None


def _read_durable_status(runtime: SupervisorRuntime) -> WorkspaceStatus:
    if not runtime.state_db.exists():
        return WorkspaceStatus(WorkspacePhase.STOPPED, SyncProgress("stopped"))
    with WorkspaceStore.open(runtime.state_db) as store:
        return store.get_status()


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


def _recv_line(sock: socket.socket) -> str:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        newline = chunk.find(b"\n")
        if newline >= 0:
            chunks.append(chunk[:newline])
            total += newline
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > _CONTROL_MAX_LINE_BYTES:
            raise ValueError("control request too large")
    return b"".join(chunks).decode("utf-8")


def _send_line(sock: socket.socket, text: str) -> None:
    sock.sendall(text.encode("utf-8") + b"\n")
