from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from . import AGENT_VERSION
from .store import (
    RemoteIndexEntry,
    RemoteStore,
    RemoteWorkspace,
    WatcherState,
    process_is_alive,
    validate_workspace_id,
    validate_workspace_root,
)
from .watcher import WatcherService, snapshot_entries

_HOME_ENV = "CODEX_REMOTE_SANDBOX_HOME"
_HOME_DIRNAME = ".codex-remote-sandbox"
_RUNTIME_ENV = "CODEX_REMOTE_SANDBOX_RUNTIME_DIR"
_RUNTIME_PREFIX = "codex-remote-sandbox"
_EVENT_BATCH_SIZE = 256


def _archive_sha256() -> str:
    digest = hashlib.sha256()
    with Path(sys.argv[0]).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        print("codex-remote-sandbox-agent " + AGENT_VERSION + " " + _archive_sha256())
        return 0
    if len(argv) == 4 and argv[0] == "_watch":
        return _run_watcher(argv[1], Path(argv[2]), argv[3])

    try:
        request = json.loads(sys.stdin.buffer.readline().decode("utf-8"))
        if not isinstance(request, dict):
            raise ValueError("agent request must be a JSON object")
        command = _expect_string(request, "command")
        payload = request.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("agent payload must be a JSON object")
        if command == "events":
            return _handle_events(payload)
        handler = _HANDLERS.get(command)
        if handler is None:
            raise ValueError("unsupported command: " + command)
        response = handler(payload)
    except (json.JSONDecodeError, KeyError, LookupError, OSError, RuntimeError, ValueError) as exc:
        _write_response(False, {}, str(exc))
        return 2

    _write_response(True, response, None)
    return 0


def _handle_register(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    root = validate_workspace_root(
        Path(_expect_string(payload, "root")),
        home=Path.home(),
    )
    home = _remote_home()
    if home == root or home in root.parents or root in home.parents:
        raise ValueError("workspace root and remote metadata home must not overlap")
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    home.chmod(0o700)
    metadata = _workspace_directory(home, workspace_id)
    state_path = metadata / "state.sqlite3"
    with (
        _exclusive_lock(_index_lock_path()),
        RemoteStore(home / "index.sqlite3") as index,
    ):
        by_id = index.index_entry(workspace_id)
        by_root = index.workspace_for_root(root)
        expected = RemoteIndexEntry(workspace_id, root, state_path)
        if by_id not in {None, expected} or by_root not in {None, expected}:
            raise ValueError("remote root or workspace is already registered")

        metadata_existed = metadata.exists()
        try:
            metadata.mkdir(parents=True, exist_ok=True, mode=0o700)
            metadata.chmod(0o700)
            with RemoteStore(state_path) as state:
                workspace = state.register_workspace(workspace_id, root, home=Path.home())
            entry = index.register_index(
                workspace_id,
                workspace.root,
                state_path,
                home=Path.home(),
            )
        except BaseException:
            if not metadata_existed:
                shutil.rmtree(metadata, ignore_errors=True)
            raise
    return {
        "workspace_id": entry.workspace_id,
        "root": str(entry.root),
        "state_path": str(entry.state_path),
    }


def _handle_start(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    with _exclusive_lock(_workspace_lock_path(workspace_id)):
        entry = _lookup_workspace(home, workspace_id)
        with RemoteStore(entry.state_path) as store:
            _require_workspace(store, entry)
            current = store.watcher_state()
            identity = _watcher_identity(current, workspace_id)
            if identity == "current":
                if current.status in {"starting", "running"}:
                    return _watcher_payload(current)
                raise RuntimeError("watcher process is already running")
            if identity == "unknown":
                raise RuntimeError("cannot verify the recorded watcher process")

            token = secrets.token_hex(24)
            log_path = _watcher_log_path(workspace_id)
            log_path.touch(mode=0o600, exist_ok=True)
            log_path.chmod(0o600)
            with log_path.open("ab", buffering=0) as log:
                process = subprocess.Popen(
                    _watcher_command(workspace_id, home, token),
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    close_fds=True,
                    start_new_session=True,
                )
            try:
                observed = store.record_watcher(
                    process.pid,
                    "starting",
                    backend=None,
                    token=token,
                )
            except BaseException:
                process.terminate()
                process.wait(timeout=5)
                raise
            return _watcher_payload(observed)


def _handle_stop(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    with _exclusive_lock(_workspace_lock_path(workspace_id)):
        entry = _lookup_workspace(home, workspace_id)
        with RemoteStore(entry.state_path) as store:
            _require_workspace(store, entry)
            current = store.watcher_state()
            identity = _watcher_identity(current, workspace_id)
            if identity == "dead":
                stopped = _record_generation_state(store, current, None, "stopped")
                return _watcher_payload(stopped)
            if identity == "mismatch":
                _record_generation_state(
                    store,
                    current,
                    None,
                    "failed",
                    error="recorded watcher pid belongs to another process",
                )
                raise RuntimeError("recorded watcher pid belongs to another process")
            if identity == "unknown":
                raise RuntimeError("cannot verify the recorded watcher process")

            assert current.pid is not None and current.token is not None
            os.kill(current.pid, signal.SIGTERM)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if _watcher_identity(current, workspace_id) != "current":
                    break
                time.sleep(0.05)
            if _watcher_identity(current, workspace_id) == "current":
                raise RuntimeError("watcher did not stop after SIGTERM")
            stopped = _record_generation_state(store, current, None, "stopped")
            return _watcher_payload(stopped)


def _handle_status(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    with _exclusive_lock(_workspace_lock_path(workspace_id)):
        entry = _lookup_workspace(home, workspace_id)
        with RemoteStore(entry.state_path) as store:
            _require_workspace(store, entry)
            state = store.watcher_state()
            identity = _watcher_identity(state, workspace_id)
            if state.status in {"starting", "running"} and identity in {"dead", "mismatch"}:
                reason = (
                    "watcher process exited"
                    if identity == "dead"
                    else "recorded watcher process identity does not match"
                )
                state = _record_generation_state(
                    store,
                    state,
                    None,
                    "failed",
                    error=reason,
                )
            payload_result = _watcher_payload(state)
            payload_result["latest_sequence"] = store.latest_sequence()
            payload_result["acknowledged_sequence"] = store.acknowledged_sequence()
            return payload_result


def _handle_events(payload: dict[str, Any]) -> int:
    try:
        workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
        after_sequence = _expect_integer(payload, "after_sequence", default=0)
        follow = _expect_boolean(payload, "follow", default=True)
        home = _remote_home()
        cursor = after_sequence
        with _exclusive_lock(_workspace_lock_path(workspace_id)):
            entry = _lookup_workspace(home, workspace_id)
            store = RemoteStore(entry.state_path)
            try:
                _require_workspace(store, entry)
            except BaseException:
                store.close()
                raise
        try:
            while True:
                events = store.events_after(cursor, limit=_EVENT_BATCH_SIZE)
                for event in events:
                    line = {
                        "sequence": event.sequence,
                        "kind": event.kind,
                        "path": event.path,
                        "destination_path": event.destination_path,
                    }
                    sys.stdout.write(
                        json.dumps(line, ensure_ascii=False, separators=(",", ":")) + "\n"
                    )
                    sys.stdout.flush()
                    cursor = event.sequence
                if events:
                    continue
                if not follow:
                    return 0
                time.sleep(0.1)
        finally:
            store.close()
    except BrokenPipeError:
        return 0
    except (KeyError, LookupError, OSError, RuntimeError, ValueError) as exc:
        _write_response(False, {}, str(exc))
        return 2


def _handle_ack(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    sequence = _expect_integer(payload, "sequence")
    home = _remote_home()
    with _exclusive_lock(_workspace_lock_path(workspace_id)):
        entry = _lookup_workspace(home, workspace_id)
        with RemoteStore(entry.state_path) as store:
            _require_workspace(store, entry)
            store.acknowledge(sequence)
            return {"acknowledged_sequence": store.acknowledged_sequence()}


def _handle_snapshot(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    with _exclusive_lock(_workspace_lock_path(workspace_id)):
        entry = _lookup_workspace(home, workspace_id)
        with RemoteStore(entry.state_path) as store:
            workspace = _require_workspace(store, entry)
            return {
                "entries": snapshot_entries(workspace.root),
                "latest_sequence": store.latest_sequence(),
            }


def _handle_forget(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    with (
        _exclusive_lock(_index_lock_path()),
        _exclusive_lock(_workspace_lock_path(workspace_id)),
    ):
        entry = _lookup_workspace(home, workspace_id)
        metadata = entry.state_path.parent
        with RemoteStore(entry.state_path) as store:
            _require_workspace(store, entry)
            watcher = store.watcher_state()
            identity = _watcher_identity(watcher, workspace_id)
            if identity in {"current", "unknown"}:
                raise RuntimeError("cannot forget a workspace while its watcher may be running")

        tombstone = metadata.with_name(f".forget-{workspace_id}-{os.getpid()}")
        if tombstone.exists():
            raise RuntimeError("stale remote forget transaction exists")
        os.replace(metadata, tombstone)
        try:
            with RemoteStore(home / "index.sqlite3") as index:
                index.remove_index(workspace_id)
            shutil.rmtree(tombstone)
        except BaseException:
            if tombstone.exists():
                os.replace(tombstone, metadata)
            with RemoteStore(home / "index.sqlite3") as index:
                index.register_index(
                    entry.workspace_id,
                    entry.root,
                    entry.state_path,
                    home=Path.home(),
                )
            raise
        _watcher_log_path(workspace_id).unlink(missing_ok=True)
        return {"workspace_id": workspace_id, "forgotten": True}


def _run_watcher(workspace_id: str, home: Path, token: str) -> int:
    service: WatcherService | None = None
    stop_requested = threading.Event()

    def request_stop(_signum: int, _frame: object | None) -> None:
        stop_requested.set()
        if service is not None:
            service.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        safe_workspace_id = validate_workspace_id(workspace_id)
        with _exclusive_lock(_workspace_lock_path(safe_workspace_id)):
            entry = _lookup_workspace(home, safe_workspace_id)
            with RemoteStore(entry.state_path) as store:
                workspace = _require_workspace(store, entry)
                store.record_watcher_for_generation(
                    os.getpid(),
                    "starting",
                    backend=None,
                    token=token,
                )
        with RemoteStore(entry.state_path) as store:
            service = WatcherService(workspace.root, store, token=token)
            if stop_requested.is_set():
                service.stop()
            service.run()
        return 0
    except BaseException as exc:
        try:
            with _exclusive_lock(_workspace_lock_path(workspace_id)):
                entry = _lookup_workspace(home, workspace_id)
                with RemoteStore(entry.state_path) as store:
                    _require_workspace(store, entry)
                    state = store.watcher_state()
                    if state.token == token and state.status != "failed":
                        store.record_watcher_for_generation(
                            None,
                            "failed",
                            backend=state.backend,
                            token=token,
                            error=str(exc),
                        )
        except BaseException:
            pass
        return 1


def _remote_home() -> Path:
    configured = os.environ.get(_HOME_ENV)
    raw = Path(configured).expanduser() if configured else Path.home() / _HOME_DIRNAME
    return raw.resolve(strict=False)


def _workspace_directory(home: Path, workspace_id: str) -> Path:
    return home / "workspaces" / validate_workspace_id(workspace_id)


def _runtime_root() -> Path:
    configured = os.environ.get(_RUNTIME_ENV)
    root = (
        Path(configured).expanduser()
        if configured
        else Path("/tmp") / f"{_RUNTIME_PREFIX}-{os.getuid()}"
    )
    if not root.is_absolute():
        raise ValueError("remote runtime directory must be absolute")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    return root


def _workspace_runtime(workspace_id: str) -> Path:
    workspaces = _runtime_root() / "workspaces"
    workspaces.mkdir(parents=True, exist_ok=True, mode=0o700)
    workspaces.chmod(0o700)
    workspace = workspaces / validate_workspace_id(workspace_id)
    workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
    workspace.chmod(0o700)
    return workspace


def _index_lock_path() -> Path:
    return _runtime_root() / "index.lock"


def _workspace_lock_path(workspace_id: str) -> Path:
    return _workspace_runtime(workspace_id) / "control.lock"


def _watcher_log_path(workspace_id: str) -> Path:
    return _workspace_runtime(workspace_id) / "watcher.log"


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    path.touch(mode=0o600, exist_ok=True)
    path.chmod(0o600)
    with path.open("rb") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _lookup_workspace(home: Path, workspace_id: str) -> RemoteIndexEntry:
    with RemoteStore(home / "index.sqlite3") as index:
        entry = index.index_entry(workspace_id)
    if entry is None:
        raise LookupError("remote workspace is not registered")
    expected_state = _workspace_directory(home, workspace_id) / "state.sqlite3"
    if entry.state_path != expected_state:
        raise RuntimeError("protected index contains an invalid workspace state path")
    return entry


def _require_workspace(store: RemoteStore, entry: RemoteIndexEntry) -> RemoteWorkspace:
    workspace = store.workspace()
    if workspace.workspace_id != entry.workspace_id or workspace.root != entry.root:
        raise RuntimeError("workspace state disagrees with the protected index")
    return workspace


def _watcher_command(workspace_id: str, home: Path, token: str) -> list[str]:
    executable = Path(sys.argv[0]).resolve(strict=False)
    if executable.suffix == ".pyz":
        return [sys.executable, str(executable), "_watch", workspace_id, str(home), token]
    package = __package__ or "remote_agent"
    return [sys.executable, "-m", package, "_watch", workspace_id, str(home), token]


def _watcher_identity(state: WatcherState, workspace_id: str) -> str:
    if not process_is_alive(state.pid):
        return "dead"
    if state.pid is None or state.token is None:
        return "unknown"
    arguments = _process_arguments(state.pid)
    if arguments is None:
        return "unknown"
    required = {"_watch", workspace_id, state.token}
    return "current" if required <= set(arguments) else "mismatch"


def _process_arguments(pid: int) -> list[str] | None:
    proc_cmdline = Path("/proc") / str(pid) / "cmdline"
    try:
        if proc_cmdline.exists():
            return [
                part.decode("utf-8", errors="surrogateescape")
                for part in proc_cmdline.read_bytes().split(b"\0")
                if part
            ]
        result = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return shlex.split(result.stdout.strip())
    except ValueError:
        return None


def _watcher_payload(state: WatcherState) -> dict[str, object]:
    return {
        "pid": state.pid,
        "status": state.status,
        "backend": state.backend,
        "started_at": state.started_at,
        "heartbeat_at": state.heartbeat_at,
        "error": state.error,
    }


def _record_generation_state(
    store: RemoteStore,
    current: WatcherState,
    pid: int | None,
    status: str,
    *,
    error: str | None = None,
) -> WatcherState:
    if current.token is None:
        return store.record_watcher(
            pid,
            status,
            backend=current.backend,
            error=error,
        )
    return store.record_watcher_for_generation(
        pid,
        status,
        backend=current.backend,
        token=current.token,
        error=error,
    )


def _write_response(ok: bool, payload: dict[str, object], error: str | None) -> None:
    response: dict[str, object] = {"ok": ok, "payload": payload}
    if error is not None:
        response["error"] = error
    print(json.dumps(response, ensure_ascii=False, separators=(",", ":")))


def _expect_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _expect_integer(payload: dict[str, Any], key: str, *, default: int | None = None) -> int:
    value = payload.get(key, default)
    if type(value) is not int or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _expect_boolean(payload: dict[str, Any], key: str, *, default: bool) -> bool:
    value = payload.get(key, default)
    if type(value) is not bool:
        raise ValueError(f"{key} must be a boolean")
    return value


_HANDLERS: dict[str, Callable[[dict[str, Any]], dict[str, object]]] = {
    "register": _handle_register,
    "start": _handle_start,
    "stop": _handle_stop,
    "status": _handle_status,
    "ack": _handle_ack,
    "snapshot": _handle_snapshot,
    "forget": _handle_forget,
}


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
