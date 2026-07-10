from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import AGENT_VERSION
from .store import (
    RemoteIndexEntry,
    RemoteStore,
    WatcherState,
    process_is_alive,
    validate_workspace_id,
)
from .watcher import WatcherService, snapshot_entries

_HOME_ENV = "CODEX_REMOTE_SANDBOX_HOME"
_HOME_DIRNAME = ".codex-remote-sandbox"


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
    if len(argv) == 3 and argv[0] == "_watch":
        return _run_watcher(argv[1], Path(argv[2]))

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
    root = Path(_expect_string(payload, "root"))
    home = _remote_home()
    metadata = _workspace_directory(home, workspace_id)
    state_path = metadata / "state.sqlite3"
    metadata.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata.chmod(0o700)

    with RemoteStore(state_path) as state:
        workspace = state.register_workspace(workspace_id, root, home=Path.home())
    with RemoteStore(home / "index.sqlite3") as index:
        entry = index.register_index(
            workspace_id,
            workspace.root,
            state_path,
            home=Path.home(),
        )
    return {
        "workspace_id": entry.workspace_id,
        "root": str(entry.root),
        "state_path": str(entry.state_path),
    }


def _handle_start(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    entry = _lookup_workspace(home, workspace_id)
    with RemoteStore(entry.state_path) as store:
        current = store.watcher_state()
        if process_is_alive(current.pid):
            if current.status in {"starting", "running"}:
                return _watcher_payload(current)
            raise RuntimeError("watcher process is already running")

        log_path = entry.state_path.parent / "watcher.log"
        log_path.touch(mode=0o600, exist_ok=True)
        log_path.chmod(0o600)
        with log_path.open("ab", buffering=0) as log:
            process = subprocess.Popen(
                _watcher_command(workspace_id, home),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                close_fds=True,
                start_new_session=True,
            )
        observed = store.watcher_state()
        if observed.pid != process.pid or observed.status != "running":
            observed = store.record_watcher(
                process.pid,
                "starting",
                backend=observed.backend,
            )
        return _watcher_payload(observed)


def _handle_stop(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    entry = _lookup_workspace(_remote_home(), workspace_id)
    with RemoteStore(entry.state_path) as store:
        current = store.watcher_state()
        if not process_is_alive(current.pid):
            stopped = store.record_watcher(None, "stopped", backend=current.backend)
            return _watcher_payload(stopped)
        assert current.pid is not None
        os.kill(current.pid, signal.SIGTERM)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and process_is_alive(current.pid):
            time.sleep(0.05)
        if process_is_alive(current.pid):
            raise RuntimeError("watcher did not stop after SIGTERM")
        stopped = store.record_watcher(None, "stopped", backend=current.backend)
        return _watcher_payload(stopped)


def _handle_status(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    entry = _lookup_workspace(_remote_home(), workspace_id)
    with RemoteStore(entry.state_path) as store:
        state = store.watcher_state()
        if state.status in {"starting", "running"} and not process_is_alive(state.pid):
            state = store.record_watcher(
                None,
                "failed",
                backend=state.backend,
                error="watcher process exited",
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
        entry = _lookup_workspace(_remote_home(), workspace_id)
        cursor = after_sequence
        with RemoteStore(entry.state_path) as store:
            while True:
                events = store.events_after(cursor)
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
                if not follow:
                    return 0
                time.sleep(0.1)
    except BrokenPipeError:
        return 0
    except (KeyError, LookupError, OSError, RuntimeError, ValueError) as exc:
        _write_response(False, {}, str(exc))
        return 2


def _handle_ack(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    sequence = _expect_integer(payload, "sequence")
    entry = _lookup_workspace(_remote_home(), workspace_id)
    with RemoteStore(entry.state_path) as store:
        store.acknowledge(sequence)
        return {"acknowledged_sequence": store.acknowledged_sequence()}


def _handle_snapshot(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    entry = _lookup_workspace(_remote_home(), workspace_id)
    with RemoteStore(entry.state_path) as store:
        workspace = store.workspace()
        return {
            "entries": snapshot_entries(workspace.root),
            "latest_sequence": store.latest_sequence(),
        }


def _handle_forget(payload: dict[str, Any]) -> dict[str, object]:
    workspace_id = validate_workspace_id(_expect_string(payload, "workspace_id"))
    home = _remote_home()
    entry = _lookup_workspace(home, workspace_id)
    with RemoteStore(entry.state_path) as store:
        watcher = store.watcher_state()
        if process_is_alive(watcher.pid):
            raise RuntimeError("cannot forget a workspace while its watcher is running")

    metadata = entry.state_path.parent
    tombstone = metadata.with_name(f".forget-{workspace_id}-{os.getpid()}")
    if tombstone.exists():
        raise RuntimeError("stale remote forget transaction exists")
    os.replace(metadata, tombstone)
    try:
        with RemoteStore(home / "index.sqlite3") as index:
            index.remove_index(workspace_id)
    except BaseException:
        os.replace(tombstone, metadata)
        raise
    shutil.rmtree(tombstone)
    return {"workspace_id": workspace_id, "forgotten": True}


def _run_watcher(workspace_id: str, home: Path) -> int:
    service: WatcherService | None = None
    stop_requested = threading.Event()

    def request_stop(_signum: int, _frame: object | None) -> None:
        stop_requested.set()
        if service is not None:
            service.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        entry = _lookup_workspace(home, validate_workspace_id(workspace_id))
        with RemoteStore(entry.state_path) as store:
            store.record_watcher(os.getpid(), "starting", backend=None)
            root = store.workspace().root
            service = WatcherService(root, store)
            if stop_requested.is_set():
                service.stop()
            service.run()
        return 0
    except BaseException as exc:
        try:
            entry = _lookup_workspace(home, workspace_id)
            with RemoteStore(entry.state_path) as store:
                store.record_watcher(None, "failed", backend=None, error=str(exc))
        except BaseException:
            pass
        return 1


def _remote_home() -> Path:
    configured = os.environ.get(_HOME_ENV)
    raw = Path(configured).expanduser() if configured else Path.home() / _HOME_DIRNAME
    home = raw.resolve(strict=False)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    home.chmod(0o700)
    return home


def _workspace_directory(home: Path, workspace_id: str) -> Path:
    return home / "workspaces" / validate_workspace_id(workspace_id)


def _lookup_workspace(home: Path, workspace_id: str) -> RemoteIndexEntry:
    with RemoteStore(home / "index.sqlite3") as index:
        entry = index.index_entry(workspace_id)
    if entry is None:
        raise LookupError("remote workspace is not registered")
    return entry


def _watcher_command(workspace_id: str, home: Path) -> list[str]:
    executable = Path(sys.argv[0]).resolve(strict=False)
    if executable.suffix == ".pyz":
        return [sys.executable, str(executable), "_watch", workspace_id, str(home)]
    package = __package__ or "remote_agent"
    return [sys.executable, "-m", package, "_watch", workspace_id, str(home)]


def _watcher_payload(state: WatcherState) -> dict[str, object]:
    return {
        "pid": state.pid,
        "status": state.status,
        "backend": state.backend,
        "started_at": state.started_at,
        "heartbeat_at": state.heartbeat_at,
        "error": state.error,
    }


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
