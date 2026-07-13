from __future__ import annotations

import base64
import contextlib
import json
import subprocess
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, BinaryIO, Literal, Protocol, cast

from remote_sandbox.agent import AgentInstall, RemoteAgentManager
from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.remote_protocol import AgentRequest, decode_response, encode_request
from remote_sandbox.ssh import SshRunner
from remote_sandbox.state import AuditSignature


class RemoteProtocolError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RemoteSnapshot:
    entries: dict[str, EntryFingerprint | MissingEntry]
    latest_sequence: int
    signatures: dict[str, AuditSignature] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RemoteExecutionEnvironment:
    available: bool
    refreshed: bool
    export_file: str | None
    captured_at: str | None
    shell: str | None
    path: str | None
    python: str | None
    python3: str | None
    warning: str | None


class _StreamProcess(Protocol):
    stdin: BinaryIO | None
    stdout: BinaryIO | None
    stderr: BinaryIO | None
    returncode: int | None

    def wait(self, timeout: float | None = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


class _AgentRunner(Protocol):
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
    ) -> _StreamProcess: ...


class _AgentManager(Protocol):
    def ensure(self, target: str) -> AgentInstall: ...


class _BoundedStderrDrainer:
    _READ_SIZE = 8192

    def __init__(self, stream: BinaryIO | None, limit: int) -> None:
        self._stream = stream
        self._limit = limit
        self._buffer = bytearray()
        self._thread = threading.Thread(target=self._drain, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def finish(self) -> bytes:
        self._thread.join()
        return bytes(self._buffer)

    def _drain(self) -> None:
        if self._stream is None:
            return
        with contextlib.suppress(OSError, ValueError):
            while chunk := self._stream.read(self._READ_SIZE):
                if len(chunk) >= self._limit:
                    self._buffer[:] = chunk[-self._limit :]
                    continue
                overflow = len(self._buffer) + len(chunk) - self._limit
                if overflow > 0:
                    del self._buffer[:overflow]
                self._buffer.extend(chunk)


class RemoteWorkspaceClient:
    def __init__(
        self,
        runner: _AgentRunner,
        *,
        target: str,
        workspace_id: str,
        agent_path: str | None = None,
        agent_manager: _AgentManager | None = None,
    ) -> None:
        if agent_path is not None and agent_manager is not None:
            raise ValueError("provide either agent_path or agent_manager")
        self._runner = runner
        self._target = target
        self._workspace_id = workspace_id
        self._agent_path = agent_path
        self._agent_manager = agent_manager or (
            RemoteAgentManager(cast(SshRunner, runner)) if agent_path is None else None
        )
        self._subscriptions: set[RemoteEventSubscription] = set()
        self._closed = False

    def ensure_agent(self) -> None:
        self._ensure_agent_path()

    def clear_master(self) -> None:
        cast(SshRunner, self._runner).clear_master(self._target)

    def probe_connection(self) -> Literal["ok", "auth", "network"]:
        return cast(SshRunner, self._runner).probe_connection(self._target)

    def register(self, root: str) -> dict[str, Any]:
        return self._call("register", {"root": root})

    def start_watcher(self) -> dict[str, Any]:
        return self._call("start")

    def stop_watcher(self) -> dict[str, Any]:
        return self._call("stop")

    def status(self) -> dict[str, Any]:
        return self._call("status")

    def execution_environment(self, *, refresh: bool = False) -> RemoteExecutionEnvironment:
        payload = self._call("execution-environment", {"refresh": refresh})
        return _parse_execution_environment(payload)

    def snapshot(self) -> RemoteSnapshot:
        payload = self._call("snapshot")
        raw_entries = payload.get("entries")
        latest_sequence = payload.get("latest_sequence")
        if not isinstance(raw_entries, list) or type(latest_sequence) is not int:
            raise RemoteProtocolError("remote snapshot payload is malformed")
        entries: dict[str, EntryFingerprint | MissingEntry] = {}
        signatures: dict[str, AuditSignature] = {}
        for raw_entry in raw_entries:
            entry = _parse_fingerprint(raw_entry)
            if isinstance(entry, MissingEntry) or entry.path in entries:
                raise RemoteProtocolError("remote snapshot contains an invalid entry")
            entries[entry.path] = entry
            signature = _parse_audit_signature(raw_entry)
            if signature is not None:
                signatures[entry.path] = signature
        return RemoteSnapshot(entries, latest_sequence, signatures)

    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        return self._fingerprint_paths("hash-paths", paths)

    def metadata_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        entries = self._fingerprint_paths("metadata-paths", paths)
        if any(
            isinstance(entry, EntryFingerprint)
            and entry.kind is EntryKind.FILE
            and entry.content_hash is not None
            for entry in entries.values()
        ):
            raise RemoteProtocolError("remote metadata payload contains a regular file hash")
        return entries

    def audit_signatures(
        self,
        paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]:
        requested = [normalize_relative_path(path) for path in paths]
        payload = self._call("metadata-paths", {"paths": requested})
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise RemoteProtocolError("remote audit signature payload is malformed")
        result: dict[str, AuditSignature | None] = {}
        for raw in raw_entries:
            entry = _parse_fingerprint(raw)
            if entry.path is None or entry.path in result:
                raise RemoteProtocolError("remote audit signature payload is malformed")
            result[entry.path] = _parse_audit_signature(raw)
        if set(result) != set(requested):
            raise RemoteProtocolError("remote audit signature payload does not match paths")
        return result

    def observations(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
    ) -> tuple[
        dict[str, EntryFingerprint | MissingEntry],
        dict[str, AuditSignature | None],
    ]:
        requested = [normalize_relative_path(path) for path in paths]
        command = "hash-paths" if with_hash else "metadata-paths"
        payload = self._call(command, {"paths": requested})
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise RemoteProtocolError("remote observation payload is malformed")
        entries: dict[str, EntryFingerprint | MissingEntry] = {}
        signatures: dict[str, AuditSignature | None] = {}
        for raw in raw_entries:
            entry = _parse_fingerprint(raw)
            if entry.path is None or entry.path in entries:
                raise RemoteProtocolError("remote observation payload is malformed")
            entries[entry.path] = entry
            signatures[entry.path] = _parse_audit_signature(raw)
        if set(entries) != set(requested):
            raise RemoteProtocolError("remote observation payload does not match paths")
        return entries, signatures

    def events_after(self, after_sequence: int) -> list[JournalEvent]:
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        if self._closed:
            raise RuntimeError("remote workspace client is closed")
        request = AgentRequest(
            "events",
            {
                "workspace_id": self._workspace_id,
                "after_sequence": after_sequence,
                "follow": False,
            },
        )
        result = self._runner.run_python_file_bytes(
            self._target,
            self._ensure_agent_path(),
            encode_request(request),
            ("events",),
        )
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            if result.stdout.strip():
                try:
                    response = decode_response(result.stdout.strip())
                except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, ValueError):
                    pass
                else:
                    detail = response.error or detail
            raise RemoteProtocolError(detail or "remote event read failed")
        events = [parse_event_line(line) for line in result.stdout.splitlines() if line]
        previous = after_sequence
        for event in events:
            if event.sequence <= previous:
                raise RemoteProtocolError("remote event sequence is not strictly increasing")
            previous = event.sequence
        return events

    def read_path(self, path: str) -> bytes | None:
        normalized = normalize_relative_path(path)
        payload = self._call("read-path", {"path": normalized})
        missing = payload.get("missing")
        data = payload.get("data")
        if missing is True and data is None:
            return None
        if missing is not False or not isinstance(data, str):
            raise RemoteProtocolError("remote path content payload is malformed")
        try:
            return base64.b64decode(data, validate=True)
        except ValueError as exc:
            raise RemoteProtocolError("remote path content payload is malformed") from exc

    def _fingerprint_paths(
        self,
        command: str,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        requested = [normalize_relative_path(path) for path in paths]
        if len(requested) != len(set(requested)):
            raise ValueError("fingerprint paths must be unique")
        payload = self._call(command, {"paths": requested})
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise RemoteProtocolError("remote fingerprint payload is malformed")
        entries: dict[str, EntryFingerprint | MissingEntry] = {}
        for raw_entry in raw_entries:
            entry = _parse_fingerprint(raw_entry)
            path = entry.path
            if path is None or path in entries:
                raise RemoteProtocolError("remote fingerprint payload contains an invalid entry")
            entries[path] = entry
        if set(entries) != set(requested):
            raise RemoteProtocolError("remote fingerprint payload does not match requested paths")
        return entries

    def acknowledge(self, sequence: int) -> int:
        if type(sequence) is not int or sequence < 0:
            raise ValueError("sequence must be a non-negative integer")
        payload = self._call("ack", {"sequence": sequence})
        acknowledged = payload.get("acknowledged_sequence")
        if type(acknowledged) is not int or acknowledged < 0:
            raise RemoteProtocolError("remote acknowledgement payload is malformed")
        for subscription in tuple(self._subscriptions):
            subscription._acknowledged(acknowledged)
        return acknowledged

    def forget(self) -> dict[str, Any]:
        return self._call("forget")

    def subscribe(self, after_sequence: int) -> RemoteEventSubscription:
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        if self._closed:
            raise RuntimeError("remote workspace client is closed")
        subscription = RemoteEventSubscription(self, after_sequence)
        self._subscriptions.add(subscription)
        return subscription

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subscription in tuple(self._subscriptions):
            subscription.close()

    def _call(
        self,
        command: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("remote workspace client is closed")
        request_payload = {"workspace_id": self._workspace_id}
        if payload is not None:
            request_payload.update(payload)
        result = self._runner.run_python_file_bytes(
            self._target,
            self._ensure_agent_path(),
            encode_request(AgentRequest(command, request_payload)),
        )
        try:
            response = decode_response(result.stdout)
        except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, ValueError) as exc:
            raise RemoteProtocolError("remote agent response is malformed") from exc
        if result.returncode != 0 or not response.ok:
            detail = response.error or result.stderr.decode("utf-8", errors="replace").strip()
            raise RemoteProtocolError(detail or f"remote agent command failed: {command}")
        return response.payload

    def _open_event_stream(self, after_sequence: int) -> _StreamProcess:
        request = AgentRequest(
            "events",
            {
                "workspace_id": self._workspace_id,
                "after_sequence": after_sequence,
                "follow": True,
            },
        )
        return self._runner.stream_python_file(
            self._target,
            self._ensure_agent_path(),
            encode_request(request),
            ("events",),
        )

    def _ensure_agent_path(self) -> str:
        if self._agent_path is not None:
            return self._agent_path
        if self._agent_manager is None:
            raise RuntimeError("remote agent manager is unavailable")
        self._agent_path = self._agent_manager.ensure(self._target).remote_path
        return self._agent_path

    def _discard_subscription(self, subscription: RemoteEventSubscription) -> None:
        self._subscriptions.discard(subscription)


class RemoteEventSubscription:
    _INITIAL_REOPEN_DELAY_S = 0.05
    _MAX_REOPEN_DELAY_S = 1.0
    _STDERR_LIMIT = 64 * 1024

    def __init__(self, client: RemoteWorkspaceClient, after_sequence: int) -> None:
        self._client = client
        self._last_acknowledged = after_sequence
        self._process: _StreamProcess | None = None
        self._stderr_drainer: _BoundedStderrDrainer | None = None
        self._closed = False
        self._close_event = threading.Event()
        self._iterating = False
        self._state_lock = threading.Lock()

    def __iter__(self) -> Iterator[JournalEvent]:
        with self._state_lock:
            if self._iterating:
                raise RuntimeError("remote event subscription already has an iterator")
            self._iterating = True
        reopen_delay = self._INITIAL_REOPEN_DELAY_S
        try:
            while True:
                with self._state_lock:
                    if self._closed:
                        return
                    after_sequence = self._last_acknowledged
                process = self._client._open_event_stream(after_sequence)
                drainer = _BoundedStderrDrainer(process.stderr, self._STDERR_LIMIT)
                drainer.start()
                with self._state_lock:
                    if self._closed:
                        publish = False
                    else:
                        self._process = process
                        self._stderr_drainer = drainer
                        publish = True
                if not publish:
                    self._terminate_process(process, drainer)
                    return
                stream = process.stdout
                if stream is None:
                    self._detach_process(process)
                    self._terminate_process(process, drainer)
                    raise RemoteProtocolError("remote event process has no stdout pipe")
                delivered_event = False
                try:
                    for line in stream:
                        with self._state_lock:
                            if self._closed:
                                return
                        delivered_event = True
                        yield parse_event_line(line)
                except (OSError, ValueError):
                    with self._state_lock:
                        if self._closed:
                            return
                    raise
                owned_drainer = self._detach_process(process)
                if owned_drainer is None:
                    with self._state_lock:
                        if self._closed:
                            return
                    raise RuntimeError("remote event process ownership was lost")
                returncode, stderr = self._wait_process(process, owned_drainer)
                if returncode != 0:
                    raise RemoteProtocolError(_stream_failure_detail(stderr, returncode))
                with self._state_lock:
                    if self._closed:
                        return
                if delivered_event:
                    reopen_delay = self._INITIAL_REOPEN_DELAY_S
                if self._wait_before_reopen(reopen_delay):
                    return
                if not delivered_event:
                    reopen_delay = min(reopen_delay * 2, self._MAX_REOPEN_DELAY_S)
        finally:
            active_process, active_drainer = self._take_process()
            if active_process is not None:
                assert active_drainer is not None
                self._terminate_process(active_process, active_drainer)
            with self._state_lock:
                self._iterating = False
                self._closed = True
                self._close_event.set()
            self._client._discard_subscription(self)

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._close_event.set()
            process = self._process
            drainer = self._stderr_drainer
            self._process = None
            self._stderr_drainer = None
        if process is not None:
            assert drainer is not None
            self._terminate_process(process, drainer)
        self._client._discard_subscription(self)

    def _acknowledged(self, sequence: int) -> None:
        with self._state_lock:
            self._last_acknowledged = max(self._last_acknowledged, sequence)

    def _wait_before_reopen(self, delay: float) -> bool:
        return self._close_event.wait(delay)

    def _detach_process(self, process: _StreamProcess) -> _BoundedStderrDrainer | None:
        with self._state_lock:
            if self._process is not process:
                return None
            drainer = self._stderr_drainer
            self._process = None
            self._stderr_drainer = None
            return drainer

    def _take_process(self) -> tuple[_StreamProcess | None, _BoundedStderrDrainer | None]:
        with self._state_lock:
            process = self._process
            drainer = self._stderr_drainer
            self._process = None
            self._stderr_drainer = None
            return process, drainer

    @staticmethod
    def _wait_process(
        process: _StreamProcess,
        drainer: _BoundedStderrDrainer,
    ) -> tuple[int, bytes]:
        try:
            returncode = process.wait()
            stderr = drainer.finish()
            return returncode, stderr
        finally:
            _close_process_pipes(process)

    @staticmethod
    def _terminate_process(
        process: _StreamProcess,
        drainer: _BoundedStderrDrainer,
    ) -> None:
        try:
            if process.stdin is not None:
                with contextlib.suppress(Exception):
                    process.stdin.close()
            if process.returncode is None:
                process.terminate()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1.0)
        finally:
            drainer.finish()
            _close_process_pipes(process)


def _parse_fingerprint(raw: object) -> EntryFingerprint | MissingEntry:
    if not isinstance(raw, dict):
        raise RemoteProtocolError("remote fingerprint must be an object")
    path = raw.get("path")
    kind = raw.get("kind")
    if not isinstance(path, str) or not isinstance(kind, str):
        raise RemoteProtocolError("remote fingerprint path and kind must be strings")
    try:
        if kind == "missing":
            return MissingEntry(path)
        return EntryFingerprint(
            path=path,
            kind=EntryKind(kind),
            size=_optional_integer(raw.get("size"), "size"),
            mtime_ns=_optional_integer(raw.get("mtime_ns"), "mtime_ns"),
            mode=_optional_integer(raw.get("mode"), "mode"),
            link_target=_optional_string(raw.get("link_target"), "link_target"),
            content_hash=_optional_string(raw.get("content_hash"), "content_hash"),
            is_placeholder=False,
        )
    except ValueError as exc:
        raise RemoteProtocolError(f"invalid remote fingerprint: {exc}") from exc


def _parse_execution_environment(payload: object) -> RemoteExecutionEnvironment:
    if not isinstance(payload, dict):
        raise RemoteProtocolError("remote execution environment payload is malformed")
    available = payload.get("available")
    refreshed = payload.get("refreshed")
    if type(available) is not bool or type(refreshed) is not bool:
        raise RemoteProtocolError("remote execution environment payload is malformed")
    values: dict[str, str | None] = {}
    for field_name in (
        "export_file",
        "captured_at",
        "shell",
        "path",
        "python",
        "python3",
        "warning",
    ):
        value = payload.get(field_name)
        if value is not None and not isinstance(value, str):
            raise RemoteProtocolError("remote execution environment payload is malformed")
        values[field_name] = value
    if available != (values["export_file"] is not None):
        raise RemoteProtocolError("remote execution environment payload is malformed")
    return RemoteExecutionEnvironment(
        available=available,
        refreshed=refreshed,
        export_file=values["export_file"],
        captured_at=values["captured_at"],
        shell=values["shell"],
        path=values["path"],
        python=values["python"],
        python3=values["python3"],
        warning=values["warning"],
    )


def _parse_audit_signature(raw: object) -> AuditSignature | None:
    if not isinstance(raw, dict) or raw.get("kind") == "missing":
        return None
    path = raw.get("path")
    kind = raw.get("kind")
    ctime_ns = raw.get("ctime_ns")
    device = raw.get("device")
    inode = raw.get("inode")
    if ctime_ns is None and device is None and inode is None:
        return None
    if (
        not isinstance(path, str)
        or not isinstance(kind, str)
        or type(ctime_ns) is not int
        or type(device) is not int
        or type(inode) is not int
    ):
        raise RemoteProtocolError("remote audit signature is malformed")
    try:
        return AuditSignature(path, EntryKind(kind), ctime_ns, device, inode)
    except ValueError as exc:
        raise RemoteProtocolError(f"invalid remote audit signature: {exc}") from exc


def _close_process_pipes(process: _StreamProcess) -> None:
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None and not stream.closed:
            with contextlib.suppress(Exception):
                stream.close()


def _stream_failure_detail(stderr: bytes, returncode: int) -> str:
    stripped = stderr.strip()
    if stripped:
        try:
            response = decode_response(stripped)
        except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, ValueError):
            detail = stripped.decode("utf-8", errors="replace")
        else:
            detail = response.error or stripped.decode("utf-8", errors="replace")
        if detail:
            return detail
    return f"remote event process failed with exit code {returncode}"


def _optional_integer(value: object, field: str) -> int | None:
    if value is None or type(value) is int:
        return value
    raise RemoteProtocolError(f"remote fingerprint {field} must be an integer or null")


def _optional_string(value: object, field: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise RemoteProtocolError(f"remote fingerprint {field} must be a string or null")


def parse_event_line(line: bytes) -> JournalEvent:
    try:
        raw = json.loads(line.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RemoteProtocolError("remote event line is malformed") from exc
    if not isinstance(raw, dict):
        raise RemoteProtocolError("remote event line is malformed")
    if "ok" in raw:
        error = raw.get("error")
        if raw.get("ok") is False and isinstance(error, str) and error:
            raise RemoteProtocolError(error)
        raise RemoteProtocolError("remote event line is malformed")
    sequence = raw.get("sequence")
    kind = raw.get("kind")
    path = raw.get("path")
    destination_path = raw.get("destination_path")
    if (
        type(sequence) is not int
        or not isinstance(kind, str)
        or not isinstance(path, str)
        or (destination_path is not None and not isinstance(destination_path, str))
    ):
        raise RemoteProtocolError("remote event line is malformed")
    try:
        return JournalEvent(
            side="remote",
            sequence=sequence,
            kind=EventKind(kind),
            path=path,
            destination_path=destination_path,
        )
    except ValueError as exc:
        raise RemoteProtocolError(f"invalid remote event: {exc}") from exc
