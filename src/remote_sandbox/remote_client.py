from __future__ import annotations

import contextlib
import json
import subprocess
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol, cast

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


class RemoteProtocolError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RemoteSnapshot:
    entries: dict[str, EntryFingerprint | MissingEntry]
    latest_sequence: int


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
        if agent_path is None:
            manager = agent_manager or RemoteAgentManager(cast(SshRunner, runner))
            agent_path = manager.ensure(target).remote_path
        self._runner = runner
        self._target = target
        self._workspace_id = workspace_id
        self._agent_path = agent_path
        self._subscriptions: set[RemoteEventSubscription] = set()
        self._closed = False

    def register(self, root: str) -> dict[str, Any]:
        return self._call("register", {"root": root})

    def start_watcher(self) -> dict[str, Any]:
        return self._call("start")

    def stop_watcher(self) -> dict[str, Any]:
        return self._call("stop")

    def status(self) -> dict[str, Any]:
        return self._call("status")

    def snapshot(self) -> RemoteSnapshot:
        payload = self._call("snapshot")
        raw_entries = payload.get("entries")
        latest_sequence = payload.get("latest_sequence")
        if not isinstance(raw_entries, list) or type(latest_sequence) is not int:
            raise RemoteProtocolError("remote snapshot payload is malformed")
        entries: dict[str, EntryFingerprint | MissingEntry] = {}
        for raw_entry in raw_entries:
            entry = _parse_fingerprint(raw_entry)
            if isinstance(entry, MissingEntry) or entry.path in entries:
                raise RemoteProtocolError("remote snapshot contains an invalid entry")
            entries[entry.path] = entry
        return RemoteSnapshot(entries, latest_sequence)

    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        requested = [normalize_relative_path(path) for path in paths]
        if len(requested) != len(set(requested)):
            raise ValueError("hash paths must be unique")
        payload = self._call("hash-paths", {"paths": requested})
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise RemoteProtocolError("remote hash payload is malformed")
        entries: dict[str, EntryFingerprint | MissingEntry] = {}
        for raw_entry in raw_entries:
            entry = _parse_fingerprint(raw_entry)
            path = entry.path
            if path is None or path in entries:
                raise RemoteProtocolError("remote hash payload contains an invalid entry")
            entries[path] = entry
        if set(entries) != set(requested):
            raise RemoteProtocolError("remote hash payload does not match requested paths")
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
            self._agent_path,
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
            self._agent_path,
            encode_request(request),
            ("events",),
        )

    def _discard_subscription(self, subscription: RemoteEventSubscription) -> None:
        self._subscriptions.discard(subscription)


class RemoteEventSubscription:
    _INITIAL_REOPEN_DELAY_S = 0.05
    _MAX_REOPEN_DELAY_S = 1.0

    def __init__(self, client: RemoteWorkspaceClient, after_sequence: int) -> None:
        self._client = client
        self._last_acknowledged = after_sequence
        self._process: _StreamProcess | None = None
        self._closed = False
        self._close_event = threading.Event()
        self._iterating = False

    def __iter__(self) -> Iterator[JournalEvent]:
        if self._iterating:
            raise RuntimeError("remote event subscription already has an iterator")
        self._iterating = True
        reopen_delay = self._INITIAL_REOPEN_DELAY_S
        try:
            while not self._closed:
                process = self._client._open_event_stream(self._last_acknowledged)
                self._process = process
                stream = process.stdout
                if stream is None:
                    self._terminate_process(process)
                    raise RemoteProtocolError("remote event process has no stdout pipe")
                delivered_event = False
                for line in stream:
                    if self._closed:
                        return
                    delivered_event = True
                    yield parse_event_line(line)
                returncode, stderr = self._wait_process(process)
                if self._process is process:
                    self._process = None
                if returncode != 0:
                    raise RemoteProtocolError(_stream_failure_detail(stderr, returncode))
                if self._closed:
                    return
                if delivered_event:
                    reopen_delay = self._INITIAL_REOPEN_DELAY_S
                if self._wait_before_reopen(reopen_delay):
                    return
                if not delivered_event:
                    reopen_delay = min(reopen_delay * 2, self._MAX_REOPEN_DELAY_S)
        finally:
            active_process = self._process
            if active_process is not None:
                self._terminate_process(active_process)
                self._process = None
            self._iterating = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._close_event.set()
        process = self._process
        if process is not None:
            self._terminate_process(process)
            self._process = None
        self._client._discard_subscription(self)

    def _acknowledged(self, sequence: int) -> None:
        self._last_acknowledged = max(self._last_acknowledged, sequence)

    def _wait_before_reopen(self, delay: float) -> bool:
        return self._close_event.wait(delay)

    @staticmethod
    def _wait_process(process: _StreamProcess) -> tuple[int, bytes]:
        try:
            returncode = process.wait()
            stderr = b"" if process.stderr is None else process.stderr.read()
            return returncode, stderr
        finally:
            _close_process_pipes(process)

    @staticmethod
    def _terminate_process(process: _StreamProcess) -> None:
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
