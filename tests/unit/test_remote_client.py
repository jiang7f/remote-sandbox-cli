from __future__ import annotations

import io
import json
import subprocess
from typing import Any

import pytest

from remote_sandbox.agent import AgentInstall
from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import EntryKind, MissingEntry
from remote_sandbox.remote_client import (
    RemoteProtocolError,
    RemoteSnapshot,
    RemoteWorkspaceClient,
    parse_event_line,
)
from remote_sandbox.remote_protocol import decode_request


class RecordingRunner:
    def __init__(self, responses: dict[str, dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, bytes, tuple[str, ...]]] = []

    def run_python_file_bytes(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        self.calls.append((target, path, input_data, args))
        request = decode_request(input_data)
        response = {
            "ok": True,
            "payload": self.responses[request.command],
        }
        return subprocess.CompletedProcess(
            ["ssh"],
            0,
            json.dumps(response, separators=(",", ":")).encode() + b"\n",
            b"",
        )


class StreamingProcess:
    def __init__(self, stdout: bytes) -> None:
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO()
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.returncode = 0
        return 0

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


class StreamingRunner(RecordingRunner):
    def __init__(
        self,
        responses: dict[str, dict[str, Any]],
        streams: list[StreamingProcess],
    ) -> None:
        super().__init__(responses)
        self.streams = streams
        self.stream_calls: list[tuple[str, str, bytes, tuple[str, ...]]] = []

    def stream_python_file(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> StreamingProcess:
        self.stream_calls.append((target, path, input_data, args))
        return self.streams.pop(0)


class RawRunner:
    def __init__(self, result: subprocess.CompletedProcess[bytes]) -> None:
        self.result = result

    def run_python_file_bytes(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        del target, path, input_data, args
        return self.result


class RecordingAgentManager:
    def __init__(self, install: AgentInstall) -> None:
        self.install = install
        self.calls: list[str] = []

    def ensure(self, target: str) -> AgentInstall:
        self.calls.append(target)
        return self.install


def test_remote_event_line_decodes_sequence_and_unicode_path() -> None:
    event = parse_event_line(
        b'{"sequence":7,"kind":"delete","path":"\xe7\xae\x97\xe6\xb3\x95.py",'
        b'"destination_path":null}\n'
    )
    assert event.sequence == 7
    assert event.kind is EventKind.DELETE
    assert event.path == "算法.py"


def test_remote_client_lifecycle_calls_use_structured_workspace_requests() -> None:
    runner = RecordingRunner(
        {
            "register": {"workspace_id": "w1", "root": "/home/u/算法"},
            "start": {"status": "running", "pid": 41},
            "status": {"status": "running", "latest_sequence": 3},
            "stop": {"status": "stopped", "pid": None},
            "ack": {"acknowledged_sequence": 3},
            "forget": {"workspace_id": "w1", "forgotten": True},
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    assert client.register("/home/u/算法")["root"] == "/home/u/算法"
    assert client.start_watcher()["status"] == "running"
    assert client.status()["latest_sequence"] == 3
    assert client.stop_watcher()["status"] == "stopped"
    assert client.acknowledge(3) == 3
    assert client.forget()["forgotten"] is True

    requests = [decode_request(call[2]) for call in runner.calls]
    assert [request.command for request in requests] == [
        "register",
        "start",
        "status",
        "stop",
        "ack",
        "forget",
    ]
    assert requests[0].payload == {"workspace_id": "w1", "root": "/home/u/算法"}
    assert [request.payload for request in requests[1:]] == [
        {"workspace_id": "w1"},
        {"workspace_id": "w1"},
        {"workspace_id": "w1"},
        {"workspace_id": "w1", "sequence": 3},
        {"workspace_id": "w1"},
    ]


def test_remote_client_ensures_versioned_agent_when_path_is_not_supplied() -> None:
    runner = RecordingRunner({"status": {"status": "stopped"}})
    manager = RecordingAgentManager(
        AgentInstall(
            version="0.2.0-dev",
            remote_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
            sha256="digest",
        )
    )

    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_manager=manager,
    )

    assert client.status()["status"] == "stopped"
    assert manager.calls == ["example-host"]
    assert runner.calls[0][1] == manager.install.remote_path


def test_remote_snapshot_parses_metadata_fingerprints_and_sequence() -> None:
    runner = RecordingRunner(
        {
            "snapshot": {
                "latest_sequence": 12,
                "entries": [
                    {
                        "path": "src/main.py",
                        "kind": "file",
                        "size": 9,
                        "mtime_ns": 17,
                        "mode": 33188,
                        "link_target": None,
                    },
                    {
                        "path": "current",
                        "kind": "symlink",
                        "size": None,
                        "mtime_ns": 18,
                        "mode": 41471,
                        "link_target": "src/main.py",
                    },
                    {
                        "path": "src",
                        "kind": "dir",
                        "size": None,
                        "mtime_ns": 19,
                        "mode": 16877,
                        "link_target": None,
                    },
                    {
                        "path": "socket",
                        "kind": "special",
                        "size": None,
                        "mtime_ns": 20,
                        "mode": 49645,
                        "link_target": None,
                    },
                ],
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    snapshot = client.snapshot()

    assert isinstance(snapshot, RemoteSnapshot)
    assert snapshot.latest_sequence == 12
    assert snapshot.entries["src/main.py"].kind is EntryKind.FILE
    assert snapshot.entries["src/main.py"].content_hash is None
    assert snapshot.entries["current"].kind is EntryKind.SYMLINK
    assert snapshot.entries["current"].link_target == "src/main.py"
    assert snapshot.entries["src"].kind is EntryKind.DIR
    assert snapshot.entries["socket"].kind is EntryKind.SPECIAL


def test_remote_hash_paths_requests_and_returns_only_selected_paths() -> None:
    runner = RecordingRunner(
        {
            "hash-paths": {
                "entries": [
                    {
                        "path": "requested.txt",
                        "kind": "file",
                        "size": 7,
                        "mtime_ns": 21,
                        "mode": 33188,
                        "link_target": None,
                        "content_hash": "file-digest",
                    },
                    {
                        "path": "link",
                        "kind": "symlink",
                        "size": 13,
                        "mtime_ns": 22,
                        "mode": 41471,
                        "link_target": "requested.txt",
                        "content_hash": "link-digest",
                    },
                    {
                        "path": "missing.txt",
                        "kind": "missing",
                        "size": None,
                        "mtime_ns": None,
                        "mode": None,
                        "link_target": None,
                        "content_hash": None,
                    },
                ]
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    entries = client.hash_paths(["requested.txt", "link", "missing.txt"])

    assert entries["requested.txt"].content_hash == "file-digest"
    assert entries["link"].kind is EntryKind.SYMLINK
    assert entries["link"].content_hash == "link-digest"
    assert entries["missing.txt"] == MissingEntry("missing.txt")
    requests = [decode_request(call[2]) for call in runner.calls]
    assert [(request.command, request.payload) for request in requests] == [
        (
            "hash-paths",
            {
                "workspace_id": "w1",
                "paths": ["requested.txt", "link", "missing.txt"],
            },
        )
    ]


def test_subscription_restarts_after_last_acknowledged_sequence() -> None:
    first_process = StreamingProcess(
        b'{"sequence":5,"kind":"modify","path":"model.py","destination_path":null}\n'
    )
    second_process = StreamingProcess(
        b'{"sequence":6,"kind":"delete","path":"old.py","destination_path":null}\n'
    )
    runner = StreamingRunner(
        {"ack": {"acknowledged_sequence": 5}},
        [first_process, second_process],
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )
    subscription = client.subscribe(after_sequence=0)
    events = iter(subscription)

    assert next(events).sequence == 5
    assert client.acknowledge(5) == 5
    assert next(events).sequence == 6

    requests = [decode_request(call[2]) for call in runner.stream_calls]
    assert [(request.command, request.payload) for request in requests] == [
        ("events", {"workspace_id": "w1", "after_sequence": 0, "follow": True}),
        ("events", {"workspace_id": "w1", "after_sequence": 5, "follow": True}),
    ]
    assert [call[3] for call in runner.stream_calls] == [("events",), ("events",)]
    subscription.close()
    assert first_process.returncode == 0
    assert second_process.terminated


def test_malformed_foreground_response_raises_protocol_error() -> None:
    runner = RawRunner(subprocess.CompletedProcess(["ssh"], 0, b"not-json\n", b""))
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.codex-remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="malformed"):
        client.status()


def test_structured_stream_error_raises_protocol_error() -> None:
    line = b'{"ok":false,"payload":{},"error":"workspace is not registered"}\n'

    with pytest.raises(RemoteProtocolError, match="workspace is not registered"):
        parse_event_line(line)
