from __future__ import annotations

import io
import json
import subprocess
import sys
import threading
from typing import Any

import pytest

from remote_sandbox.agent import AgentInstall
from remote_sandbox.journal import EventKind
from remote_sandbox.manifest import EntryKind, MissingEntry
from remote_sandbox.remote_client import (
    RemoteExecutionEnvironment,
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
    def __init__(
        self,
        stdout: bytes,
        *,
        stderr: bytes = b"",
        exit_code: int = 0,
    ) -> None:
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self.returncode: int | None = None
        self.exit_code = exit_code
        self.terminated = False
        self.killed = False
        self.wait_calls: list[float | None] = []

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls.append(timeout)
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode

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


class LaunchBarrierRunner(StreamingRunner):
    def __init__(self, process: StreamingProcess) -> None:
        super().__init__({}, [process])
        self.process_created = threading.Event()
        self.release_process = threading.Event()

    def stream_python_file(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> StreamingProcess:
        self.stream_calls.append((target, path, input_data, args))
        process = self.streams.pop(0)
        self.process_created.set()
        assert self.release_process.wait(timeout=1.0)
        return process


class RealSubprocessStreamingRunner(RecordingRunner):
    def __init__(self) -> None:
        super().__init__({})
        self.processes: list[subprocess.Popen[bytes]] = []

    def stream_python_file(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.Popen[bytes]:
        del target, path, args
        script = (
            "import sys\n"
            "sys.stdin.buffer.read()\n"
            "sys.stderr.buffer.write(b'x' * (4 * 1024 * 1024))\n"
            "sys.stderr.buffer.write(b'\\nuseful-stderr-tail\\n')\n"
            "sys.stderr.buffer.flush()\n"
            "raise SystemExit(7)\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdin is not None
        process.stdin.write(input_data)
        process.stdin.close()
        self.processes.append(process)
        return process


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


class ForegroundEventsRunner(RecordingRunner):
    def run_python_file_bytes(
        self,
        target: str,
        path: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        self.calls.append((target, path, input_data, args))
        return subprocess.CompletedProcess(
            ["ssh"],
            0,
            b'{"sequence":4,"kind":"move","path":"old.py",'
            b'"destination_path":"new.py"}\n',
            b"",
        )


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
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
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


def test_remote_client_parses_execution_environment_summary() -> None:
    runner = RecordingRunner(
        {
            "execution-environment": {
                "available": True,
                "refreshed": True,
                "export_file": "/home/u/.remote-sandbox/workspaces/w1/environment.sh",
                "captured_at": "2026-07-13T10:00:00+00:00",
                "shell": "/bin/bash",
                "path": "/home/u/bin:/usr/bin",
                "python": "/home/u/bin/python",
                "python3": "/usr/bin/python3",
                "warning": None,
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    environment = client.execution_environment(refresh=True)

    assert environment == RemoteExecutionEnvironment(
        available=True,
        refreshed=True,
        export_file="/home/u/.remote-sandbox/workspaces/w1/environment.sh",
        captured_at="2026-07-13T10:00:00+00:00",
        shell="/bin/bash",
        path="/home/u/bin:/usr/bin",
        python="/home/u/bin/python",
        python3="/usr/bin/python3",
        warning=None,
    )
    request = decode_request(runner.calls[0][2])
    assert request.command == "execution-environment"
    assert request.payload == {"workspace_id": "w1", "refresh": True}


def test_remote_client_rejects_malformed_execution_environment() -> None:
    runner = RecordingRunner(
        {
            "execution-environment": {
                "available": True,
                "refreshed": False,
                "export_file": None,
                "captured_at": None,
                "shell": None,
                "path": None,
                "python": None,
                "python3": None,
                "warning": None,
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="execution environment"):
        client.execution_environment()


def test_remote_client_ensures_versioned_agent_when_path_is_not_supplied() -> None:
    runner = RecordingRunner({"status": {"status": "stopped"}})
    manager = RecordingAgentManager(
        AgentInstall(
            version="0.2.0-dev",
            remote_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
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
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
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


def test_remote_snapshot_parses_audit_identity_fields() -> None:
    runner = RecordingRunner(
        {
            "snapshot": {
                "latest_sequence": 1,
                "entries": [
                    {
                        "path": "a.py",
                        "kind": "file",
                        "size": 1,
                        "mtime_ns": 2,
                        "mode": 33188,
                        "link_target": None,
                        "ctime_ns": 3,
                        "device": 4,
                        "inode": 5,
                    }
                ],
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/test/agent.pyz",
    )

    snapshot = client.snapshot()

    signature = snapshot.signatures["a.py"]
    assert (signature.ctime_ns, signature.device, signature.inode) == (3, 4, 5)


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
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
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


def test_remote_metadata_paths_never_returns_regular_file_hashes() -> None:
    runner = RecordingRunner(
        {
            "metadata-paths": {
                "entries": [
                    {
                        "path": "requested.txt",
                        "kind": "file",
                        "size": 7,
                        "mtime_ns": 21,
                        "mode": 33188,
                        "link_target": None,
                        "content_hash": None,
                    }
                ]
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="agent.pyz",
    )

    entries = client.metadata_paths(["requested.txt"])

    assert entries["requested.txt"].content_hash is None
    request = decode_request(runner.calls[0][2])
    assert request.command == "metadata-paths"
    assert request.payload["paths"] == ["requested.txt"]


def test_remote_events_after_uses_finite_structured_event_command() -> None:
    runner = ForegroundEventsRunner({})
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="agent.pyz",
    )

    events = client.events_after(3)

    assert [(event.sequence, event.path, event.destination_path) for event in events] == [
        (4, "old.py", "new.py")
    ]
    request = decode_request(runner.calls[0][2])
    assert request.command == "events"
    assert request.payload == {
        "workspace_id": "w1",
        "after_sequence": 3,
        "follow": False,
    }
    assert runner.calls[0][3] == ("events",)


def test_remote_read_path_returns_structured_binary_content() -> None:
    runner = RecordingRunner(
        {"read-path": {"missing": False, "data": "YmluYXJ5AGRhdGE="}}
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="agent.pyz",
    )

    assert client.read_path("data.bin") == b"binary\0data"
    request = decode_request(runner.calls[0][2])
    assert request.command == "read-path"
    assert request.payload == {"workspace_id": "w1", "path": "data.bin"}


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
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
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


def test_subscription_raises_nonzero_stream_error_without_reopening() -> None:
    process = StreamingProcess(
        b"",
        stderr=b'{"ok":false,"payload":{},"error":"watcher stopped"}\n',
        exit_code=2,
    )
    runner = StreamingRunner({}, [process])
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="watcher stopped"):
        next(iter(client.subscribe(after_sequence=0)))

    assert len(runner.stream_calls) == 1
    assert process.stdin.closed
    assert process.stdout.closed
    assert process.stderr.closed
    assert process.wait_calls == [None]


def test_repeated_failed_subscriptions_do_not_accumulate() -> None:
    processes = [
        StreamingProcess(b"", stderr=b"watcher stopped\n", exit_code=2)
        for _ in range(3)
    ]
    runner = StreamingRunner({}, processes)
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    for _ in range(3):
        with pytest.raises(RemoteProtocolError, match="watcher stopped"):
            next(iter(client.subscribe(after_sequence=0)))
        assert client._subscriptions == set()


def test_subscription_backs_off_between_repeated_clean_eof_reopens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streams = [
        StreamingProcess(b""),
        StreamingProcess(b""),
        StreamingProcess(
            b'{"sequence":8,"kind":"modify","path":"ready.py",'
            b'"destination_path":null}\n'
        ),
    ]
    runner = StreamingRunner({}, streams)
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )
    subscription = client.subscribe(after_sequence=0)
    delays: list[float] = []
    monkeypatch.setattr(
        subscription,
        "_wait_before_reopen",
        lambda delay: delays.append(delay) or False,
    )

    assert next(iter(subscription)).sequence == 8

    assert len(runner.stream_calls) == 3
    assert delays == sorted(delays)
    assert len(delays) == 2
    assert delays[0] > 0
    assert delays[1] > delays[0]
    subscription.close()


def test_close_interrupts_reopen_backoff_without_spawning_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = StreamingProcess(b"")
    runner = StreamingRunner({}, [process])
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )
    subscription = client.subscribe(after_sequence=0)
    entered_backoff = threading.Event()
    original_wait = subscription._wait_before_reopen

    def observed_wait(delay: float) -> bool:
        del delay
        entered_backoff.set()
        return original_wait(10.0)

    monkeypatch.setattr(subscription, "_wait_before_reopen", observed_wait)
    iterator = iter(subscription)
    stopped = threading.Event()

    def consume() -> None:
        with pytest.raises(StopIteration):
            next(iterator)
        stopped.set()

    thread = threading.Thread(target=consume)
    thread.start()
    assert entered_backoff.wait(timeout=1.0)

    subscription.close()
    subscription.close()
    thread.join(timeout=1.0)

    assert stopped.is_set()
    assert not thread.is_alive()
    assert len(runner.stream_calls) == 1


def test_close_during_stream_launch_terminates_late_process_without_reopening() -> None:
    process = StreamingProcess(b"")
    runner = LaunchBarrierRunner(process)
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )
    subscription = client.subscribe(after_sequence=0)
    iterator = iter(subscription)
    stopped = threading.Event()
    failures: list[BaseException] = []

    def consume() -> None:
        try:
            next(iterator)
        except StopIteration:
            stopped.set()
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=consume)
    thread.start()
    assert runner.process_created.wait(timeout=1.0)

    subscription.close()
    subscription.close()
    runner.release_process.set()
    thread.join(timeout=1.0)

    assert failures == []
    assert stopped.is_set()
    assert not thread.is_alive()
    assert len(runner.stream_calls) == 1
    assert process.terminated
    assert process.wait_calls == [1.0]
    assert process.stdin.closed
    assert process.stdout.closed
    assert process.stderr.closed


@pytest.mark.timeout(5)
def test_subscription_drains_large_stderr_without_pipe_deadlock() -> None:
    runner = RealSubprocessStreamingRunner()
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )
    subscription = client.subscribe(after_sequence=0)
    iterator = iter(subscription)
    finished = threading.Event()
    failures: list[BaseException] = []

    def consume() -> None:
        try:
            next(iterator)
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    thread = threading.Thread(target=consume)
    thread.start()
    completed_promptly = finished.wait(timeout=2.0)
    if not completed_promptly:
        subscription.close()
    thread.join(timeout=1.0)
    subscription.close()

    assert completed_promptly
    assert not thread.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], RemoteProtocolError)
    assert "useful-stderr-tail" in str(failures[0])
    assert len(str(failures[0]).encode("utf-8")) <= 66 * 1024
    assert len(runner.processes) == 1
    process = runner.processes[0]
    assert process.returncode == 7
    assert process.stdin is not None and process.stdin.closed
    assert process.stdout is not None and process.stdout.closed
    assert process.stderr is not None and process.stderr.closed


def test_malformed_foreground_response_raises_protocol_error() -> None:
    runner = RawRunner(subprocess.CompletedProcess(["ssh"], 0, b"not-json\n", b""))
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="malformed"):
        client.status()


def test_structured_stream_error_raises_protocol_error() -> None:
    line = b'{"ok":false,"payload":{},"error":"workspace is not registered"}\n'

    with pytest.raises(RemoteProtocolError, match="workspace is not registered"):
        parse_event_line(line)


@pytest.mark.parametrize("sequence", [True, "3", -1])
def test_acknowledge_rejects_non_integer_or_negative_sequence(sequence: object) -> None:
    runner = RecordingRunner({"ack": {"acknowledged_sequence": 0}})
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(ValueError, match="non-negative integer"):
        client.acknowledge(sequence)  # type: ignore[arg-type]

    assert runner.calls == []


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"acknowledged_sequence": "3"},
        {"acknowledged_sequence": True},
        {"acknowledged_sequence": -1},
    ],
)
def test_acknowledge_wraps_malformed_remote_sequence(payload: dict[str, object]) -> None:
    runner = RecordingRunner({"ack": payload})
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="acknowledgement payload is malformed"):
        client.acknowledge(3)


def test_invalid_missing_fingerprint_path_is_a_protocol_error() -> None:
    runner = RecordingRunner(
        {
            "hash-paths": {
                "entries": [
                    {
                        "path": "../escape",
                        "kind": "missing",
                        "size": None,
                        "mtime_ns": None,
                        "mode": None,
                        "link_target": None,
                        "content_hash": None,
                    }
                ]
            }
        }
    )
    client = RemoteWorkspaceClient(
        runner,
        target="example-host",
        workspace_id="w1",
        agent_path="~/.remote-sandbox/agents/0.2.0-dev/agent.pyz",
    )

    with pytest.raises(RemoteProtocolError, match="invalid remote fingerprint"):
        client.hash_paths(["requested.txt"])
