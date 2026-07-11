from __future__ import annotations

import json
import socket
import threading
import time

import pytest
from helpers.sync_harness import SupervisorHarness

import remote_sandbox.daemon as daemon_module
from remote_sandbox.daemon import DaemonError
from remote_sandbox.status import WorkspacePhase


def _raw_request(
    supervisor_fixture: SupervisorHarness,
    payload: bytes,
) -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2.0)
        client.connect(str(supervisor_fixture.supervisor.runtime.socket))
        client.sendall(payload)
        client.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while chunk := client.recv(4096):
            chunks.append(chunk)
    return b"".join(chunks)


def _wait_for_handlers(supervisor_fixture: SupervisorHarness, count: int) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with supervisor_fixture.supervisor._control._connection_lock:  # type: ignore[attr-defined]
            active = len(  # type: ignore[attr-defined]
                supervisor_fixture.supervisor._control._connection_threads
            )
        if active == count:
            return
        time.sleep(0.001)
    raise AssertionError(f"control handler count did not reach {count}, current={active}")


def test_control_accepts_partial_status_request_at_orderly_eof(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()

    response = json.loads(_raw_request(supervisor_fixture, b"status"))

    assert response["running"] is True
    assert response["phase"] in {"starting", "initial-syncing"}
    _wait_for_handlers(supervisor_fixture, 0)


@pytest.mark.parametrize(
    ("payload", "expected_log"),
    [
        (b'mutate {"kind":\n', "Expecting"),
        (
            b"x" * (daemon_module._CONTROL_MAX_LINE_BYTES + 1),
            "control request too large",
        ),
    ],
)
def test_control_rejects_malformed_or_oversized_requests_and_releases_handler(
    supervisor_fixture: SupervisorHarness,
    caplog: pytest.LogCaptureFixture,
    payload: bytes,
    expected_log: str,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()

    assert _raw_request(supervisor_fixture, payload) == b""
    _wait_for_handlers(supervisor_fixture, 0)

    assert expected_log in caplog.text
    assert supervisor_fixture.client.control_status().running is True


def test_control_unknown_verb_returns_error_without_invoking_handlers(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()

    assert _raw_request(supervisor_fixture, b"unknown request\n") == b"error\n"
    _wait_for_handlers(supervisor_fixture, 0)
    assert supervisor_fixture.client.control_status().running is True


def test_control_handler_exception_is_contained_and_slot_is_reusable(
    supervisor_fixture: SupervisorHarness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()
    supervisor_fixture.supervisor._control._on_sync = (  # type: ignore[attr-defined]
        lambda: (_ for _ in ()).throw(RuntimeError("sync handler failed"))
    )

    assert _raw_request(supervisor_fixture, b"sync\n") == b""
    _wait_for_handlers(supervisor_fixture, 0)

    assert "sync handler failed" in caplog.text
    assert supervisor_fixture.client.control_status().running is True


def test_client_disconnect_during_mutation_releases_waiter_and_handler(
    supervisor_fixture: SupervisorHarness,
) -> None:
    started = threading.Event()
    release = threading.Event()

    def mutation(_kind: str, _payload: dict[str, object]) -> dict[str, object]:
        started.set()
        release.wait(timeout=2.0)
        return {"resolved": True}

    supervisor_fixture.supervisor._mutation_handler = mutation  # type: ignore[attr-defined]
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()

    request = json.dumps(
        {"kind": "resolve", "payload": {"path": "model.py"}},
        separators=(",", ":"),
    )
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(supervisor_fixture.supervisor.runtime.socket))
    client.sendall(f"mutate {request}\n".encode())
    client.close()

    assert started.wait(timeout=2.0)
    release.set()
    _wait_for_handlers(supervisor_fixture, 0)

    assert supervisor_fixture.client.control_status().phase is WorkspacePhase.READY


@pytest.mark.parametrize(
    ("reply", "message"),
    [
        ("[]", "response is malformed"),
        ('{"ok":false}', "mutation failed"),
        ('{"ok":true,"result":[]}', "result is malformed"),
    ],
)
def test_mutation_client_rejects_malformed_responses(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
    reply: str,
    message: str,
) -> None:
    monkeypatch.setattr(supervisor_fixture.client, "_request", lambda *_args, **_kwargs: reply)

    with pytest.raises(DaemonError, match=message):
        supervisor_fixture.client.mutate("resolve", {"path": "model.py"})


def test_mutation_client_rejects_empty_kind_and_unavailable_endpoint(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        supervisor_fixture.client.mutate("", {})

    monkeypatch.setattr(supervisor_fixture.client, "_request", lambda *_args, **_kwargs: None)
    with pytest.raises(DaemonError, match="endpoint is unresponsive"):
        supervisor_fixture.client.mutate("resolve", {})
