import threading
import time

import pytest
from helpers.sync_harness import SupervisorHarness

import remote_sandbox.daemon as daemon_module
from remote_sandbox.daemon import DaemonError
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def test_supervisor_publishes_starting_before_initial_sync(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()
    status = supervisor_fixture.store.get_status()
    assert status.phase is WorkspacePhase.STARTING
    assert supervisor_fixture.client.status().running is True


def test_control_sync_runs_incremental_engine(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    supervisor_fixture.client.wait_for_phase(WorkspacePhase.READY, timeout=2.0)
    assert supervisor_fixture.client.sync() is True
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and "event" not in supervisor_fixture.engine.reasons:
        time.sleep(0.01)
    assert "event" in supervisor_fixture.engine.reasons


def test_control_status_includes_the_full_workspace_progress(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()
    expected = WorkspaceStatus(
        WorkspacePhase.INITIAL_SYNCING,
        SyncProgress("planning", files_done=2, files_total=5),
        pending=3,
        conflicts=1,
    )
    supervisor_fixture.store.set_status(expected)

    status = supervisor_fixture.client.control_status()

    assert status.workspace_status == expected


def test_control_status_exposes_initial_sync_start_generation(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.initial_sync.block_before_scan()
    supervisor_fixture.start_in_thread()
    supervisor_fixture.store.publish_initial_sync_started(
        WorkspaceStatus(WorkspacePhase.INITIAL_SYNCING, SyncProgress("scanning"))
    )

    status = supervisor_fixture.client.control_status()

    assert status.initial_sync_generation == 1


def test_graceful_stop_cleans_runtime_and_publishes_stopped(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    assert supervisor_fixture.client.stop() is True
    assert supervisor_fixture.thread is not None
    supervisor_fixture.thread.join(timeout=2.0)
    assert not supervisor_fixture.supervisor.runtime.pidfile.exists()
    assert not supervisor_fixture.supervisor.runtime.socket.exists()
    assert supervisor_fixture.store.get_status().phase is WorkspacePhase.STOPPED


def test_control_mutation_keeps_supervisor_and_watchers_alive(
    supervisor_fixture: SupervisorHarness,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda kind, payload: calls.append((kind, payload)) or {"resolved": "model.py"}
    )
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    before = supervisor_fixture.client.control_status()

    result = supervisor_fixture.client.mutate("resolve", {"path": "model.py"})
    after = supervisor_fixture.client.control_status()

    assert result == {"resolved": "model.py"}
    assert before.pid == after.pid
    assert supervisor_fixture.remote.start_watcher_calls == 1
    assert supervisor_fixture.remote.subscribe_calls >= 1
    assert supervisor_fixture.remote.closed is False
    assert calls == [("resolve", {"path": "model.py"})]


def test_control_mutation_cannot_overlap_incremental_engine(
    supervisor_fixture: SupervisorHarness,
) -> None:
    overlap: list[bool] = []
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda _kind, _payload: overlap.append(supervisor_fixture.engine.active.is_set()) or {}
    )
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    supervisor_fixture.engine.release.clear()
    assert supervisor_fixture.client.sync() is True
    assert supervisor_fixture.engine.active.wait(timeout=2.0)
    completed = threading.Event()

    def mutate() -> None:
        supervisor_fixture.client.mutate("resolve", {"path": "model.py"})
        completed.set()

    thread = threading.Thread(target=mutate)
    thread.start()
    time.sleep(0.05)
    assert not completed.is_set()
    supervisor_fixture.engine.release.set()
    thread.join(timeout=2.0)

    assert completed.is_set()
    assert overlap == [False]


def test_control_mutation_failure_returns_error_without_stopping_supervisor(
    supervisor_fixture: SupervisorHarness,
) -> None:
    def fail(_kind: str, _payload: dict[str, object]) -> dict[str, object]:
        raise ValueError("selected source changed")

    supervisor_fixture.supervisor._mutation_handler = fail  # type: ignore[attr-defined]
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    before = supervisor_fixture.store.get_status()

    try:
        supervisor_fixture.client.mutate("resolve", {"path": "model.py"})
    except Exception as exc:
        assert str(exc) == "selected source changed"
    else:
        raise AssertionError("mutation failure was not returned to the client")

    assert supervisor_fixture.client.control_status().running is True
    assert supervisor_fixture.store.get_status() == before


def test_queued_mutation_timeout_cancels_before_worker_drain(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda kind, _payload: calls.append(kind) or {}
    )
    monkeypatch.setattr(daemon_module, "_MUTATION_TIMEOUT_S", 0.01)

    with pytest.raises(DaemonError, match="timed out"):
        supervisor_fixture.supervisor.request_mutation("resolve", {"path": "model.py"})

    supervisor_fixture.supervisor._run_pending_mutations()  # type: ignore[attr-defined]
    assert calls == []
    assert supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]


def test_stop_fails_all_queued_mutations_promptly(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda _kind, _payload: {}
    )
    monkeypatch.setattr(daemon_module, "_MUTATION_TIMEOUT_S", 5.0)
    errors: list[str] = []

    def request(path: str) -> None:
        try:
            supervisor_fixture.supervisor.request_mutation("resolve", {"path": path})
        except DaemonError as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=request, args=(f"{index}.py",)) for index in range(2)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 1.0
    while (
        supervisor_fixture.supervisor._mutations.qsize() < 2  # type: ignore[attr-defined]
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)

    supervisor_fixture.supervisor.stop()
    for thread in threads:
        thread.join(timeout=0.2)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == ["supervisor stopped before mutation started"] * 2
    assert supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]


def test_started_mutation_waits_for_definitive_result_after_timeout(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    result: list[dict[str, object]] = []
    monkeypatch.setattr(daemon_module, "_MUTATION_TIMEOUT_S", 0.01)

    def handler(_kind: str, _payload: dict[str, object]) -> dict[str, object]:
        started.set()
        release.wait(timeout=1.0)
        return {"owner": "worker"}

    supervisor_fixture.supervisor._mutation_handler = handler  # type: ignore[attr-defined]

    requester = threading.Thread(
        target=lambda: result.append(
            supervisor_fixture.supervisor.request_mutation("resolve", {"path": "model.py"})
        )
    )
    requester.start()
    deadline = time.monotonic() + 1.0
    while (
        supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)
    worker = threading.Thread(
        target=supervisor_fixture.supervisor._run_pending_mutations  # type: ignore[attr-defined]
    )
    worker.start()
    assert started.wait(timeout=1.0)
    time.sleep(0.03)

    assert requester.is_alive()
    release.set()
    requester.join(timeout=1.0)
    worker.join(timeout=1.0)
    assert result == [{"owner": "worker"}]


def test_timeout_and_worker_claim_have_one_owner(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daemon_module, "_MUTATION_TIMEOUT_S", 0.02)
    calls = 0
    outcome: list[str] = []

    def handler(_kind: str, _payload: dict[str, object]) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {}

    supervisor_fixture.supervisor._mutation_handler = handler  # type: ignore[attr-defined]

    def request() -> None:
        try:
            supervisor_fixture.supervisor.request_mutation("resolve", {"path": "model.py"})
        except DaemonError:
            outcome.append("cancelled")
        else:
            outcome.append("completed")

    requester = threading.Thread(target=request)
    requester.start()
    deadline = time.monotonic() + 1.0
    while (
        supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)
    time.sleep(0.018)
    worker = threading.Thread(
        target=supervisor_fixture.supervisor._run_pending_mutations  # type: ignore[attr-defined]
    )
    worker.start()
    requester.join(timeout=1.0)
    worker.join(timeout=1.0)

    assert outcome in (["cancelled"], ["completed"])
    assert calls == (1 if outcome == ["completed"] else 0)
    assert supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]


def test_control_stop_fails_queued_mutation_while_engine_is_busy(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda _kind, _payload: {}
    )
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    supervisor_fixture.engine.release.clear()
    assert supervisor_fixture.client.sync() is True
    assert supervisor_fixture.engine.active.wait(timeout=1.0)
    errors: list[str] = []

    def mutate() -> None:
        try:
            supervisor_fixture.client.mutate("resolve", {"path": "model.py"})
        except DaemonError as exc:
            errors.append(str(exc))

    requester = threading.Thread(target=mutate)
    requester.start()
    deadline = time.monotonic() + 1.0
    while (
        supervisor_fixture.supervisor._mutations.empty()  # type: ignore[attr-defined]
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)

    assert supervisor_fixture.client.control_status().running is True
    assert supervisor_fixture.client.stop() is True
    requester.join(timeout=0.2)
    supervisor_fixture.engine.release.set()

    assert not requester.is_alive()
    assert errors == ["supervisor stopped before mutation started"]
