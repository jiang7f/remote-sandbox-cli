import threading
import time

from helpers.sync_harness import SupervisorHarness

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
