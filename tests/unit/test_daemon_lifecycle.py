import threading
import time

import pytest
from helpers.sync_harness import SupervisorHarness

import remote_sandbox.daemon as daemon_module
from remote_sandbox.daemon import DaemonError
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus

_SATURATED_MUTATION_REQUESTS = 16
_EXPECTED_MUTATION_WAITER_LIMIT = 12


def _pending_mutation_count(supervisor_fixture: SupervisorHarness) -> int:
    pending = getattr(supervisor_fixture.supervisor, "_pending_mutations", None)
    if pending is not None:
        return len(pending)
    return supervisor_fixture.supervisor._mutations.qsize()  # type: ignore[attr-defined]


def _start_socket_mutations(
    supervisor_fixture: SupervisorHarness,
    count: int,
) -> tuple[list[threading.Thread], list[str]]:
    outcomes: list[str] = []
    outcomes_lock = threading.Lock()

    def mutate(index: int) -> None:
        try:
            supervisor_fixture.client.mutate(
                "resolve",
                {"path": f"model-{index}.py"},
            )
        except DaemonError as exc:
            outcome = str(exc)
        else:
            outcome = "completed"
        with outcomes_lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=mutate, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    return threads, outcomes


def _wait_for_saturated_mutation_waiters(
    supervisor_fixture: SupervisorHarness,
    threads: list[threading.Thread],
) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        pending = _pending_mutation_count(supervisor_fixture)
        finished = sum(not thread.is_alive() for thread in threads)
        handler_count = len(
            supervisor_fixture.supervisor._control._connection_threads  # type: ignore[attr-defined]
        )
        if handler_count >= _SATURATED_MUTATION_REQUESTS or (
            pending == _EXPECTED_MUTATION_WAITER_LIMIT - 1
            and finished
            >= _SATURATED_MUTATION_REQUESTS - _EXPECTED_MUTATION_WAITER_LIMIT
        ):
            return
        time.sleep(0.001)
    raise AssertionError(
        "mutation waiters did not saturate "
        f"pending={_pending_mutation_count(supervisor_fixture)} "
        f"handlers={len(supervisor_fixture.supervisor._control._connection_threads)}"  # type: ignore[attr-defined]
    )


def _join_threads(threads: list[threading.Thread], timeout: float = 1.0) -> None:
    for thread in threads:
        thread.join(timeout=timeout)


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


def test_subscription_thread_uses_captured_iterable_when_stop_clears_field(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = supervisor_fixture.supervisor

    class ImmediateThread:
        def __init__(
            self,
            *,
            target: object,
            name: str,
            daemon: bool,
            args: tuple[object, ...] = (),
        ) -> None:
            del name, daemon
            self._target = target
            self._args = args

        def start(self) -> None:
            supervisor._subscription = None  # type: ignore[attr-defined]
            self._target(*self._args)  # type: ignore[operator]

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            del timeout

    monkeypatch.setattr(daemon_module.threading, "Thread", ImmediateThread)

    supervisor._start_subscription()  # type: ignore[attr-defined]


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
    assert _pending_mutation_count(supervisor_fixture) == 0


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
        _pending_mutation_count(supervisor_fixture) < 2
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)

    supervisor_fixture.supervisor.stop()
    for thread in threads:
        thread.join(timeout=0.2)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == ["supervisor stopped before mutation started"] * 2
    assert _pending_mutation_count(supervisor_fixture) == 0


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
        _pending_mutation_count(supervisor_fixture) == 0
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
        _pending_mutation_count(supervisor_fixture) == 0
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
    assert _pending_mutation_count(supervisor_fixture) == 0


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
        _pending_mutation_count(supervisor_fixture) == 0
        and time.monotonic() < deadline
    ):
        time.sleep(0.001)

    assert supervisor_fixture.client.control_status().running is True
    assert supervisor_fixture.client.stop() is True
    requester.join(timeout=0.2)
    supervisor_fixture.engine.release.set()

    assert not requester.is_alive()
    assert errors == ["supervisor stopped before mutation started"]


def test_reserved_control_capacity_survives_saturated_mutation_waiters(
    supervisor_fixture: SupervisorHarness,
) -> None:
    mutation_started = threading.Event()
    release_mutation = threading.Event()

    def block_mutation(
        _kind: str,
        _payload: dict[str, object],
    ) -> dict[str, object]:
        mutation_started.set()
        release_mutation.wait()
        return {}

    supervisor_fixture.supervisor._mutation_handler = block_mutation  # type: ignore[attr-defined]
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    threads, _outcomes = _start_socket_mutations(
        supervisor_fixture,
        _SATURATED_MUTATION_REQUESTS,
    )

    try:
        assert mutation_started.wait(timeout=1.0)
        _wait_for_saturated_mutation_waiters(supervisor_fixture, threads)
        started = time.monotonic()
        status = supervisor_fixture.client.control_status()
        status_elapsed = time.monotonic() - started
        started = time.monotonic()
        stopped = supervisor_fixture.client.stop()
        stop_elapsed = time.monotonic() - started

        assert status.running is True
        assert stopped is True
        assert status_elapsed < 0.5
        assert stop_elapsed < 0.5
    finally:
        supervisor_fixture.supervisor.stop()
        release_mutation.set()
        _join_threads(threads)


def test_repeated_queued_timeouts_remove_pending_requests_immediately(
    supervisor_fixture: SupervisorHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    supervisor_fixture.supervisor._mutation_handler = (  # type: ignore[attr-defined]
        lambda kind, _payload: calls.append(kind) or {}
    )
    monkeypatch.setattr(daemon_module, "_MUTATION_TIMEOUT_S", 0.002)

    for index in range(50):
        with pytest.raises(DaemonError, match="timed out"):
            supervisor_fixture.supervisor.request_mutation(
                "resolve",
                {"path": f"model-{index}.py"},
            )
        assert _pending_mutation_count(supervisor_fixture) == 0

    supervisor_fixture.supervisor._run_pending_mutations()  # type: ignore[attr-defined]

    assert calls == []
    assert _pending_mutation_count(supervisor_fixture) == 0


def test_saturated_shutdown_releases_handlers_and_pending_requests(
    supervisor_fixture: SupervisorHarness,
) -> None:
    mutation_started = threading.Event()
    release_mutation = threading.Event()

    def block_mutation(
        _kind: str,
        _payload: dict[str, object],
    ) -> dict[str, object]:
        mutation_started.set()
        release_mutation.wait()
        return {}

    supervisor_fixture.supervisor._mutation_handler = block_mutation  # type: ignore[attr-defined]
    supervisor_fixture.store.mark_initial_sync_completed()
    supervisor_fixture.start_in_thread()
    threads, outcomes = _start_socket_mutations(
        supervisor_fixture,
        _SATURATED_MUTATION_REQUESTS,
    )

    try:
        assert mutation_started.wait(timeout=1.0)
        _wait_for_saturated_mutation_waiters(supervisor_fixture, threads)
        assert supervisor_fixture.client.stop() is True
    finally:
        supervisor_fixture.supervisor.stop()
        release_mutation.set()
        _join_threads(threads)
    assert supervisor_fixture.thread is not None
    supervisor_fixture.thread.join(timeout=2.0)

    assert not supervisor_fixture.thread.is_alive()
    assert all(not thread.is_alive() for thread in threads)
    assert len(outcomes) == _SATURATED_MUTATION_REQUESTS
    assert _pending_mutation_count(supervisor_fixture) == 0
    assert not supervisor_fixture.supervisor._control._connection_threads  # type: ignore[attr-defined]
