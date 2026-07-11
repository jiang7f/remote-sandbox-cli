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
