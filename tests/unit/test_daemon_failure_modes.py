from helpers.sync_harness import SupervisorHarness

from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def test_remote_watcher_crash_becomes_degraded_and_requests_audit(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_watcher_crash()
    supervisor_fixture.supervisor.handle_subscription_failure(
        supervisor_fixture.remote.failure
    )
    status = supervisor_fixture.store.get_status()
    assert status.phase is WorkspacePhase.DEGRADED
    assert supervisor_fixture.supervisor.audit_requested is True


def test_live_pid_without_control_socket_is_never_reported_stopped(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.publish_live_pid_without_socket()
    status = supervisor_fixture.client.status()
    assert status.phase in {WorkspacePhase.STARTING, WorkspacePhase.DEGRADED}


def test_dead_pid_with_stale_durable_state_is_failed(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.store.set_status(
        WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle"))
    )
    supervisor_fixture.supervisor.runtime.metadata_root.mkdir(parents=True, exist_ok=True)
    supervisor_fixture.supervisor.runtime.pidfile.write_text("99999999\n", encoding="utf-8")
    status = supervisor_fixture.client.status()
    assert status.running is False
    assert status.phase is WorkspacePhase.FAILED
