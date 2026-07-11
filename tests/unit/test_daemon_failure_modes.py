from pathlib import Path

import pytest
from helpers.sync_harness import SupervisorHarness

import remote_sandbox.daemon as daemon_module
from remote_sandbox.daemon import DaemonError, DaemonStatus
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


def test_workspace_fallback_projects_live_degraded_phase_over_durable_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable = WorkspaceStatus(
        WorkspacePhase.READY,
        SyncProgress("idle", files_done=4, files_total=4),
        pending=2,
        conflicts=3,
        last_error="durable warning",
        last_sync_at=123.0,
    )
    synthesized = DaemonStatus(
        running=True,
        pid=4321,
        last_error="control unavailable",
        conn_state="degraded",
        phase=WorkspacePhase.DEGRADED,
        workspace_status=durable,
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "control_status",
        lambda _self: (_ for _ in ()).throw(DaemonError("unavailable")),
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "status",
        lambda _self: synthesized,
    )
    monkeypatch.setattr(daemon_module, "runtime_dir", lambda: Path("/tmp/task15-runtime"))

    status = daemon_module.daemon_workspace_status(
        "00000000-0000-4000-8000-000000000015"
    )

    assert status == WorkspaceStatus(
        WorkspacePhase.DEGRADED,
        durable.progress,
        pending=2,
        conflicts=3,
        last_error="control unavailable",
        last_sync_at=123.0,
    )


def test_workspace_fallback_projects_failed_phase_and_preserves_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable = WorkspaceStatus(
        WorkspacePhase.READY,
        SyncProgress("idle"),
        pending=5,
        conflicts=2,
        last_sync_at=456.0,
    )
    synthesized = DaemonStatus(
        running=False,
        pid=99999999,
        last_error="supervisor process is not running",
        conn_state="failed",
        phase=WorkspacePhase.FAILED,
        workspace_status=durable,
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "control_status",
        lambda _self: (_ for _ in ()).throw(DaemonError("unavailable")),
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "status",
        lambda _self: synthesized,
    )
    monkeypatch.setattr(daemon_module, "runtime_dir", lambda: Path("/tmp/task15-runtime"))

    status = daemon_module.daemon_workspace_status(
        "00000000-0000-4000-8000-000000000015"
    )

    assert status == WorkspaceStatus(
        WorkspacePhase.FAILED,
        durable.progress,
        pending=5,
        conflicts=2,
        last_error="supervisor process is not running",
        last_sync_at=456.0,
    )


def test_workspace_control_status_remains_authoritative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_workspace = WorkspaceStatus(
        WorkspacePhase.READY,
        SyncProgress("idle"),
        conflicts=1,
    )
    control = DaemonStatus(
        running=True,
        pid=4321,
        phase=WorkspacePhase.DEGRADED,
        workspace_status=control_workspace,
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "control_status",
        lambda _self: control,
    )
    monkeypatch.setattr(
        daemon_module.SupervisorClient,
        "status",
        lambda _self: pytest.fail("fallback must not run"),
    )
    monkeypatch.setattr(daemon_module, "runtime_dir", lambda: Path("/tmp/task15-runtime"))

    assert daemon_module.daemon_workspace_status(
        "00000000-0000-4000-8000-000000000015"
    ) == control_workspace
