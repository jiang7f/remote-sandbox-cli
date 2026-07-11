from helpers.sync_harness import SupervisorHarness

from remote_sandbox.status import WorkspacePhase


def test_password_auth_failure_becomes_disconnected(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_auth_failure()
    supervisor_fixture.supervisor.handle_subscription_failure(
        supervisor_fixture.remote.failure
    )
    assert supervisor_fixture.store.get_status().phase is WorkspacePhase.DISCONNECTED


def test_network_failure_retries_without_requesting_password(
    supervisor_fixture: SupervisorHarness,
) -> None:
    supervisor_fixture.remote.raise_network_failure()
    delay = supervisor_fixture.supervisor.handle_subscription_failure(
        supervisor_fixture.remote.failure
    )
    assert delay == 2.0
    assert supervisor_fixture.store.get_status().phase is WorkspacePhase.DISCONNECTED
    assert supervisor_fixture.remote.clear_master_calls == 1
