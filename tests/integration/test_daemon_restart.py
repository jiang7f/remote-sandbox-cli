from helpers.sync_harness import DaemonPairHarness


def test_restart_replays_unacknowledged_events(daemon_pair: DaemonPairHarness) -> None:
    daemon_pair.append_remote_change("after-crash.txt", b"x")
    daemon_pair.kill_local_daemon()
    assert daemon_pair.runtime.pidfile.exists()
    daemon_pair.start_local_daemon()
    daemon_pair.wait_until_ready()
    assert (daemon_pair.local / "after-crash.txt").read_bytes() == b"x"
    assert daemon_pair.remote_client.acknowledged_sequence() == 1
    assert daemon_pair.initial_sync_repeated is False
    assert daemon_pair.restart_order[:3] == [
        "local-watcher",
        "remote-watcher",
        "engine:restart",
    ]
    assert "subscription" in daemon_pair.restart_order
    assert daemon_pair.ack_after_commit is True
