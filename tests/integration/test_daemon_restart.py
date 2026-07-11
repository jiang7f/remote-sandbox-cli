from helpers.sync_harness import DaemonPairHarness


def test_restart_replays_unacknowledged_events(daemon_pair: DaemonPairHarness) -> None:
    daemon_pair.append_remote_change("after-crash.txt", b"x")
    daemon_pair.kill_local_daemon()
    daemon_pair.start_local_daemon()
    daemon_pair.wait_until_ready()
    assert (daemon_pair.local / "after-crash.txt").read_bytes() == b"x"
    assert daemon_pair.remote_client.acknowledged_sequence() == 1
    assert daemon_pair.initial_sync.run_calls == 0
