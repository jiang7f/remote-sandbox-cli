from __future__ import annotations

import time

from helpers.sync_harness import DaemonPairHarness


def test_real_local_watcher_daemon_propagates_within_two_seconds(
    daemon_pair: DaemonPairHarness,
) -> None:
    daemon_pair.wait_until_ready()
    destination = daemon_pair.remote / "watcher-latency.txt"
    started = time.monotonic()
    (daemon_pair.local / "watcher-latency.txt").write_bytes(b"observed")
    deadline = started + 2.0
    while time.monotonic() < deadline and not destination.exists():
        time.sleep(0.01)
    elapsed = time.monotonic() - started

    assert destination.read_bytes() == b"observed"
    assert elapsed < 2.0
    assert "engine:event" in daemon_pair.restart_order
