from __future__ import annotations

import time
from statistics import median

import pytest


@pytest.mark.performance
def test_small_remote_delete_reaches_local_within_two_seconds(performance_pair) -> None:
    path = performance_pair.remote / "delete-me.txt"
    path.write_text("x", encoding="utf-8")
    performance_pair.wait_until_synced(path)

    started = time.monotonic()
    path.unlink()
    performance_pair.wait_until_missing(performance_pair.local / "delete-me.txt")
    elapsed = time.monotonic() - started

    print(f"small_remote_delete_seconds={elapsed:.6f}")
    assert elapsed < 2.0


@pytest.mark.performance
def test_noop_cycle_hashes_no_unchanged_files(performance_pair) -> None:
    performance_pair.populate(5_000)
    performance_pair.initial_sync()
    performance_pair.hash_counter.reset()

    performance_pair.engine.run_once("noop")

    print(f"noop_hash_count={performance_pair.hash_counter.count}")
    assert performance_pair.hash_counter.count == 0


@pytest.mark.performance
def test_batch_transport_is_close_to_direct_rsync_and_uses_one_session(
    performance_pair,
) -> None:
    performance_pair.populate(5_000)
    batch = performance_pair.initial_transfer_batch()
    benchmark_root = performance_pair.direct.parent / "ratio-samples"
    performance_pair.measure_direct_rsync(benchmark_root / "warm-direct")
    performance_pair.measure_batch_transport(batch, benchmark_root / "warm-codex")

    direct_samples: list[float] = []
    codex_samples: list[float] = []
    sessions: list[int] = []
    payload_sizes: list[int] = []
    for index in range(5):
        direct_destination = benchmark_root / f"direct-{index}"
        codex_destination = benchmark_root / f"codex-{index}"
        if index % 2 == 0:
            direct_samples.append(performance_pair.measure_direct_rsync(direct_destination))
            codex, session_count, payload_size = performance_pair.measure_batch_transport(
                batch,
                codex_destination,
            )
        else:
            codex, session_count, payload_size = performance_pair.measure_batch_transport(
                batch,
                codex_destination,
            )
            direct_samples.append(performance_pair.measure_direct_rsync(direct_destination))
        codex_samples.append(codex)
        sessions.append(session_count)
        payload_sizes.append(payload_size)

    direct_median = median(direct_samples)
    codex_median = median(codex_samples)
    threshold = max(direct_median * 1.5, direct_median + 1.0)
    print("direct_rsync_samples=" + ",".join(f"{value:.6f}" for value in direct_samples))
    print("codex_batch_transport_samples=" + ",".join(f"{value:.6f}" for value in codex_samples))
    print(f"direct_rsync_median={direct_median:.6f}")
    print(f"codex_batch_transport_median={codex_median:.6f}")
    print(f"direct_rsync_range={max(direct_samples) - min(direct_samples):.6f}")
    print(f"codex_batch_transport_range={max(codex_samples) - min(codex_samples):.6f}")
    print(f"transport_sessions={sessions}")
    print(f"transport_progress_payloads={payload_sizes}")
    assert codex_median <= threshold
    assert sessions == [1] * 5
    assert max(payload_sizes) <= 256


@pytest.mark.performance
def test_full_initial_coordinator_meets_wall_progress_and_persistence_gates(
    performance_pair,
) -> None:
    performance_pair.populate(5_000)
    coordinator_seconds = performance_pair.measure_initial_sync()
    first_progress_seconds = performance_pair.transaction_counter.first_progress_seconds

    print(f"codex_initial_sync_seconds={coordinator_seconds:.6f}")
    print(f"first_progress_seconds={first_progress_seconds}")
    print(f"sqlite_commits={performance_pair.transaction_counter.commits}")
    print(f"max_progress_payload={max(performance_pair.transport.progress_payload_sizes)}")
    performance_pair.assert_final_base_and_echoes()
    assert coordinator_seconds <= 8.0
    assert first_progress_seconds is not None and first_progress_seconds < 1.0
    assert performance_pair.transaction_counter.commits < 100
    assert max(performance_pair.transport.progress_payload_sizes) <= 256
