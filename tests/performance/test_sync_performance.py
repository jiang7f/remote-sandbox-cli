from __future__ import annotations

from statistics import median

import pytest


@pytest.mark.performance
def test_noop_cycle_hashes_no_unchanged_files(performance_pair) -> None:
    performance_pair.populate(5_000)
    performance_pair.initial_sync()
    performance_pair.hash_counter.reset()

    performance_pair.engine.run_once("noop")

    print(f"noop_local_hash_count={performance_pair.hash_counter.local_count}")
    print(f"noop_remote_hash_count={performance_pair.hash_counter.remote_count}")
    print(f"noop_total_hash_count={performance_pair.hash_counter.count}")
    assert performance_pair.hash_counter.local_count == 0
    assert performance_pair.hash_counter.remote_count == 0
    assert performance_pair.hash_counter.count == 0


@pytest.mark.performance
def test_batch_transport_is_close_to_direct_rsync_and_uses_one_session(
    performance_pair,
) -> None:
    performance_pair.populate(5_000)
    batch = performance_pair.initial_transfer_batch()
    benchmark_root = performance_pair.direct.parent / "ratio-samples"
    performance_pair.measure_direct_rsync(benchmark_root / "warm-direct")
    performance_pair.measure_batch_transport(batch, benchmark_root / "warm-rsb")

    direct_samples: list[float] = []
    rsb_samples: list[float] = []
    process_counts: list[int] = []
    payload_sizes: list[int] = []
    for index in range(5):
        direct_destination = benchmark_root / f"direct-{index}"
        rsb_destination = benchmark_root / f"rsb-{index}"
        if index % 2 == 0:
            direct_samples.append(performance_pair.measure_direct_rsync(direct_destination))
            rsb, process_count, payload_size = performance_pair.measure_batch_transport(
                batch,
                rsb_destination,
            )
        else:
            rsb, process_count, payload_size = performance_pair.measure_batch_transport(
                batch,
                rsb_destination,
            )
            direct_samples.append(performance_pair.measure_direct_rsync(direct_destination))
        rsb_samples.append(rsb)
        process_counts.append(process_count)
        payload_sizes.append(payload_size)

    direct_median = median(direct_samples)
    rsb_median = median(rsb_samples)
    threshold = max(direct_median * 2.0, direct_median + 2.0)
    print("direct_rsync_samples=" + ",".join(f"{value:.6f}" for value in direct_samples))
    print("rsb_batch_transport_samples=" + ",".join(f"{value:.6f}" for value in rsb_samples))
    print(f"direct_rsync_median={direct_median:.6f}")
    print(f"rsb_batch_transport_median={rsb_median:.6f}")
    print(f"rsb_batch_transport_overhead={rsb_median - direct_median:.6f}")
    print(f"direct_rsync_range={max(direct_samples) - min(direct_samples):.6f}")
    print(f"rsb_batch_transport_range={max(rsb_samples) - min(rsb_samples):.6f}")
    print(f"transport_processes={process_counts}")
    print(f"transport_progress_payloads={payload_sizes}")
    assert rsb_median <= threshold
    assert process_counts == [1] * 5
    assert max(payload_sizes) <= 256


@pytest.mark.performance
def test_full_initial_coordinator_meets_wall_progress_and_persistence_gates(
    performance_pair,
) -> None:
    performance_pair.populate(5_000)
    coordinator_seconds = performance_pair.measure_initial_sync()
    first_progress_seconds = performance_pair.transaction_counter.first_progress_seconds

    print(f"rsb_initial_sync_seconds={coordinator_seconds:.6f}")
    print(f"first_progress_seconds={first_progress_seconds}")
    print(f"sqlite_commits={performance_pair.transaction_counter.commits}")
    print(f"max_progress_payload={max(performance_pair.transport.progress_payload_sizes)}")
    performance_pair.assert_final_base_and_echoes()
    assert coordinator_seconds <= 8.0
    assert first_progress_seconds is not None and first_progress_seconds < 1.0
    assert performance_pair.transaction_counter.commits < 100
    assert max(performance_pair.transport.progress_payload_sizes) <= 256
