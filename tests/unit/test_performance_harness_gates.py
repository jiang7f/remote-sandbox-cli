from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from helpers.sync_harness import make_performance_pair

from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox.manifest import fingerprint_local
from remote_sandbox.transport import LocalPairTransport, TransferBatch, TransferResult


def test_batch_measurement_times_the_complete_transfer_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pair = make_performance_pair(tmp_path)
    pair.populate(1)
    batch = pair.initial_transfer_batch()
    original = LocalPairTransport.transfer

    def delayed_transfer(
        self: LocalPairTransport,
        selected: TransferBatch,
        on_progress: Callable[[TransferResult], None],
    ) -> TransferResult:
        time.sleep(0.05)
        return original(self, selected, on_progress)

    monkeypatch.setattr(LocalPairTransport, "transfer", delayed_transfer)
    try:
        elapsed, _processes, _payload = pair.measure_batch_transport(
            batch,
            tmp_path / "measured",
        )
    finally:
        pair.close()

    assert elapsed >= 0.05


def test_batch_measurement_counts_actual_subprocess_launches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pair = make_performance_pair(tmp_path)
    pair.populate(1)
    batch = pair.initial_transfer_batch()

    def two_process_transfer(
        _self: LocalPairTransport,
        selected: TransferBatch,
        on_progress: Callable[[TransferResult], None],
    ) -> TransferResult:
        subprocess.run(["true"], check=True)
        subprocess.run(["true"], check=True)
        result = TransferResult(tuple(item.path for item in selected.items), ())
        on_progress(result)
        return result

    monkeypatch.setattr(LocalPairTransport, "transfer", two_process_transfer)
    try:
        _elapsed, processes, _payload = pair.measure_batch_transport(
            batch,
            tmp_path / "counted",
        )
    finally:
        pair.close()

    assert processes == 2


def test_hash_counter_counts_local_and_remote_content_hashes(tmp_path: Path) -> None:
    pair = make_performance_pair(tmp_path)
    pair.populate(1)
    path = "files/000/file-00000.txt"
    remote_path = pair.remote / path
    remote_path.parent.mkdir(parents=True)
    remote_path.write_bytes((pair.local / path).read_bytes())
    pair.hash_counter.reset()
    try:
        with ProtectedLocalRoot(pair.local) as local:
            local.fingerprint(path, with_hash=True)
        fingerprint_local(pair.remote, path, with_hash=True)

        assert pair.hash_counter.local_count == 1
        assert pair.hash_counter.remote_count == 1
        assert pair.hash_counter.count == 2
    finally:
        pair.close()
