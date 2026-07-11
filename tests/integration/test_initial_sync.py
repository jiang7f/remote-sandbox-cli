from __future__ import annotations

from helpers.sync_harness import InitialPairHarness

from remote_sandbox.manifest import EntryKind
from remote_sandbox.placeholder import (
    PlaceholderMetadata,
    decode_placeholder,
    encode_placeholder,
)
from remote_sandbox.status import WorkspaceStatus
from remote_sandbox.transport import TransferDirection


def test_remote_source_bulk_sync_starts_watchers_before_copy(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "a.txt").write_text("a", encoding="utf-8")

    result = initial_pair.coordinator.run()

    assert result.direction.value == "remote-to-local"
    assert initial_pair.local_watcher.started_before_transfer is True
    assert initial_pair.remote_watcher.started_before_transfer is True
    assert (initial_pair.local / "a.txt").read_text(encoding="utf-8") == "a"
    assert initial_pair.store.get_status().phase.value == "ready"
    assert initial_pair.store.get_initial_sync_watermarks() is None
    assert initial_pair.transport.transfer_calls == 1


def test_local_source_bulk_sync_uses_the_same_immediate_coordinator(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.local / "local.txt").write_text("local", encoding="utf-8")

    result = initial_pair.coordinator.run()

    assert result.direction.value == "local-to-remote"
    assert (initial_pair.remote / "local.txt").read_text(encoding="utf-8") == "local"
    assert initial_pair.transport.transfer_calls == 1


def test_both_empty_reaches_ready_without_a_transfer(initial_pair: InitialPairHarness) -> None:
    result = initial_pair.coordinator.run()

    assert result.direction.value == "empty"
    assert initial_pair.transport.transfer_calls == 0
    assert initial_pair.store.get_status().progress.stage == "ready"


def test_both_empty_persists_zero_total_transferring_stage(
    initial_pair: InitialPairHarness,
    monkeypatch,
) -> None:
    statuses: list[WorkspaceStatus] = []
    original = initial_pair.store.set_status

    def record(status: WorkspaceStatus) -> None:
        statuses.append(status)
        original(status)

    monkeypatch.setattr(initial_pair.store, "set_status", record)

    initial_pair.coordinator.run()

    _assert_zero_transfer_stage_sequence(statuses)


def test_large_remote_file_becomes_a_validated_local_placeholder(
    initial_pair: InitialPairHarness,
) -> None:
    initial_pair.set_placeholder_limit(4)
    (initial_pair.remote / "weights.bin").write_bytes(b"0123456789")

    result = initial_pair.coordinator.run()

    metadata = decode_placeholder(
        (initial_pair.local / "weights.bin").read_bytes(),
        expected_path="weights.bin",
    )
    assert metadata is not None
    assert metadata.size == 10
    assert (initial_pair.remote / "weights.bin").read_bytes() == b"0123456789"
    assert result.placeholders == 1
    assert {
        path for call in initial_pair.remote_client.hash_calls for path in call
    } == {"weights.bin"}
    assert initial_pair.transport.transfer_calls == 0


def test_placeholder_only_persists_zero_total_transferring_stage(
    initial_pair: InitialPairHarness,
    monkeypatch,
) -> None:
    initial_pair.set_placeholder_limit(4)
    (initial_pair.remote / "weights.bin").write_bytes(b"0123456789")
    statuses: list[WorkspaceStatus] = []
    original = initial_pair.store.set_status

    def record(status: WorkspaceStatus) -> None:
        statuses.append(status)
        original(status)

    monkeypatch.setattr(initial_pair.store, "set_status", record)

    initial_pair.coordinator.run()

    _assert_zero_transfer_stage_sequence(statuses)


def test_plan_contains_explicit_deterministic_entry_types(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.local / "pkg").mkdir()
    (initial_pair.local / "pkg" / "z.txt").write_text("z", encoding="utf-8")
    (initial_pair.local / "a-link").symlink_to("pkg/z.txt")

    initial_pair.coordinator.run()

    batch = initial_pair.transport.batches[0]
    assert batch.direction is TransferDirection.PUSH
    assert [item.path for item in batch.items] == ["pkg", "a-link", "pkg/z.txt"]
    assert [item.expected_source.kind for item in batch.items] == [
        EntryKind.DIR,
        EntryKind.SYMLINK,
        EntryKind.FILE,
    ]


def test_quick_initial_snapshot_does_not_hash_regular_file_fingerprints(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.remote / "small.txt").write_text("small", encoding="utf-8")

    initial_pair.coordinator.run()

    batch = initial_pair.transport.batches[0]
    assert batch.items[0].expected_source.content_hash is None
    assert initial_pair.remote_client.snapshot_calls == 1


def test_progress_persists_all_stages_and_live_transfer_fields(
    initial_pair: InitialPairHarness,
    monkeypatch,
) -> None:
    (initial_pair.remote / "a.txt").write_text("alpha", encoding="utf-8")
    statuses: list[WorkspaceStatus] = []
    original = initial_pair.store.set_status

    def record(status: WorkspaceStatus) -> None:
        statuses.append(status)
        original(status)

    monkeypatch.setattr(initial_pair.store, "set_status", record)

    initial_pair.coordinator.run()

    stages = [status.progress.stage for status in statuses]
    assert stages.index("scanning") < stages.index("planning")
    assert stages.index("planning") < stages.index("transferring")
    assert stages.index("transferring") < stages.index("replaying")
    assert stages[-1] == "ready"
    live = [
        status.progress
        for status in statuses
        if status.progress.stage == "transferring" and status.progress.files_done
    ][-1]
    assert live.files_done == live.files_total == 1
    assert live.bytes_done == live.bytes_total == 5
    assert live.current_path == "a.txt"
    assert live.elapsed_seconds >= 0


def test_local_placeholder_text_is_never_uploaded_as_regular_content(
    initial_pair: InitialPairHarness,
) -> None:
    content = encode_placeholder(
        PlaceholderMetadata("weights.bin", 100, 5, "a" * 64)
    )
    (initial_pair.local / "weights.bin").write_bytes(content)

    initial_pair.coordinator.run()

    assert not (initial_pair.remote / "weights.bin").exists()
    assert initial_pair.transport.transfer_calls == 0


def test_initial_snapshot_excludes_git_and_policy_ignored_entries(
    initial_pair: InitialPairHarness,
) -> None:
    (initial_pair.local / ".git").mkdir()
    (initial_pair.local / ".git" / "config").write_text("secret", encoding="utf-8")
    (initial_pair.local / ".rsbignore").write_text("ignored.txt\n", encoding="utf-8")
    initial_pair.engine.policy = initial_pair.coordinator.policy = type(
        initial_pair.coordinator.policy
    ).from_file(initial_pair.local / ".rsbignore")
    initial_pair.engine.local_metadata.policy = initial_pair.engine.policy
    (initial_pair.local / "ignored.txt").write_text("ignored", encoding="utf-8")
    (initial_pair.local / "kept.txt").write_text("kept", encoding="utf-8")

    initial_pair.coordinator.run()

    assert not (initial_pair.remote / ".git").exists()
    assert not (initial_pair.remote / "ignored.txt").exists()
    assert (initial_pair.remote / "kept.txt").read_text(encoding="utf-8") == "kept"


def _assert_zero_transfer_stage_sequence(statuses: list[WorkspaceStatus]) -> None:
    stages = [status.progress.stage for status in statuses]
    planning = stages.index("planning")
    transferring = stages.index("transferring")
    replaying = stages.index("replaying")
    ready = len(stages) - 1
    assert planning < transferring < replaying < ready
    progress = statuses[transferring].progress
    assert progress.files_done == progress.files_total == 0
    assert progress.bytes_done == progress.bytes_total == 0
    assert stages[ready] == "ready"
