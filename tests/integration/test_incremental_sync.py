import shutil

from helpers.sync_harness import SyncPair

from remote_sandbox.journal import EventKind
from remote_sandbox.transport import TransferDirection


def test_local_modify_and_remote_delete_are_reconciled_incrementally(
    sync_pair: SyncPair,
) -> None:
    (sync_pair.local / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.remote / "local.txt").write_text("old", encoding="utf-8")
    (sync_pair.local / "remote.txt").write_text("delete", encoding="utf-8")
    (sync_pair.remote / "remote.txt").write_text("delete", encoding="utf-8")
    sync_pair.seed_current_base()

    sync_pair.append_local_modify("local.txt", b"new")
    sync_pair.append_remote_delete("remote.txt")
    result = sync_pair.engine.run_once("integration")

    assert set(result.completed) == {"local.txt", "remote.txt"}
    assert (sync_pair.remote / "local.txt").read_bytes() == b"new"
    assert not (sync_pair.local / "remote.txt").exists()
    assert sync_pair.transport.transfer_calls == 1
    assert sync_pair.remote_client.snapshot_calls == 0


def test_move_coalesces_source_and_destination_without_full_snapshot(sync_pair: SyncPair) -> None:
    (sync_pair.local / "old.txt").write_bytes(b"content")
    (sync_pair.remote / "old.txt").write_bytes(b"content")
    sync_pair.seed_current_base()
    (sync_pair.local / "old.txt").rename(sync_pair.local / "new.txt")
    sync_pair.store.append_event("local", EventKind.MOVE, "old.txt", "new.txt")

    result = sync_pair.engine.run_once("move")

    assert result.completed == ("new.txt", "old.txt")
    assert not (sync_pair.remote / "old.txt").exists()
    assert (sync_pair.remote / "new.txt").read_bytes() == b"content"
    assert sync_pair.remote_client.metadata_calls[0] == ("new.txt", "old.txt")
    assert sync_pair.remote_client.snapshot_calls == 0


def test_engine_uses_one_transfer_batch_per_direction(sync_pair: SyncPair) -> None:
    for path in ("push.txt", "pull.txt"):
        (sync_pair.local / path).write_bytes(b"base")
        shutil.copy2(sync_pair.local / path, sync_pair.remote / path)
    sync_pair.seed_current_base()
    sync_pair.append_local_modify("push.txt", b"local")
    (sync_pair.remote / "pull.txt").write_bytes(b"remote")
    sync_pair.remote_client.append_event(EventKind.MODIFY, "pull.txt")

    result = sync_pair.engine.run_once("both-directions")

    assert result.completed == ("pull.txt", "push.txt")
    assert [batch.direction for batch in sync_pair.transport.batches] == [
        TransferDirection.PUSH,
        TransferDirection.PULL,
    ]
