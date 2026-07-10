import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import remote_sandbox.transport as transport_module
from remote_sandbox._transport_remote import (
    REMOTE_DELETE_EXPECTED_CODE,
    REMOTE_FINALIZE_RSYNC_CODE,
    REMOTE_STAGE_RSYNC_CODE,
)
from remote_sandbox.manifest import EntryFingerprint, fingerprint_local
from remote_sandbox.transport import (
    LocalPairTransport,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferItem,
)


def test_batch_push_copies_multiple_files_and_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    (source / "b.txt").write_text("b", encoding="utf-8")
    (source / "link").symlink_to("a.txt")
    transport = LocalPairTransport(source, destination, engine="rsync")
    transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            tuple(TransferItem(path, None, None) for path in ("a.txt", "b.txt", "link")),
        ),
        lambda _progress: None,
    )
    assert (destination / "a.txt").read_text(encoding="utf-8") == "a"
    assert (destination / "link").readlink() == Path("a.txt")


def test_rsync_directory_item_creates_only_the_listed_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "tree").mkdir()
    (source / "tree" / "child.txt").write_text("child", encoding="utf-8")
    LocalPairTransport(source, destination, engine="rsync").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("tree", None, None),),
        ),
        lambda _progress: None,
    )
    assert (destination / "tree").is_dir()
    assert not (destination / "tree" / "child.txt").exists()


@pytest.mark.parametrize("direction", [TransferDirection.PUSH, TransferDirection.PULL])
def test_rsync_real_pair_handles_nested_unicode_spaces_leading_dash_and_directory(
    tmp_path: Path,
    direction: TransferDirection,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local if direction is TransferDirection.PUSH else remote
    destination = remote if direction is TransferDirection.PUSH else local
    (source / "实验 data").mkdir()
    (source / "实验 data" / "-value.txt").write_text("payload", encoding="utf-8")
    (source / "tree").mkdir()
    (source / "tree" / "child.txt").write_text("child", encoding="utf-8")
    (source / "outside-link").symlink_to("实验 data/-value.txt")

    result = LocalPairTransport(local, remote, engine="rsync").transfer(
        TransferBatch(
            direction,
            tuple(
                TransferItem(path, None, None)
                for path in ("实验 data/-value.txt", "tree/child.txt", "outside-link")
            ),
        ),
        lambda _progress: None,
    )

    assert result.completed == ("实验 data/-value.txt", "tree/child.txt", "outside-link")
    assert (destination / "实验 data" / "-value.txt").read_text(encoding="utf-8") == "payload"
    assert (destination / "tree" / "child.txt").read_text(encoding="utf-8") == "child"
    assert (destination / "outside-link").readlink() == Path("实验 data/-value.txt")


@pytest.mark.parametrize("side", ["source", "destination"])
def test_rsync_rejects_parent_symlink_escape_before_launch(tmp_path: Path, side: str) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    root = source if side == "source" else destination
    (root / "escape").symlink_to(outside, target_is_directory=True)
    if side == "destination":
        (source / "escape").mkdir()
        (source / "escape" / "value.txt").write_text("value", encoding="utf-8")

    with pytest.raises(ValueError, match="symlink parent"):
        LocalPairTransport(source, destination, engine="rsync").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("escape/value.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert not (outside / "value.txt").exists()


def test_rsync_preflight_checks_expected_source_and_destination(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("new", encoding="utf-8")
    (destination / "a.txt").write_text("old", encoding="utf-8")
    wrong_source = fingerprint_local(destination, "a.txt", with_hash=True)
    actual_source = fingerprint_local(source, "a.txt", with_hash=True)

    with pytest.raises(TransferError, match="source fingerprint mismatch"):
        LocalPairTransport(source, destination, engine="rsync").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", wrong_source, None),),
            ),
            lambda _progress: None,
        )
    assert actual_source != wrong_source

    (destination / "a.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(TransferError, match="destination fingerprint mismatch"):
        LocalPairTransport(source, destination, engine="rsync").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (
                    TransferItem(
                        "a.txt",
                        actual_source,
                        fingerprint_local(source, "a.txt", with_hash=True),
                    ),
                ),
            ),
            lambda _progress: None,
        )


def test_rsync_source_mutation_is_reported_without_false_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "stable.txt").write_text("stable", encoding="utf-8")
    (source / "moving.txt").write_text("before", encoding="utf-8")
    transport = LocalPairTransport(source, destination, engine="rsync")
    original = transport._transfer_rsync

    def transfer_then_mutate(
        batch: TransferBatch,
        src: Path,
        dst: Path,
        before: object,
    ) -> None:
        original(batch, src, dst, before)  # type: ignore[arg-type]
        (source / "moving.txt").write_text("after", encoding="utf-8")

    monkeypatch.setattr(transport, "_transfer_rsync", transfer_then_mutate)
    progress: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem("stable.txt", None, None),
                TransferItem("moving.txt", None, None),
            ),
        ),
        lambda current: progress.append((current.completed, current.changed_during_transfer)),
    )
    assert result.completed == ("stable.txt",)
    assert result.changed_during_transfer == ("moving.txt",)
    assert progress == [(("stable.txt",), ())]


def test_rsync_zero_exit_never_bypasses_post_transfer_verification(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    transport = LocalPairTransport(source, destination, engine="rsync")
    monkeypatch.setattr(transport, "_transfer_rsync", lambda *args: None)
    with pytest.raises(TransferError, match="post-transfer verification"):
        transport.transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", None, None),),
            ),
            lambda _progress: None,
        )


def test_rsync_nonzero_reports_partial_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    monkeypatch.setattr(
        transport_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 23, b">f a.txt\n", b"partial"),
    )
    with pytest.raises(TransferError, match="partial"):
        LocalPairTransport(source, destination, engine="rsync").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", None, None),),
            ),
            lambda _progress: None,
        )


def test_delete_local_is_child_first_idempotent_and_symlink_safe(tmp_path: Path) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    outside = tmp_path / "outside"
    local.mkdir()
    remote.mkdir()
    outside.mkdir()
    (local / "dir").mkdir()
    (local / "dir" / "file.txt").write_text("x", encoding="utf-8")
    (outside / "keep.txt").write_text("keep", encoding="utf-8")
    (local / "link").symlink_to(outside, target_is_directory=True)
    transport = LocalPairTransport(local, remote)
    transport.delete_local(("dir", "dir/file.txt", "missing", "link"))
    assert not (local / "dir").exists()
    assert not os.path.lexists(local / "link")
    assert (outside / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_delete_rejects_nonempty_directory_and_parent_symlink(tmp_path: Path) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    outside = tmp_path / "outside"
    local.mkdir()
    remote.mkdir()
    outside.mkdir()
    (local / "dir").mkdir()
    (local / "dir" / "keep.txt").write_text("keep", encoding="utf-8")
    transport = LocalPairTransport(local, remote)
    with pytest.raises((OSError, TransferError)):
        transport.delete_local(("dir",))
    (local / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises((ValueError, OSError, TransferError)):
        transport.delete_local(("escape/keep.txt",))


def test_verified_local_delete_restores_replacement_racing_with_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    path = "value.txt"
    (local / path).write_bytes(b"expected")
    expected = fingerprint_local(local, path, with_hash=True)
    assert isinstance(expected, EntryFingerprint)
    original_rename = os.rename
    replaced = False

    def replace_before_quarantine(
        source: str,
        destination: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal replaced
        if source == path and not replaced:
            replaced = True
            (local / path).write_bytes(b"concurrent")
        original_rename(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )

    monkeypatch.setattr(
        "remote_sandbox._transport_fingerprint.os.rename",
        replace_before_quarantine,
    )

    result = LocalPairTransport(local, remote).delete_local({path: expected})

    assert result.completed == ()
    assert result.changed_during_transfer == (path,)
    assert (local / path).read_bytes() == b"concurrent"


def test_remote_verified_delete_program_preserves_fingerprint_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "remote"
    root.mkdir()
    path = "value.txt"
    (root / path).write_bytes(b"expected")
    expected = fingerprint_local(root, path, with_hash=True)
    assert isinstance(expected, EntryFingerprint)
    (root / path).write_bytes(b"concurrent")
    payload = json.dumps(
        {
            "entries": [
                {
                    "path": expected.path,
                    "missing": False,
                    "kind": expected.kind.value,
                    "size": expected.size,
                    "mtime_ns": expected.mtime_ns,
                    "mode": expected.mode,
                    "link_target": expected.link_target,
                    "content_hash": expected.content_hash,
                }
            ]
        }
    ).encode()

    result = subprocess.run(
        [sys.executable, "-c", REMOTE_DELETE_EXPECTED_CODE, str(root)],
        input=payload,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr.decode()
    assert json.loads(result.stdout) == {"completed": [], "changed": [path]}
    assert (root / path).read_bytes() == b"concurrent"


def test_remote_rsync_stage_and_finalize_programs_use_workspace_descriptors(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "nested").mkdir()
    (source / "nested" / "value.txt").write_text("value", encoding="utf-8")
    staged = subprocess.run(
        [
            sys.executable,
            "-c",
            REMOTE_STAGE_RSYNC_CODE,
            str(source),
            "nested/value.txt",
        ],
        check=False,
        capture_output=True,
    )
    assert staged.returncode == 0, staged.stderr.decode(errors="replace")
    stage_path = staged.stdout.decode().strip()
    finalized = subprocess.run(
        [
            sys.executable,
            "-c",
            REMOTE_FINALIZE_RSYNC_CODE,
            str(destination),
            stage_path,
            "nested/value.txt",
        ],
        check=False,
        capture_output=True,
    )
    assert finalized.returncode == 0, finalized.stderr.decode(errors="replace")
    assert (destination / "nested" / "value.txt").read_text(encoding="utf-8") == "value"
    assert not Path(stage_path).exists()


def test_remote_rsync_stage_rejects_source_parent_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    outside = tmp_path / "outside"
    source.mkdir()
    outside.mkdir()
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    (source / "escape").symlink_to(outside, target_is_directory=True)
    staged = subprocess.run(
        [
            sys.executable,
            "-c",
            REMOTE_STAGE_RSYNC_CODE,
            str(source),
            "escape/value.txt",
        ],
        check=False,
        capture_output=True,
    )
    assert staged.returncode != 0
