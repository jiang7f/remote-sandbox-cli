from __future__ import annotations

import errno
import os
import threading
import time
from pathlib import Path

import pytest

import remote_sandbox._transport_local as local_transport_module
import remote_sandbox._transport_paths as transport_paths
from remote_sandbox._transport_fingerprint import ProtectedLocalRoot
from remote_sandbox._transport_local import LocalPairTransport
from remote_sandbox.manifest import fingerprint_local
from remote_sandbox.transport import (
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferItem,
)


def _push_batch(local: Path, remote: Path, path: str) -> TransferBatch:
    return TransferBatch(
        TransferDirection.PUSH,
        (
            TransferItem(
                path,
                fingerprint_local(local, path, with_hash=True),
                fingerprint_local(remote, path, with_hash=True),
            ),
        ),
    )


def test_regular_file_staging_uses_hardlink_on_same_filesystem(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "model.py"
    source.write_bytes(b"original")
    staging = tmp_path / "staging"

    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(("model.py",), staging, error_type=TransferError)

    staged = staging / "model.py"
    assert staged.read_bytes() == b"original"
    assert staged.stat().st_ino == source.stat().st_ino
    assert staged.stat().st_dev == source.stat().st_dev


def test_in_place_source_mutation_is_reported_and_destination_is_not_hardlinked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "model.py"
    source.write_bytes(b"original")
    batch = _push_batch(local, remote, "model.py")
    run = local_transport_module.subprocess.run

    def mutate_then_run(*args: object, **kwargs: object) -> object:
        source.write_bytes(b"mutated-in-place")
        return run(*args, **kwargs)

    monkeypatch.setattr(local_transport_module.subprocess, "run", mutate_then_run)
    result = LocalPairTransport(local, remote).transfer(batch, lambda _progress: None)

    destination = remote / "model.py"
    assert result.completed == ()
    assert result.changed_during_transfer == ("model.py",)
    assert destination.stat().st_ino != source.stat().st_ino


def test_source_replacement_keeps_staged_snapshot_and_reports_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "model.py"
    source.write_bytes(b"original")
    batch = _push_batch(local, remote, "model.py")
    run = local_transport_module.subprocess.run

    def replace_then_run(*args: object, **kwargs: object) -> object:
        replacement = local / "replacement.py"
        replacement.write_bytes(b"replacement")
        os.replace(replacement, source)
        return run(*args, **kwargs)

    monkeypatch.setattr(local_transport_module.subprocess, "run", replace_then_run)
    result = LocalPairTransport(local, remote).transfer(batch, lambda _progress: None)

    destination = remote / "model.py"
    assert result.completed == ()
    assert result.changed_during_transfer == ("model.py",)
    assert destination.read_bytes() == b"original"
    assert destination.stat().st_ino != source.stat().st_ino


def test_symlink_staging_preserves_link_without_hardlinking_target(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "target.txt").write_bytes(b"target")
    (source_root / "link.txt").symlink_to("target.txt")
    staging = tmp_path / "staging"

    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(("link.txt",), staging, error_type=TransferError)

    staged = staging / "link.txt"
    assert staged.is_symlink()
    assert staged.readlink() == Path("target.txt")
    assert staged.lstat().st_ino != (source_root / "link.txt").lstat().st_ino


@pytest.mark.parametrize("error_number", [errno.EXDEV, errno.EOPNOTSUPP])
def test_hardlink_staging_falls_back_to_descriptor_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_number: int,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "model.py"
    source.write_bytes(b"content")
    staging = tmp_path / "staging"
    calls = 0

    def unsupported_link(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise OSError(error_number, "hardlinks unavailable")

    monkeypatch.setattr(transport_paths.os, "link", unsupported_link)
    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(("model.py",), staging, error_type=TransferError)

    staged = staging / "model.py"
    assert calls == 1
    assert staged.read_bytes() == b"content"
    assert staged.stat().st_ino != source.stat().st_ino


def test_successful_transfer_installs_copy_not_source_hardlink(tmp_path: Path) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "model.py"
    source.write_bytes(b"content")

    result = LocalPairTransport(local, remote).transfer(
        _push_batch(local, remote, "model.py"),
        lambda _progress: None,
    )

    destination = remote / "model.py"
    assert result.completed == ("model.py",)
    assert destination.read_bytes() == b"content"
    assert destination.stat().st_ino != source.stat().st_ino


def test_staging_reuses_one_parent_descriptor_per_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source"
    parent = source_root / "pkg"
    parent.mkdir(parents=True)
    for name in ("a.py", "b.py", "c.py"):
        (parent / name).write_text(name, encoding="utf-8")
    walk_parent = transport_paths._walk_parent
    calls: list[tuple[str, ...]] = []

    def record_walk(root_fd: int, parts: list[str], *, create: bool) -> int:
        calls.append(tuple(parts))
        return walk_parent(root_fd, parts, create=create)

    monkeypatch.setattr(transport_paths, "_walk_parent", record_walk)
    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(
            ("pkg/a.py", "pkg/b.py", "pkg/c.py"),
            tmp_path / "staging",
            error_type=TransferError,
        )

    assert calls == [("pkg",)]


def test_staging_rejects_symlink_parent_and_removes_partial_stage(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    outside = tmp_path / "outside"
    source_root.mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    (source_root / "alias").symlink_to(outside, target_is_directory=True)
    staging = tmp_path / "staging"

    with ProtectedLocalRoot(source_root) as protected, pytest.raises(OSError):
        protected.stage(("alias/secret.txt",), staging, error_type=TransferError)

    assert not staging.exists()


def test_grouped_staging_preserves_mixed_directories_files_and_symlinks(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    package = source_root / "pkg"
    package.mkdir(parents=True)
    (package / "model.py").write_text("model", encoding="utf-8")
    (package / "current.py").symlink_to("model.py")
    staging = tmp_path / "staging"

    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(
            ("pkg", "pkg/model.py", "pkg/current.py"),
            staging,
            error_type=TransferError,
        )

    assert (staging / "pkg").is_dir()
    assert (staging / "pkg/model.py").read_text(encoding="utf-8") == "model"
    assert (staging / "pkg/current.py").is_symlink()
    assert (staging / "pkg/current.py").readlink() == Path("model.py")


def test_staging_error_closes_parent_and_removes_partial_tree(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    special = source_root / "pipe"
    os.mkfifo(special)
    staging = tmp_path / "staging"

    with (
        ProtectedLocalRoot(source_root) as protected,
        pytest.raises(TransferError, match="special files are not transferable"),
    ):
        protected.stage(("pipe",), staging, error_type=TransferError)

    assert not staging.exists()


def test_staging_uses_at_most_four_parent_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source"
    paths = []
    for index in range(8):
        path = source_root / f"pkg-{index}/model.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(index), encoding="utf-8")
        paths.append(path.relative_to(source_root).as_posix())
    copy_entry = transport_paths._copy_from_descriptor
    active = 0
    maximum = 0
    lock = threading.Lock()

    def record_concurrency(*args: object, **kwargs: object) -> None:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        try:
            time.sleep(0.02)
            copy_entry(*args, **kwargs)  # type: ignore[arg-type]
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(transport_paths, "_copy_from_descriptor", record_concurrency)
    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(tuple(paths), tmp_path / "staging", error_type=TransferError)

    assert 1 < maximum <= 4


def test_copy_fallback_remains_sequential_across_parent_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source"
    paths = []
    for index in range(6):
        path = source_root / f"pkg-{index}/model.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(index), encoding="utf-8")
        paths.append(path.relative_to(source_root).as_posix())
    copyfileobj = transport_paths.shutil.copyfileobj
    active = 0
    maximum = 0
    lock = threading.Lock()

    monkeypatch.setattr(
        transport_paths.os,
        "link",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError(errno.EXDEV, "cross-device")),
    )

    def record_copy(*args: object, **kwargs: object) -> None:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        try:
            time.sleep(0.01)
            copyfileobj(*args, **kwargs)  # type: ignore[arg-type]
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(transport_paths.shutil, "copyfileobj", record_copy)
    with ProtectedLocalRoot(source_root) as protected:
        protected.stage(tuple(paths), tmp_path / "staging", error_type=TransferError)

    assert maximum == 1


def test_parallel_staging_is_deterministic_for_mixed_entries(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    paths = []
    for index in range(8):
        parent = source_root / f"pkg-{index}"
        parent.mkdir(parents=True)
        (parent / "model.py").write_text(str(index), encoding="utf-8")
        (parent / "current.py").symlink_to("model.py")
        paths.extend((f"pkg-{index}", f"pkg-{index}/model.py", f"pkg-{index}/current.py"))

    snapshots: list[list[tuple[str, str, int]]] = []
    for run in range(2):
        staging = tmp_path / f"staging-{run}"
        with ProtectedLocalRoot(source_root) as protected:
            protected.stage(tuple(paths), staging, error_type=TransferError)
        snapshot = []
        for path in sorted(staging.rglob("*")):
            relative = path.relative_to(staging).as_posix()
            kind = "link" if path.is_symlink() else "dir" if path.is_dir() else "file"
            content = path.readlink().as_posix() if path.is_symlink() else ""
            snapshot.append((relative, f"{kind}:{content}", path.lstat().st_mode & 0o777))
        snapshots.append(snapshot)

    assert snapshots[0] == snapshots[1]


def test_local_rsync_uses_whole_private_stage_without_itemize_or_files_from(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    (local / "model.py").write_text("model", encoding="utf-8")
    run = local_transport_module.subprocess.run
    captured: list[tuple[list[str], object]] = []

    def capture(argv: list[str], **kwargs: object) -> object:
        captured.append((argv, kwargs.get("input")))
        return run(argv, **kwargs)

    monkeypatch.setattr(local_transport_module.subprocess, "run", capture)
    LocalPairTransport(local, remote).transfer(
        _push_batch(local, remote, "model.py"),
        lambda _progress: None,
    )

    assert len(captured) == 1
    argv, stdin = captured[0]
    assert "--files-from=-" not in argv
    assert "--itemize-changes" not in argv
    assert stdin is None
