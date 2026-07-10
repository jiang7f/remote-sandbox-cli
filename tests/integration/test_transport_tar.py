import io
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Literal, cast

import pytest

import remote_sandbox._transport_paths as transport_paths
import remote_sandbox.transport as transport_module
from remote_sandbox._transport_remote import REMOTE_CREATE_CODE, REMOTE_EXTRACT_CODE
from remote_sandbox.ssh import _DELETE_WORKSPACE_PATH_CODE
from remote_sandbox.transport import (
    LocalPairTransport,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferItem,
)


def test_tar_fallback_copies_multiple_files_and_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested" / "b.txt").write_text("b", encoding="utf-8")
    (source / "link").symlink_to("a.txt")
    transport = LocalPairTransport(source, destination, engine="tar")
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            tuple(TransferItem(path, None, None) for path in ("a.txt", "nested/b.txt", "link")),
        ),
        lambda _progress: None,
    )
    assert result.changed_during_transfer == ()
    assert (destination / "nested" / "b.txt").read_text(encoding="utf-8") == "b"
    assert (destination / "link").readlink() == Path("a.txt")


@pytest.mark.parametrize("direction", [TransferDirection.PUSH, TransferDirection.PULL])
def test_tar_real_pair_handles_directory_unicode_leading_dash_and_replacement(
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
    (source / "实验 data" / "-value.txt").write_text("new", encoding="utf-8")
    (source / "tree").mkdir()
    (source / "tree" / "child.txt").write_text("child", encoding="utf-8")
    (source / "link").symlink_to("实验 data/-value.txt")
    (destination / "实验 data").mkdir()
    (destination / "实验 data" / "-value.txt").write_text("old", encoding="utf-8")

    result = LocalPairTransport(local, remote, engine="tar").transfer(
        TransferBatch(
            direction,
            tuple(
                TransferItem(path, None, None)
                for path in ("实验 data/-value.txt", "tree/child.txt", "link")
            ),
        ),
        lambda _progress: None,
    )
    assert result.changed_during_transfer == ()
    assert (destination / "实验 data" / "-value.txt").read_text(encoding="utf-8") == "new"
    assert (destination / "tree" / "child.txt").read_text(encoding="utf-8") == "child"
    assert (destination / "link").readlink() == Path("实验 data/-value.txt")


def test_tar_directory_item_creates_only_the_listed_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "tree").mkdir()
    (source / "tree" / "unlisted.txt").write_text("unlisted", encoding="utf-8")
    LocalPairTransport(source, destination, engine="tar").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("tree", None, None),),
        ),
        lambda _progress: None,
    )
    assert (destination / "tree").is_dir()
    assert not (destination / "tree" / "unlisted.txt").exists()


def test_tar_batch_with_directory_and_explicit_child_finalizes_once(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "tree").mkdir()
    (source / "tree" / "child.txt").write_text("child", encoding="utf-8")
    result = LocalPairTransport(source, destination, engine="tar").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (
                TransferItem("tree", None, None),
                TransferItem("tree/child.txt", None, None),
            ),
        ),
        lambda _progress: None,
    )
    assert result.completed == ("tree", "tree/child.txt")
    assert (destination / "tree" / "child.txt").read_text(encoding="utf-8") == "child"


@pytest.mark.parametrize("source_kind", ["file", "directory"])
def test_tar_atomically_replaces_across_entry_kinds(tmp_path: Path, source_kind: str) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    if source_kind == "file":
        (source / "entry").write_text("file", encoding="utf-8")
        (destination / "entry").mkdir()
        (destination / "entry" / "old.txt").write_text("old", encoding="utf-8")
    else:
        (source / "entry").mkdir()
        (destination / "entry").write_text("old", encoding="utf-8")
    LocalPairTransport(source, destination, engine="tar").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("entry", None, None),),
        ),
        lambda _progress: None,
    )
    if source_kind == "file":
        assert (destination / "entry").read_text(encoding="utf-8") == "file"
    else:
        assert (destination / "entry").is_dir()


def test_tar_creation_disables_macos_copyfile_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    original = subprocess.run
    observed: list[dict[str, str]] = []

    def capturing_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        env = cast(dict[str, str], kwargs.get("env"))
        observed.append(env)
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(transport_module.subprocess, "run", capturing_run)
    LocalPairTransport(source, destination, engine="tar").transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", None, None),),
        ),
        lambda _progress: None,
    )
    assert observed[0]["COPYFILE_DISABLE"] == "1"


@pytest.mark.parametrize("engine", ["rsync", "tar"])
def test_parent_symlink_swap_after_preflight_never_reads_outside_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: str,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "safe").mkdir()
    (source / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    transport = LocalPairTransport(
        source,
        destination,
        engine=cast(Literal["rsync", "tar"], engine),
    )
    method_name = "_transfer_rsync" if engine == "rsync" else "_transfer_tar"
    original = getattr(transport, method_name)

    def swap_then_transfer(*args: object) -> None:
        (source / "safe").rename(source / "original-safe")
        (source / "safe").symlink_to(outside, target_is_directory=True)
        original(*args)

    monkeypatch.setattr(transport, method_name, swap_then_transfer)
    with pytest.raises((ValueError, TransferError, OSError)):
        transport.transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("safe/value.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert not (destination / "safe" / "value.txt").exists()


@pytest.mark.parametrize("engine", ["rsync", "tar"])
def test_destination_parent_swap_after_preflight_never_writes_outside_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: str,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "safe").mkdir()
    (source / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (destination / "safe").mkdir()
    transport = LocalPairTransport(
        source,
        destination,
        engine=cast(Literal["rsync", "tar"], engine),
    )
    method_name = "_transfer_rsync" if engine == "rsync" else "_transfer_tar"
    original = getattr(transport, method_name)

    def swap_then_transfer(*args: object) -> None:
        (destination / "safe").rename(destination / "original-safe")
        (destination / "safe").symlink_to(outside, target_is_directory=True)
        original(*args)

    monkeypatch.setattr(transport, method_name, swap_then_transfer)
    with pytest.raises((ValueError, TransferError, OSError)):
        transport.transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("safe/value.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert not (outside / "value.txt").exists()


def test_delete_parent_swap_never_deletes_outside_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    outside = tmp_path / "outside"
    local.mkdir()
    remote.mkdir()
    outside.mkdir()
    (local / "safe").mkdir()
    (local / "safe" / "value.txt").write_text("inside", encoding="utf-8")
    (outside / "value.txt").write_text("outside", encoding="utf-8")
    original = transport_paths._walk_parent
    swapped = False

    def swap_then_walk(root_fd: int, parts: list[str], *, create: bool) -> int:
        nonlocal swapped
        if not swapped:
            swapped = True
            (local / "safe").rename(local / "original-safe")
            (local / "safe").symlink_to(outside, target_is_directory=True)
        return original(root_fd, parts, create=create)

    monkeypatch.setattr(transport_paths, "_walk_parent", swap_then_walk)
    with pytest.raises((TransferError, OSError)):
        LocalPairTransport(local, remote).delete_local(("safe/value.txt",))
    assert (outside / "value.txt").read_text(encoding="utf-8") == "outside"


def test_tar_extract_rejects_absolute_traversal_control_and_special_members(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "malicious.tar"
    with tarfile.open(archive, "w") as handle:
        for name in ("/absolute", "../traversal", "control\nname"):
            info = tarfile.TarInfo(name)
            info.size = 1
            handle.addfile(info, io.BytesIO(b"x"))
        fifo = tarfile.TarInfo("fifo")
        fifo.type = tarfile.FIFOTYPE
        handle.addfile(fifo)
    with pytest.raises((ValueError, TransferError)):
        transport_module._extract_tar_archive(archive, tmp_path / "staging")


def test_tar_extract_rejects_member_beneath_archive_symlink(tmp_path: Path) -> None:
    archive = tmp_path / "symlink-parent.tar"
    with tarfile.open(archive, "w") as handle:
        link = tarfile.TarInfo("escape")
        link.type = tarfile.SYMTYPE
        link.linkname = "../outside"
        handle.addfile(link)
        payload = tarfile.TarInfo("escape/payload.txt")
        payload.size = 7
        handle.addfile(payload, io.BytesIO(b"escaped"))
    with pytest.raises(TransferError, match="symlink parent"):
        transport_module._extract_tar_archive(archive, tmp_path / "staging")
    assert not (tmp_path / "outside" / "payload.txt").exists()


def test_tar_destination_parent_symlink_cannot_escape(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    outside = tmp_path / "outside"
    source.mkdir()
    destination.mkdir()
    outside.mkdir()
    (source / "escape").mkdir()
    (source / "escape" / "payload.txt").write_text("payload", encoding="utf-8")
    (destination / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink parent"):
        LocalPairTransport(source, destination, engine="tar").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("escape/payload.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert not (outside / "payload.txt").exists()


def test_tar_subprocess_failure_removes_temporary_staging(
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
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 2, b"", b"tar failed"),
    )
    with pytest.raises(TransferError, match="tar failed"):
        LocalPairTransport(source, destination, engine="tar").transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", None, None),),
            ),
            lambda _progress: None,
        )
    assert list(destination.iterdir()) == []


def test_remote_tar_programs_round_trip_without_dereferencing_symlinks(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "nested").mkdir()
    (source / "nested" / "value.txt").write_text("value", encoding="utf-8")
    (source / "link").symlink_to("nested/value.txt")
    created = subprocess.run(
        [sys.executable, "-c", REMOTE_CREATE_CODE, str(source), "nested/value.txt", "link"],
        check=False,
        capture_output=True,
    )
    assert created.returncode == 0, created.stderr.decode(errors="replace")
    extracted = subprocess.run(
        [
            sys.executable,
            "-c",
            REMOTE_EXTRACT_CODE,
            str(destination),
            "nested/value.txt",
            "link",
        ],
        check=False,
        input=created.stdout,
        capture_output=True,
    )
    assert extracted.returncode == 0, extracted.stderr.decode(errors="replace")
    assert (destination / "nested" / "value.txt").read_text(encoding="utf-8") == "value"
    assert (destination / "link").readlink() == Path("nested/value.txt")


def test_tar_source_mutation_is_reported_per_item(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "a.txt").write_text("before", encoding="utf-8")
    transport = LocalPairTransport(source, destination, engine="tar")
    original = transport._transfer_tar

    def transfer_then_mutate(batch: TransferBatch, src: Path, dst: Path) -> None:
        original(batch, src, dst)
        (source / "a.txt").write_text("after", encoding="utf-8")

    monkeypatch.setattr(transport, "_transfer_tar", transfer_then_mutate)
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", None, None),),
        ),
        lambda _progress: None,
    )
    assert result.completed == ()
    assert result.changed_during_transfer == ("a.txt",)


def test_remote_delete_program_is_missing_safe_nonempty_safe_and_no_follow(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "dir").mkdir()
    (root / "dir" / "child.txt").write_text("child", encoding="utf-8")
    nonempty = subprocess.run(
        [sys.executable, "-c", _DELETE_WORKSPACE_PATH_CODE, str(root), "dir"],
        check=False,
        capture_output=True,
    )
    assert nonempty.returncode != 0
    assert (root / "dir" / "child.txt").exists()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    escaped = subprocess.run(
        [sys.executable, "-c", _DELETE_WORKSPACE_PATH_CODE, str(root), "escape/missing"],
        check=False,
        capture_output=True,
    )
    assert escaped.returncode != 0
    missing = subprocess.run(
        [sys.executable, "-c", _DELETE_WORKSPACE_PATH_CODE, str(root), "missing"],
        check=False,
        capture_output=True,
    )
    assert missing.returncode == 0


def test_local_pull_root_swap_finalizes_through_held_descriptor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    outside = tmp_path / "outside"
    local.mkdir()
    remote.mkdir()
    outside.mkdir()
    (remote / "value.txt").write_text("remote", encoding="utf-8")
    transport = LocalPairTransport(local, remote, engine="tar")
    original = transport._transfer_tar

    def swap_then_transfer(*args: object) -> None:
        local.rename(tmp_path / "original-local")
        local.symlink_to(outside, target_is_directory=True)
        original(*args)

    monkeypatch.setattr(transport, "_transfer_tar", swap_then_transfer)
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PULL,
            (TransferItem("value.txt", None, None),),
        ),
        lambda _progress: None,
    )
    assert result.completed == ("value.txt",)
    assert (tmp_path / "original-local" / "value.txt").read_text(encoding="utf-8") == "remote"
    assert not (outside / "value.txt").exists()


def test_remote_push_root_swap_never_uses_mutated_workspace_path(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as handle:
        member = tarfile.TarInfo("value.txt")
        member.size = len(b"payload")
        handle.addfile(member, io.BytesIO(b"payload"))
    marker = (
        'root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))\n'
    )
    instrumented = REMOTE_EXTRACT_CODE.replace(
        marker,
        marker
        + 'print("ROOT_OPEN", file=sys.stderr, flush=True)\n'
        + "import time\n"
        + "time.sleep(0.3)\n",
        1,
    )
    assert instrumented != REMOTE_EXTRACT_CODE
    process = subprocess.Popen(
        [sys.executable, "-c", instrumented, str(root), "value.txt"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stderr is not None
    assert process.stderr.readline() == b"ROOT_OPEN\n"
    root.rename(tmp_path / "original-root")
    root.symlink_to(outside, target_is_directory=True)
    outside.chmod(0)
    try:
        assert process.stdin is not None
        process.stdin.write(archive.getvalue())
        process.stdin.close()
        returncode = process.wait(timeout=5)
        stderr = process.stderr.read().decode(errors="replace")
    finally:
        outside.chmod(0o755)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert returncode == 0, stderr
    assert (tmp_path / "original-root" / "value.txt").read_text(encoding="utf-8") == "payload"
    assert not (outside / "value.txt").exists()
