from __future__ import annotations

import os
import stat
import tarfile
from collections.abc import Callable
from pathlib import Path

from remote_sandbox.manifest import workspace_path


def extract_tar_archive(
    archive: Path,
    staging: Path,
    *,
    validate_member: Callable[[str], str],
    error_type: type[Exception],
) -> None:
    staging.mkdir(mode=0o700, parents=True, exist_ok=False)
    with tarfile.open(archive, "r:") as handle:
        members = handle.getmembers()
        names: list[str] = []
        symlinks: set[str] = set()
        for member in members:
            name = validate_member(member.name.rstrip("/"))
            if name in names:
                raise error_type(f"duplicate tar member: {name}")
            names.append(name)
            if member.issym():
                symlinks.add(name)
            elif not (member.isfile() or member.isdir()):
                raise error_type(f"unsupported tar member type: {name}")

        for name in names:
            if any(_is_parent(link, name) for link in symlinks):
                raise error_type(f"tar member has symlink parent: {name}")

        for member, name in zip(members, names, strict=True):
            destination = workspace_path(staging, name)
            _mkdir_parents(staging, destination.parent)
            if member.isdir():
                destination.mkdir(mode=member.mode & 0o777, exist_ok=False)
                continue
            if member.issym():
                os.symlink(member.linkname, destination)
                continue
            source = handle.extractfile(member)
            if source is None:
                raise error_type(f"tar file member has no payload: {name}")
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(destination, flags, member.mode & 0o777)
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as output:
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
            finally:
                source.close()
                os.close(descriptor)
            os.chmod(destination, stat.S_IMODE(member.mode), follow_symlinks=False)


def _mkdir_parents(staging: Path, parent: Path) -> None:
    relative = parent.relative_to(staging)
    current = staging
    for part in relative.parts:
        current = workspace_path(staging, (current / part).relative_to(staging).as_posix())
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            continue
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise ValueError(f"tar extraction parent is not a directory: {current}")


def _is_parent(parent: str, path: str) -> bool:
    return path.startswith(f"{parent}/")
