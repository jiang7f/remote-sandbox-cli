from __future__ import annotations

import contextlib
import errno
import os
import secrets
import shutil
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_HARDLINK_FALLBACK_ERRORS = {
    errno.EXDEV,
    errno.EPERM,
    errno.EACCES,
    errno.EMLINK,
    getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
    errno.EOPNOTSUPP,
}
_STAGING_WORKERS = 4


class _HardlinkUnavailable(OSError):
    pass


def stage_entries_from_fd(
    root_fd: int,
    paths: tuple[str, ...],
    staging: Path,
    *,
    error_type: type[Exception],
) -> None:
    staging.mkdir(mode=0o700, parents=True, exist_ok=False)
    grouped: dict[tuple[str, ...], list[tuple[str, str]]] = {}
    for relative in paths:
        parts = relative.split("/")
        grouped.setdefault(tuple(parts[:-1]), []).append((parts[-1], relative))
    try:
        _stage_grouped_entries(
            root_fd,
            grouped,
            staging,
            workers=_STAGING_WORKERS,
            require_hardlinks=True,
            try_hardlinks=True,
            error_type=error_type,
        )
    except _HardlinkUnavailable:
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(mode=0o700, parents=True, exist_ok=False)
        try:
            _stage_grouped_entries(
                root_fd,
                grouped,
                staging,
                workers=1,
                require_hardlinks=False,
                try_hardlinks=False,
                error_type=error_type,
            )
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _stage_grouped_entries(
    root_fd: int,
    grouped: dict[tuple[str, ...], list[tuple[str, str]]],
    staging: Path,
    *,
    workers: int,
    require_hardlinks: bool,
    try_hardlinks: bool,
    error_type: type[Exception],
) -> None:
    by_depth: dict[int, list[tuple[tuple[str, ...], list[tuple[str, str]]]]] = {}
    for parent_parts, leaves in grouped.items():
        by_depth.setdefault(len(parent_parts), []).append((parent_parts, leaves))
    for depth in sorted(by_depth):
        groups = sorted(by_depth[depth], key=lambda item: item[0])

        def stage_group(
            item: tuple[tuple[str, ...], list[tuple[str, str]]],
        ) -> None:
            parent_parts, leaves = item
            parent_fd = _walk_parent(root_fd, list(parent_parts), create=False)
            try:
                destination_parent = staging.joinpath(*parent_parts)
                destination_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                for leaf, relative in leaves:
                    _copy_from_descriptor(
                        parent_fd,
                        leaf,
                        staging / relative,
                        parent_ready=True,
                        require_hardlink=require_hardlinks,
                        try_hardlink=try_hardlinks,
                        error_type=error_type,
                    )
            finally:
                os.close(parent_fd)

        if workers == 1 or len(groups) == 1:
            for group in groups:
                stage_group(group)
        else:
            with ThreadPoolExecutor(max_workers=min(workers, len(groups))) as executor:
                tuple(executor.map(stage_group, groups))


def finalize_entries_from_fd(
    root_fd: int,
    staging: Path,
    paths: tuple[str, ...],
    *,
    error_type: type[Exception],
) -> None:
    for relative in _top_level_paths(paths):
        parts = relative.split("/")
        parent_fd = _walk_parent(root_fd, parts[:-1], create=True)
        try:
            _install_at(
                staging / relative,
                parent_fd,
                parts[-1],
                error_type=error_type,
            )
        finally:
            os.close(parent_fd)


def delete_entries_from_fd(
    root_fd: int,
    paths: tuple[str, ...],
    *,
    error_type: type[Exception],
) -> None:
    for relative in paths:
        parts = relative.split("/")
        try:
            parent_fd = _walk_parent(root_fd, parts[:-1], create=False)
        except FileNotFoundError:
            continue
        try:
            try:
                entry = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
                os.rmdir(parts[-1], dir_fd=parent_fd)
            else:
                os.unlink(parts[-1], dir_fd=parent_fd)
        except OSError as exc:
            raise error_type(f"local workspace delete failed: {relative}: {exc}") from exc
        finally:
            os.close(parent_fd)


def _copy_from_descriptor(
    parent_fd: int,
    leaf: str,
    destination: Path,
    *,
    parent_ready: bool = False,
    require_hardlink: bool = False,
    try_hardlink: bool = True,
    error_type: type[Exception],
) -> None:
    try:
        entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise error_type(f"source path became unavailable: {destination}: {exc}") from exc
    if not parent_ready:
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_ISDIR(entry.st_mode):
        destination.mkdir(mode=stat.S_IMODE(entry.st_mode), exist_ok=True)
        return
    if stat.S_ISLNK(entry.st_mode):
        os.symlink(os.readlink(leaf, dir_fd=parent_fd), destination)
        return
    if not stat.S_ISREG(entry.st_mode):
        raise error_type(f"special files are not transferable: {destination}")
    if try_hardlink:
        try:
            os.link(
                leaf,
                destination,
                src_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            if exc.errno not in _HARDLINK_FALLBACK_ERRORS:
                raise error_type(
                    f"source path became unavailable: {destination}: {exc}"
                ) from exc
            if require_hardlink:
                raise _HardlinkUnavailable(exc.errno, str(exc)) from exc
        else:
            staged = destination.stat(follow_symlinks=False)
            if (staged.st_dev, staged.st_ino) != (entry.st_dev, entry.st_ino):
                destination.unlink(missing_ok=True)
                raise error_type(f"source changed while staging: {destination}")
            return
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(leaf, flags, dir_fd=parent_fd)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
            raise error_type(f"source changed while opening: {destination}")
        with (
            os.fdopen(descriptor, "rb", closefd=False) as source,
            destination.open("xb") as output,
        ):
            shutil.copyfileobj(source, output, length=1024 * 1024)
        os.chmod(destination, stat.S_IMODE(entry.st_mode), follow_symlinks=False)
    finally:
        os.close(descriptor)


def _install_at(
    source: Path,
    parent_fd: int,
    leaf: str,
    *,
    error_type: type[Exception],
) -> None:
    temporary = f".remote-sandbox-new-{secrets.token_hex(8)}"
    backup = f".remote-sandbox-old-{secrets.token_hex(8)}"
    try:
        try:
            os.rename(source, temporary, dst_dir_fd=parent_fd)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            _copy_path_to_directory(source, parent_fd, temporary)
        had_destination = False
        try:
            os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            had_destination = True
            os.rename(leaf, backup, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        try:
            os.rename(temporary, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        except BaseException:
            if had_destination:
                os.rename(backup, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            raise
        if had_destination:
            _remove_at(parent_fd, backup)
    except OSError as exc:
        with contextlib.suppress(OSError):
            _remove_at(parent_fd, temporary)
        raise error_type(f"atomic local replacement failed: {leaf}: {exc}") from exc


def _copy_path_to_directory(source: Path, parent_fd: int, name: str) -> None:
    entry = source.lstat()
    if stat.S_ISLNK(entry.st_mode):
        os.symlink(os.readlink(source), name, dir_fd=parent_fd)
        return
    if stat.S_ISREG(entry.st_mode):
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(name, flags, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
        try:
            with (
                source.open("rb") as input_file,
                os.fdopen(descriptor, "wb", closefd=False) as output,
            ):
                shutil.copyfileobj(input_file, output, length=1024 * 1024)
        finally:
            os.close(descriptor)
        return
    if not stat.S_ISDIR(entry.st_mode):
        raise OSError(errno.EINVAL, "unsupported staged entry")
    os.mkdir(name, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
    descriptor = _open_directory(name, dir_fd=parent_fd)
    try:
        for child in source.iterdir():
            _copy_path_to_directory(child, descriptor, child.name)
    finally:
        os.close(descriptor)


def _remove_at(parent_fd: int, name: str) -> None:
    try:
        entry = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
        descriptor = _open_directory(name, dir_fd=parent_fd)
        try:
            for child in os.listdir(descriptor):
                _remove_at(descriptor, child)
        finally:
            os.close(descriptor)
        os.rmdir(name, dir_fd=parent_fd)
        return
    os.unlink(name, dir_fd=parent_fd)


def _walk_parent(root_fd: int, parts: list[str], *, create: bool) -> int:
    descriptor = os.dup(root_fd)
    try:
        for part in parts:
            try:
                child = _open_directory(part, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, 0o700, dir_fd=descriptor)
                child = _open_directory(part, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_directory(path: str, *, dir_fd: int | None = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    return os.open(path, flags, dir_fd=dir_fd)


def _top_level_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        path
        for path in paths
        if not any(other != path and path.startswith(f"{other}/") for other in paths)
    )
