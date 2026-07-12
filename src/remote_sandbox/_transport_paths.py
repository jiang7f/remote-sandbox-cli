from __future__ import annotations

import contextlib
import errno
import hashlib
import os
import secrets
import shutil
import stat
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.state import AuditSignature

_HARDLINK_FALLBACK_ERRORS = {
    errno.EXDEV,
    errno.EPERM,
    errno.EACCES,
    errno.EMLINK,
    getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
    errno.EOPNOTSUPP,
}
_STAGING_WORKERS = 4
_VERIFY_CLEANUP_WORKERS = 16


class _HardlinkUnavailable(OSError):
    pass


class StagedSourceChanged(OSError):
    pass


def stage_entries_from_fd(
    root_fd: int,
    paths: tuple[str, ...],
    staging: Path,
    *,
    expected_entries: Mapping[str, EntryFingerprint | MissingEntry] | None = None,
    expected_signatures: Mapping[str, AuditSignature | None] | None = None,
    observed_entries: dict[str, EntryFingerprint | MissingEntry] | None = None,
    observe_entry: Callable[
        [int, str, str, os.stat_result],
        EntryFingerprint,
    ]
    | None = None,
    error_type: type[Exception],
) -> dict[str, AuditSignature | None]:
    staging.mkdir(mode=0o700, parents=True, exist_ok=False)
    grouped: dict[tuple[str, ...], list[tuple[str, str]]] = {}
    for relative in paths:
        parts = relative.split("/")
        grouped.setdefault(tuple(parts[:-1]), []).append((parts[-1], relative))
    staged_signatures: dict[str, AuditSignature | None] = {}
    try:
        _stage_grouped_entries(
            root_fd,
            grouped,
            staging,
            workers=_STAGING_WORKERS,
            require_hardlinks=True,
            try_hardlinks=True,
            expected_entries=expected_entries,
            expected_signatures=expected_signatures,
            observed_entries=observed_entries,
            observe_entry=observe_entry,
            staged_signatures=staged_signatures,
            error_type=error_type,
        )
    except _HardlinkUnavailable:
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(mode=0o700, parents=True, exist_ok=False)
        staged_signatures.clear()
        if observed_entries is not None:
            observed_entries.clear()
        try:
            _stage_grouped_entries(
                root_fd,
                grouped,
                staging,
                workers=1,
                require_hardlinks=False,
                try_hardlinks=False,
                expected_entries=expected_entries,
                expected_signatures=expected_signatures,
                observed_entries=observed_entries,
                observe_entry=observe_entry,
                staged_signatures=staged_signatures,
                error_type=error_type,
            )
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return staged_signatures


def _stage_grouped_entries(
    root_fd: int,
    grouped: dict[tuple[str, ...], list[tuple[str, str]]],
    staging: Path,
    *,
    workers: int,
    require_hardlinks: bool,
    try_hardlinks: bool,
    expected_entries: Mapping[str, EntryFingerprint | MissingEntry] | None,
    expected_signatures: Mapping[str, AuditSignature | None] | None,
    observed_entries: dict[str, EntryFingerprint | MissingEntry] | None,
    observe_entry: Callable[
        [int, str, str, os.stat_result],
        EntryFingerprint,
    ]
    | None,
    staged_signatures: dict[str, AuditSignature | None],
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
                        relative=relative,
                        expected_entry=(
                            expected_entries.get(relative)
                            if expected_entries is not None
                            else None
                        ),
                        expected_signature=(
                            expected_signatures.get(relative)
                            if expected_signatures is not None
                            else None
                        ),
                        validate_expectation=(
                            expected_entries is not None or expected_signatures is not None
                        ),
                        observed_entries=observed_entries,
                        observe_entry=observe_entry,
                        staged_signatures=staged_signatures,
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
) -> dict[
    str,
    tuple[EntryFingerprint | MissingEntry, AuditSignature | None],
]:
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
    return _installed_observations(root_fd, paths)


def verify_and_cleanup_stage_from_fd(
    root_fd: int,
    paths: tuple[str, ...],
    staging: Path,
    *,
    expected_entries: Mapping[str, EntryFingerprint | MissingEntry],
    expected_signatures: Mapping[str, AuditSignature | None],
    error_type: type[Exception],
) -> set[str]:
    grouped: dict[tuple[str, ...], list[tuple[str, str]]] = {}
    for relative in paths:
        parts = relative.split("/")
        grouped.setdefault(tuple(parts[:-1]), []).append((parts[-1], relative))
    by_depth: dict[int, list[tuple[str, ...]]] = {}
    for parent_parts in grouped:
        by_depth.setdefault(len(parent_parts), []).append(parent_parts)

    def verify_group(parent_parts: tuple[str, ...]) -> set[str]:
        group_changed: set[str] = set()
        leaves = grouped[parent_parts]
        try:
            parent_fd = _walk_parent(root_fd, list(parent_parts), create=False)
        except (OSError, ValueError):
            parent_fd = None
        try:
            for leaf, relative in leaves:
                if parent_fd is None:
                    group_changed.add(relative)
                else:
                    try:
                        entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                        stable = _matches_stage_expectation(
                            parent_fd,
                            leaf,
                            relative,
                            entry,
                            expected_entries.get(relative),
                            expected_signatures.get(relative),
                        )
                    except OSError:
                        stable = False
                    if not stable:
                        group_changed.add(relative)
                _remove_staged_leaf(
                    staging / relative,
                    relative,
                    expected_entries.get(relative),
                    error_type,
                )
        finally:
            if parent_fd is not None:
                os.close(parent_fd)
        return group_changed

    changed: set[str] = set()
    for depth in sorted(by_depth, reverse=True):
        groups = sorted(by_depth[depth])
        if len(groups) == 1:
            changed.update(verify_group(groups[0]))
            continue
        with ThreadPoolExecutor(
            max_workers=min(_VERIFY_CLEANUP_WORKERS, len(groups))
        ) as executor:
            for group_changed in executor.map(verify_group, groups):
                changed.update(group_changed)
    parent_directories = {
        staging.joinpath(*parts[:depth])
        for parts in grouped
        for depth in range(1, len(parts) + 1)
    }
    for directory in sorted(
        parent_directories,
        key=lambda path: (-len(path.relative_to(staging).parts), os.fspath(path)),
    ):
        try:
            directory.rmdir()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise error_type(f"staged source cleanup failed: {directory}: {exc}") from exc
    return changed


def _remove_staged_leaf(
    candidate: Path,
    relative: str,
    expected_entry: EntryFingerprint | MissingEntry | None,
    error_type: type[Exception],
) -> None:
    try:
        if (
            isinstance(expected_entry, EntryFingerprint)
            and expected_entry.kind is EntryKind.DIR
        ):
            candidate.rmdir()
        else:
            candidate.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise error_type(f"staged source cleanup failed: {relative}: {exc}") from exc


def _installed_observations(
    root_fd: int,
    paths: tuple[str, ...],
) -> dict[
    str,
    tuple[EntryFingerprint | MissingEntry, AuditSignature | None],
]:
    grouped: dict[tuple[str, ...], list[tuple[str, str]]] = {}
    for relative in paths:
        parts = relative.split("/")
        grouped.setdefault(tuple(parts[:-1]), []).append((parts[-1], relative))
    observed: dict[
        str,
        tuple[EntryFingerprint | MissingEntry, AuditSignature | None],
    ] = {}
    for parent_parts, leaves in grouped.items():
        try:
            parent_fd = _walk_parent(root_fd, list(parent_parts), create=False)
        except FileNotFoundError:
            observed.update(
                (relative, (MissingEntry(relative), None))
                for _leaf, relative in leaves
            )
            continue
        try:
            for leaf, relative in leaves:
                try:
                    entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                except FileNotFoundError:
                    observed[relative] = (MissingEntry(relative), None)
                    continue
                observed[relative] = (
                    _metadata_fingerprint(parent_fd, leaf, relative, entry),
                    _signature_from_stat(relative, entry),
                )
        finally:
            os.close(parent_fd)
    return {relative: observed[relative] for relative in paths}


def _metadata_fingerprint(
    parent_fd: int,
    leaf: str,
    relative: str,
    entry: os.stat_result,
) -> EntryFingerprint:
    kind = _entry_kind(entry.st_mode)
    if kind is EntryKind.SYMLINK:
        target = os.readlink(leaf, dir_fd=parent_fd)
        return EntryFingerprint(
            relative,
            kind,
            None,
            entry.st_mtime_ns,
            entry.st_mode,
            target,
            hashlib.sha256(os.fsencode(target)).hexdigest(),
        )
    return EntryFingerprint(
        relative,
        kind,
        entry.st_size if kind is EntryKind.FILE else None,
        entry.st_mtime_ns,
        entry.st_mode,
    )


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
    relative: str | None = None,
    expected_entry: EntryFingerprint | MissingEntry | None = None,
    expected_signature: AuditSignature | None = None,
    validate_expectation: bool = False,
    observed_entries: dict[str, EntryFingerprint | MissingEntry] | None = None,
    observe_entry: Callable[
        [int, str, str, os.stat_result],
        EntryFingerprint,
    ]
    | None = None,
    staged_signatures: dict[str, AuditSignature | None] | None = None,
    error_type: type[Exception],
) -> None:
    try:
        entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise error_type(f"source path became unavailable: {destination}: {exc}") from exc
    if observed_entries is not None:
        if relative is None or observe_entry is None:
            raise RuntimeError("staged observation requires a relative path and observer")
        observed_entries[relative] = observe_entry(parent_fd, leaf, relative, entry)
    if validate_expectation and (
        relative is None
        or not _matches_stage_expectation(
            parent_fd,
            leaf,
            relative,
            entry,
            expected_entry,
            expected_signature,
        )
    ):
        raise StagedSourceChanged(f"source changed before staging: {relative}")
    if not parent_ready:
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_ISDIR(entry.st_mode):
        destination.mkdir(mode=stat.S_IMODE(entry.st_mode), exist_ok=True)
        destination.chmod(stat.S_IMODE(entry.st_mode), follow_symlinks=False)
        _record_staged_signature(
            parent_fd,
            leaf,
            relative,
            entry,
            allow_ctime_change=False,
            staged_signatures=staged_signatures,
        )
        return
    if stat.S_ISLNK(entry.st_mode):
        os.symlink(os.readlink(leaf, dir_fd=parent_fd), destination)
        _record_staged_signature(
            parent_fd,
            leaf,
            relative,
            entry,
            allow_ctime_change=False,
            staged_signatures=staged_signatures,
        )
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
            if (
                staged.st_mode,
                staged.st_size,
                staged.st_mtime_ns,
            ) != (
                entry.st_mode,
                entry.st_size,
                entry.st_mtime_ns,
            ):
                destination.unlink(missing_ok=True)
                raise error_type(f"source changed while staging: {destination}")
            if relative is not None and staged_signatures is not None:
                staged_signatures[relative] = _signature_from_stat(relative, staged)
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
        _record_staged_signature(
            parent_fd,
            leaf,
            relative,
            entry,
            allow_ctime_change=False,
            staged_signatures=staged_signatures,
        )
    finally:
        os.close(descriptor)


def _matches_stage_expectation(
    parent_fd: int,
    leaf: str,
    relative: str,
    entry: os.stat_result,
    expected_entry: EntryFingerprint | MissingEntry | None,
    expected_signature: AuditSignature | None,
) -> bool:
    if not isinstance(expected_entry, EntryFingerprint) or expected_signature is None:
        return False
    if expected_signature != _signature_from_stat(relative, entry):
        return False
    if (
        expected_entry.kind is not _entry_kind(entry.st_mode)
        or expected_entry.size != (entry.st_size if stat.S_ISREG(entry.st_mode) else None)
        or expected_entry.mtime_ns != entry.st_mtime_ns
        or expected_entry.mode != entry.st_mode
    ):
        return False
    if stat.S_ISLNK(entry.st_mode):
        return expected_entry.link_target == os.readlink(leaf, dir_fd=parent_fd)
    return expected_entry.link_target is None


def _record_staged_signature(
    parent_fd: int,
    leaf: str,
    relative: str | None,
    before: os.stat_result,
    *,
    allow_ctime_change: bool,
    staged_signatures: dict[str, AuditSignature | None] | None,
) -> None:
    if relative is None or staged_signatures is None:
        return
    after = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    stable_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
    )
    stable_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
    )
    if stable_before != stable_after or (
        not allow_ctime_change and before.st_ctime_ns != after.st_ctime_ns
    ):
        raise StagedSourceChanged(f"source changed while staging: {relative}")
    staged_signatures[relative] = _signature_from_stat(relative, after)


def _signature_from_stat(relative: str, entry: os.stat_result) -> AuditSignature:
    return AuditSignature(
        relative,
        _entry_kind(entry.st_mode),
        entry.st_ctime_ns,
        entry.st_dev,
        entry.st_ino,
    )


def _entry_kind(mode: int) -> EntryKind:
    if stat.S_ISLNK(mode):
        return EntryKind.SYMLINK
    if stat.S_ISDIR(mode):
        return EntryKind.DIR
    if stat.S_ISREG(mode):
        return EntryKind.FILE
    return EntryKind.SPECIAL


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
        os.chmod(
            name,
            stat.S_IMODE(entry.st_mode),
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        return
    if not stat.S_ISDIR(entry.st_mode):
        raise OSError(errno.EINVAL, "unsupported staged entry")
    os.mkdir(name, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
    os.chmod(
        name,
        stat.S_IMODE(entry.st_mode),
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
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
