from __future__ import annotations

import errno
import hashlib
import os
import stat
from pathlib import Path

from remote_sandbox._transport_paths import (
    delete_entries_from_fd,
    finalize_entries_from_fd,
    stage_entries_from_fd,
)
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)


class LocalPathChanged(RuntimeError):
    pass


class ProtectedLocalRoot:
    def __init__(self, root: Path) -> None:
        self.path = root
        self._descriptor = _open_directory(os.fspath(root))
        self._closed = False

    def __enter__(self) -> ProtectedLocalRoot:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            os.close(self._descriptor)
            self._closed = True

    def fingerprint(
        self,
        relative_path: str,
        *,
        with_hash: bool,
    ) -> EntryFingerprint | MissingEntry:
        normalized = normalize_relative_path(relative_path)
        parts = normalized.split("/")
        try:
            parent_fd, chain = _open_verified_parent(self._descriptor, parts[:-1])
        except FileNotFoundError:
            return MissingEntry(normalized)
        try:
            try:
                entry = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return MissingEntry(normalized)
            result = _fingerprint_leaf(
                parent_fd,
                parts[-1],
                normalized,
                entry,
                with_hash=with_hash,
            )
            _verify_leaf(parent_fd, parts[-1], entry, normalized)
            _verify_parent_chain(chain, normalized)
            return result
        finally:
            os.close(parent_fd)
            for descriptor, _name, _identity in chain:
                os.close(descriptor)

    def stage(
        self,
        paths: tuple[str, ...],
        staging: Path,
        *,
        error_type: type[Exception],
    ) -> None:
        stage_entries_from_fd(
            self._descriptor,
            paths,
            staging,
            error_type=error_type,
        )

    def finalize(
        self,
        staging: Path,
        paths: tuple[str, ...],
        *,
        error_type: type[Exception],
    ) -> None:
        finalize_entries_from_fd(
            self._descriptor,
            staging,
            paths,
            error_type=error_type,
        )

    def delete(
        self,
        paths: tuple[str, ...],
        *,
        error_type: type[Exception],
    ) -> None:
        delete_entries_from_fd(self._descriptor, paths, error_type=error_type)


ParentIdentity = tuple[int, int]
ParentChain = list[tuple[int, str, ParentIdentity]]


def _open_verified_parent(root_fd: int, parts: list[str]) -> tuple[int, ParentChain]:
    descriptor = os.dup(root_fd)
    chain: ParentChain = []
    try:
        for part in parts:
            before = os.stat(part, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
                raise ValueError(f"symlink parent escapes workspace: {part}")
            parent_copy = os.dup(descriptor)
            try:
                child = _open_directory(part, dir_fd=descriptor)
            except OSError as exc:
                os.close(parent_copy)
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise LocalPathChanged(f"fingerprint parent changed: {part}") from exc
                raise
            opened = os.fstat(child)
            identity = (opened.st_dev, opened.st_ino)
            if identity != (before.st_dev, before.st_ino):
                os.close(parent_copy)
                os.close(child)
                raise LocalPathChanged(f"fingerprint parent changed: {part}")
            chain.append((parent_copy, part, identity))
            os.close(descriptor)
            descriptor = child
        return descriptor, chain
    except BaseException:
        os.close(descriptor)
        for parent_fd, _name, _identity in chain:
            os.close(parent_fd)
        raise


def _fingerprint_leaf(
    parent_fd: int,
    leaf: str,
    path: str,
    entry: os.stat_result,
    *,
    with_hash: bool,
) -> EntryFingerprint:
    if stat.S_ISLNK(entry.st_mode):
        target = os.readlink(leaf, dir_fd=parent_fd)
        return EntryFingerprint(
            path,
            EntryKind.SYMLINK,
            None,
            entry.st_mtime_ns,
            entry.st_mode,
            target,
            hashlib.sha256(os.fsencode(target)).hexdigest(),
        )
    if stat.S_ISDIR(entry.st_mode):
        return EntryFingerprint(
            path,
            EntryKind.DIR,
            None,
            entry.st_mtime_ns,
            entry.st_mode,
        )
    if stat.S_ISREG(entry.st_mode):
        descriptor = os.open(
            leaf,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                raise LocalPathChanged(f"fingerprint leaf changed: {path}")
            digest = _hash_descriptor(descriptor) if with_hash else None
        finally:
            os.close(descriptor)
        return EntryFingerprint(
            path,
            EntryKind.FILE,
            entry.st_size,
            entry.st_mtime_ns,
            entry.st_mode,
            content_hash=digest,
        )
    return EntryFingerprint(
        path,
        EntryKind.SPECIAL,
        None,
        entry.st_mtime_ns,
        entry.st_mode,
    )


def _verify_leaf(
    parent_fd: int,
    leaf: str,
    before: os.stat_result,
    path: str,
) -> None:
    try:
        after = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise LocalPathChanged(f"fingerprint leaf changed: {path}") from exc
    if (after.st_dev, after.st_ino, after.st_mode, after.st_size, after.st_mtime_ns) != (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
    ):
        raise LocalPathChanged(f"fingerprint leaf changed: {path}")


def _verify_parent_chain(chain: ParentChain, path: str) -> None:
    for parent_fd, name, identity in chain:
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise LocalPathChanged(f"fingerprint parent changed: {path}") from exc
        if (
            not stat.S_ISDIR(current.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or (current.st_dev, current.st_ino) != identity
        ):
            raise LocalPathChanged(f"fingerprint parent changed: {path}")


def _hash_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _open_directory(path: str, *, dir_fd: int | None = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    return os.open(path, flags, dir_fd=dir_fd)
