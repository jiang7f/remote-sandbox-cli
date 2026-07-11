from __future__ import annotations

import errno
import hashlib
import os
import secrets
import stat
from collections.abc import Callable, Mapping
from pathlib import Path

from remote_sandbox._transport_paths import (
    delete_entries_from_fd,
    finalize_entries_from_fd,
    stage_entries_from_fd,
    verify_and_cleanup_stage_from_fd,
)
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.state import AuditSignature


class LocalPathChanged(ValueError):
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
        fingerprint, _signature = self.observe(relative_path, with_hash=with_hash)
        return fingerprint

    def fingerprints(
        self,
        relative_paths: tuple[str, ...],
        *,
        with_hash: bool,
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        return {
            path: fingerprint
            for path, (fingerprint, _signature) in self.observations(
                relative_paths,
                with_hash=with_hash,
            ).items()
        }

    def observations(
        self,
        relative_paths: tuple[str, ...],
        *,
        with_hash: bool,
    ) -> dict[str, tuple[EntryFingerprint | MissingEntry, AuditSignature | None]]:
        grouped: dict[tuple[str, ...], list[tuple[str, str]]] = {}
        ordered: list[str] = []
        for relative_path in relative_paths:
            normalized = normalize_relative_path(relative_path)
            ordered.append(normalized)
            parts = normalized.split("/")
            grouped.setdefault(tuple(parts[:-1]), []).append((parts[-1], normalized))
        observed: dict[
            str,
            tuple[EntryFingerprint | MissingEntry, AuditSignature | None],
        ] = {}
        for parent_parts, leaves in grouped.items():
            try:
                parent_fd, chain = _open_verified_parent(self._descriptor, list(parent_parts))
            except FileNotFoundError:
                observed.update(
                    (path, (MissingEntry(path), None)) for _leaf, path in leaves
                )
                continue
            try:
                for leaf, path in leaves:
                    try:
                        entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        observed[path] = (MissingEntry(path), None)
                        continue
                    fingerprint = _fingerprint_leaf(
                        parent_fd,
                        leaf,
                        path,
                        entry,
                        with_hash=with_hash,
                    )
                    _verify_leaf(parent_fd, leaf, entry, path)
                    observed[path] = (
                        fingerprint,
                        AuditSignature(
                            path,
                            _entry_kind(entry.st_mode),
                            entry.st_ctime_ns,
                            entry.st_dev,
                            entry.st_ino,
                        ),
                    )
                _verify_parent_chain(chain, leaves[-1][1])
            finally:
                os.close(parent_fd)
                for descriptor, _name, _identity in chain:
                    os.close(descriptor)
        return {path: observed[path] for path in ordered}

    def observe(
        self,
        relative_path: str,
        *,
        with_hash: bool,
    ) -> tuple[EntryFingerprint | MissingEntry, AuditSignature | None]:
        normalized = normalize_relative_path(relative_path)
        parts = normalized.split("/")
        try:
            parent_fd, chain = _open_verified_parent(self._descriptor, parts[:-1])
        except FileNotFoundError:
            return MissingEntry(normalized), None
        try:
            try:
                entry = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return MissingEntry(normalized), None
            result = _fingerprint_leaf(
                parent_fd,
                parts[-1],
                normalized,
                entry,
                with_hash=with_hash,
            )
            _verify_leaf(parent_fd, parts[-1], entry, normalized)
            _verify_parent_chain(chain, normalized)
            signature = AuditSignature(
                normalized,
                _entry_kind(entry.st_mode),
                entry.st_ctime_ns,
                entry.st_dev,
                entry.st_ino,
            )
            return result, signature
        finally:
            os.close(parent_fd)
            for descriptor, _name, _identity in chain:
                os.close(descriptor)

    def stage(
        self,
        paths: tuple[str, ...],
        staging: Path,
        *,
        expected_entries: Mapping[str, EntryFingerprint | MissingEntry] | None = None,
        expected_signatures: Mapping[str, AuditSignature | None] | None = None,
        error_type: type[Exception],
    ) -> dict[str, AuditSignature | None]:
        return stage_entries_from_fd(
            self._descriptor,
            paths,
            staging,
            expected_entries=expected_entries,
            expected_signatures=expected_signatures,
            error_type=error_type,
        )

    def stage_observations(
        self,
        paths: tuple[str, ...],
        staging: Path,
        *,
        error_type: type[Exception],
    ) -> tuple[
        dict[str, EntryFingerprint | MissingEntry],
        dict[str, AuditSignature | None],
    ]:
        observed: dict[str, EntryFingerprint | MissingEntry] = {}

        def observe_entry(
            parent_fd: int,
            leaf: str,
            relative: str,
            entry: os.stat_result,
        ) -> EntryFingerprint:
            return _fingerprint_leaf(
                parent_fd,
                leaf,
                relative,
                entry,
                with_hash=True,
            )

        try:
            signatures = stage_entries_from_fd(
                self._descriptor,
                paths,
                staging,
                observed_entries=observed,
                observe_entry=observe_entry,
                error_type=error_type,
            )
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ValueError("symlink parent escapes workspace") from exc
            raise
        return (
            {path: observed[path] for path in paths},
            {path: signatures[path] for path in paths},
        )

    def finalize(
        self,
        staging: Path,
        paths: tuple[str, ...],
        *,
        error_type: type[Exception],
    ) -> dict[
        str,
        tuple[EntryFingerprint | MissingEntry, AuditSignature | None],
    ]:
        return finalize_entries_from_fd(
            self._descriptor,
            staging,
            paths,
            error_type=error_type,
        )

    def verify_and_cleanup_stage(
        self,
        paths: tuple[str, ...],
        staging: Path,
        *,
        expected_entries: Mapping[str, EntryFingerprint | MissingEntry],
        expected_signatures: Mapping[str, AuditSignature | None],
        error_type: type[Exception],
    ) -> set[str]:
        return verify_and_cleanup_stage_from_fd(
            self._descriptor,
            paths,
            staging,
            expected_entries=expected_entries,
            expected_signatures=expected_signatures,
            error_type=error_type,
        )

    def delete(
        self,
        paths: tuple[str, ...],
        *,
        error_type: type[Exception],
    ) -> None:
        delete_entries_from_fd(self._descriptor, paths, error_type=error_type)

    def delete_expected(
        self,
        expected: Mapping[str, EntryFingerprint | MissingEntry],
        *,
        error_type: type[Exception],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        completed: list[str] = []
        changed: list[str] = []
        for path, expected_entry in expected.items():
            if self._quarantine_delete(path, expected_entry, error_type=error_type):
                completed.append(path)
            else:
                changed.append(path)
        return tuple(completed), tuple(changed)

    def read_entry(
        self,
        relative_path: str,
    ) -> tuple[EntryFingerprint | MissingEntry, bytes | None]:
        normalized = normalize_relative_path(relative_path)
        parts = normalized.split("/")
        try:
            parent_fd, chain = _open_verified_parent(self._descriptor, parts[:-1])
        except FileNotFoundError:
            return MissingEntry(normalized), None
        try:
            try:
                entry = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return MissingEntry(normalized), None
            fingerprint, content = _read_leaf(parent_fd, parts[-1], normalized, entry)
            _verify_leaf(parent_fd, parts[-1], entry, normalized)
            _verify_parent_chain(chain, normalized)
            return fingerprint, content
        finally:
            os.close(parent_fd)
            for descriptor, _name, _identity in chain:
                os.close(descriptor)

    def audit_signature(self, relative_path: str) -> AuditSignature | None:
        _fingerprint, signature = self.observe(relative_path, with_hash=False)
        return signature

    def walk_paths(self, is_ignored: Callable[[str], bool]) -> tuple[str, ...]:
        paths: list[str] = []
        _walk_descriptor(self._descriptor, "", is_ignored, paths)
        return tuple(paths)

    def _quarantine_delete(
        self,
        relative_path: str,
        expected: EntryFingerprint | MissingEntry,
        *,
        error_type: type[Exception],
    ) -> bool:
        normalized = normalize_relative_path(relative_path)
        if expected.path != normalized:
            raise ValueError("expected deletion fingerprint path does not match")
        parts = normalized.split("/")
        try:
            parent_fd, chain = _open_verified_parent(self._descriptor, parts[:-1])
        except FileNotFoundError:
            return isinstance(expected, MissingEntry)
        quarantine = f".remote-sandbox-delete-{secrets.token_hex(8)}"
        try:
            try:
                os.rename(
                    parts[-1],
                    quarantine,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
            except FileNotFoundError:
                return isinstance(expected, MissingEntry)
            try:
                entry = os.stat(quarantine, dir_fd=parent_fd, follow_symlinks=False)
                observed = _fingerprint_leaf(
                    parent_fd,
                    quarantine,
                    normalized,
                    entry,
                    with_hash=True,
                )
                _verify_leaf(parent_fd, quarantine, entry, normalized)
                _verify_parent_chain(chain, normalized)
                if not _matches_expected(expected, observed):
                    _restore_quarantine(parent_fd, quarantine, parts[-1], error_type)
                    return False
                if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
                    os.rmdir(quarantine, dir_fd=parent_fd)
                else:
                    os.unlink(quarantine, dir_fd=parent_fd)
                return True
            except BaseException:
                _restore_quarantine(parent_fd, quarantine, parts[-1], error_type)
                raise
        except OSError as exc:
            raise error_type(f"verified workspace delete failed: {normalized}: {exc}") from exc
        finally:
            os.close(parent_fd)
            for descriptor, _name, _identity in chain:
                os.close(descriptor)


ParentIdentity = tuple[int, int]
ParentChain = list[tuple[int, str, ParentIdentity]]


def _open_verified_parent(root_fd: int, parts: list[str]) -> tuple[int, ParentChain]:
    descriptor = os.dup(root_fd)
    chain: ParentChain = []
    try:
        for part in parts:
            before = os.stat(part, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
                raise LocalPathChanged(f"symlink parent changed: {part}")
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


def _walk_descriptor(
    descriptor: int,
    prefix: str,
    is_ignored: Callable[[str], bool],
    paths: list[str],
) -> None:
    with os.scandir(descriptor) as iterator:
        names = sorted(entry.name for entry in iterator)
    for name in names:
        path = name if not prefix else f"{prefix}/{name}"
        if is_ignored(path):
            continue
        try:
            metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            continue
        paths.append(path)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            continue
        try:
            child = _open_directory(name, dir_fd=descriptor)
        except (FileNotFoundError, NotADirectoryError):
            continue
        try:
            _walk_descriptor(child, path, is_ignored, paths)
        finally:
            os.close(child)


def _entry_kind(mode: int) -> EntryKind:
    if stat.S_ISLNK(mode):
        return EntryKind.SYMLINK
    if stat.S_ISDIR(mode):
        return EntryKind.DIR
    if stat.S_ISREG(mode):
        return EntryKind.FILE
    return EntryKind.SPECIAL


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


def _read_leaf(
    parent_fd: int,
    leaf: str,
    path: str,
    entry: os.stat_result,
) -> tuple[EntryFingerprint, bytes]:
    if stat.S_ISLNK(entry.st_mode):
        target = os.readlink(leaf, dir_fd=parent_fd)
        content = os.fsencode(target)
        return (
            EntryFingerprint(
                path,
                EntryKind.SYMLINK,
                None,
                entry.st_mtime_ns,
                entry.st_mode,
                target,
                hashlib.sha256(content).hexdigest(),
            ),
            content,
        )
    if stat.S_ISDIR(entry.st_mode):
        return (
            EntryFingerprint(
                path,
                EntryKind.DIR,
                None,
                entry.st_mtime_ns,
                entry.st_mode,
            ),
            b"",
        )
    if not stat.S_ISREG(entry.st_mode):
        return (
            EntryFingerprint(
                path,
                EntryKind.SPECIAL,
                None,
                entry.st_mtime_ns,
                entry.st_mode,
            ),
            b"",
        )
    descriptor = os.open(
        leaf,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
            raise LocalPathChanged(f"read leaf changed: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            content = source.read()
        after = os.fstat(descriptor)
        if (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            entry.st_dev,
            entry.st_ino,
            entry.st_mode,
            entry.st_size,
            entry.st_mtime_ns,
        ):
            raise LocalPathChanged(f"read leaf changed: {path}")
    finally:
        os.close(descriptor)
    return (
        EntryFingerprint(
            path,
            EntryKind.FILE,
            entry.st_size,
            entry.st_mtime_ns,
            entry.st_mode,
            content_hash=hashlib.sha256(content).hexdigest(),
        ),
        content,
    )


def _matches_expected(
    expected: EntryFingerprint | MissingEntry,
    observed: EntryFingerprint | MissingEntry,
) -> bool:
    if expected == observed:
        return True
    return (
        isinstance(expected, EntryFingerprint)
        and isinstance(observed, EntryFingerprint)
        and expected.kind is EntryKind.DIR
        and observed.kind is EntryKind.DIR
        and expected.mode == observed.mode
    )


def _restore_quarantine(
    parent_fd: int,
    quarantine: str,
    leaf: str,
    error_type: type[Exception],
) -> None:
    try:
        os.stat(quarantine, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    try:
        os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        os.rename(quarantine, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    else:
        recovery = f".remote-sandbox-recovered-{secrets.token_hex(8)}"
        os.rename(quarantine, recovery, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        raise error_type(
            f"concurrent replacement preserved as {recovery} while restoring {leaf}"
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
    if (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ) != (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
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
