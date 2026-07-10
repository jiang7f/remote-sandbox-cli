from __future__ import annotations

import errno
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, TypeAlias, cast

from remote_sandbox._transport_fingerprint import (
    LocalPathChanged,
    ProtectedLocalRoot,
)
from remote_sandbox._transport_remote import (
    REMOTE_CLEANUP_RSYNC_CODE,
    REMOTE_CREATE_CODE,
    REMOTE_EXTRACT_CODE,
    REMOTE_FINALIZE_RSYNC_CODE,
    REMOTE_PREPARE_RSYNC_CODE,
    REMOTE_RSYNC_PROBE_CODE,
    REMOTE_STAGE_RSYNC_CODE,
)
from remote_sandbox._transport_tar import extract_tar_archive
from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    MissingEntry,
    content_identity,
    normalize_relative_path,
)
from remote_sandbox.reconcile import ActionType, SyncAction
from remote_sandbox.ssh import (
    SubprocessSshRunner,
    ssh_control_opts,
    validate_remote_path,
    validate_target,
)

FingerprintState: TypeAlias = EntryFingerprint | MissingEntry
LocalFingerprintState: TypeAlias = FingerprintState | LocalPathChanged
ProgressCallback: TypeAlias = Callable[["TransferResult"], None]

_SAFE_RSYNC_REMOTE = re.compile(r"\A[A-Za-z0-9_./@+-]+\Z")


class TransferError(RuntimeError):
    pass


class RsyncPathUnsupported(TransferError):
    pass


class _RsyncUnavailable(TransferError):
    pass


class TransferDirection(StrEnum):
    PUSH = "push"
    PULL = "pull"


@dataclass(frozen=True, slots=True)
class TransferItem:
    path: str
    expected_source: FingerprintState | None
    expected_destination: FingerprintState | None

    def __post_init__(self) -> None:
        if type(self.path) is not str:
            raise ValueError("transfer item path must be a string")
        normalized = normalize_relative_path(self.path)
        object.__setattr__(self, "path", normalized)
        _validate_expected(self.expected_source, normalized, "source")
        _validate_expected(self.expected_destination, normalized, "destination")


@dataclass(frozen=True, slots=True)
class TransferBatch:
    direction: TransferDirection
    items: tuple[TransferItem, ...]

    def __post_init__(self) -> None:
        if type(self.direction) is not TransferDirection:
            raise ValueError("transfer direction must be a TransferDirection")
        if type(self.items) is not tuple or not self.items:
            raise ValueError("transfer batch items must be a non-empty tuple")
        if any(type(item) is not TransferItem for item in self.items):
            raise ValueError("transfer batch items must contain TransferItem values")
        paths = tuple(item.path for item in self.items)
        if len(paths) != len(set(paths)):
            raise ValueError("transfer batch paths must be unique")

    @classmethod
    def from_actions(cls, actions: tuple[SyncAction, ...]) -> TransferBatch:
        if type(actions) is not tuple or not actions:
            raise ValueError("actions must be a non-empty tuple")
        if any(type(action) is not SyncAction for action in actions):
            raise ValueError("actions must contain SyncAction values")
        directions: list[TransferDirection] = []
        items: list[TransferItem] = []
        for action in actions:
            if action.type is ActionType.PUSH:
                directions.append(TransferDirection.PUSH)
                items.append(
                    TransferItem(action.path, action.expected_local, action.expected_remote)
                )
            elif action.type is ActionType.PULL:
                directions.append(TransferDirection.PULL)
                items.append(
                    TransferItem(action.path, action.expected_remote, action.expected_local)
                )
            else:
                raise ValueError("all actions must be a transfer action")
        if len(set(directions)) != 1:
            raise ValueError("all actions must have the same transfer direction")
        return cls(directions[0], tuple(items))


@dataclass(frozen=True, slots=True)
class TransferResult:
    completed: tuple[str, ...]
    changed_during_transfer: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.completed) is not tuple or type(self.changed_during_transfer) is not tuple:
            raise ValueError("transfer result paths must be tuples")
        completed = _validated_result_paths(self.completed, "completed")
        changed = _validated_result_paths(self.changed_during_transfer, "changed")
        if set(completed) & set(changed):
            raise ValueError("completed and changed paths must be disjoint")


@dataclass(frozen=True, slots=True)
class RsyncCapabilities:
    protect_args: bool
    secluded_args: bool

    def __post_init__(self) -> None:
        if type(self.protect_args) is not bool or type(self.secluded_args) is not bool:
            raise ValueError("rsync capabilities must be booleans")


def build_rsync_argv(
    batch: TransferBatch,
    local_root: Path,
    target: str,
    remote_root: str,
    *,
    capabilities: RsyncCapabilities,
) -> list[str]:
    if type(batch) is not TransferBatch:
        raise ValueError("batch must be a TransferBatch")
    local = _directory_operand(local_root)
    validated_target = validate_target(target)
    validated_remote = validate_remote_path(remote_root)
    optional_flag: str | None = None
    if capabilities.protect_args:
        optional_flag = "--protect-args"
    elif capabilities.secluded_args:
        optional_flag = "--secluded-args"
    elif not (
        _SAFE_RSYNC_REMOTE.fullmatch(validated_target)
        and _SAFE_RSYNC_REMOTE.fullmatch(validated_remote)
    ):
        raise RsyncPathUnsupported("rsync remote target or root requires argument protection")

    remote_path = (
        validated_remote if validated_remote == "/" else f"{validated_remote.rstrip('/')}/"
    )
    remote = f"{validated_target}:{remote_path}"
    source, destination = (local, remote)
    if batch.direction is TransferDirection.PULL:
        source, destination = remote, local
    argv = [
        "rsync",
        "--archive",
        "--links",
        "--times",
        "--itemize-changes",
        "--files-from=-",
    ]
    if optional_flag is not None:
        argv.append(optional_flag)
    argv.extend(("--rsh", shlex.join(("ssh", *ssh_control_opts())), source, destination))
    return argv


def validate_tar_member(name: str) -> str:
    if type(name) is not str:
        raise ValueError("tar member name must be a string")
    if "\\" in name:
        raise ValueError("tar member name must use POSIX separators")
    normalized = normalize_relative_path(name)
    if normalized != name:
        raise ValueError("tar member name must already be normalized")
    return normalized


def probe_rsync_capabilities(*, executable: str = "rsync") -> RsyncCapabilities:
    def supported(flag: str) -> bool:
        try:
            result = subprocess.run(
                [executable, flag, "--version"],
                check=False,
                capture_output=True,
            )
        except OSError:
            return False
        return result.returncode == 0

    return RsyncCapabilities(
        protect_args=supported("--protect-args"),
        secluded_args=supported("--secluded-args"),
    )


class _RemoteFingerprinter(Protocol):
    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, FingerprintState]: ...


class _WorkspaceRunner(Protocol):
    def run_workspace_python_bytes(
        self,
        target: str,
        root: str,
        code: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]: ...

    def delete_workspace_path(self, target: str, root: str, path: str) -> None: ...


class BatchTransport:
    def __init__(
        self,
        local_root: Path,
        target: str,
        remote_root: str,
        remote_fingerprinter: _RemoteFingerprinter,
        *,
        runner: _WorkspaceRunner | None = None,
        capabilities: RsyncCapabilities | None = None,
        remote_rsync_available: bool | None = None,
    ) -> None:
        self._local_root = local_root
        self._target = validate_target(target)
        self._remote_root = validate_remote_path(remote_root)
        self._remote = remote_fingerprinter
        self._runner = runner or SubprocessSshRunner()
        self._capabilities = capabilities or probe_rsync_capabilities()
        self._remote_rsync_available = remote_rsync_available

    def transfer(self, batch: TransferBatch, on_progress: ProgressCallback) -> TransferResult:
        if type(batch) is not TransferBatch:
            raise ValueError("batch must be a TransferBatch")
        if not callable(on_progress):
            raise ValueError("on_progress must be callable")
        paths = tuple(item.path for item in batch.items)
        with ProtectedLocalRoot(self._local_root) as local:
            local_before = {
                path: local.fingerprint(path, with_hash=True) for path in paths
            }
            remote_before = self._remote_snapshot(paths)
            source_before = self._preflight(batch, local_before, remote_before)

            use_tar = self._needs_tar(batch, source_before)
            if use_tar:
                self._transfer_tar(batch, local)
            else:
                try:
                    self._transfer_rsync(batch, source_before, local)
                except _RsyncUnavailable:
                    self._transfer_tar(batch, local)

            local_after = self._local_postflight(local, paths)
            remote_after = self._remote_snapshot(paths)
            return self._verified_result(
                batch,
                source_before,
                local_after,
                remote_after,
                on_progress,
            )

    def delete_local(self, paths: Iterable[str]) -> None:
        LocalPairTransport._delete(self._local_root, tuple(paths))

    def delete_remote(self, paths: Iterable[str]) -> None:
        for path in _normalized_delete_paths(paths):
            self._runner.delete_workspace_path(self._target, self._remote_root, path)

    def _remote_snapshot(self, paths: tuple[str, ...]) -> dict[str, FingerprintState]:
        observed = self._remote.hash_paths(paths)
        if not isinstance(observed, Mapping) or set(observed) != set(paths):
            raise TransferError("remote fingerprint response does not match transfer paths")
        result: dict[str, FingerprintState] = {}
        for path in paths:
            entry = observed[path]
            if type(entry) not in {EntryFingerprint, MissingEntry} or entry.path != path:
                raise TransferError(f"invalid remote fingerprint: {path}")
            result[path] = entry
        return result

    @staticmethod
    def _preflight(
        batch: TransferBatch,
        local: Mapping[str, FingerprintState],
        remote: Mapping[str, FingerprintState],
    ) -> dict[str, FingerprintState]:
        source_before: dict[str, FingerprintState] = {}
        for item in batch.items:
            source = local[item.path]
            destination = remote[item.path]
            if batch.direction is TransferDirection.PULL:
                source, destination = destination, source
            _require_expected(item.path, "source", item.expected_source, source)
            _require_expected(item.path, "destination", item.expected_destination, destination)
            source_before[item.path] = source
        return source_before

    def _needs_tar(
        self,
        batch: TransferBatch,
        source_before: Mapping[str, FingerprintState],
    ) -> bool:
        if shutil.which("rsync") is None:
            return True
        try:
            build_rsync_argv(
                batch,
                self._local_root,
                self._target,
                self._remote_root,
                capabilities=self._capabilities,
            )
        except RsyncPathUnsupported:
            return True
        if not self._remote_has_rsync():
            return True
        for path, entry in source_before.items():
            if isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.SYMLINK:
                target = entry.link_target
                if target is None or target.startswith("/"):
                    return True
                staged_target = posixpath.normpath(
                    posixpath.join(posixpath.dirname(path), target)
                )
                if (
                    staged_target in {".", ".."}
                    or staged_target.startswith("../")
                    or not _target_present_in_stage(staged_target, source_before)
                ):
                    return True
        return False

    def _remote_has_rsync(self) -> bool:
        if self._remote_rsync_available is None:
            result = self._runner.run_workspace_python_bytes(
                self._target,
                self._remote_root,
                REMOTE_RSYNC_PROBE_CODE,
                b"",
            )
            self._remote_rsync_available = result.returncode == 0
        return self._remote_rsync_available

    def _transfer_rsync(
        self,
        batch: TransferBatch,
        source_before: Mapping[str, FingerprintState],
        local: ProtectedLocalRoot,
    ) -> None:
        paths = tuple(item.path for item in batch.items)
        with tempfile.TemporaryDirectory(prefix="remote-sandbox-rsync-") as raw:
            local_stage = Path(raw) / "local"
            remote_stage: str | None = None
            if batch.direction is TransferDirection.PUSH:
                local.stage(
                    paths,
                    local_stage,
                    error_type=TransferError,
                )
                remote_stage = self._remote_rsync_stage(
                    REMOTE_PREPARE_RSYNC_CODE,
                    (),
                )
            else:
                local_stage.mkdir()
                remote_stage = self._remote_rsync_stage(
                    REMOTE_STAGE_RSYNC_CODE,
                    paths,
                )
            try:
                argv = build_rsync_argv(
                    batch,
                    local_stage,
                    self._target,
                    remote_stage,
                    capabilities=self._capabilities,
                )
                try:
                    result = subprocess.run(
                        argv,
                        check=False,
                        input=_path_input(batch, source_before),
                        capture_output=True,
                    )
                except OSError as exc:
                    if exc.errno in {errno.ENOENT, errno.EACCES, errno.ENOEXEC}:
                        raise _RsyncUnavailable(
                            f"rsync transfer could not start: {exc}"
                        ) from exc
                    raise TransferError(f"rsync transfer could not start: {exc}") from exc
                if result.returncode != 0:
                    detail = (result.stderr or result.stdout).decode(
                        "utf-8", errors="replace"
                    ).strip()
                    raise TransferError(
                        f"rsync transfer failed: {detail}"
                        if detail
                        else "rsync transfer failed"
                    )
                if batch.direction is TransferDirection.PULL:
                    local.finalize(
                        local_stage,
                        paths,
                        error_type=TransferError,
                    )
                else:
                    finalized = self._runner.run_workspace_python_bytes(
                        self._target,
                        self._remote_root,
                        REMOTE_FINALIZE_RSYNC_CODE,
                        b"",
                        (remote_stage, *paths),
                    )
                    self._check_remote_stage_result(finalized, "remote rsync finalization")
                    remote_stage = None
            finally:
                if remote_stage is not None:
                    self._runner.run_workspace_python_bytes(
                        self._target,
                        self._remote_root,
                        REMOTE_CLEANUP_RSYNC_CODE,
                        b"",
                        (remote_stage,),
                    )

    def _remote_rsync_stage(self, code: str, paths: tuple[str, ...]) -> str:
        result = self._runner.run_workspace_python_bytes(
            self._target,
            self._remote_root,
            code,
            b"",
            paths,
        )
        self._check_remote_stage_result(result, "remote rsync staging")
        try:
            stage = result.stdout.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise TransferError("remote rsync staging returned an invalid path") from exc
        if not stage.startswith("/") or "\n" in stage or "\0" in stage:
            raise TransferError("remote rsync staging returned an invalid path")
        return stage

    @staticmethod
    def _check_remote_stage_result(
        result: subprocess.CompletedProcess[bytes],
        operation: str,
    ) -> None:
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).decode(
                "utf-8", errors="replace"
            ).strip()
            raise TransferError(f"{operation} failed: {detail}" if detail else operation)

    def _transfer_tar(self, batch: TransferBatch, local: ProtectedLocalRoot) -> None:
        paths = tuple(item.path for item in batch.items)
        if batch.direction is TransferDirection.PUSH:
            with tempfile.TemporaryDirectory(prefix="remote-sandbox-push-") as raw:
                temporary = Path(raw)
                archive = temporary / "batch.tar"
                _create_tar_archive(local, paths, archive)
                _extract_tar_archive(archive, temporary / "validation")
                result = self._runner.run_workspace_python_bytes(
                    self._target,
                    self._remote_root,
                    REMOTE_EXTRACT_CODE,
                    archive.read_bytes(),
                    paths,
                )
        else:
            result = self._runner.run_workspace_python_bytes(
                self._target,
                self._remote_root,
                REMOTE_CREATE_CODE,
                b"",
                paths,
            )
            if result.returncode == 0:
                with tempfile.TemporaryDirectory(prefix="remote-sandbox-pull-") as raw:
                    temporary = Path(raw)
                    archive = temporary / "batch.tar"
                    archive.write_bytes(result.stdout)
                    staging = temporary / "staging"
                    _extract_tar_archive(archive, staging)
                    local.finalize(
                        staging,
                        paths,
                        error_type=TransferError,
                    )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
            raise TransferError(
                f"tar transfer failed: {detail}" if detail else "tar transfer failed"
            )

    @staticmethod
    def _verified_result(
        batch: TransferBatch,
        source_before: Mapping[str, FingerprintState],
        local_after: Mapping[str, LocalFingerprintState],
        remote_after: Mapping[str, FingerprintState],
        on_progress: ProgressCallback,
    ) -> TransferResult:
        completed: list[str] = []
        changed: list[str] = []
        for item in batch.items:
            source = local_after[item.path]
            destination = remote_after[item.path]
            if batch.direction is TransferDirection.PULL:
                source, destination = destination, source
            if isinstance(source, LocalPathChanged):
                changed.append(item.path)
                continue
            if isinstance(destination, LocalPathChanged):
                raise TransferError(f"post-transfer destination changed: {item.path}")
            if source != source_before[item.path]:
                changed.append(item.path)
                continue
            if not _same_content(source, destination):
                raise TransferError(f"post-transfer verification failed: {item.path}")
            completed.append(item.path)
            on_progress(TransferResult(tuple(completed), ()))
        return TransferResult(tuple(completed), tuple(changed))

    @staticmethod
    def _local_postflight(
        local: ProtectedLocalRoot,
        paths: tuple[str, ...],
    ) -> dict[str, LocalFingerprintState]:
        observed: dict[str, LocalFingerprintState] = {}
        for path in paths:
            try:
                observed[path] = local.fingerprint(path, with_hash=True)
            except LocalPathChanged as exc:
                observed[path] = exc
        return observed


def _validate_expected(value: object, path: str, side: str) -> None:
    if value is None:
        return
    if type(value) not in {EntryFingerprint, MissingEntry}:
        raise ValueError(f"expected {side} must be an EntryFingerprint, MissingEntry, or None")
    expected = cast(FingerprintState, value)
    if expected.path != path:
        raise ValueError(f"expected {side} path must match transfer item path")


def _validated_result_paths(paths: tuple[str, ...], label: str) -> tuple[str, ...]:
    normalized = tuple(normalize_relative_path(path) for path in paths)
    if normalized != paths:
        raise ValueError(f"transfer result {label} paths must be normalized")
    if len(paths) != len(set(paths)):
        raise ValueError(f"transfer result {label} paths must be unique")
    return normalized


def _require_expected(
    path: str,
    side: str,
    expected: FingerprintState | None,
    observed: FingerprintState,
) -> None:
    if expected is not None and expected != observed:
        raise TransferError(f"preflight {side} fingerprint mismatch: {path}")


def _same_content(source: FingerprintState, destination: FingerprintState) -> bool:
    if isinstance(source, MissingEntry) or isinstance(destination, MissingEntry):
        return isinstance(source, MissingEntry) and isinstance(destination, MissingEntry)
    return content_identity(source) == content_identity(destination)


def _target_present_in_stage(
    target: str,
    entries: Mapping[str, FingerprintState],
) -> bool:
    exact = entries.get(target)
    if isinstance(exact, EntryFingerprint) and exact.kind is not EntryKind.SYMLINK:
        return True
    return any(path.startswith(f"{target}/") for path in entries)


def _directory_operand(root: Path) -> str:
    return os.fspath(root.resolve()) + "/"


def _path_input(
    batch: TransferBatch,
    source_entries: Mapping[str, FingerprintState] | None = None,
) -> bytes:
    del source_entries
    paths = [item.path for item in batch.items]
    return ("\n".join(paths) + "\n").encode("utf-8")


def _check_tar_result(result: subprocess.CompletedProcess[bytes], operation: str) -> None:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
        message = f"tar {operation} failed"
        raise TransferError(f"{message}: {detail}" if detail else message)


def _create_tar_archive(
    source: ProtectedLocalRoot,
    paths: tuple[str, ...],
    archive: Path,
) -> None:
    environment = os.environ.copy()
    environment["COPYFILE_DISABLE"] = "1"
    staging = archive.parent / "safe-source"
    source.stage(paths, staging, error_type=TransferError)
    try:
        result = subprocess.run(
            [
                "tar",
                "-C",
                os.fspath(staging),
                "-cf",
                os.fspath(archive),
                "--no-recursion",
                "--",
                *paths,
            ],
            check=False,
            capture_output=True,
            env=environment,
        )
        _check_tar_result(result, "create")
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _extract_tar_archive(archive: Path, staging: Path) -> None:
    extract_tar_archive(
        archive,
        staging,
        validate_member=validate_tar_member,
        error_type=TransferError,
    )


def _normalized_delete_paths(paths: Iterable[str]) -> tuple[str, ...]:
    if isinstance(paths, (str, bytes)):
        raise ValueError("delete paths must be an iterable of relative paths")
    return tuple(
        sorted(
            {normalize_relative_path(path) for path in paths},
            key=lambda path: (-path.count("/"), path),
        )
    )
from remote_sandbox._transport_local import LocalPairTransport  # noqa: E402
