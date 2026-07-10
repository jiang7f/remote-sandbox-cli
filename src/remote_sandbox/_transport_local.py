from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from remote_sandbox._transport_fingerprint import LocalPathChanged, ProtectedLocalRoot
from remote_sandbox.transport import (
    FingerprintState,
    ProgressCallback,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferResult,
    _create_tar_archive,
    _directory_operand,
    _extract_tar_archive,
    _normalized_delete_paths,
    _path_input,
    _require_expected,
    _same_content,
)


class LocalPairTransport:
    def __init__(
        self,
        local_root: Path,
        remote_root: Path,
        *,
        engine: Literal["rsync", "tar"] = "rsync",
    ) -> None:
        if engine not in {"rsync", "tar"}:
            raise ValueError("transport engine must be rsync or tar")
        self._local_root = local_root
        self._remote_root = remote_root
        self._engine = engine

    def transfer(self, batch: TransferBatch, on_progress: ProgressCallback) -> TransferResult:
        if type(batch) is not TransferBatch:
            raise ValueError("batch must be a TransferBatch")
        if not callable(on_progress):
            raise ValueError("on_progress must be callable")
        source_path, destination_path = self._roots(batch.direction)
        with (
            ProtectedLocalRoot(source_path) as source,
            ProtectedLocalRoot(destination_path) as destination,
        ):
            before = self._preflight(batch, source, destination)
            if self._engine == "rsync":
                self._transfer_rsync(batch, source, destination, before)
            else:
                self._transfer_tar(batch, source, destination)
            return self._verify(batch, source, destination, before, on_progress)

    def delete_local(
        self,
        paths: tuple[str, ...] | Mapping[str, FingerprintState],
    ) -> TransferResult:
        return self._delete_verified(self._local_root, paths)

    def delete_remote(
        self,
        paths: tuple[str, ...] | Mapping[str, FingerprintState],
    ) -> TransferResult:
        return self._delete_verified(self._remote_root, paths)

    def _roots(self, direction: TransferDirection) -> tuple[Path, Path]:
        if direction is TransferDirection.PUSH:
            return self._local_root, self._remote_root
        return self._remote_root, self._local_root

    @staticmethod
    def _delete_verified(
        root: Path,
        paths: tuple[str, ...] | Mapping[str, FingerprintState],
    ) -> TransferResult:
        if not isinstance(paths, Mapping):
            normalized = _normalized_delete_paths(paths)
            LocalPairTransport._delete(root, normalized)
            return TransferResult(normalized, ())
        normalized_paths = _normalized_delete_paths(paths)
        expected = {path: paths[path] for path in normalized_paths}
        with ProtectedLocalRoot(root) as protected:
            completed, changed = protected.delete_expected(expected, error_type=TransferError)
        return TransferResult(completed, changed)

    def _preflight(
        self,
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
    ) -> dict[str, FingerprintState]:
        observed: dict[str, FingerprintState] = {}
        for item in batch.items:
            source_entry = source.fingerprint(item.path, with_hash=True)
            destination_entry = destination.fingerprint(item.path, with_hash=True)
            _require_expected(item.path, "source", item.expected_source, source_entry)
            _require_expected(
                item.path,
                "destination",
                item.expected_destination,
                destination_entry,
            )
            observed[item.path] = source_entry
        return observed

    def _transfer_rsync(
        self,
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
        source_before: Mapping[str, FingerprintState],
    ) -> None:
        paths = tuple(item.path for item in batch.items)
        with tempfile.TemporaryDirectory(prefix="remote-sandbox-local-rsync-") as raw:
            temporary = Path(raw)
            safe_source = temporary / "source"
            safe_destination = temporary / "destination"
            source.stage(paths, safe_source, error_type=TransferError)
            safe_destination.mkdir()
            argv = [
                "rsync",
                "--archive",
                "--links",
                "--times",
                "--itemize-changes",
                "--files-from=-",
                _directory_operand(safe_source),
                _directory_operand(safe_destination),
            ]
            result = subprocess.run(
                argv,
                check=False,
                input=_path_input(batch, source_before),
                capture_output=True,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout).decode(
                    "utf-8", errors="replace"
                ).strip()
                raise TransferError(
                    f"rsync transfer failed: {detail}" if detail else "rsync transfer failed"
                )
            destination.finalize(
                safe_destination,
                paths,
                error_type=TransferError,
            )

    def _transfer_tar(
        self,
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="remote-sandbox-tar-") as raw:
            temporary = Path(raw)
            archive = temporary / "batch.tar"
            staging = temporary / "staging"
            _create_tar_archive(source, tuple(item.path for item in batch.items), archive)
            _extract_tar_archive(archive, staging)
            paths = tuple(item.path for item in batch.items)
            destination.finalize(
                staging,
                paths,
                error_type=TransferError,
            )

    def _verify(
        self,
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
        before: dict[str, FingerprintState],
        on_progress: ProgressCallback,
    ) -> TransferResult:
        completed: list[str] = []
        changed: list[str] = []
        for item in batch.items:
            try:
                source_after = source.fingerprint(item.path, with_hash=True)
            except LocalPathChanged:
                changed.append(item.path)
                continue
            if source_after != before[item.path]:
                changed.append(item.path)
                continue
            try:
                destination_after = destination.fingerprint(item.path, with_hash=True)
            except LocalPathChanged as exc:
                raise TransferError(
                    f"post-transfer destination changed: {item.path}"
                ) from exc
            if not _same_content(source_after, destination_after):
                raise TransferError(f"post-transfer verification failed: {item.path}")
            completed.append(item.path)
            on_progress(TransferResult(tuple(completed), ()))
        return TransferResult(tuple(completed), tuple(changed))

    @staticmethod
    def _delete(root: Path, paths: tuple[str, ...]) -> None:
        with ProtectedLocalRoot(root) as protected:
            protected.delete(
                _normalized_delete_paths(paths),
                error_type=TransferError,
            )
