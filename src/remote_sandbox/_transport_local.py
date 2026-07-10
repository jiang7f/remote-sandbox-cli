from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from remote_sandbox._transport_paths import delete_entries, finalize_entries, stage_entries
from remote_sandbox.manifest import (
    fingerprint_local,
    workspace_path,
)
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
        source, destination = self._roots(batch.direction)
        before = self._preflight(batch, source, destination)
        if self._engine == "rsync":
            self._transfer_rsync(batch, source, destination, before)
        else:
            self._transfer_tar(batch, source, destination)
        return self._verify(batch, source, destination, before, on_progress)

    def delete_local(self, paths: tuple[str, ...]) -> None:
        self._delete(self._local_root, paths)

    def delete_remote(self, paths: tuple[str, ...]) -> None:
        self._delete(self._remote_root, paths)

    def _roots(self, direction: TransferDirection) -> tuple[Path, Path]:
        if direction is TransferDirection.PUSH:
            return self._local_root, self._remote_root
        return self._remote_root, self._local_root

    def _preflight(
        self,
        batch: TransferBatch,
        source: Path,
        destination: Path,
    ) -> dict[str, FingerprintState]:
        observed: dict[str, FingerprintState] = {}
        for item in batch.items:
            workspace_path(source, item.path)
            workspace_path(destination, item.path)
            source_entry = fingerprint_local(source, item.path, with_hash=True)
            destination_entry = fingerprint_local(destination, item.path, with_hash=True)
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
        source: Path,
        destination: Path,
        source_before: Mapping[str, FingerprintState],
    ) -> None:
        paths = tuple(item.path for item in batch.items)
        with tempfile.TemporaryDirectory(prefix="remote-sandbox-local-rsync-") as raw:
            temporary = Path(raw)
            safe_source = temporary / "source"
            safe_destination = temporary / "destination"
            stage_entries(source, paths, safe_source, error_type=TransferError)
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
            finalize_entries(
                safe_destination,
                destination,
                paths,
                error_type=TransferError,
            )

    def _transfer_tar(self, batch: TransferBatch, source: Path, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".remote-sandbox-tar-", dir=destination) as raw:
            temporary = Path(raw)
            archive = temporary / "batch.tar"
            staging = temporary / "staging"
            _create_tar_archive(source, tuple(item.path for item in batch.items), archive)
            _extract_tar_archive(archive, staging)
            paths = tuple(item.path for item in batch.items)
            finalize_entries(
                staging,
                destination,
                paths,
                error_type=TransferError,
            )

    def _verify(
        self,
        batch: TransferBatch,
        source: Path,
        destination: Path,
        before: dict[str, FingerprintState],
        on_progress: ProgressCallback,
    ) -> TransferResult:
        completed: list[str] = []
        changed: list[str] = []
        for item in batch.items:
            source_after = fingerprint_local(source, item.path, with_hash=True)
            if source_after != before[item.path]:
                changed.append(item.path)
                continue
            destination_after = fingerprint_local(destination, item.path, with_hash=True)
            if not _same_content(source_after, destination_after):
                raise TransferError(f"post-transfer verification failed: {item.path}")
            completed.append(item.path)
            on_progress(TransferResult(tuple(completed), ()))
        return TransferResult(tuple(completed), tuple(changed))

    @staticmethod
    def _delete(root: Path, paths: tuple[str, ...]) -> None:
        delete_entries(
            root,
            _normalized_delete_paths(paths),
            error_type=TransferError,
        )
