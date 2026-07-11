from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from remote_sandbox._transport_fingerprint import LocalPathChanged, ProtectedLocalRoot
from remote_sandbox._transport_paths import StagedSourceChanged
from remote_sandbox.manifest import EntryFingerprint
from remote_sandbox.state import AuditSignature
from remote_sandbox.transport import (
    FingerprintState,
    ProgressCallback,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferPreflightError,
    TransferResult,
    _create_tar_archive,
    _directory_operand,
    _extract_tar_archive,
    _normalized_delete_paths,
    _protocol_verified_destination,
    _require_expected,
    _same_content,
    _VerifiedProgressBatcher,
)


@dataclass(slots=True)
class _LocalPreflight:
    source_entries: dict[str, FingerprintState]
    source_signatures: dict[str, AuditSignature | None]
    destination_observations: dict[
        str,
        tuple[FingerprintState, AuditSignature | None],
    ] = field(default_factory=dict)
    changed_paths: set[str] = field(default_factory=set)
    staged_source: Path | None = None
    staged_destination: Path | None = None


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
            if self._engine == "rsync":
                paths = tuple(item.path for item in batch.items)
                with tempfile.TemporaryDirectory(prefix="remote-sandbox-local-rsync-") as raw:
                    temporary = Path(raw)
                    try:
                        before = self._preflight_rsync(
                            batch,
                            source,
                            destination,
                            temporary,
                        )
                    except StagedSourceChanged:
                        return TransferResult((), paths)
                    if before.changed_paths:
                        return TransferResult((), paths)
                    assert before.staged_source is not None
                    assert before.staged_destination is not None
                    before.staged_destination.mkdir()
                    self._transfer_rsync(batch, source, destination, before)
                    before.changed_paths |= source.verify_and_cleanup_stage(
                        paths,
                        before.staged_source,
                        expected_entries=before.source_entries,
                        expected_signatures=before.source_signatures,
                        error_type=TransferError,
                    )
                    return self._verify(batch, source, destination, before, on_progress)
            before = self._preflight(batch, source, destination)
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
    ) -> _LocalPreflight:
        paths = tuple(item.path for item in batch.items)
        with ThreadPoolExecutor(max_workers=2) as executor:
            source_future = executor.submit(source.observations, paths, with_hash=True)
            destination_future = executor.submit(
                destination.fingerprints,
                paths,
                with_hash=True,
            )
            source_observations = source_future.result()
            destination_entries = destination_future.result()
        source_entries = {
            path: fingerprint for path, (fingerprint, _signature) in source_observations.items()
        }
        source_signatures = {
            path: signature for path, (_fingerprint, signature) in source_observations.items()
        }
        for item in batch.items:
            source_entry = source_entries[item.path]
            destination_entry = destination_entries[item.path]
            _require_expected(item.path, "source", item.expected_source, source_entry)
            _require_expected(
                item.path,
                "destination",
                item.expected_destination,
                destination_entry,
            )
        return _LocalPreflight(source_entries, source_signatures)

    @staticmethod
    def _preflight_rsync(
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
        temporary: Path,
    ) -> _LocalPreflight:
        paths = tuple(item.path for item in batch.items)
        staged_source = temporary / "source"
        staged_destination = temporary / "destination"
        with ThreadPoolExecutor(max_workers=2) as executor:
            source_future = executor.submit(
                source.stage_observations,
                paths,
                staged_source,
                error_type=TransferError,
            )
            destination_future = executor.submit(
                destination.fingerprints,
                paths,
                with_hash=True,
            )
            source_entries, source_signatures = source_future.result()
            destination_entries = destination_future.result()
        changed_paths: set[str] = set()
        for item in batch.items:
            try:
                _require_expected(
                    item.path,
                    "source",
                    item.expected_source,
                    source_entries[item.path],
                )
            except TransferPreflightError:
                if not _hash_only_source_mismatch(
                    item.expected_source,
                    source_entries[item.path],
                ):
                    raise
                changed_paths.add(item.path)
            _require_expected(
                item.path,
                "destination",
                item.expected_destination,
                destination_entries[item.path],
            )
        return _LocalPreflight(
            source_entries,
            source_signatures,
            changed_paths=changed_paths,
            staged_source=staged_source,
            staged_destination=staged_destination,
        )

    def _transfer_rsync(
        self,
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
        source_before: _LocalPreflight,
    ) -> None:
        paths = tuple(item.path for item in batch.items)
        if source_before.staged_source is None or source_before.staged_destination is None:
            raise TransferError("rsync staging was not prepared")
        argv = [
            "rsync",
            "--archive",
            "--links",
            "--times",
            _directory_operand(source_before.staged_source),
            _directory_operand(source_before.staged_destination),
        ]
        result = subprocess.run(
            argv,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).decode(
                "utf-8", errors="replace"
            ).strip()
            raise TransferError(
                f"rsync transfer failed: {detail}" if detail else "rsync transfer failed"
            )
        source_before.destination_observations = destination.finalize(
            source_before.staged_destination,
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
        before: _LocalPreflight,
        on_progress: ProgressCallback,
    ) -> TransferResult:
        if before.staged_source is None:
            return self._verify_hashed_source(
                batch,
                source,
                destination,
                before,
                on_progress,
            )
        return self._verified_observations(
            batch,
            before,
            before.destination_observations,
            on_progress,
        )

    @staticmethod
    def _verify_hashed_source(
        batch: TransferBatch,
        source: ProtectedLocalRoot,
        destination: ProtectedLocalRoot,
        before: _LocalPreflight,
        on_progress: ProgressCallback,
    ) -> TransferResult:
        completed: list[str] = []
        verified: list[EntryFingerprint] = []
        changed: list[str] = []
        progress = _VerifiedProgressBatcher(on_progress)
        paths = tuple(item.path for item in batch.items)
        try:
            try:
                source_after = source.fingerprints(paths, with_hash=True)
                destination_after = destination.fingerprints(paths, with_hash=True)
            except LocalPathChanged:
                source_after = {}
                destination_after = {}
                for item in batch.items:
                    try:
                        source_after[item.path] = source.fingerprint(
                            item.path,
                            with_hash=True,
                        )
                    except LocalPathChanged:
                        changed.append(item.path)
                        continue
                    try:
                        destination_after[item.path] = destination.fingerprint(
                            item.path,
                            with_hash=True,
                        )
                    except LocalPathChanged as exc:
                        raise TransferError(
                            f"post-transfer destination changed: {item.path}"
                        ) from exc
            for item in batch.items:
                if item.path in changed:
                    continue
                source_entry = source_after[item.path]
                if source_entry != before.source_entries[item.path]:
                    changed.append(item.path)
                    continue
                destination_entry = destination_after[item.path]
                if not _same_content(source_entry, destination_entry):
                    raise TransferError(f"post-transfer verification failed: {item.path}")
                if not isinstance(destination_entry, EntryFingerprint):
                    raise TransferError(f"post-transfer destination is missing: {item.path}")
                completed.append(item.path)
                verified.append(destination_entry)
                progress.add(destination_entry)
        finally:
            progress.flush()
        return TransferResult(tuple(completed), tuple(changed), tuple(verified))

    @staticmethod
    def _verified_observations(
        batch: TransferBatch,
        before: _LocalPreflight,
        destination_after: Mapping[
            str,
            tuple[FingerprintState, AuditSignature | None],
        ],
        on_progress: ProgressCallback,
    ) -> TransferResult:
        completed: list[str] = []
        verified: list[EntryFingerprint] = []
        changed: list[str] = []
        progress = _VerifiedProgressBatcher(on_progress)
        try:
            for item in batch.items:
                if item.path in before.changed_paths:
                    changed.append(item.path)
                    continue
                source_before = before.source_entries[item.path]
                destination_observation = destination_after.get(item.path)
                if destination_observation is None:
                    raise TransferError(
                        f"post-transfer verification failed: {item.path}"
                    )
                destination_entry, _destination_signature = destination_observation
                verified_entry = _protocol_verified_destination(
                    source_before,
                    destination_entry,
                )
                if verified_entry is None:
                    raise TransferError(f"post-transfer verification failed: {item.path}")
                completed.append(item.path)
                verified.append(verified_entry)
                progress.add(verified_entry)
        finally:
            progress.flush()
        return TransferResult(tuple(completed), tuple(changed), tuple(verified))

    @staticmethod
    def _delete(root: Path, paths: tuple[str, ...]) -> None:
        with ProtectedLocalRoot(root) as protected:
            protected.delete(
                _normalized_delete_paths(paths),
                error_type=TransferError,
            )


def _hash_only_source_mismatch(
    expected: FingerprintState | None,
    observed: FingerprintState,
) -> bool:
    return (
        isinstance(expected, EntryFingerprint)
        and isinstance(observed, EntryFingerprint)
        and expected.kind is observed.kind
        and expected.size == observed.size
        and expected.mtime_ns == observed.mtime_ns
        and expected.mode == observed.mode
        and expected.link_target == observed.link_target
        and expected.content_hash is not None
        and expected.content_hash != observed.content_hash
    )
