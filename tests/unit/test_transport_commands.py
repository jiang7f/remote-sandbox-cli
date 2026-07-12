import dataclasses
import errno
import shlex
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import cast

import pytest

from remote_sandbox._transport_remote import (
    REMOTE_CLEANUP_RSYNC_CODE,
    REMOTE_PREPARE_RSYNC_CODE,
    REMOTE_STAGE_RSYNC_CODE,
)
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry, fingerprint_local
from remote_sandbox.reconcile import ActionType, SyncAction
from remote_sandbox.ssh import SubprocessSshRunner, ssh_control_opts
from remote_sandbox.state import AuditSignature
from remote_sandbox.transport import (
    BatchTransport,
    RsyncCapabilities,
    RsyncPathUnsupported,
    TransferBatch,
    TransferDirection,
    TransferError,
    TransferItem,
    TransferResult,
    build_rsync_argv,
    probe_rsync_capabilities,
    validate_tar_member,
)


class _RemoteFingerprinter:
    def __init__(
        self,
        responses: Iterable[dict[str, EntryFingerprint | MissingEntry]],
        *,
        signatures: Iterable[dict[str, AuditSignature | None]] | None = None,
        audit_response: dict[str, AuditSignature | None] | None = None,
    ) -> None:
        self._responses = list(responses)
        self._signatures = (
            list(signatures)
            if signatures is not None
            else [
                self._default_signatures(response, index)
                for index, response in enumerate(self._responses)
            ]
        )
        self._audit_response = audit_response
        self._index = 0
        self.calls: list[tuple[tuple[str, ...], bool]] = []
        self.audit_calls: list[tuple[str, ...]] = []

    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        entries, _signatures = self.observations(paths, with_hash=True)
        return entries

    def observations(
        self,
        paths: Iterable[str],
        *,
        with_hash: bool,
    ) -> tuple[
        dict[str, EntryFingerprint | MissingEntry],
        dict[str, AuditSignature | None],
    ]:
        normalized = tuple(paths)
        self.calls.append((normalized, with_hash))
        response = self._responses[self._index]
        signatures = self._signatures[self._index]
        self._index += 1
        if with_hash:
            return response, signatures
        return {
            path: (
                dataclasses.replace(entry, content_hash=None)
                if isinstance(entry, EntryFingerprint) and entry.kind is EntryKind.FILE
                else entry
            )
            for path, entry in response.items()
        }, signatures

    def audit_signatures(
        self,
        paths: Iterable[str],
    ) -> dict[str, AuditSignature | None]:
        normalized = tuple(paths)
        self.audit_calls.append(normalized)
        if self._audit_response is not None:
            return self._audit_response
        return self._signatures[self._index]

    @staticmethod
    def _default_signatures(
        response: dict[str, EntryFingerprint | MissingEntry],
        index: int,
    ) -> dict[str, AuditSignature | None]:
        return {
            path: (
                AuditSignature(path, entry.kind, 100 + index, 1, position + 1)
                if isinstance(entry, EntryFingerprint)
                else None
            )
            for position, (path, entry) in enumerate(response.items())
        }


class _TransportRunner:
    def __init__(self) -> None:
        self.transport_calls: list[tuple[str, str, str, bytes, tuple[str, ...]]] = []
        self.delete_calls: list[tuple[str, str, str]] = []

    def run_workspace_python_bytes(
        self,
        target: str,
        root: str,
        code: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        self.transport_calls.append((target, root, code, input_data, args))
        if code in {REMOTE_PREPARE_RSYNC_CODE, REMOTE_STAGE_RSYNC_CODE}:
            return subprocess.CompletedProcess(
                ["ssh"],
                0,
                b"/tmp/remote-sandbox-rsync-test\n",
                b"",
            )
        return subprocess.CompletedProcess(["ssh"], 0, b"", b"")

    def delete_workspace_path(self, target: str, root: str, path: str) -> None:
        self.delete_calls.append((target, root, path))


def test_rsync_uses_one_files_from_session_and_preserves_links(tmp_path: Path) -> None:
    batch = TransferBatch(
        TransferDirection.PUSH,
        (TransferItem("a.py", None, None), TransferItem("dir/link", None, None)),
    )
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "host",
        "/remote",
        capabilities=RsyncCapabilities(protect_args=True, secluded_args=False),
    )
    joined = " ".join(argv)
    assert "--archive" in argv
    assert "--links" in argv
    assert "--files-from=-" in argv
    assert joined.count("ssh") <= 1


def test_optional_argument_protection_flag_is_omitted_when_unsupported(tmp_path: Path) -> None:
    batch = TransferBatch(TransferDirection.PUSH, (TransferItem("a.py", None, None),))
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "host",
        "/remote",
        capabilities=RsyncCapabilities(protect_args=False, secluded_args=False),
    )
    assert "--protect-args" not in argv
    assert "--secluded-args" not in argv


def test_unsafe_remote_root_falls_back_when_argument_protection_is_unavailable(
    tmp_path: Path,
) -> None:
    batch = TransferBatch(TransferDirection.PUSH, (TransferItem("a.py", None, None),))
    with pytest.raises(RsyncPathUnsupported):
        build_rsync_argv(
            batch,
            tmp_path,
            "host",
            "/remote/with space",
            capabilities=RsyncCapabilities(protect_args=False, secluded_args=False),
        )


@pytest.mark.parametrize("name", ["../escape", "/absolute", "a/../../escape", "line\nbreak"])
def test_tar_member_validation_rejects_escape_and_control_characters(name: str) -> None:
    with pytest.raises(ValueError):
        validate_tar_member(name)


def test_tar_member_validation_accepts_unicode_spaces_and_leading_dash() -> None:
    assert validate_tar_member("­Ϊǩ data/file.txt") == "­Ϊǩ data/file.txt"


def test_transfer_models_are_frozen_and_validate_normalized_unique_paths() -> None:
    item = TransferItem("pkg/../a.py", None, None)
    assert item.path == "a.py"
    with pytest.raises(dataclasses.FrozenInstanceError):
        item.path = "other.py"  # type: ignore[misc]
    with pytest.raises(ValueError, match="non-empty tuple"):
        TransferBatch(TransferDirection.PUSH, ())
    with pytest.raises(ValueError, match="unique"):
        TransferBatch(
            TransferDirection.PUSH,
            (item, TransferItem("a.py", None, None)),
        )
    with pytest.raises(ValueError):
        TransferItem("line\nbreak", None, None)


def test_transfer_result_is_immutable() -> None:
    result = TransferResult(("a.py",), ())
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.completed = ()  # type: ignore[misc]


def test_transfer_result_rejects_invalid_or_overlapping_paths() -> None:
    with pytest.raises(ValueError, match="tuples"):
        TransferResult(cast(tuple[str, ...], ["a.py"]), ())
    with pytest.raises(ValueError, match="unique"):
        TransferResult(("a.py", "a.py"), ())
    with pytest.raises(ValueError, match="disjoint"):
        TransferResult(("a.py",), ("a.py",))
    with pytest.raises(ValueError):
        TransferResult(("line\nbreak",), ())


def test_rsync_push_argv_uses_only_portable_flags_and_isolated_ssh(tmp_path: Path) -> None:
    batch = TransferBatch(TransferDirection.PUSH, (TransferItem("a.py", None, None),))
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "user@example-host",
        "/srv/work",
        capabilities=RsyncCapabilities(protect_args=False, secluded_args=True),
    )
    assert argv == [
        "rsync",
        "--archive",
        "--links",
        "--times",
        "--itemize-changes",
        "--files-from=-",
        "--secluded-args",
        "--rsh",
        shlex.join(("ssh", *ssh_control_opts())),
        f"{tmp_path.resolve()}/",
        "user@example-host:/srv/work/",
    ]


def test_rsync_pull_reverses_operands_and_handles_root(tmp_path: Path) -> None:
    batch = TransferBatch(TransferDirection.PULL, (TransferItem("a.py", None, None),))
    argv = build_rsync_argv(
        batch,
        tmp_path,
        "host",
        "/",
        capabilities=RsyncCapabilities(protect_args=True, secluded_args=True),
    )
    assert argv[-2:] == ["host:/", f"{tmp_path.resolve()}/"]
    assert argv.count("--protect-args") == 1
    assert "--secluded-args" not in argv


def test_capability_probe_tests_each_optional_flag_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0 if "--secluded-args" in argv else 1,
            b"",
            b"",
        )

    monkeypatch.setattr("remote_sandbox.transport.subprocess.run", fake_run)
    capabilities = probe_rsync_capabilities()
    assert capabilities == RsyncCapabilities(protect_args=False, secluded_args=True)
    assert calls == [
        ["rsync", "--protect-args", "--version"],
        ["rsync", "--secluded-args", "--version"],
    ]


@pytest.mark.parametrize(
    "name",
    ["", ".", "a/../b", "a\\b", "tab\tname", "nul\0name"],
)
def test_tar_member_validation_rejects_non_structural_names(name: str) -> None:
    with pytest.raises(ValueError):
        validate_tar_member(name)


def test_transfer_batch_from_sync_actions_maps_expected_sides_without_base_updates() -> None:
    local = MissingEntry("a.py")
    remote = MissingEntry("a.py")
    push = SyncAction(ActionType.PUSH, "a.py", local, remote, local)
    batch = TransferBatch.from_actions((push,))
    assert batch == TransferBatch(
        TransferDirection.PUSH,
        (TransferItem("a.py", local, remote),),
    )
    with pytest.raises(ValueError, match="same transfer direction"):
        TransferBatch.from_actions(
            (
                push,
                SyncAction(
                    ActionType.PULL,
                    "b.py",
                    MissingEntry("b.py"),
                    MissingEntry("b.py"),
                    MissingEntry("b.py"),
                ),
            )
        )
    with pytest.raises(ValueError, match="transfer action"):
        TransferBatch.from_actions(
            (SyncAction(ActionType.UPDATE_BASE, "a.py", local, remote, local),)
        )


def test_batch_transport_runs_one_verified_rsync_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    remote = _RemoteFingerprinter(
        [
            {"a.txt": MissingEntry("a.txt")},
            {"a.txt": source},
        ]
    )
    calls: list[tuple[list[str], bytes]] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append((argv, cast(bytes, kwargs["input"])))
        return subprocess.CompletedProcess(argv, 0, b">f+++++++ a.txt\n", b"")

    monkeypatch.setattr("remote_sandbox.transport.subprocess.run", fake_run)
    result = BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=_TransportRunner(),
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    ).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", source, MissingEntry("a.txt")),),
        ),
        lambda _progress: None,
    )
    assert result.completed == ("a.txt",)
    assert len(calls) == 1
    assert calls[0][1] == b"a.txt\n"
    assert remote.calls == [(("a.txt",), True), (("a.txt",), False)]
    assert remote.audit_calls == []
    assert result.verified_fingerprints == (source,)


def test_pull_restores_directory_mode_narrowed_by_receiver_umask(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    source = EntryFingerprint("tree", EntryKind.DIR, None, 1_700_000_000_000_000_000, 0o40775)
    source_signature = AuditSignature("tree", EntryKind.DIR, 100, 1, 1)
    remote = _RemoteFingerprinter(
        [{"tree": source}, {"tree": source}],
        signatures=[{"tree": source_signature}, {"tree": source_signature}],
    )

    def finalize(
        _self: object,
        _staging: Path,
        _paths: tuple[str, ...],
        **_kwargs: object,
    ) -> dict[str, tuple[EntryFingerprint, AuditSignature]]:
        (local_root / "tree").mkdir(mode=0o755)
        narrowed = dataclasses.replace(source, mode=0o40755)
        return {"tree": (narrowed, AuditSignature("tree", EntryKind.DIR, 200, 2, 2))}

    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, b"", b""),
    )
    monkeypatch.setattr(
        "remote_sandbox.transport.ProtectedLocalRoot.finalize",
        finalize,
    )

    result = BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=_TransportRunner(),
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    ).transfer(
        TransferBatch(
            TransferDirection.PULL,
            (TransferItem("tree", source, MissingEntry("tree")),),
        ),
        lambda _progress: None,
    )

    assert (local_root / "tree").stat().st_mode & 0o777 == 0o775
    assert result.verified_fingerprints[0].mode & 0o777 == 0o775


@pytest.mark.parametrize(
    ("field", "value"),
    (("size", 0), ("mode", 0o100600), ("mtime_ns", 1)),
)
def test_rsync_protocol_proof_rejects_incompatible_destination_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    destination = dataclasses.replace(source, **{field: value}, content_hash=None)
    remote = _RemoteFingerprinter(
        [
            {"a.txt": MissingEntry("a.txt")},
            {"a.txt": destination},
        ]
    )
    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, b"", b""),
    )

    with pytest.raises(TransferError, match="post-transfer verification"):
        BatchTransport(
            local_root,
            "host",
            "/remote",
            remote,
            runner=_TransportRunner(),
            capabilities=RsyncCapabilities(False, False),
            remote_rsync_available=True,
        ).transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", source, MissingEntry("a.txt")),),
            ),
            lambda _progress: None,
        )


class _CleanupFailureRunner(_TransportRunner):
    def run_workspace_python_bytes(
        self,
        target: str,
        root: str,
        code: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        if code == REMOTE_CLEANUP_RSYNC_CODE:
            self.transport_calls.append((target, root, code, input_data, args))
            return subprocess.CompletedProcess(["ssh"], 17, b"", b"cleanup failed")
        return super().run_workspace_python_bytes(target, root, code, input_data, args)


def test_remote_cleanup_helper_removes_staging_directory() -> None:
    staging = Path(tempfile.mkdtemp(prefix="remote-sandbox-rsync-cleanup-success-"))
    (staging / "payload").write_bytes(b"payload")

    result = subprocess.run(
        [sys.executable, "-c", REMOTE_CLEANUP_RSYNC_CODE, "workspace", str(staging)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not staging.exists()


def test_remote_cleanup_helper_propagates_removal_failure() -> None:
    staging = Path(tempfile.mkdtemp(prefix="remote-sandbox-rsync-cleanup-failure-"))
    (staging / "payload").write_bytes(b"payload")
    injected = (
        "import shutil\n"
        "def controlled(path, ignore_errors=False):\n"
        "    if ignore_errors:\n"
        "        return\n"
        "    raise PermissionError('injected cleanup failure')\n"
        "shutil.rmtree = controlled\n"
    )
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                injected + REMOTE_CLEANUP_RSYNC_CODE,
                "workspace",
                str(staging),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "injected cleanup failure" in result.stderr
        assert staging.exists()
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def test_remote_rsync_cleanup_failure_prevents_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    source = EntryFingerprint("a.txt", EntryKind.FILE, 1, 1, 0o100644, None, "hash")
    remote = _RemoteFingerprinter([{"a.txt": source}, {"a.txt": source}])
    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, b"", b""),
    )
    monkeypatch.setattr(
        "remote_sandbox.transport.ProtectedLocalRoot.finalize",
        lambda _self, _staging, _paths, **_kwargs: {
            "a.txt": (source, AuditSignature("a.txt", EntryKind.FILE, 1, 1, 1))
        },
    )

    with pytest.raises(TransferError, match="remote rsync cleanup failed.*cleanup failed"):
        BatchTransport(
            local_root,
            "host",
            "/remote",
            remote,
            runner=_CleanupFailureRunner(),
            capabilities=RsyncCapabilities(False, False),
            remote_rsync_available=True,
        ).transfer(
            TransferBatch(
                TransferDirection.PULL,
                (TransferItem("a.txt", source, MissingEntry("a.txt")),),
            ),
            lambda _progress: None,
        )


def test_remote_rsync_transfer_and_cleanup_failures_are_both_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 23, b"", b"transfer failed"),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        BatchTransport(
            local_root,
            "host",
            "/remote",
            _RemoteFingerprinter([{"a.txt": MissingEntry("a.txt")}]),
            runner=_CleanupFailureRunner(),
            capabilities=RsyncCapabilities(False, False),
            remote_rsync_available=True,
        ).transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", source, MissingEntry("a.txt")),),
            ),
            lambda _progress: None,
        )

    failures = tuple(str(failure) for failure in raised.value.exceptions)
    assert any("transfer failed" in failure for failure in failures)
    assert any("cleanup failed" in failure for failure in failures)


def test_batch_transport_uses_tar_for_unsafe_unprotected_remote_root(
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    remote = _RemoteFingerprinter(
        [
            {"a.txt": MissingEntry("a.txt")},
            {"a.txt": source},
        ]
    )
    runner = _TransportRunner()
    result = BatchTransport(
        local_root,
        "host",
        "/remote with space",
        remote,
        runner=runner,
        capabilities=RsyncCapabilities(False, False),
    ).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", source, MissingEntry("a.txt")),),
        ),
        lambda _progress: None,
    )
    assert result.completed == ("a.txt",)
    assert len(runner.transport_calls) == 1
    assert runner.transport_calls[0][:2] == ("host", "/remote with space")
    assert runner.transport_calls[0][3]
    assert runner.transport_calls[0][4] == ("a.txt",)


def test_batch_transport_uses_tar_when_local_or_remote_rsync_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    remote = _RemoteFingerprinter(
        [{"a.txt": MissingEntry("a.txt")}, {"a.txt": source}]
    )
    runner = _TransportRunner()
    monkeypatch.setattr("remote_sandbox.transport.shutil.which", lambda _name: None)
    BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=runner,
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    ).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", source, MissingEntry("a.txt")),),
        ),
        lambda _progress: None,
    )
    assert len(runner.transport_calls) == 1

    remote = _RemoteFingerprinter(
        [{"a.txt": MissingEntry("a.txt")}, {"a.txt": source}]
    )
    runner = _TransportRunner()
    monkeypatch.setattr("remote_sandbox.transport.shutil.which", lambda _name: "/usr/bin/rsync")
    BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=runner,
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=False,
    ).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", source, MissingEntry("a.txt")),),
        ),
        lambda _progress: None,
    )
    assert len(runner.transport_calls) == 1


@pytest.mark.parametrize(
    "launch_error",
    [
        FileNotFoundError(errno.ENOENT, "rsync disappeared"),
        PermissionError(errno.EACCES, "rsync is not executable"),
        OSError(errno.ENOEXEC, "invalid rsync executable"),
    ],
)
def test_batch_transport_falls_back_when_rsync_launch_becomes_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    launch_error: OSError,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    remote = _RemoteFingerprinter(
        [{"a.txt": MissingEntry("a.txt")}, {"a.txt": source}]
    )
    transport = BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=_TransportRunner(),
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    )
    used_tar = False

    def fake_tar(_batch: TransferBatch, _local: object) -> None:
        nonlocal used_tar
        used_tar = True

    monkeypatch.setattr("remote_sandbox.transport.shutil.which", lambda _name: "/usr/bin/rsync")
    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(launch_error),
    )
    monkeypatch.setattr(transport, "_transfer_tar", fake_tar)
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("a.txt", source, MissingEntry("a.txt")),),
        ),
        lambda _progress: None,
    )
    assert used_tar
    assert result.completed == ("a.txt",)


def test_batch_transport_does_not_hide_unrelated_rsync_launch_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "a.txt").write_text("a", encoding="utf-8")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "a.txt", with_hash=True))
    remote = _RemoteFingerprinter([{"a.txt": MissingEntry("a.txt")}])
    transport = BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=_TransportRunner(),
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    )
    monkeypatch.setattr("remote_sandbox.transport.shutil.which", lambda _name: "/usr/bin/rsync")
    monkeypatch.setattr(
        "remote_sandbox.transport.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.EIO, "I/O failure")),
    )
    monkeypatch.setattr(
        transport,
        "_transfer_tar",
        lambda *args: pytest.fail("unrelated rsync errors must not fall back"),
    )
    with pytest.raises(TransferError, match="I/O failure"):
        transport.transfer(
            TransferBatch(
                TransferDirection.PUSH,
                (TransferItem("a.txt", source, MissingEntry("a.txt")),),
            ),
            lambda _progress: None,
        )

def test_batch_transport_uses_tar_when_symlink_target_is_not_staged(tmp_path: Path) -> None:
    local_root = tmp_path / "local"
    outside = tmp_path / "outside"
    local_root.mkdir()
    outside.mkdir()
    (local_root / "link").symlink_to("../outside")
    source = cast(EntryFingerprint, fingerprint_local(local_root, "link", with_hash=True))
    remote = _RemoteFingerprinter(
        [{"link": MissingEntry("link")}, {"link": source}]
    )
    runner = _TransportRunner()
    BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=runner,
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    ).transfer(
        TransferBatch(
            TransferDirection.PUSH,
            (TransferItem("link", source, MissingEntry("link")),),
        ),
        lambda _progress: None,
    )
    assert len(runner.transport_calls) == 1


@pytest.mark.parametrize("target", ["missing.txt", "other.txt"])
def test_batch_pull_uses_tar_when_remote_symlink_target_is_not_staged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target: str,
) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    source = EntryFingerprint(
        "link",
        EntryKind.SYMLINK,
        None,
        1,
        0o120777,
        target,
        "link-hash",
    )
    remote = _RemoteFingerprinter([{"link": source}, {"link": source}])
    transport = BatchTransport(
        local_root,
        "host",
        "/remote",
        remote,
        runner=_TransportRunner(),
        capabilities=RsyncCapabilities(False, False),
        remote_rsync_available=True,
    )
    used_tar = False

    def fake_tar(batch: TransferBatch, _local: object) -> None:
        nonlocal used_tar
        used_tar = True
        (local_root / batch.items[0].path).symlink_to(target)

    monkeypatch.setattr(transport, "_transfer_tar", fake_tar)
    monkeypatch.setattr(
        transport,
        "_transfer_rsync",
        lambda *args: pytest.fail("pull symlink must not use rsync"),
    )
    result = transport.transfer(
        TransferBatch(
            TransferDirection.PULL,
            (TransferItem("link", source, MissingEntry("link")),),
        ),
        lambda _progress: None,
    )
    assert used_tar
    assert result.completed == ("link",)


def test_batch_transport_remote_delete_is_child_first_and_normalized(tmp_path: Path) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    runner = _TransportRunner()
    transport = BatchTransport(
        local_root,
        "host",
        "/remote",
        _RemoteFingerprinter([]),
        runner=runner,
        capabilities=RsyncCapabilities(False, False),
    )
    transport.delete_remote(("dir", "dir/child", "missing", "dir/./child"))
    assert runner.delete_calls == [
        ("host", "/remote", "dir/child"),
        ("host", "/remote", "dir"),
        ("host", "/remote", "missing"),
    ]


def test_structured_workspace_python_call_keeps_payload_on_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        observed["argv"] = argv
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, b"ok", b"")

    monkeypatch.setattr("remote_sandbox.ssh.subprocess.run", fake_run)
    payload = b"archive-$()-bytes"
    result = SubprocessSshRunner().run_workspace_python_bytes(
        "host",
        "/remote root",
        "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())",
        payload,
        ("a path",),
    )
    assert result.stdout == b"ok"
    assert cast(dict[str, object], observed["kwargs"])["input"] == payload
    assert payload not in " ".join(cast(list[str], observed["argv"])).encode()
    assert any(value.startswith("ControlPath=") for value in cast(list[str], observed["argv"]))
