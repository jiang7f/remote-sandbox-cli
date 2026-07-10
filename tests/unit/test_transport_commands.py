import dataclasses
import shlex
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import cast

import pytest

from remote_sandbox._transport_remote import (
    REMOTE_PREPARE_RSYNC_CODE,
    REMOTE_STAGE_RSYNC_CODE,
)
from remote_sandbox.manifest import EntryFingerprint, MissingEntry, fingerprint_local
from remote_sandbox.reconcile import ActionType, SyncAction
from remote_sandbox.ssh import SubprocessSshRunner, ssh_control_opts
from remote_sandbox.transport import (
    BatchTransport,
    RsyncCapabilities,
    RsyncPathUnsupported,
    TransferBatch,
    TransferDirection,
    TransferItem,
    TransferResult,
    build_rsync_argv,
    probe_rsync_capabilities,
    validate_tar_member,
)


class _RemoteFingerprinter:
    def __init__(self, responses: Iterable[dict[str, EntryFingerprint | MissingEntry]]) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, ...]] = []

    def hash_paths(
        self,
        paths: Iterable[str],
    ) -> dict[str, EntryFingerprint | MissingEntry]:
        normalized = tuple(paths)
        self.calls.append(normalized)
        return next(self._responses)


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
    assert remote.calls == [("a.txt",), ("a.txt",)]


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
