from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from remote_sandbox.manifest import normalize_relative_path, workspace_path
from remote_sandbox.ssh import SubprocessSshRunner
from remote_sandbox.transport import validate_tar_member


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "/absolute",
        "a/../../escape",
        "line\nbreak",
        "tab\tbreak",
        "delete\x7fbreak",
        "C:\\Windows\\system.ini",
    ],
)
def test_unsafe_relative_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        normalize_relative_path(path)


def test_workspace_path_rejects_symlink_parent(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink parent"):
        workspace_path(root, "linked/escape.txt")


@pytest.mark.parametrize(
    "path",
    ["../escape", "/absolute", "line\nbreak", "a\\b", "a/../renamed"],
)
def test_tar_path_surface_rejects_non_structural_members(path: str) -> None:
    with pytest.raises(ValueError):
        validate_tar_member(path)


def test_remote_delete_passes_normalized_path_as_a_separate_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, str, bytes, tuple[str, ...]]] = []
    runner = SubprocessSshRunner()

    def fake_run(
        target: str,
        root: str,
        code: str,
        input_data: bytes,
        args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append((target, root, code, input_data, args))
        return subprocess.CompletedProcess(["ssh"], 0, b"", b"")

    monkeypatch.setattr(runner, "run_workspace_python_bytes", fake_run)

    runner.delete_workspace_path("host", "/srv/work", "dir/../safe.txt")

    assert calls[0][1] == "/srv/work"
    assert calls[0][3] == b""
    assert calls[0][4] == ("safe.txt",)


@pytest.mark.parametrize(
    "path",
    ["../escape", "/absolute", "line\nbreak", "linked\\escape"],
)
def test_remote_delete_rejects_unsafe_workspace_paths(path: str) -> None:
    runner = SubprocessSshRunner()

    with pytest.raises(ValueError):
        runner.delete_workspace_path("host", "/srv/work", path)
