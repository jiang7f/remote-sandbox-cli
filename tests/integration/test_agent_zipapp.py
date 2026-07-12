import hashlib
import os
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from remote_sandbox.agent import RemoteAgentManager, build_agent_zipapp
from remote_sandbox.remote_agent import AGENT_VERSION
from remote_sandbox.remote_protocol import (
    AgentRequest,
    AgentResponse,
    decode_response,
    encode_request,
)


class RecordingRunner:
    def __init__(self, self_check: str | None = None) -> None:
        self.self_check = self_check
        self.uploads: list[tuple[str, str, bytes]] = []
        self.python_calls: list[tuple[str, str, tuple[str, ...]]] = []

    def write_bytes_atomic(self, target: str, path: str, content: bytes) -> None:
        self.uploads.append((target, path, content))

    def run_python_file(self, target: str, path: str, args: tuple[str, ...]) -> str:
        self.python_calls.append((target, path, args))
        if self.self_check is not None:
            return self.self_check
        content = self.uploads[-1][2]
        digest = hashlib.sha256(content).hexdigest()
        return f"remote-sandbox-agent {AGENT_VERSION} {digest}\n"


def test_agent_zipapp_self_check(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()

    result = subprocess.run(
        ["python3", str(archive), "self-check"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout == f"remote-sandbox-agent {AGENT_VERSION} {digest}\n"


def test_agent_zipapp_runs_on_python_310(tmp_path: Path) -> None:
    python_310 = os.environ.get("RSB_PYTHON_310")
    if not python_310:
        pytest.skip("the explicit quality gate supplies RSB_PYTHON_310")
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()

    result = subprocess.run(
        [python_310, str(archive), "self-check"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == f"remote-sandbox-agent {AGENT_VERSION} {digest}\n"


def test_agent_zipapp_returns_structured_error_for_unsupported_request(tmp_path: Path) -> None:
    archive = build_agent_zipapp(tmp_path / "agent.pyz")
    request = AgentRequest("not-a-command", {"root": "/home/u/算法测试"})

    result = subprocess.run(
        ["python3", str(archive)],
        input=encode_request(request),
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert decode_response(result.stdout) == AgentResponse(
        ok=False,
        payload={},
        error="unsupported command: not-a-command",
    )


def test_agent_zipapp_has_stable_bytes_and_self_contained_layout(tmp_path: Path) -> None:
    first = build_agent_zipapp(tmp_path / "first.pyz")
    second = build_agent_zipapp(tmp_path / "second.pyz")

    assert first.read_bytes() == second.read_bytes()
    assert first.read_bytes().startswith(b"#!/usr/bin/env python3\n")
    assert stat.S_IMODE(first.stat().st_mode) == 0o755
    with zipfile.ZipFile(first) as archive:
        entries = archive.infolist()
        assert [entry.filename for entry in entries] == [
            "__main__.py",
            "remote_agent/",
            "remote_agent/__init__.py",
            "remote_agent/__main__.py",
            "remote_agent/inotify.py",
            "remote_agent/paths.py",
            "remote_agent/store.py",
            "remote_agent/watcher.py",
        ]
        assert [entry.date_time for entry in entries] == [(1980, 1, 1, 0, 0, 0)] * 8
        assert [entry.compress_type for entry in entries] == [zipfile.ZIP_STORED] * 8
        assert [stat.S_IMODE(entry.external_attr >> 16) for entry in entries] == [
            0o644,
            0o755,
            0o644,
            0o644,
            0o644,
            0o644,
            0o644,
            0o644,
        ]


def test_agent_zipapp_is_identical_across_builder_timezones(tmp_path: Path) -> None:
    timezones = ("UTC", "Asia/Shanghai", "America/New_York")
    script = """\
import hashlib
import sys
from pathlib import Path

from remote_sandbox.agent import build_agent_zipapp

archive = build_agent_zipapp(Path(sys.argv[1]))
print(hashlib.sha256(archive.read_bytes()).hexdigest())
"""
    builds: list[tuple[str, Path, subprocess.CompletedProcess[str]]] = []
    for index, timezone in enumerate(timezones):
        archive = tmp_path / f"agent-{index}.pyz"
        result = subprocess.run(
            [sys.executable, "-c", script, str(archive)],
            capture_output=True,
            env={**os.environ, "TZ": timezone},
            text=True,
            check=False,
        )
        builds.append((timezone, archive, result))

    failures = {
        timezone: result.stderr
        for timezone, _archive, result in builds
        if result.returncode != 0
    }
    assert not failures

    contents = [archive.read_bytes() for _timezone, archive, _result in builds]
    reported_hashes = [result.stdout.strip() for _timezone, _archive, result in builds]
    calculated_hashes = [hashlib.sha256(content).hexdigest() for content in contents]
    assert contents == [contents[0]] * len(timezones)
    assert reported_hashes == calculated_hashes == [calculated_hashes[0]] * len(timezones)


def test_remote_agent_manager_uploads_atomically_outside_workspace(tmp_path: Path) -> None:
    runner = RecordingRunner()

    install = RemoteAgentManager(runner).ensure("ZJU_2")

    expected_path = f"~/.remote-sandbox/agents/{AGENT_VERSION}/agent.pyz"
    assert runner.uploads == [("ZJU_2", expected_path, runner.uploads[0][2])]
    assert runner.python_calls == [("ZJU_2", expected_path, ("self-check",))]
    assert install.version == AGENT_VERSION
    assert install.remote_path == expected_path
    assert install.sha256 == hashlib.sha256(runner.uploads[0][2]).hexdigest()
    assert str(tmp_path) not in install.remote_path


@pytest.mark.parametrize(
    "self_check",
    [
        "remote-sandbox-agent wrong-version ignored\n",
        f"remote-sandbox-agent {AGENT_VERSION} wrong-checksum\n",
    ],
)
def test_remote_agent_manager_rejects_failed_self_check(self_check: str) -> None:
    runner = RecordingRunner(self_check)

    with pytest.raises(RuntimeError, match="self-check failed"):
        RemoteAgentManager(runner).ensure("ZJU_2")
