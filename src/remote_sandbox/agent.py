from __future__ import annotations

import hashlib
import os
import posixpath
import shutil
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from remote_sandbox.namespace import DEV_NAMESPACE
from remote_sandbox.remote_agent import AGENT_VERSION
from remote_sandbox.ssh import SshRunner

_ARCHIVE_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_ARCHIVE_SHEBANG = b"#!/usr/bin/env python3\n"
_ARCHIVE_MAIN = """from remote_agent.__main__ import main
import sys

raise SystemExit(main(sys.argv[1:]))
"""


@dataclass(frozen=True, slots=True)
class AgentInstall:
    version: str
    remote_path: str
    sha256: str


def _write_deterministic_zipapp(staging: Path, destination: Path) -> None:
    with destination.open("wb") as raw_archive:
        raw_archive.write(_ARCHIVE_SHEBANG)
        with zipfile.ZipFile(raw_archive, "w", compression=zipfile.ZIP_STORED) as archive:
            paths = sorted(
                staging.rglob("*"),
                key=lambda path: path.relative_to(staging).as_posix(),
            )
            for path in paths:
                is_directory = path.is_dir()
                archive_name = path.relative_to(staging).as_posix()
                if is_directory:
                    archive_name += "/"

                entry = zipfile.ZipInfo(archive_name, date_time=_ARCHIVE_DATE_TIME)
                entry.create_system = 3
                entry.compress_type = zipfile.ZIP_STORED
                permissions = 0o755 if is_directory else 0o644
                file_type = stat.S_IFDIR if is_directory else stat.S_IFREG
                entry.external_attr = ((file_type | permissions) << 16) | (
                    0x10 if is_directory else 0
                )
                archive.writestr(entry, b"" if is_directory else path.read_bytes())


def build_agent_zipapp(destination: Path) -> Path:
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    package_source = Path(__file__).with_name("remote_agent")

    with tempfile.TemporaryDirectory(prefix="codex-remote-agent-build-") as temporary:
        staging = Path(temporary) / "staging"
        package_destination = staging / "remote_agent"
        shutil.copytree(
            package_source,
            package_destination,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        (staging / "__main__.py").write_text(_ARCHIVE_MAIN, encoding="utf-8")

        temporary_archive = Path(temporary) / "agent.pyz"
        _write_deterministic_zipapp(staging, temporary_archive)
        temporary_archive.chmod(0o755)
        os.replace(temporary_archive, destination)
    return destination


class RemoteAgentManager:
    def __init__(self, runner: SshRunner) -> None:
        self._runner = runner

    def ensure(self, target: str) -> AgentInstall:
        with tempfile.TemporaryDirectory(prefix="codex-remote-agent-build-") as temporary:
            archive = build_agent_zipapp(Path(temporary) / "agent.pyz")
            content = archive.read_bytes()

        digest = hashlib.sha256(content).hexdigest()
        remote_path = posixpath.join(
            "~",
            DEV_NAMESPACE.home_dirname,
            "agents",
            AGENT_VERSION,
            "agent.pyz",
        )
        self._runner.write_bytes_atomic(target, remote_path, content)
        output = self._runner.run_python_file(target, remote_path, ("self-check",))
        expected = f"codex-remote-sandbox-agent {AGENT_VERSION} {digest}"
        if output.strip() != expected:
            raise RuntimeError(
                f"Remote agent self-check failed: expected {expected!r}, got {output.strip()!r}"
            )
        return AgentInstall(version=AGENT_VERSION, remote_path=remote_path, sha256=digest)
