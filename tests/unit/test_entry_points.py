import tomllib
from pathlib import Path


def test_project_exposes_only_rsb_command() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["name"] == "remote-sandbox"
    assert project["scripts"] == {"rsb": "remote_sandbox.cli:main"}
