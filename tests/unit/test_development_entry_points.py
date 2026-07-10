import tomllib
from pathlib import Path


def test_project_exposes_only_development_commands() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["name"] == "codex-remote-sandbox"
    assert project["scripts"] == {
        "codex-remote-sandbox": "remote_sandbox.cli:main",
        "codex-rsb": "remote_sandbox.cli:main",
    }
    assert "rsb" not in project["scripts"]
