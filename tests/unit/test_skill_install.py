from __future__ import annotations

from pathlib import Path

import pytest

import remote_sandbox.skill_install as skill_install


def test_repository_skill_matches_bundled_skill() -> None:
    repository_root = Path(__file__).parents[2]
    repository_skill = repository_root / "skills" / "remote-sandbox"
    bundled_skill = Path(skill_install.__file__).with_name("bundled_skill")

    assert (repository_skill / "SKILL.md").read_bytes() == (
        bundled_skill / "SKILL.md"
    ).read_bytes()
    assert (repository_skill / "agents" / "openai.yaml").read_bytes() == (
        bundled_skill / "agents" / "openai.yaml"
    ).read_bytes()


def test_bundled_skill_keeps_ai_on_the_fast_path() -> None:
    skill = (
        Path(skill_install.__file__).with_name("bundled_skill") / "SKILL.md"
    ).read_text(encoding="utf-8")

    assert "Default to `rsb run`" in skill
    assert "without a preliminary status call" in skill
    assert "Never use `rsb status --watch`" in skill
    assert "Do not ask the user to restate rsb mechanics" in skill


def test_install_codex_skill_is_idempotent(tmp_path: Path) -> None:
    first = skill_install.install_codex_skill(codex_home=tmp_path)
    second = skill_install.install_codex_skill(codex_home=tmp_path)

    assert first.changed is True
    assert second.changed is False
    assert first.path == tmp_path / "skills" / "remote-sandbox"
    assert (first.path / "SKILL.md").read_text(encoding="utf-8").startswith("---\n")
    assert (first.path / "agents" / "openai.yaml").is_file()


def test_install_codex_skill_requires_force_for_different_copy(tmp_path: Path) -> None:
    target = tmp_path / "skills" / "remote-sandbox"
    target.mkdir(parents=True)
    (target / "SKILL.md").write_text("custom\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--force"):
        skill_install.install_codex_skill(codex_home=tmp_path)

    result = skill_install.install_codex_skill(codex_home=tmp_path, force=True)

    assert result.changed is True
    assert "name: remote-sandbox" in (target / "SKILL.md").read_text(encoding="utf-8")


def test_uninstall_codex_skill_is_idempotent(tmp_path: Path) -> None:
    installed = skill_install.install_codex_skill(codex_home=tmp_path)

    first = skill_install.uninstall_codex_skill(codex_home=tmp_path)
    second = skill_install.uninstall_codex_skill(codex_home=tmp_path)

    assert first.changed is True
    assert first.retained_extra_files is False
    assert second.changed is False
    assert installed.path.exists() is False


def test_uninstall_preserves_modified_and_extra_files(tmp_path: Path) -> None:
    installed = skill_install.install_codex_skill(codex_home=tmp_path)
    skill_file = installed.path / "SKILL.md"
    skill_file.write_text("custom\n", encoding="utf-8")
    extra = installed.path / "notes.txt"
    extra.write_text("keep\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--force"):
        skill_install.uninstall_codex_skill(codex_home=tmp_path)

    result = skill_install.uninstall_codex_skill(codex_home=tmp_path, force=True)

    assert result.changed is True
    assert result.retained_extra_files is True
    assert skill_file.exists() is False
    assert extra.read_text(encoding="utf-8") == "keep\n"


def test_default_codex_home_honors_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))

    assert skill_install.default_codex_home() == tmp_path
