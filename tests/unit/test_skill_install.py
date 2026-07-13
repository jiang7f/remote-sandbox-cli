from __future__ import annotations

import os
from pathlib import Path

import pytest

import remote_sandbox.cli as cli
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


def test_bundled_skill_uses_explicit_conversation_mode_and_announces_paths() -> None:
    skill = (
        Path(skill_install.__file__).with_name("bundled_skill") / "SKILL.md"
    ).read_text(encoding="utf-8")

    assert "Activate only when the user explicitly mentions" in skill
    assert "possible binding name such as `sfs` alone" in skill
    assert "keep rsb enabled for the rest of the conversation" in skill
    assert "Disable rsb only when the user explicitly says" in skill
    assert "rsb status <name> --paths" in skill
    assert "rsb status --paths" in skill
    assert "Do not ask the user to confirm an existing binding" in skill
    assert "Do not run status or repeat the path announcement on later turns" in skill
    assert "Default to `rsb run`" in skill
    assert "Do not use `rsb status --watch`" in skill


def test_bundled_skill_uses_localized_multiline_binding_notice() -> None:
    skill = (
        Path(skill_install.__file__).with_name("bundled_skill") / "SKILL.md"
    ).read_text(encoding="utf-8")

    assert "Match the user's current language" in skill
    assert "local and remote paths" in skill
    assert "separate lines" in skill
    assert "Local path:" in skill
    assert "Remote path:" in skill
    assert "本地目录：" in skill
    assert "远程目录：" in skill
    assert "Using rsb binding" not in skill


def test_bundled_skill_requires_read_only_runtime_discovery_before_mutation() -> None:
    skill = (
        Path(skill_install.__file__).with_name("bundled_skill") / "SKILL.md"
    ).read_text(encoding="utf-8")

    assert "A missing executable in the clean non-interactive environment" in skill
    assert "does not prove" in skill
    assert "machine lacks that runtime" in skill
    assert "rsb env show <name>" in skill
    assert "rsb env refresh <name>" in skill
    assert "Do not create an environment or install dependencies" in skill
    assert "explicit user approval" in skill


def test_install_codex_skill_is_idempotent(tmp_path: Path) -> None:
    first = skill_install.install_codex_skill(codex_home=tmp_path)
    second = skill_install.install_codex_skill(codex_home=tmp_path)

    assert first.changed is True
    assert first.reinstalled is False
    assert second.changed is False
    assert second.reinstalled is False
    assert first.path == tmp_path / "skills" / "remote-sandbox"
    assert (first.path / "SKILL.md").read_text(encoding="utf-8").startswith("---\n")
    assert (first.path / "agents" / "openai.yaml").is_file()


def test_install_codex_skill_force_reinstalls_identical_copy(tmp_path: Path) -> None:
    installed = skill_install.install_codex_skill(codex_home=tmp_path)
    skill_path = installed.path / "SKILL.md"
    old_mtime_ns = 1_000_000_000
    os.utime(skill_path, ns=(old_mtime_ns, old_mtime_ns))

    result = skill_install.install_codex_skill(codex_home=tmp_path, force=True)

    assert result.changed is True
    assert result.reinstalled is True
    assert skill_path.stat().st_mtime_ns > old_mtime_ns


def test_cli_force_install_reports_reinstalled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    assert cli.main(["skill", "install"]) == 0
    capsys.readouterr()

    assert cli.main(["skill", "install", "--force"]) == 0

    output = capsys.readouterr().out
    assert output == (
        f"Reinstalled remote-sandbox skill at {tmp_path / 'skills' / 'remote-sandbox'}\n"
    )


def test_cli_repeated_install_reports_up_to_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    assert cli.main(["skill", "install"]) == 0
    capsys.readouterr()

    assert cli.main(["skill", "install"]) == 0

    output = capsys.readouterr().out
    assert output == (
        f"Already up to date remote-sandbox skill at {tmp_path / 'skills' / 'remote-sandbox'}\n"
    )


def test_install_codex_skill_requires_force_for_different_copy(tmp_path: Path) -> None:
    target = tmp_path / "skills" / "remote-sandbox"
    target.mkdir(parents=True)
    (target / "SKILL.md").write_text("custom\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--force"):
        skill_install.install_codex_skill(codex_home=tmp_path)

    result = skill_install.install_codex_skill(codex_home=tmp_path, force=True)

    assert result.changed is True
    assert result.reinstalled is True
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
