from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SKILL_NAME = "remote-sandbox"


@dataclass(frozen=True, slots=True)
class SkillInstallResult:
    path: Path
    changed: bool


@dataclass(frozen=True, slots=True)
class SkillUninstallResult:
    path: Path
    changed: bool
    retained_extra_files: bool


def default_codex_home() -> Path:
    configured = os.environ.get("CODEX_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".codex"


def install_codex_skill(
    *,
    codex_home: Path | None = None,
    force: bool = False,
) -> SkillInstallResult:
    source = Path(__file__).with_name("bundled_skill")
    target = (codex_home or default_codex_home()) / "skills" / SKILL_NAME
    files = {
        Path("SKILL.md"): (source / "SKILL.md").read_bytes(),
        Path("agents/openai.yaml"): (source / "agents" / "openai.yaml").read_bytes(),
    }

    changed = any(not (target / relative).exists() or (target / relative).read_bytes() != content
                  for relative, content in files.items())
    if target.exists() and changed and not force:
        raise FileExistsError(
            f"{target} already exists with different content; rerun with --force to update it"
        )
    if not changed:
        return SkillInstallResult(target, False)

    for relative, content in files.items():
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.tmp")
        temporary.write_bytes(content)
        temporary.replace(destination)
    return SkillInstallResult(target, True)


def uninstall_codex_skill(
    *,
    codex_home: Path | None = None,
    force: bool = False,
) -> SkillUninstallResult:
    source = Path(__file__).with_name("bundled_skill")
    target = (codex_home or default_codex_home()) / "skills" / SKILL_NAME
    files = {
        Path("SKILL.md"): (source / "SKILL.md").read_bytes(),
        Path("agents/openai.yaml"): (source / "agents" / "openai.yaml").read_bytes(),
    }
    existing = [relative for relative in files if (target / relative).exists()]
    if not existing:
        return SkillUninstallResult(target, False, target.exists())

    modified = [
        relative
        for relative in existing
        if (target / relative).read_bytes() != files[relative]
    ]
    if modified and not force:
        names = ", ".join(str(path) for path in modified)
        raise FileExistsError(
            f"installed skill has modified files ({names}); rerun with --force to remove them"
        )

    for relative in existing:
        (target / relative).unlink()
    agents = target / "agents"
    if agents.exists() and not any(agents.iterdir()):
        agents.rmdir()
    if target.exists() and not any(target.iterdir()):
        target.rmdir()
    return SkillUninstallResult(target, True, target.exists())
