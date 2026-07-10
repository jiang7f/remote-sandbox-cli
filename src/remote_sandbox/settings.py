from __future__ import annotations

import os
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_PLACEHOLDER_LIMIT = 10 * 1000 * 1000
CONFIG_FILE_NAME = "config.toml"

# Directories/files that are almost never worth syncing. Applied as SOFT defaults (a project
# `.rsbignore` can re-enable any of them), so binding a tree that contains a venv or caches
# just works without `rsb init`. Stored in config.toml so the user can edit the list.
#
# `.git/` is ignored by default on purpose: a git repo is a transactional database, and
# syncing its index/refs/pack files file-by-file with lag risks a corrupt/inconsistent repo
# (git's own answer for moving work between machines is push/pull). The model here is "commit
# locally, run remotely", so history stays on the local side. To use git ON the remote
# (e.g. `git describe` in a build), re-enable it in `.rsbignore` with `[sync]` + `.git/`.
DEFAULT_IGNORES: tuple[str, ...] = (
    ".git/",
    ".venv/",
    "venv/",
    "env/",
    "node_modules/",
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".ipynb_checkpoints/",
)


class SettingsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Settings:
    placeholder_limit: int = DEFAULT_PLACEHOLDER_LIMIT
    default_ignores: tuple[str, ...] = DEFAULT_IGNORES


def remote_sandbox_home() -> Path:
    override = os.environ.get("REMOTE_SANDBOX_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".remote-sandbox"


def settings_path() -> Path:
    return remote_sandbox_home() / CONFIG_FILE_NAME


def load_settings(path: Path | None = None) -> Settings:
    config_path = path or settings_path()
    if not config_path.exists():
        return Settings()
    try:
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise SettingsError(f"Invalid settings file {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SettingsError(f"Invalid settings file {config_path}")
    return _settings_from_dict(data, config_path)


def save_settings(settings: Settings, path: Path | None = None) -> None:
    config_path = path or settings_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f'placeholder_limit = "{format_size_compact(settings.placeholder_limit)}"']
    # Write the ignore list out (even when unchanged) so the user can see and edit it.
    ignores = ", ".join(f'"{_toml_escape(pattern)}"' for pattern in settings.default_ignores)
    lines.append(f"default_ignores = [{ignores}]")
    content = "\n".join(lines) + "\n"
    fd, tmp_name = tempfile.mkstemp(
        prefix="config.",
        suffix=".tmp",
        dir=config_path.parent,
        text=True,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, config_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def set_placeholder_limit(value: str, path: Path | None = None) -> Settings:
    limit = parse_size(value)
    # Preserve any customized default_ignores rather than resetting them.
    current = load_settings(path)
    settings = Settings(placeholder_limit=limit, default_ignores=current.default_ignores)
    save_settings(settings, path)
    return settings


def parse_size(value: str) -> int:
    upper = value.strip().upper()
    if not upper:
        raise ValueError("empty size")
    units = (
        ("TB", 1000**4),
        ("GB", 1000**3),
        ("MB", 1000**2),
        ("KB", 1000),
        ("B", 1),
    )
    for suffix, multiplier in units:
        if upper.endswith(suffix):
            number = upper[: -len(suffix)].strip()
            return _parse_positive_int(number) * multiplier
    return _parse_positive_int(upper)


def format_size_compact(size: int) -> str:
    for suffix, multiplier in (
        ("TB", 1000**4),
        ("GB", 1000**3),
        ("MB", 1000**2),
        ("KB", 1000),
    ):
        if size >= multiplier and size % multiplier == 0:
            return f"{size // multiplier}{suffix}"
    return f"{size}B"


def format_size(size: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1000 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1000
    return f"{size} B"


def _settings_from_dict(data: dict[str, Any], path: Path) -> Settings:
    raw_limit = data.get("placeholder_limit", DEFAULT_PLACEHOLDER_LIMIT)
    try:
        if isinstance(raw_limit, int):
            placeholder_limit = raw_limit
        elif isinstance(raw_limit, str):
            placeholder_limit = parse_size(raw_limit)
        else:
            raise ValueError
    except ValueError as exc:
        raise SettingsError(
            f"Invalid placeholder_limit in {path}: use a value like 10MB or 1GB"
        ) from exc
    return Settings(
        placeholder_limit=placeholder_limit,
        default_ignores=_default_ignores_from_dict(data, path),
    )


def _default_ignores_from_dict(data: dict[str, Any], path: Path) -> tuple[str, ...]:
    if "default_ignores" not in data:
        return DEFAULT_IGNORES
    raw = data["default_ignores"]
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise SettingsError(
            f"Invalid default_ignores in {path}: expected a list of strings"
        )
    return tuple(raw)


def _toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _parse_positive_int(value: str) -> int:
    if not value.isdecimal():
        raise ValueError("not an integer")
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("must be positive")
    return parsed
