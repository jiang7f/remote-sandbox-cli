from __future__ import annotations

import os
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from remote_sandbox import namespace

DEFAULT_PLACEHOLDER_LIMIT = 10 * 1000 * 1000
CONFIG_FILE_NAME = "config.toml"


class SettingsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Settings:
    placeholder_limit: int = DEFAULT_PLACEHOLDER_LIMIT


def remote_sandbox_home() -> Path:
    return namespace.tool_home()


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
    content = f'placeholder_limit = "{format_size_compact(settings.placeholder_limit)}"\n'
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
    settings = Settings(placeholder_limit=limit)
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
    return Settings(placeholder_limit=placeholder_limit)


def _parse_positive_int(value: str) -> int:
    if not value.isdecimal():
        raise ValueError("not an integer")
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("must be positive")
    return parsed
