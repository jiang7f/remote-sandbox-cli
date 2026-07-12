from __future__ import annotations

from pathlib import Path

import pytest

import remote_sandbox.settings as settings


@pytest.mark.parametrize(
    "value,expected",
    [("1", 1), ("2B", 2), ("3KB", 3_000), ("4 mb", 4_000_000), ("1GB", 10**9)],
)
def test_parse_and_format_sizes(value: str, expected: int) -> None:
    assert settings.parse_size(value) == expected
    assert settings.format_size(expected).endswith(("B", "KB", "MB", "GB", "TB"))


@pytest.mark.parametrize("value", ["", "0", "-1", "1.5MB", "bad"])
def test_parse_size_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        settings.parse_size(value)


def test_save_and_load_settings_atomically(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    settings.save_settings(settings.Settings(placeholder_limit=100_000_000), path)

    assert settings.load_settings(path) == settings.Settings(100_000_000)
    assert path.stat().st_mode & 0o077 == 0
    assert settings.format_size_compact(100_000_000) == "100MB"
    assert settings.format_size_compact(1500) == "1500B"


def test_set_placeholder_limit_uses_requested_path(tmp_path: Path) -> None:
    path = tmp_path / "settings.toml"
    result = settings.set_placeholder_limit("25MB", path)

    assert result.placeholder_limit == 25_000_000
    assert settings.load_settings(path) == result


def test_load_missing_and_integer_setting(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    assert settings.load_settings(path) == settings.Settings()
    path.write_text("placeholder_limit = 123\n", encoding="utf-8")
    assert settings.load_settings(path) == settings.Settings(123)


@pytest.mark.parametrize(
    "content",
    ["not toml =", "placeholder_limit = false\n", "placeholder_limit = \"0\"\n"],
)
def test_load_rejects_invalid_configuration(tmp_path: Path, content: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(settings.SettingsError, match="Invalid"):
        settings.load_settings(path)


def test_remote_sandbox_home_uses_formal_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(tmp_path / "state"))
    assert settings.remote_sandbox_home() == tmp_path / "state"
    assert settings.settings_path() == tmp_path / "state" / "config.toml"
