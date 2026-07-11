from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import remote_sandbox.ssh_config as ssh_config
from remote_sandbox.ssh_config import SshHost


def test_load_configured_hosts_reads_includes_and_requires_explicit_identity(
    tmp_path: Path,
) -> None:
    included = tmp_path / "hosts.conf"
    included.write_text(
        "Host gpu\n  HostName gpu.example\n  IdentityFile ~/.ssh/gpu-key\n",
        encoding="utf-8",
    )
    config = tmp_path / "config"
    config.write_text(
        "Include hosts.conf\n"
        "Host no-key\n  HostName no-key.example\n"
        "Host *.pattern\n  IdentityFile ~/.ssh/pattern\n",
        encoding="utf-8",
    )

    hosts = ssh_config.load_configured_hosts(
        config_path=config,
        resolver=lambda alias: SshHost(alias, None, "resolved", ()),
    )

    assert hosts == [SshHost("gpu", "gpu.example", "resolved", ("~/.ssh/gpu-key",))]


def test_config_include_cycle_and_missing_include_are_safe(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.write_text("Include second missing-*\nHost one\n", encoding="utf-8")
    second.write_text("Include first\nHost two\n", encoding="utf-8")

    hosts = ssh_config.load_configured_hosts(
        config_path=first,
        require_identity=False,
        resolver=lambda alias: SshHost(alias, alias + ".example", None, ()),
    )

    assert [host.alias for host in hosts] == ["two", "one"]


def test_parse_config_supports_equals_quotes_duplicates_and_negation() -> None:
    hosts = ssh_config.parse_ssh_config_text(
        "# comment\n"
        "Host=alpha !blocked beta\n"
        "  HostName alpha.example # trailing\n"
        "  User \"name#kept\"\n"
        "  IdentityFile ~/.ssh/a\n"
        "Host alpha\n  HostName ignored.example\n"
        "Host * wildcard? [group]\n"
    )

    assert hosts == [
        SshHost("alpha", "alpha.example", '"name#kept"', ("~/.ssh/a",)),
        SshHost("beta", "alpha.example", '"name#kept"', ("~/.ssh/a",)),
    ]


def test_resolve_ssh_host_parses_last_values(monkeypatch: pytest.MonkeyPatch) -> None:
    result = subprocess.CompletedProcess(
        ["ssh"],
        0,
        "hostname old\nhostname final\nuser test\nidentityfile one\nidentityfile two\n",
        "",
    )
    monkeypatch.setattr(ssh_config.subprocess, "run", lambda *args, **kwargs: result)

    assert ssh_config.resolve_ssh_host("gpu") == SshHost(
        "gpu",
        "final",
        "test",
        ("one", "two"),
    )


def test_resolve_ssh_host_reports_ssh_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    result = subprocess.CompletedProcess(["ssh"], 255, "", "bad config")
    monkeypatch.setattr(ssh_config.subprocess, "run", lambda *args, **kwargs: result)

    with pytest.raises(RuntimeError, match="bad config"):
        ssh_config.resolve_ssh_host("bad")


def test_load_hosts_uses_parsed_values_when_resolver_fails(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.write_text(
        "Host gpu\n  HostName parsed.example\n  IdentityFile ~/.ssh/key\n",
        encoding="utf-8",
    )

    def fail(_alias: str) -> SshHost:
        raise OSError("unavailable")

    assert ssh_config.load_configured_hosts(config_path=config, resolver=fail) == [
        SshHost("gpu", "parsed.example", None, ("~/.ssh/key",))
    ]


def test_identity_display_collapses_defaults_and_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "home", lambda: Path("/home/test"))

    assert ssh_config.display_identity_files(()) == "-"
    assert ssh_config.display_identity_files(
        ("/home/test/.ssh/id_ed25519", "/home/test/.ssh/custom")
    ) == "default,~/.ssh/custom"


def test_missing_or_unreadable_config_returns_no_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing"
    assert ssh_config.load_configured_hosts(config_path=missing) == []
    config = tmp_path / "config"
    config.write_text("Host gpu\n", encoding="utf-8")
    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: (_ for _ in ()).throw(OSError()))
    assert ssh_config.load_configured_hosts(config_path=config) == []
