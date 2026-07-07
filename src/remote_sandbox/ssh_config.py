from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SshHost:
    alias: str
    hostname: str | None
    user: str | None
    identity_files: tuple[str, ...]


_DEFAULT_IDENTITY_FILES = {
    "~/.ssh/id_rsa",
    "~/.ssh/id_ecdsa",
    "~/.ssh/id_ecdsa_sk",
    "~/.ssh/id_ed25519",
    "~/.ssh/id_ed25519_sk",
    "~/.ssh/id_xmss",
    "~/.ssh/id_dsa",
}


def default_ssh_config_path() -> Path:
    return Path.home() / ".ssh" / "config"


def load_configured_hosts(
    *,
    config_path: Path | None = None,
    require_identity: bool = True,
    resolver: Callable[[str], SshHost] | None = None,
) -> list[SshHost]:
    path = config_path or default_ssh_config_path()
    if not path.exists():
        return []
    parsed_hosts = parse_ssh_config_text(_read_ssh_config_tree(path))
    explicit_identity_aliases = {host.alias for host in parsed_hosts if host.identity_files}
    host_resolver = resolver or resolve_ssh_host
    hosts = [_resolve_with_fallback(host, host_resolver) for host in parsed_hosts]
    if require_identity:
        hosts = [host for host in hosts if host.alias in explicit_identity_aliases]
    return hosts


def _read_ssh_config_tree(path: Path, seen: set[Path] | None = None) -> str:
    resolved = path.expanduser().resolve()
    visited = seen or set()
    if resolved in visited:
        return ""
    visited.add(resolved)
    try:
        content = resolved.read_text(encoding="utf-8")
    except OSError:
        return ""
    chunks: list[str] = []
    for raw_line in content.splitlines():
        line = _strip_comment(raw_line).strip()
        key, value = _split_ssh_config_line(line)
        if key.lower() != "include" or not value.strip():
            chunks.append(raw_line)
            continue
        for pattern in value.split():
            chunks.append(_read_include_pattern(pattern, base_dir=resolved.parent, seen=visited))
    return "\n".join(chunks)


def _read_include_pattern(pattern: str, *, base_dir: Path, seen: set[Path]) -> str:
    expanded = Path(pattern).expanduser()
    if not expanded.is_absolute():
        expanded = base_dir / expanded
    matches = sorted(expanded.parent.glob(expanded.name))
    if not matches and expanded.exists():
        matches = [expanded]
    return "\n".join(_read_ssh_config_tree(path, seen) for path in matches if path.is_file())


def resolve_ssh_host(alias: str) -> SshHost:
    result = subprocess.run(
        ["ssh", "-G", alias],
        check=False,
        text=True,
        capture_output=True,
        timeout=5.0,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"ssh -G failed for {alias}")
    options: dict[str, list[str]] = {}
    for raw_line in result.stdout.splitlines():
        key, _, value = raw_line.partition(" ")
        key = key.lower()
        value = value.strip()
        if key:
            options.setdefault(key, []).append(value)
    return SshHost(
        alias=alias,
        hostname=_last_option(options, "hostname"),
        user=_last_option(options, "user"),
        identity_files=tuple(options.get("identityfile", [])),
    )


def parse_ssh_config_text(content: str) -> list[SshHost]:
    hosts: list[SshHost] = []
    seen_aliases: set[str] = set()
    current_aliases: list[str] = []
    current_options: dict[str, list[str]] = {}

    def flush() -> None:
        nonlocal current_aliases, current_options
        if not current_aliases:
            return
        for alias in current_aliases:
            if _is_pattern_host(alias):
                continue
            if alias in seen_aliases:
                continue
            seen_aliases.add(alias)
            hosts.append(
                SshHost(
                    alias=alias,
                    hostname=_last_option(current_options, "hostname"),
                    user=_last_option(current_options, "user"),
                    identity_files=tuple(current_options.get("identityfile", [])),
                )
            )
        current_aliases = []
        current_options = {}

    for raw_line in content.splitlines():
        line = _strip_comment(raw_line).strip()
        if not line:
            continue
        key, value = _split_ssh_config_line(line)
        key = key.lower()
        if key == "host":
            flush()
            current_aliases = [part for part in value.split() if part and not part.startswith("!")]
            current_options = {}
            continue
        if not current_aliases:
            continue
        current_options.setdefault(key, []).append(value)
    flush()
    return hosts


def display_identity_files(identity_files: tuple[str, ...]) -> str:
    if not identity_files:
        return "-"
    display: list[str] = []
    saw_default = False
    for identity_file in identity_files:
        normalized = identity_file.replace(str(Path.home()), "~", 1)
        if normalized in _DEFAULT_IDENTITY_FILES:
            saw_default = True
        else:
            display.append(normalized)
    if saw_default:
        display.insert(0, "default")
    return ",".join(display)


def _resolve_with_fallback(
    host: SshHost,
    resolver: Callable[[str], SshHost],
) -> SshHost:
    try:
        resolved = resolver(host.alias)
    except Exception:
        return host
    return SshHost(
        alias=host.alias,
        hostname=resolved.hostname or host.hostname,
        user=resolved.user or host.user,
        identity_files=resolved.identity_files or host.identity_files,
    )


def _strip_comment(line: str) -> str:
    in_quote = False
    for index, char in enumerate(line):
        if char == '"':
            in_quote = not in_quote
        elif char == "#" and not in_quote:
            return line[:index]
    return line


def _split_ssh_config_line(line: str) -> tuple[str, str]:
    space_key, space_sep, space_value = line.partition(" ")
    equal_key, equal_sep, equal_value = line.partition("=")
    if equal_sep and (not space_sep or len(equal_key) < len(space_key)):
        return equal_key.strip(), equal_value.strip()
    return space_key.strip(), space_value.strip()


def _is_pattern_host(alias: str) -> bool:
    return alias == "*" or any(char in alias for char in "*?[]")


def _last_option(options: dict[str, list[str]], key: str) -> str | None:
    values = options.get(key)
    if not values:
        return None
    return values[-1]
