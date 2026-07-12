from __future__ import annotations

_HARD_IGNORED_NAMES = {".git", ".remote-sandbox"}
_INTERNAL_PREFIXES = (
    ".remote-sandbox-new-",
    ".remote-sandbox-old-",
    ".remote-sandbox-delete-",
    ".remote-sandbox-recovered-",
)


def name_is_hard_ignored(name: str) -> bool:
    return name in _HARD_IGNORED_NAMES or name.startswith(_INTERNAL_PREFIXES)


def path_parts_are_hard_ignored(parts: tuple[str, ...]) -> bool:
    return any(name_is_hard_ignored(part) for part in parts)
