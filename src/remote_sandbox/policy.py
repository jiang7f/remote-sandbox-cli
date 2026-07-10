from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from remote_sandbox.manifest import (
    EntryFingerprint,
    EntryKind,
    FileEntry,
    normalize_relative_path,
)


class ReplicaSide(StrEnum):
    LOCAL = "local"
    REMOTE = "remote"


class PolicyDecision(StrEnum):
    SYNC = "sync"
    PLACEHOLDER = "placeholder"
    IGNORE = "ignore"


POLICY_FILE_NAME = ".rsbignore"

# Environment, cache, OS, and editor files that do not travel well between hosts. These are soft
# defaults so an explicit `.rsbignore` sync rule can accept the portability cost.
_PORTABILITY_IGNORE_PATTERNS = (
    ".venv/**",
    "*/.venv/**",
    "venv/**",
    "*/venv/**",
    "__pycache__/**",
    "*/__pycache__/**",
    ".pytest_cache/**",
    "*/.pytest_cache/**",
    ".mypy_cache/**",
    "*/.mypy_cache/**",
    ".ruff_cache/**",
    "*/.ruff_cache/**",
    ".tox/**",
    "*/.tox/**",
    ".nox/**",
    "*/.nox/**",
    "node_modules/**",
    "*/node_modules/**",
    ".DS_Store",
    "*/.DS_Store",
    "._*",
    "*/._*",
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    "Thumbs.db",
    "*/Thumbs.db",
    "*.swp",
    "*.swo",
    "*~",
    ".#*",
)

_HARD_IGNORE_PATTERNS = (
    ".git/",
    "*/.git/",
    ".remote-sandbox/",
    "*/.remote-sandbox/",
    ".codex-remote-sandbox/",
    "*/.codex-remote-sandbox/",
)


class PolicyEngine(Protocol):
    def is_ignored(self, path: str) -> bool: ...

    def classify(
        self,
        entry: FileEntry | EntryFingerprint,
        *,
        side: ReplicaSide,
    ) -> PolicyDecision: ...


@dataclass(frozen=True, slots=True)
class PolicyRule:
    decision: PolicyDecision
    pattern: str
    explicit: bool = True


class StaticPolicyEngine:
    def __init__(
        self,
        *,
        ignore_patterns: tuple[str, ...] = (),
        placeholder_patterns: tuple[str, ...] = (),
        sync_patterns: tuple[str, ...] = (),
        large_file_threshold: int | None = None,
    ) -> None:
        self._hard_ignore_patterns = _HARD_IGNORE_PATTERNS
        rules: list[PolicyRule] = []
        rules.extend(
            PolicyRule(PolicyDecision.IGNORE, pattern, explicit=False)
            for pattern in _PORTABILITY_IGNORE_PATTERNS
        )
        rules.extend(PolicyRule(PolicyDecision.IGNORE, pattern) for pattern in ignore_patterns)
        rules.extend(
            PolicyRule(PolicyDecision.PLACEHOLDER, pattern) for pattern in placeholder_patterns
        )
        rules.extend(PolicyRule(PolicyDecision.SYNC, pattern) for pattern in sync_patterns)
        self._rules = tuple(rules)
        self._large_file_threshold = large_file_threshold

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        large_file_threshold: int | None = None,
    ) -> StaticPolicyEngine:
        if not path.exists():
            return cls(large_file_threshold=large_file_threshold)
        return cls.from_lines(
            path.read_text(encoding="utf-8").splitlines(),
            large_file_threshold=large_file_threshold,
        )

    @classmethod
    def from_lines(
        cls,
        lines: list[str],
        *,
        large_file_threshold: int | None = None,
    ) -> StaticPolicyEngine:
        engine = cls(large_file_threshold=large_file_threshold)
        rules = list(engine._rules)
        section = PolicyDecision.IGNORE
        for line_no, raw_line in enumerate(lines, start=1):
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("[") and stripped.endswith("]"):
                section = _parse_section(stripped, line_no)
                continue
            if _is_placeholder_size_directive(stripped):
                raise ValueError(
                    "placeholder-size moved to user settings; "
                    "run `rsb set placeholder-limit 10MB` instead"
                )
            decision, pattern = _parse_rule(stripped, section, line_no)
            rules.append(PolicyRule(decision, pattern))
        parsed = cls(large_file_threshold=large_file_threshold)
        parsed._rules = tuple(rules)
        return parsed

    def is_ignored(self, path: str) -> bool:
        normalized = normalize_relative_path(path)
        decision, _explicit = self._decision_for_path(normalized)
        return decision == PolicyDecision.IGNORE

    def classify(
        self,
        entry: FileEntry | EntryFingerprint,
        *,
        side: ReplicaSide,
    ) -> PolicyDecision:
        decision, explicit = self._decision_for_path(entry.path)
        if decision == PolicyDecision.IGNORE:
            return PolicyDecision.IGNORE
        if entry.kind == EntryKind.DIR and decision == PolicyDecision.PLACEHOLDER:
            return PolicyDecision.SYNC
        if explicit:
            return decision
        if (
            side == ReplicaSide.REMOTE
            and entry.kind == EntryKind.FILE
            and entry.size is not None
            and self._large_file_threshold is not None
            and entry.size > self._large_file_threshold
        ):
            return PolicyDecision.PLACEHOLDER
        return decision

    def _decision_for_path(self, path: str) -> tuple[PolicyDecision, bool]:
        if any(_matches(pattern, path) for pattern in self._hard_ignore_patterns):
            return PolicyDecision.IGNORE, True
        decision = PolicyDecision.SYNC
        explicit = False
        for rule in self._rules:
            if _matches(rule.pattern, path):
                decision = rule.decision
                explicit = rule.explicit
        return decision, explicit


def _matches(pattern: str, path: str) -> bool:
    normalized_pattern = pattern.strip().replace("\\", "/")
    normalized_path = normalize_relative_path(path)
    if normalized_pattern.endswith("/"):
        directory_pattern = normalized_pattern.rstrip("/")
        return fnmatch.fnmatchcase(normalized_path, directory_pattern) or fnmatch.fnmatchcase(
            normalized_path, directory_pattern + "/*"
        )
    if normalized_pattern.endswith("/**"):
        directory_pattern = normalized_pattern[:-3].rstrip("/")
        return fnmatch.fnmatchcase(normalized_path, directory_pattern) or fnmatch.fnmatchcase(
            normalized_path, directory_pattern + "/*"
        )
    return fnmatch.fnmatchcase(normalized_path, normalized_pattern)


def _parse_section(value: str, line_no: int) -> PolicyDecision:
    section = value[1:-1].strip()
    try:
        decision = PolicyDecision(section)
    except ValueError as exc:
        raise ValueError(f"Invalid policy section on line {line_no}: {value}") from exc
    return decision


def _parse_rule(value: str, section: PolicyDecision, line_no: int) -> tuple[PolicyDecision, str]:
    parts = value.split(maxsplit=1)
    if len(parts) == 2:
        try:
            decision = PolicyDecision(parts[0])
        except ValueError:
            return section, value
        return decision, parts[1]
    if len(parts) != 1:
        raise ValueError(f"Invalid policy line {line_no}: {value}")
    return section, parts[0]


def _is_placeholder_size_directive(value: str) -> bool:
    key, sep, raw_size = value.partition(":")
    del raw_size
    return key.strip() == "placeholder-size" and sep == ":"
