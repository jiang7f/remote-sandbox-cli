from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from remote_sandbox.agent import bootstrap_agent, remote_agent_path
from remote_sandbox.lock import workspace_lock
from remote_sandbox.marker import STATE_FILE, local_meta_dir
from remote_sandbox.policy import POLICY_FILE_NAME, StaticPolicyEngine
from remote_sandbox.reconcile import build_plan
from remote_sandbox.scan import scan_local_manifest, scan_remote_manifest
from remote_sandbox.settings import load_settings
from remote_sandbox.ssh import SshRunner
from remote_sandbox.state import StateStore
from remote_sandbox.sync import execute_plan


@dataclass(frozen=True, slots=True)
class SyncProgress:
    """A point-in-time snapshot of an in-flight sync, for status/progress display.

    ``phase`` is a coarse label (``scanning-remote``, ``scanning-local``, ``planning``,
    ``transferring``, ``done``). File/byte totals are ``0`` until the plan is known; the
    transfer phase fills them in and increments ``*_done`` as actions complete.
    """

    phase: str
    files_total: int = 0
    files_done: int = 0
    bytes_total: int = 0
    bytes_done: int = 0
    current_path: str | None = None


ProgressCallback = Callable[[SyncProgress], None]


def _noop_progress(_progress: SyncProgress) -> None:
    return None


@dataclass
class SyncSession:
    """One workspace's sync unit: scan local + remote, plan, execute.

    Reusable by both the foreground `bind` flow and the background daemon.
    `sync_once(already_locked=True)` skips acquiring `workspace_lock` for callers
    (the daemon) that already hold it for their whole lifetime; otherwise it takes
    the cross-process file lock for the duration of a single sync.
    """

    local_root: Path
    runner: SshRunner
    target: str
    remote: str
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.policy = self._load_policy()

    def current_policy(self) -> StaticPolicyEngine:
        return self.policy

    def sync_once(
        self,
        *,
        already_locked: bool = False,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        report = on_progress or _noop_progress
        with self._lock:
            if already_locked:
                self._sync_locked(report)
            else:
                with workspace_lock(self.local_root):
                    self._sync_locked(report)

    def _sync_locked(self, report: ProgressCallback) -> None:
        self.policy = self._load_policy()
        policy = self.policy
        if not self.runner.exists(self.target, remote_agent_path(self.remote)):
            bootstrap_agent(self.runner, self.target, self.remote)
        state_path = local_meta_dir(self.local_root) / STATE_FILE
        with StateStore.open(state_path) as store:
            # Load the local hash cache up front so the local scan can skip re-hashing files
            # whose (size, mtime) are unchanged — the main reason a no-op sync was slow.
            hash_cache = store.load_hash_cache()
            report(SyncProgress(phase="scanning-remote"))
            remote_entries = scan_remote_manifest(self.runner, self.target, self.remote)
            report(SyncProgress(phase="scanning-local"))
            local_entries = scan_local_manifest(self.local_root, policy, hash_cache=hash_cache)
            store.save_hash_cache(hash_cache)
            base_entries = store.list_base()
            report(SyncProgress(phase="planning"))
            plan = build_plan(
                base_entries=base_entries,
                local_entries=local_entries,
                remote_entries=remote_entries,
                policy_engine=policy,
            )
            execute_plan(
                plan,
                local_root=self.local_root,
                runner=self.runner,
                target=self.target,
                remote_root=self.remote,
                state=store,
                on_progress=report,
            )
        report(SyncProgress(phase="done"))

    def _load_policy(self) -> StaticPolicyEngine:
        settings = load_settings()
        return StaticPolicyEngine.from_file(
            _policy_file_path(self.local_root),
            large_file_threshold=settings.placeholder_limit,
            default_ignore_patterns=settings.default_ignores,
        )


def _policy_file_path(local_root: Path) -> Path:
    return local_root / POLICY_FILE_NAME
