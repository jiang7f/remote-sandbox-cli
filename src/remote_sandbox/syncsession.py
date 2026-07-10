from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

from remote_sandbox._legacy_reconcile import build_plan
from remote_sandbox.agent import bootstrap_agent, remote_agent_path
from remote_sandbox.lock import workspace_lock
from remote_sandbox.marker import METADATA_DIR
from remote_sandbox.policy import POLICY_FILE_NAME, StaticPolicyEngine
from remote_sandbox.scan import scan_local_manifest, scan_remote_manifest
from remote_sandbox.settings import load_settings
from remote_sandbox.ssh import SshRunner
from remote_sandbox.state import StateStore
from remote_sandbox.sync import execute_plan


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

    def sync_once(self, *, already_locked: bool = False) -> None:
        with self._lock:
            if already_locked:
                self._sync_locked()
            else:
                with workspace_lock(self.local_root):
                    self._sync_locked()

    def _sync_locked(self) -> None:
        self.policy = self._load_policy()
        policy = self.policy
        if not self.runner.exists(self.target, remote_agent_path(self.remote)):
            bootstrap_agent(self.runner, self.target, self.remote)
        local_entries = scan_local_manifest(self.local_root, policy)
        remote_entries = scan_remote_manifest(self.runner, self.target, self.remote)
        state_path = self.local_root / METADATA_DIR / "state.sqlite3"
        with StateStore.open(state_path) as store:
            base_entries = store.list_base()
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
            )

    def _load_policy(self) -> StaticPolicyEngine:
        return StaticPolicyEngine.from_file(
            _policy_file_path(self.local_root),
            large_file_threshold=load_settings().placeholder_limit,
        )


def _policy_file_path(local_root: Path) -> Path:
    return local_root / POLICY_FILE_NAME
