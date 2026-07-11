# Task 13 Implementation Report

## Implementation and architecture summary

- Replaced the production `Daemon` and its startup `SyncSession` pass with `WorkspaceSupervisor`.
- Added `SupervisorClient` and JSON control requests for `status`, `sync`, `resume`, and `stop`. The existing `poke_daemon` API maps to `sync` so protected CLI callers continue to work.
- Moved pid, lock, log, state, and control socket identity outside synchronized trees. Durable files use the workspace metadata directory keyed by `workspace_id`. The socket uses the codex runtime namespace keyed by the same id.
- Published durable `STARTING`, pid, and the control socket before component construction, agent installation, local scanning, hashing, or transfer.
- Made `RemoteWorkspaceClient` agent installation lazy so constructing production components cannot make an early remote call.
- Made `WorkspaceSupervisor` the sole owner of initial sync, restart audit, journal replay, incremental engine cycles, local watcher lifecycle, remote subscription lifecycle, retry state, and shutdown cleanup.
- Added schema version 6 with an additive `initial_sync_completed` workspace state. Completion survives reopen and is written only after successful initial sync.
- Restart starts watchers, appends a durable audit request, replays unacknowledged journals through `SyncEngine`, and relies on the engine transaction to acknowledge remote events only after commit.
- Migrated the protected daemon authentication intent. Every connection failure clears the stale SSH master and probes without prompting. Password authentication remains `DISCONNECTED` until foreground resume. Network and key-capable reconnects use exponential delays starting at 2 seconds and capped at 30 seconds. Reachable watcher failure becomes `DEGRADED`, requests audit, and restarts the remote watcher.
- Added truthful status synthesis. A live pid without a control socket is `STARTING` or `DEGRADED`. A dead pid or stale non-stopped durable state is `FAILED`.
- Configured `RotatingFileHandler` with `maxBytes=5 * 1024 * 1024`, `backupCount=3`, UTF-8 encoding, and user-only permissions. The detached child sets umask `077` so rotated files remain private.
- Removed the Task 5 startup compatibility snapshot from `watch.py`. `LocalChangeDetector`, the temporary `LocalWatcher` alias, and the legacy detector/on-change overload had no remaining consumers after daemon migration.

## TDD RED and GREEN evidence

### Lifecycle

RED command:

```text
uv run pytest tests/unit/test_daemon_lifecycle.py -v
```

Relevant RED output:

```text
ImportError: cannot import name 'SupervisorClient' from 'remote_sandbox.daemon'
```

After the interface was introduced, a diagnostic run exposed a false-positive startup wait caused by the live-pid fallback and a macOS AF_UNIX path longer than 100 bytes. `wait_until_running` was corrected to require a real control response and test sockets use a short `/tmp` path.

GREEN output:

```text
tests/unit/test_daemon_lifecycle.py::test_supervisor_publishes_starting_before_initial_sync PASSED
1 passed in 0.58s
```

### Reconnect

RED command:

```text
uv run pytest tests/unit/test_daemon_reconnect.py -v
```

Relevant RED output:

```text
AttributeError: 'WorkspaceSupervisor' object has no attribute 'handle_subscription_failure'
2 failed in 0.16s
```

GREEN coverage confirmed password disconnect, network retry delay `2.0`, and stale master cleanup.

### Failure modes

RED command:

```text
uv run pytest tests/unit/test_daemon_failure_modes.py -v
```

Relevant RED output:

```text
AssertionError: DISCONNECTED is not DEGRADED
AttributeError: 'DaemonStatus' object has no attribute 'phase'
2 failed in 0.03s
```

GREEN coverage confirmed reachable watcher crashes request audit and live pid without socket is never stopped.

### Restart

RED command:

```text
uv run pytest tests/integration/test_daemon_restart.py -v
```

Relevant RED output:

```text
AssertionError: supervisor did not become ready
1 failed in 6.07s
```

GREEN command and output:

```text
uv run pytest tests/integration/test_daemon_restart.py \
  tests/unit/test_daemon_lifecycle.py tests/unit/test_daemon_reconnect.py \
  tests/unit/test_daemon_failure_modes.py -v
6 passed in 1.61s
```

The restart test verifies the remote file is present locally, remote acknowledged sequence is `1`, and initial sync run count remains `0`.

### Durable initial sync completion

RED command:

```text
uv run pytest tests/unit/test_status_store.py::test_initial_sync_completion_survives_reopen -v
```

Relevant RED output:

```text
AttributeError: 'WorkspaceStore' object has no attribute 'initial_sync_completed'
1 failed in 0.04s
```

GREEN output:

```text
1 passed in 0.02s
```

## Full verification

```text
uv run pytest
414 passed, 1 skipped in 12.04s

uv run ruff check src tests
All checks passed!

uv run mypy src
Success: no issues found in 49 source files

uv run python -m compileall -q src tests
exit 0

git diff --check
exit 0

uv run python -c '<parse remote_agent files with feature_version=(3,10)>'
Python 3.10 grammar OK: 5 remote-agent files
```

After removing the Task 5 compatibility snapshot, the relevant watcher and supervisor regression set was rerun:

```text
43 passed in 2.84s
```

Ruff, mypy, compileall, and `git diff --check` were rerun after that cleanup and remained clean.

## Files changed

- `src/remote_sandbox/daemon.py`
- `src/remote_sandbox/remote_client.py`
- `src/remote_sandbox/state.py`
- `src/remote_sandbox/watch.py`
- `tests/helpers/sync_harness.py`
- `tests/unit/conftest.py`
- `tests/integration/conftest.py`
- `tests/unit/test_daemon_lifecycle.py`
- `tests/unit/test_daemon_reconnect.py`
- `tests/unit/test_daemon_failure_modes.py`
- `tests/unit/test_daemon_logging.py`
- `tests/integration/test_daemon_restart.py`
- `tests/unit/test_status_store.py`
- `.superpowers/sdd/task-13-report.md`

## Legacy cleanup status

- Removed the old daemon production dependency on `SyncSession` and removed the old full-scan daemon implementation.
- Removed the Task 5 watcher compatibility snapshot because it had no production or test consumers.
- Deferred deletion of `_legacy_reconcile.py`, `sync.py`, `syncsession.py`, and legacy `StateStore` because protected Task 16 callers remain.
- Exact protected consumer in `src/remote_sandbox/cli.py` is the `SyncExecutionError` import, the `SyncSession` import, and `_sync_now()` calling `SyncSession(...).sync_once()` at lines 44 through 45 and 375 through 380.
- Exact additional production consumer is `src/remote_sandbox/fetch.py`, which opens legacy `StateStore` at line 42.

## Proof that cli.py was untouched and unstaged

- Hash before Task 13 edits: `fbff4ce2da913aaba734193473054922d901132d5f03eade88f91bc1290b4682`.
- Hash after implementation and verification: `fbff4ce2da913aaba734193473054922d901132d5f03eade88f91bc1290b4682`.
- The existing worktree diff remains `17` inserted lines and `1` deleted line.
- `src/remote_sandbox/cli.py` is excluded from the Task 13 staging command and remains unstaged.

## Concerns and deferred boundaries

- `cli.py` foreground `_sync_now` and `fetch.py` still use synchronized-tree legacy metadata. They are explicitly deferred to Task 16 because this task was forbidden from editing the protected CLI and removing their dependencies would break production callers.
- The remote agent source was not changed. Its standard-library-only and Python 3.10-compatible boundary was revalidated with a Python 3.10 grammar parse.
