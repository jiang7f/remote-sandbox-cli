from __future__ import annotations

import argparse
import os
import secrets
import shutil
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from remote_sandbox.bind import BindError, bind_workspace
from remote_sandbox.daemon import (
    DaemonError,
    DaemonPhase,
    DaemonStatus,
    StopResult,
    daemon_status,
    ensure_daemon,
    poke_and_wait_for_sync,
    poke_daemon,
    stop_daemon_result,
)
from remote_sandbox.fetch import FetchError, fetch_placeholders
from remote_sandbox.lock import WorkspaceLockError
from remote_sandbox.marker import METADATA_DIR, read_local_marker, remove_local_metadata
from remote_sandbox.peek import PeekError, peek_placeholder
from remote_sandbox.registry import (
    BindingRecord,
    RegistryError,
    current_workspace_record,
    delete_binding_record,
    find_binding_record,
    list_binding_records,
    registry_path,
)
from remote_sandbox.resources import ProbeResult, probe_target_resources
from remote_sandbox.settings import (
    SettingsError,
    format_size,
    format_size_compact,
    load_settings,
    set_placeholder_limit,
    settings_path,
)
from remote_sandbox.shell import enter_shell_loop
from remote_sandbox.ssh import SshError, SubprocessSshRunner
from remote_sandbox.ssh_config import SshHost, load_configured_hosts
from remote_sandbox.sync import SyncExecutionError
from remote_sandbox.syncsession import SyncSession

CLI_ERRORS = (
    BindError,
    FetchError,
    PeekError,
    RegistryError,
    SettingsError,
    SshError,
    SyncExecutionError,
    WorkspaceLockError,
    DaemonError,
    OSError,
    ValueError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=_program_name())
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List SSH-configured servers")

    subparsers.add_parser("status", help="List local workspace bindings")

    start = subparsers.add_parser("start", help="Start the sync daemon for a binding")
    start.add_argument("name", nargs="?", help="Connection name; defaults to current workspace")

    stop = subparsers.add_parser("stop", help="Stop the sync daemon for a binding")
    stop.add_argument("name", nargs="?", help="Connection name; defaults to current workspace")

    shell = subparsers.add_parser("shell", help="Open a wrapped remote shell for a binding")
    shell.add_argument("name", nargs="?", help="Connection name; defaults to current workspace")

    run = subparsers.add_parser("run", help="Run one command in a bound remote workspace")
    run.add_argument("items", nargs=argparse.REMAINDER, help="[name] -- command")

    set_parser = subparsers.add_parser("set", help="Set user defaults")
    set_subparsers = set_parser.add_subparsers(dest="setting", required=True)
    placeholder_limit = set_subparsers.add_parser(
        "placeholder-limit",
        help="Set the global placeholder size limit, e.g. 10MB",
    )
    placeholder_limit.add_argument("value", help="Size such as 10MB, 512MB, or 1GB")

    enter = subparsers.add_parser("enter", help="Browse a remote server before binding")
    enter.add_argument("target", help="OpenSSH target, e.g. a Host alias or user@host")
    enter.add_argument(
        "-r",
        "--remote",
        default="~",
        help="Initial remote path for browsing; defaults to ~",
    )
    enter.add_argument("-l", "--local", default=".", help="Local workspace path; defaults to cwd")

    connect = subparsers.add_parser("connect", help="Bind a local workspace to a remote path")
    connect.add_argument("target", help="OpenSSH target, e.g. a Host alias or user@host")
    connect.add_argument(
        "-r",
        "--remote",
        required=True,
        help="Remote workspace path: /abs, ~, or ~/path",
    )
    connect.add_argument("-l", "--local", default=".", help="Local workspace path; defaults to cwd")
    connect.add_argument("--name", default=None, help="Connection name for rsb reconnect")
    connect.add_argument(
        "--no-shell",
        action="store_true",
        help="Bind/sync only; do not enter the remote shell",
    )

    reconnect = subparsers.add_parser("reconnect", help="Reconnect a named workspace")
    reconnect.add_argument("name", help="Connection name shown by rsb list/status")
    reconnect.add_argument(
        "-l",
        "--local",
        default=None,
        help="Repair the saved local path before reconnecting",
    )
    reconnect.add_argument(
        "--no-shell",
        action="store_true",
        help="Sync only; do not enter the remote shell",
    )

    forget = subparsers.add_parser("forget", help="Remove a saved connection record")
    forget.add_argument("name", help="Connection name to remove")

    fetch = subparsers.add_parser("fetch", help="Fetch placeholder file content")
    fetch.add_argument("path", nargs="?", help="Workspace-relative placeholder path")
    fetch.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Fetch every placeholder in the current workspace",
    )

    peek = subparsers.add_parser("peek", help="Print part of a placeholder's remote content")
    peek.add_argument("path", help="Workspace-relative placeholder path")
    direction = peek.add_mutually_exclusive_group()
    direction.add_argument(
        "--lines",
        type=int,
        default=40,
        help="Print the first N lines; defaults to 40",
    )
    direction.add_argument(
        "--tail",
        type=int,
        help="Print the last N lines",
    )
    return parser


def confirm_prompt(prompt: str) -> bool:
    try:
        return input(prompt).strip().lower() in {"y", "yes"}
    except EOFError as exc:
        raise ValueError(
            "confirmation input unavailable; rerun in a terminal and answer y/N"
        ) from exc


def fetch_placeholder(*, path: str | None, fetch_all: bool) -> int:
    if fetch_all and path is not None:
        raise ValueError("Use either a path or --all, not both")
    if not fetch_all and path is None:
        raise ValueError("fetch requires a path or --all")
    count, cancelled = fetch_placeholders(
        local_root=_workspace_root_for_cwd(Path.cwd()),
        runner=_runner_for_cwd(),
        path=path,
        fetch_all=fetch_all,
        confirm=confirm_prompt,
    )
    if count:
        noun = "file" if count == 1 else "files"
        print(f"Fetched {count} placeholder {noun}")
    elif cancelled:
        print("Fetch cancelled")
    else:
        print("No placeholders found")
    return 0


def peek_file(*, path: str, lines: int, tail: bool) -> int:
    content = peek_placeholder(
        local_root=_workspace_root_for_cwd(Path.cwd()),
        runner=_runner_for_cwd(),
        path=path,
        lines=lines,
        tail=tail,
    )
    sys.stdout.buffer.write(content)
    return 0


def list_servers() -> int:
    settings = load_settings()
    hosts = load_configured_hosts(require_identity=False)
    records = list_binding_records()
    if not hosts:
        print(f"placeholder-limit: {format_size_compact(settings.placeholder_limit)}")
        print("No SSH hosts found in ~/.ssh/config")
        return 0

    probes: dict[str, ProbeResult] = {}
    with ThreadPoolExecutor(max_workers=min(len(hosts), 8)) as executor:
        future_to_alias = {
            executor.submit(probe_target_resources, host.alias): host.alias for host in hosts
        }
        for future in as_completed(future_to_alias):
            alias = future_to_alias[future]
            try:
                probes[alias] = future.result()
            except Exception as exc:  # pragma: no cover - defensive for executor failures
                probes[alias] = ProbeResult.failed(alias, str(exc))

    print(
        _format_servers_table(
            hosts,
            probes=probes,
            records=records,
            placeholder_limit=settings.placeholder_limit,
        )
    )
    return 0


def show_status() -> int:
    records = list_binding_records()
    if not records:
        print("No bound workspaces")
        return 0
    current = current_workspace_record(None, Path.cwd())
    print(
        _format_status_table(
            records,
            current_workspace_id=current.workspace_id if current is not None else None,
        )
    )
    return 0


def start_binding_daemon(name: str | None) -> int:
    record = _record_for_execution(name)
    local_root = _require_live_workspace(record)
    _ensure_master(record.target)
    status = ensure_daemon(local_root)
    pid = f" pid={status.pid}" if status.pid is not None else ""
    print(f"Daemon running for {record.name}:{pid}")
    return 0


def stop_binding_daemon(name: str | None) -> int:
    record = _record_for_execution(name)
    result = stop_daemon_result(Path(record.local_path))
    if result is StopResult.STOPPED:
        print(f"Stopped daemon for {record.name}")
    elif result is StopResult.NOT_RUNNING:
        print(f"No daemon running for {record.name}")
    else:
        print(f"Stop requested for {record.name}, but daemon is still shutting down")
    return 0


def forget_connection(name: str) -> int:
    record = find_binding_record(name)
    if record is None:
        print(f"{_error_prefix()} no connection named {name!r}", file=sys.stderr)
        return 2
    local_root = Path(record.local_path)
    status = daemon_status(local_root)
    if status.running:
        pid = f" (pid={status.pid})" if status.pid is not None else ""
        if not confirm_prompt(
            f"A sync daemon is running for {record.name}{pid}; a transfer may be in progress. "
            "Stop it and forget anyway? [y/N] "
        ):
            print(f"Kept connection {record.name}")
            return 0
        if stop_daemon_result(local_root) is StopResult.TIMEOUT:
            print(
                f"{_error_prefix()} daemon for {record.name} did not stop; "
                "try again once it settles",
                file=sys.stderr,
            )
            return 2
    delete_binding_record(record.name)
    if remove_local_metadata(local_root):
        print(f"Forgot connection {record.name}; removed {local_root / METADATA_DIR}")
    else:
        print(f"Forgot connection {record.name}")
    return 0


def open_wrapped_shell(name: str | None) -> int:
    return _open_wrapped_shell_for_record(_record_for_execution(name))


def _open_wrapped_shell_for_record(record: BindingRecord) -> int:
    local_root = _require_live_workspace(record)
    runner = _connected_runner(record.target)
    _ensure_daemon_quietly(local_root)

    barrier_seen = False

    def on_barrier(_status: int) -> None:
        nonlocal barrier_seen
        barrier_seen = True
        _poke_or_restart_daemon(local_root, "shell")

    code = runner.interactive_shell(
        record.target,
        record.remote_path,
        on_barrier=on_barrier,
    )
    if not barrier_seen:
        _poke_or_restart_daemon(local_root, "shell")
    return code


def _ensure_daemon_quietly(local_root: Path) -> None:
    """Start the sync daemon without ever letting a startup hiccup crash the caller.

    The daemon is a detached background process: even if the readiness wait times out,
    the process keeps running and syncing. A foreground command (connect/shell/run) must
    never abort — nor print a traceback — just because the daemon was slow to publish its
    socket. Downgrade any DaemonError to a one-line warning and continue.
    """
    try:
        ensure_daemon(local_root)
    except DaemonError as exc:
        print(
            f"{_error_prefix()} note: sync daemon still starting ({exc}); "
            "check `rsb status`",
            file=sys.stderr,
        )


def _poke_or_restart_daemon(local_root: Path, source: str) -> None:
    if poke_daemon(local_root, source):
        return
    _ensure_daemon_quietly(local_root)
    if not poke_daemon(local_root, source):
        print(
            f"{_error_prefix()} warning: daemon did not accept sync notification after {source}",
            file=sys.stderr,
        )


def _ensure_daemon_for_record(record: BindingRecord) -> None:
    """Start the daemon for a --no-shell bind and show live initial-sync progress.

    The remote is the source of truth, so the binding is usable the moment the daemon is
    up; we still block here rendering [m:ss] progress until the first sync settles so the
    user sees the transfer complete (or learns it is continuing in the background) instead
    of the command returning to a silent prompt mid-transfer.
    """
    local_root = Path(record.local_path)
    _ensure_daemon_quietly(local_root)
    _await_initial_sync(local_root)


def run_remote_command(items: list[str]) -> int:
    record_name, command_argv = _parse_run_items(items)
    record = _record_for_execution(record_name)
    local_root = _require_live_workspace(record)
    runner = _connected_runner(record.target)
    _ensure_daemon_quietly(local_root)
    result = runner.run_command(
        record.target,
        record.remote_path,
        tuple(command_argv),
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    # The command's exit code is authoritative and returned unconditionally. Syncing the
    # remote's output back is a best-effort follow-up that must never change the exit code
    # or raise (Phase 3 hardens the separation further).
    _sync_now(local_root, runner, record)
    return result.returncode


def _sync_now(local_root: Path, runner: SubprocessSshRunner, record: BindingRecord) -> None:
    """Best-effort: land the remote's output files locally after `rsb run`.

    Fully decoupled from the command's exit code — this never raises and never changes
    what `run` returns. It prefers to let the *daemon* do the sync (poke it and wait for
    one fresh cycle to finish) so the CLI does not open a competing SyncSession and fight
    the daemon for the workspace lock, which used to surface as a spurious traceback while
    the command itself had actually succeeded. Only when no daemon is running does it fall
    back to a single foreground sync.
    """
    if poke_and_wait_for_sync(local_root, "run"):
        print("[rsb] synced", file=sys.stderr)
        return
    status = daemon_status(local_root)
    if status.running:
        # Daemon is up but the fresh-sync wait did not confirm success (degraded/timeout).
        # Do not foreground-sync into its lock; it will retry. Report and move on.
        print(
            f"{_error_prefix()} note: sync still settling in the background; "
            "check `rsb status`",
            file=sys.stderr,
        )
        return
    try:
        SyncSession(
            local_root=local_root,
            runner=runner,
            target=record.target,
            remote=record.remote_path,
        ).sync_once()
        print("[rsb] synced", file=sys.stderr)
    except CLI_ERRORS as exc:
        # A sync hiccup must never fail the command; surface it and let the daemon retry.
        print(f"{_error_prefix()} sync after run failed: {exc}", file=sys.stderr)
        _poke_or_restart_daemon(local_root, "run")


def _parse_run_items(items: list[str]) -> tuple[str | None, list[str]]:
    try:
        separator = items.index("--")
    except ValueError as exc:
        raise ValueError("run requires -- before the command") from exc
    before = items[:separator]
    command = items[separator + 1 :]
    if not command:
        raise ValueError("run requires a command after --")
    if len(before) > 1:
        raise ValueError("run accepts at most one connection name before --")
    return (before[0] if before else None), command


def _record_for_execution(name: str | None) -> BindingRecord:
    if name is not None:
        record = find_binding_record(name)
        if record is None:
            raise RegistryError(f"no connection named {name!r}; run rsb status")
        return record
    record = current_workspace_record(None, Path.cwd())
    if record is None:
        raise RegistryError("current directory is not bound; pass a connection name")
    return record


def _ensure_master(target: str) -> None:
    """Establish the shared SSH master (prompting for a password once) from the foreground."""
    SubprocessSshRunner().ensure_master(target)


def _connected_runner(target: str) -> SubprocessSshRunner:
    """A runner with its SSH master already established, so later calls never re-prompt."""
    runner = SubprocessSshRunner()
    runner.ensure_master(target)
    return runner


def _await_initial_sync(local_root: Path, *, timeout: float = 3600.0) -> bool:
    """Render live [m:ss] progress lines until the daemon settles, then return.

    Prints staged progress — starting -> scanning -> planning -> syncing X/Y — so a large
    first sync shows what it is doing instead of sitting silent. Returns True once the
    daemon reaches `ready`, False on `degraded`/timeout. Non-fatal either way: the daemon
    keeps syncing in the background regardless.
    """
    start = time.monotonic()
    deadline = start + timeout
    last_line = ""
    last_emit = -1.0
    while time.monotonic() < deadline:
        elapsed = time.monotonic() - start
        try:
            status: DaemonStatus | None = daemon_status(local_root)
        except CLI_ERRORS:
            status = None
        if status is not None and status.running and status.phase is DaemonPhase.READY:
            _print_progress_line(elapsed, "ready")
            return True
        if status is not None and status.running and status.phase is DaemonPhase.DEGRADED:
            detail = _one_line(status.last_error or "sync failed", max_len=80)
            _print_progress_line(elapsed, f"sync issue: {detail} (daemon will retry)")
            return False
        line = _describe_sync(status)
        now = time.monotonic()
        if line != last_line or now - last_emit >= 1.0:
            _print_progress_line(elapsed, line)
            last_line = line
            last_emit = now
        time.sleep(0.15)
    _print_progress_line(
        time.monotonic() - start, "still syncing in the background; check `rsb status`"
    )
    return False


def _describe_sync(status: DaemonStatus | None) -> str:
    """One human-readable progress line (no timestamp) for the current daemon state."""
    if status is None or not status.running:
        return "starting daemon..."
    sync_phase = status.sync_phase
    if sync_phase == "scanning-remote":
        return "scanning remote..."
    if sync_phase == "scanning-local":
        return "scanning local..."
    if sync_phase == "planning":
        total = status.files_total
        return f"planning sync... {total} entries" if total else "planning sync..."
    if sync_phase == "transferring" and status.files_total:
        done = status.files_done or 0
        total = status.files_total
        bytes_part = ""
        if status.bytes_total:
            bytes_part = (
                f", {format_size(status.bytes_done or 0)}/{format_size(status.bytes_total)}"
            )
        path = status.current_path
        path_part = f"  {_one_line(path, max_len=48)}" if path else ""
        return f"syncing... {done}/{total} files{bytes_part}{path_part}"
    return "syncing..."


def _print_progress_line(elapsed: float, message: str) -> None:
    minutes, seconds = divmod(int(elapsed), 60)
    print(f"[{minutes}:{seconds:02d}] {message}", file=sys.stderr)


def _require_live_workspace(record: BindingRecord) -> Path:
    """Resolve a record's local root, failing with an actionable message if it is stale.

    A binding can outlive its local directory (for example a temporary workspace that
    was cleaned up). The low-level daemon only sees a missing marker and reports the
    cryptic "not a bound workspace"; surface the recovery steps instead.
    """
    local = Path(record.local_path)
    if not local.exists() or read_local_marker(local) is None:
        raise RegistryError(
            f"connection {record.name!r} points to a missing or unbound local path: "
            f"{record.local_path}. "
            f"Run `rsb reconnect {record.name} --local <new-path>` to repair it, "
            f"or `rsb forget {record.name}` to remove it from {registry_path()}"
        )
    return local


def enter_and_bind(*, target: str, remote: str, local: Path, open_shell: bool) -> int:
    runner = SubprocessSshRunner()
    remote_cwd = remote
    decision = {"declined": False}

    def _confirm(prompt: str) -> bool:
        answer = confirm_prompt(prompt)
        decision["declined"] = not answer
        return answer

    while True:
        decision["declined"] = False
        enter_result = enter_shell_loop(target, remote_cwd, nonce=secrets.token_hex(8))
        if enter_result.exit_code != 0:
            return enter_result.exit_code
        if enter_result.remote is None:
            return 0
        selected_local = Path(enter_result.local) if enter_result.local is not None else local
        try:
            result = bind_workspace(
                target=target,
                remote=enter_result.remote,
                local=selected_local,
                runner=runner,
                confirm=_confirm,
                connection_name=enter_result.name,
            )
        except CLI_ERRORS as exc:
            if decision["declined"]:
                # Answered N: keep browsing, resuming at the directory they tried to bind.
                remote_cwd = enter_result.remote
                continue
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2
        _print_connection(result.connection)
        if open_shell:
            return _open_wrapped_shell_for_record(result.connection)
        _ensure_daemon_for_record(result.connection)
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list":
        try:
            return list_servers()
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "status":
        try:
            return show_status()
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "start":
        try:
            return start_binding_daemon(args.name)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "stop":
        try:
            return stop_binding_daemon(args.name)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "shell":
        if not _has_tty():
            print(f"{_error_prefix()} shell requires an interactive TTY", file=sys.stderr)
            return 2
        try:
            return open_wrapped_shell(args.name)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "run":
        try:
            return run_remote_command(args.items)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "set" and args.setting == "placeholder-limit":
        try:
            settings = set_placeholder_limit(args.value)
        except (ValueError, OSError, SettingsError) as exc:
            print(
                f"{_error_prefix()} could not update placeholder-limit "
                f"at {settings_path()}: {exc}. "
                "Use a value like 10MB or 1GB and check REMOTE_SANDBOX_HOME permissions.",
                file=sys.stderr,
            )
            return 2
        print(
            "placeholder-limit: "
            f"{format_size_compact(settings.placeholder_limit)} "
            f"({settings_path()})"
        )
        return 0

    if args.command == "enter":
        if not _has_tty():
            print(f"{_error_prefix()} enter requires an interactive TTY", file=sys.stderr)
            return 2
        try:
            return enter_and_bind(
                target=args.target,
                remote=args.remote,
                local=Path(args.local),
                open_shell=True,
            )
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "connect":
        if not args.no_shell and not _has_tty():
            print(
                f"{_error_prefix()} interactive shell requires a TTY; "
                "rerun in a terminal or pass --no-shell",
                file=sys.stderr,
            )
            return 2
        try:
            result = bind_workspace(
                target=args.target,
                remote=args.remote,
                local=Path(args.local),
                runner=_connected_runner(args.target),
                confirm=confirm_prompt,
                connection_name=args.name,
            )
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2
        _print_connection(result.connection)
        if args.no_shell:
            _ensure_daemon_for_record(result.connection)
            return 0
        return _open_wrapped_shell_for_record(result.connection)

    if args.command == "reconnect":
        try:
            record = find_binding_record(args.name)
            if record is None:
                print(
                    f"{_error_prefix()} no connection named {args.name!r}; run rsb status",
                    file=sys.stderr,
                )
                return 2
            local = Path(args.local) if args.local is not None else Path(record.local_path)
            if not local.exists():
                print(
                    f"{_error_prefix()} connection "
                    f"{record.name!r} points to missing local path: {record.local_path}. "
                    f"Run `rsb reconnect {record.name} --local <new-path>` to repair it, "
                    f"or `rsb forget {record.name}` to remove it from {registry_path()}",
                    file=sys.stderr,
                )
                return 2
            if not args.no_shell and not _has_tty():
                print(
                    f"{_error_prefix()} interactive shell requires a TTY; "
                    "rerun in a terminal or pass --no-shell",
                    file=sys.stderr,
                )
                return 2
            result = bind_workspace(
                target=record.target,
                remote=record.remote_path,
                local=local,
                runner=_connected_runner(record.target),
                confirm=confirm_prompt,
                connection_name=record.name,
            )
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2
        _print_connection(result.connection)
        if args.no_shell:
            _ensure_daemon_for_record(result.connection)
            return 0
        return _open_wrapped_shell_for_record(result.connection)

    if args.command == "forget":
        try:
            return forget_connection(args.name)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "fetch":
        try:
            return fetch_placeholder(path=args.path, fetch_all=args.all)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    if args.command == "peek":
        try:
            return peek_file(
                path=args.path,
                lines=args.tail if args.tail is not None else args.lines,
                tail=args.tail is not None,
            )
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2

    parser.error(f"unknown command: {args.command}")
    return 2


def _format_servers_table(
    hosts: list[SshHost],
    *,
    probes: dict[str, ProbeResult],
    records: list[BindingRecord],
    placeholder_limit: int,
) -> str:
    records_by_target: dict[str, list[BindingRecord]] = {}
    for record in records:
        records_by_target.setdefault(record.target, []).append(record)
    headers = ["TARGET", "BOUND", "CONNECTIONS", "CPU", "MEM", "GPU"]
    rows_wide = []
    rows_tall = []
    for host in hosts:
        probe = probes.get(host.alias)
        if probe is not None and (probe.error is not None or probe.resources is None):
            continue
        target_records = sorted(records_by_target.get(host.alias, ()), key=lambda item: item.name)
        bound = "yes" if target_records else "-"
        names = [record.name for record in target_records]
        cpu, mem, gpu = (
            _list_resource_columns(probe)
            if probe is not None
            else ("pending", "pending", "pending")
        )
        # Wide layout comma-joins the names; the narrow fallback lists one per line so a
        # long connection list no longer blows the table past the terminal width.
        joined = ", ".join(names) if names else "-"
        stacked = "\n".join(names) if names else "-"
        rows_wide.append([host.alias, bound, joined, cpu, mem, gpu])
        rows_tall.append([host.alias, bound, stacked, cpu, mem, gpu])
    table = _format_table(headers, rows_wide)
    body = table if _table_fits(table) else _format_records_vertical(headers, rows_tall)
    return "\n".join(
        [
            f"placeholder-limit: {format_size_compact(placeholder_limit)}",
            "",
            body,
        ]
    )


def _format_status_table(
    records: list[BindingRecord],
    *,
    current_workspace_id: str | None,
) -> str:
    sorted_records = sorted(
        records,
        key=lambda record: (record.target, record.name, record.local_path, record.remote_path),
    )
    headers = ["NAME", "REMOTE", "LOCAL", "REMOTE_PATH", "DAEMON", "CURRENT"]
    rows = []
    for record in sorted_records:
        current = "*" if record.workspace_id == current_workspace_id else ""
        daemon = _daemon_column(record)
        rows.append(
            [
                record.name,
                record.target,
                record.local_path,
                record.remote_path,
                daemon,
                current,
            ]
        )
    table = _format_table(headers, rows)
    # A single long temp path can push the horizontal table past the terminal width,
    # so it wraps and the columns misalign (looks like garbled output). Fall back to a
    # per-record vertical layout that never wraps when it would not fit.
    if _table_fits(table):
        return table
    return _format_records_vertical(headers, rows)


def _format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [_display_width(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], _display_width(value))

    def format_row(row: list[str]) -> str:
        cells = [
            value + " " * (widths[index] - _display_width(value))
            for index, value in enumerate(row)
        ]
        return "  ".join(cells).rstrip()

    return "\n".join([format_row(headers), *(format_row(row) for row in rows)])


def _terminal_width() -> int | None:
    if not os.isatty(1):
        return None
    return shutil.get_terminal_size(fallback=(80, 24)).columns


def _table_fits(table: str) -> bool:
    width = _terminal_width()
    if width is None:
        return True
    return all(_display_width(line) <= width for line in table.splitlines())


def _format_records_vertical(headers: list[str], rows: list[list[str]]) -> str:
    label_width = max(_display_width(header) for header in headers)
    indent = " " * (label_width + 2)
    blocks = []
    for row in rows:
        lines = []
        for header, value in zip(headers, row, strict=True):
            pad = " " * (label_width - _display_width(header))
            parts = value.split("\n") if value else [""]
            lines.append(f"{header}{pad}  {parts[0]}".rstrip())
            # A multi-line cell (e.g. one connection name per line) aligns under the value.
            lines.extend(f"{indent}{cont}".rstrip() for cont in parts[1:])
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _display_width(value: str) -> int:
    width = 0
    for char in value:
        if unicodedata.combining(char):
            continue
        width += 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1
    return width



def _daemon_column(record: BindingRecord) -> str:
    try:
        status = daemon_status(Path(record.local_path))
    except CLI_ERRORS:
        return "unknown"
    if not status.running:
        return "stopped"
    label = status.phase.value if status.phase is not None else "running"
    suffix = f":{status.pid}" if status.pid is not None else ""
    if (
        status.phase in {DaemonPhase.INITIAL_SYNCING, DaemonPhase.SYNCING}
        and status.files_total
    ):
        return f"{label}{suffix} {status.files_done}/{status.files_total}"
    if status.phase is DaemonPhase.DEGRADED and status.last_error:
        return f"{label}{suffix} (last: {_one_line(status.last_error, max_len=40)})"
    return f"{label}{suffix}"


def _list_resource_columns(result: ProbeResult) -> tuple[str, str, str]:
    if result.error is not None or result.resources is None:
        error = f"error: {_one_line(result.error or 'probe failed', max_len=90)}"
        return error, "-", "-"
    resources = result.resources
    cpu = f"{resources.cpu.load_1m:.2f}/{resources.cpu.count}"
    mem = f"{resources.memory.used_pct:.1f}%"
    if not resources.gpus:
        gpu = "none"
    else:
        gpu = " ".join(
            f"{item.index}:{item.util_pct}% {item.mem_used_mb}/{item.mem_total_mb}MB"
            for item in resources.gpus
        )
    return cpu, mem, gpu


def _one_line(value: str, *, max_len: int) -> str:
    compact = " ".join(value.split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1] + "..."


def _workspace_root_for_cwd(cwd: Path) -> Path:
    record = current_workspace_record(None, cwd)
    if record is not None:
        return Path(record.local_path)
    return cwd


def _runner_for_cwd() -> SubprocessSshRunner:
    """A runner with the SSH master established for the current workspace's target."""
    record = current_workspace_record(None, Path.cwd())
    if record is None:
        return SubprocessSshRunner()
    return _connected_runner(record.target)


def _print_connection(record: BindingRecord) -> None:
    print(
        "Connected "
        f"{record.name}: {record.target}:{record.remote_path} <-> {record.local_path}"
    )


def _has_tty() -> bool:
    return os.isatty(0) and os.isatty(1)


def _program_name() -> str:
    name = Path(sys.argv[0]).name
    if name in {"rsb", "remote-sandbox"}:
        return name
    return "rsb"


def _error_prefix() -> str:
    return f"{_program_name()}:"


if __name__ == "__main__":
    raise SystemExit(main())
