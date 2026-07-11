from __future__ import annotations

import argparse
import contextlib
import io
import os
import secrets
import shutil
import sys
import time
import traceback
import unicodedata
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from remote_sandbox.agent import RemoteAgentManager
from remote_sandbox.bind import bind_workspace
from remote_sandbox.daemon import (
    DaemonError,
    StopResult,
    SupervisorClient,
    SupervisorRuntime,
    daemon_control_status,
    daemon_status,
    ensure_daemon,
    poke_daemon,
    stop_daemon_result,
    wait_for_daemon_control,
)
from remote_sandbox.fetch import fetch_all_prompt
from remote_sandbox.namespace import runtime_dir
from remote_sandbox.peek import peek_placeholder
from remote_sandbox.registry import (
    BindingRecord,
    RegistryError,
    current_workspace_record,
    delete_binding_record,
    find_binding_record,
    list_binding_records,
    registry_path,
)
from remote_sandbox.remote_agent import AGENT_VERSION
from remote_sandbox.remote_client import RemoteWorkspaceClient
from remote_sandbox.resources import ProbeResult, probe_target_resources
from remote_sandbox.settings import (
    format_size_compact,
    load_settings,
    set_placeholder_limit,
    settings_path,
)
from remote_sandbox.shell import (
    ConnectRequestEvent,
    ConnectResponse,
    InitialShellDirection,
    ReadyProbeResult,
    enter_shell_loop,
)
from remote_sandbox.ssh import SubprocessSshRunner
from remote_sandbox.ssh_config import SshHost, load_configured_hosts
from remote_sandbox.state import ConflictRecord, WorkspaceStore
from remote_sandbox.status import WorkspacePhase, WorkspaceStatus
from remote_sandbox.transport import BatchTransport
from remote_sandbox.workspace import read_workspace_spec, workspace_paths


@dataclass(frozen=True, slots=True)
class RemoteCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True, slots=True)
class ConnectedWorkspace:
    record: BindingRecord
    created: bool
    initial_sync_generation_before: int


@dataclass(slots=True)
class CliServices:
    registry: Path
    cwd: Callable[[], Path]
    list_records: Callable[[Path], list[BindingRecord]]
    find_record: Callable[[str, Path], BindingRecord | None]
    current_record: Callable[[Path, Path], BindingRecord | None]
    workspace_status: Callable[[BindingRecord], WorkspaceStatus]
    daemon_status: Callable[[BindingRecord], object]
    ensure_supervisor: Callable[[BindingRecord], object]
    request_sync: Callable[[BindingRecord], bool]
    run_remote: Callable[[BindingRecord, tuple[str, ...]], RemoteCommandResult]
    connect_workspace: Callable[[str, str, Path, str | None], ConnectedWorkspace]
    wait_initial_sync: Callable[[BindingRecord, int], WorkspaceStatus]
    fetch_placeholders: Callable[
        [BindingRecord, str | None, bool, Callable[[str], bool]],
        tuple[int, bool],
    ]
    peek_placeholder: Callable[[BindingRecord, str, int, bool], bytes]
    list_conflicts: Callable[[BindingRecord], list[ConflictRecord]]
    resolve_conflict: Callable[[BindingRecord, str, bool], None]
    stop_local_supervisor: Callable[[BindingRecord], None]
    stop_remote_watcher: Callable[[BindingRecord], None]
    delete_remote_workspace: Callable[[BindingRecord], None]
    prune_remote_agent: Callable[[BindingRecord], None]
    delete_local_workspace: Callable[[BindingRecord], None]
    delete_registry_record: Callable[[BindingRecord], None]
    watch_sleep: Callable[[float], None] = time.sleep
    watch_limit: int | None = None


@dataclass(frozen=True, slots=True)
class CapturedCliResult:
    exit_code: int
    stdout: str
    stderr: str


def invoke_cli(argv: list[str], *, services: CliServices) -> CapturedCliResult:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            exit_code = _dispatch_services(build_parser().parse_args(argv), services)
        except Exception as exc:
            debug = "--debug" in argv
            if debug:
                traceback.print_exc()
            else:
                print(f"codex-rsb: {_one_line(str(exc), max_len=500)}", file=sys.stderr)
            exit_code = 2
    return CapturedCliResult(exit_code, stdout.getvalue(), stderr.getvalue())


_DEFAULT_IGNORE_CONTENT = """# Common generated content that stays local.
.venv/
venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
node_modules/

# Git metadata is always local-only and cannot be re-enabled.
"""


def _dispatch_services(args: argparse.Namespace, services: CliServices) -> int:
    if args.command == "init":
        policy_path = services.cwd() / ".rsbignore"
        if not policy_path.exists():
            policy_path.write_text(_DEFAULT_IGNORE_CONTENT, encoding="utf-8")
            print(f"Initialized {policy_path}")
        else:
            print(f"Already initialized {policy_path}")
        return 0

    if args.command == "status":
        records = services.list_records(services.registry)
        if args.name is not None:
            record = services.find_record(args.name, services.registry)
            if record is None:
                raise RegistryError(f"no connection named {args.name!r}")
            records = [record]
        if not records:
            print("No bound workspaces")
            return 0
        iteration = 0
        while True:
            if iteration:
                print("\x1b[H\x1b[2J", end="")
            print(_service_status_table(records, services))
            iteration += 1
            if not args.watch or (
                services.watch_limit is not None and iteration >= services.watch_limit
            ):
                break
            try:
                services.watch_sleep(1.0)
            except KeyboardInterrupt:
                break
        return 0

    if args.command == "run":
        name, command = _parse_run_items(args.items)
        record = _service_record(services, name)
        services.ensure_supervisor(record)
        result = services.run_remote(record, tuple(command))
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)
        try:
            if not services.request_sync(record):
                raise DaemonError("supervisor did not accept the sync request")
        except Exception as exc:
            if args.debug:
                traceback.print_exc()
            else:
                print(
                    "codex-rsb: sync after run failed: "
                    + _one_line(str(exc), max_len=500),
                    file=sys.stderr,
                )
        return result.returncode

    if args.command == "connect" and args.no_shell:
        connection = services.connect_workspace(
            args.target,
            args.remote,
            Path(args.local),
            args.name,
        )
        record = connection.record
        services.ensure_supervisor(record)
        status = (
            services.wait_initial_sync(
                record,
                connection.initial_sync_generation_before,
            )
            if connection.created
            else services.workspace_status(record)
        )
        if status.phase.value in {"failed", "stopped"}:
            raise DaemonError(status.last_error or "workspace supervisor failed to start")
        print(
            f"Connected {record.name}: {record.target}:{record.remote_path} "
            f"<-> {record.local_path}"
        )
        _print_no_shell_status(record, status, initial=connection.created)
        return 0

    if args.command == "reconnect" and args.no_shell:
        existing = services.find_record(args.name, services.registry)
        if existing is None:
            raise RegistryError(
                f"no connection named {args.name!r}; run codex-rsb status"
            )
        local = Path(args.local) if args.local is not None else Path(existing.local_path)
        connection = services.connect_workspace(
            existing.target,
            existing.remote_path,
            local,
            existing.name,
        )
        rebound = connection.record
        services.ensure_supervisor(rebound)
        _print_connection(rebound)
        _print_no_shell_status(
            rebound,
            services.workspace_status(rebound),
            initial=False,
        )
        return 0

    if args.command == "fetch":
        record = _service_record(services, None)
        count, cancelled = services.fetch_placeholders(
            record,
            args.path,
            args.all,
            confirm_prompt,
        )
        if count:
            noun = "file" if count == 1 else "files"
            print(f"Fetched {count} placeholder {noun}")
        elif cancelled:
            print("Fetch cancelled")
        else:
            print("No placeholders found")
        return 0

    if args.command == "peek":
        record = _service_record(services, None)
        content = services.peek_placeholder(
            record,
            args.path,
            args.tail if args.tail is not None else args.lines,
            args.tail is not None,
        )
        sys.stdout.write(content.decode("utf-8", errors="surrogateescape"))
        return 0

    if args.command == "conflicts":
        record = _service_record(services, args.name)
        paths = [
            conflict.path
            for conflict in services.list_conflicts(record)
        ]
        if not paths:
            print(f"No unresolved conflicts for {record.name}")
        else:
            print("\n".join(paths))
        return 0

    if args.command == "resolve":
        record = _service_record(services, None)
        services.resolve_conflict(record, args.path, bool(args.use_local))
        source = "local" if args.use_local else "remote"
        print(f"Resolved {args.path} using {source}")
        return 0

    if args.command == "forget":
        record = services.find_record(args.name, services.registry)
        if record is None:
            print(f"Connection {args.name} is already forgotten")
            return 0
        services.stop_local_supervisor(record)
        if args.local_only:
            services.delete_local_workspace(record)
            services.delete_registry_record(record)
            residue = f"~/.codex-remote-sandbox/workspaces/{record.workspace_id}"
            print(f"Forgot {record.name} locally. Remote residue remains at {residue}")
            return 0
        services.stop_remote_watcher(record)
        services.delete_remote_workspace(record)
        services.prune_remote_agent(record)
        services.delete_local_workspace(record)
        services.delete_registry_record(record)
        print(f"Forgot connection {record.name}")
        return 0

    raise ValueError(f"command service is not implemented: {args.command}")


def _service_record(services: CliServices, name: str | None) -> BindingRecord:
    if name is not None:
        record = services.find_record(name, services.registry)
    else:
        record = services.current_record(services.registry, services.cwd())
    if record is None:
        if name is not None:
            raise RegistryError(f"no connection named {name!r}")
        raise RegistryError("current directory is not bound; pass a connection name")
    return record


def _service_status_table(records: list[BindingRecord], services: CliServices) -> str:
    rows: list[list[str]] = []
    guidance: list[str] = []
    for record in records:
        status = services.workspace_status(record)
        rows.append(
            [
                record.name,
                status.phase.value,
                status.progress.stage,
                str(status.pending),
                str(status.conflicts),
                _one_line(status.last_error or "", max_len=100),
            ]
        )
        if status.phase.value == "disconnected":
            guidance.append(
                f"{record.name} is disconnected. Run `codex-rsb reconnect {record.name}` "
                "in the foreground to re-enter authentication."
            )
    table = _format_table(
        ["NAME", "PHASE", "PROGRESS", "PENDING", "CONFLICTS", "ERROR"],
        rows,
    )
    return table if not guidance else table + "\n\n" + "\n".join(guidance)


def default_cli_services() -> CliServices:
    registry = registry_path()

    def status_for(record: BindingRecord) -> WorkspaceStatus:
        from remote_sandbox.daemon import daemon_workspace_status

        return daemon_workspace_status(record.workspace_id)

    def ensure_supervisor(record: BindingRecord) -> object:
        local_root = _require_live_workspace(record)
        status = ensure_daemon(local_root)
        if status.phase.value == "starting":
            status = wait_for_daemon_control(local_root, 5.0)
        return status

    def request_sync(record: BindingRecord) -> bool:
        return poke_daemon(Path(record.local_path), "cli")

    def run_remote(
        record: BindingRecord,
        argv: tuple[str, ...],
    ) -> RemoteCommandResult:
        runner = _connected_runner(record.target)
        result = runner.run_command(record.target, record.remote_path, argv)
        return RemoteCommandResult(result.returncode, result.stdout, result.stderr)

    def connect_workspace(
        target: str,
        remote: str,
        local: Path,
        name: str | None,
    ) -> ConnectedWorkspace:
        result = bind_workspace(
            target=target,
            remote=remote,
            local=local,
            runner=_connected_runner(target),
            confirm=confirm_prompt,
            connection_name=name,
        )
        with WorkspaceStore.open(workspace_paths(result.connection.workspace_id).state_db) as store:
            generation = store.initial_sync_started_generation()
        return ConnectedWorkspace(result.connection, result.created, generation)

    def wait_initial_sync(record: BindingRecord, generation: int) -> WorkspaceStatus:
        status = _supervisor_client(record).wait_for_initial_sync_started(generation, 5.0)
        return status.workspace_status or status_for(record)

    def list_conflicts(record: BindingRecord) -> list[ConflictRecord]:
        with WorkspaceStore.open(workspace_paths(record.workspace_id).state_db) as store:
            return store.list_conflicts(unresolved_only=True)

    def resolve_conflict(record: BindingRecord, path: str, use_local: bool) -> None:
        ensure_supervisor(record)
        _supervisor_client(record).mutate(
            "resolve",
            {"path": path, "use_local": use_local},
        )

    def fetch_registered(
        record: BindingRecord,
        path: str | None,
        fetch_all: bool,
        confirm: Callable[[str], bool],
    ) -> tuple[int, bool]:
        if fetch_all:
            with WorkspaceStore.open(workspace_paths(record.workspace_id).state_db) as store:
                prompt = fetch_all_prompt(Path(record.local_path), store)
            if prompt is None:
                return 0, False
            if not confirm(prompt):
                return 0, True
        ensure_supervisor(record)
        result = _supervisor_client(record).mutate(
            "fetch",
            {"path": path, "fetch_all": fetch_all},
        )
        count = result.get("count")
        cancelled = result.get("cancelled")
        if type(count) is not int or type(cancelled) is not bool:
            raise DaemonError("supervisor fetch result is malformed")
        return count, cancelled

    def peek_registered(
        record: BindingRecord,
        path: str,
        lines: int,
        tail: bool,
    ) -> bytes:
        runner, remote, _transport = _production_remote_components(record)
        del runner
        try:
            with WorkspaceStore.open(workspace_paths(record.workspace_id).state_db) as store:
                return peek_placeholder(
                    local_root=Path(record.local_path),
                    store=store,
                    remote=remote,
                    path=path,
                    lines=lines,
                    tail=tail,
                )
        finally:
            remote.close()

    def stop_local(record: BindingRecord) -> None:
        result = stop_daemon_result(Path(record.local_path))
        if result is StopResult.TIMEOUT:
            raise DaemonError(f"supervisor for {record.name} did not stop")

    def stop_remote(record: BindingRecord) -> None:
        _with_remote_client(record, lambda client: client.stop_watcher())

    def delete_remote(record: BindingRecord) -> None:
        _with_remote_client(record, lambda client: client.forget())

    def prune_remote(record: BindingRecord) -> None:
        others = [
            candidate
            for candidate in list_binding_records(registry)
            if candidate.workspace_id != record.workspace_id and candidate.target == record.target
        ]
        if others:
            return
        runner = _connected_runner(record.target)
        runner.delete_path(
            record.target,
            f"~/.codex-remote-sandbox/agents/{AGENT_VERSION}/agent.pyz",
        )

    def delete_local(record: BindingRecord) -> None:
        metadata_root = workspace_paths(record.workspace_id).root
        if metadata_root.exists():
            shutil.rmtree(metadata_root)
        if metadata_root.exists():
            raise RegistryError(f"Local metadata still exists: {metadata_root}")

    def delete_registry(record: BindingRecord) -> None:
        if not delete_binding_record(
            record.name,
            registry,
            workspace_id=record.workspace_id,
        ):
            raise RegistryError(f"Connection {record.name} changed during cleanup")

    return CliServices(
        registry=registry,
        cwd=Path.cwd,
        list_records=lambda path: list_binding_records(path),
        find_record=lambda name, path: find_binding_record(name, path),
        current_record=lambda path, cwd: current_workspace_record(path, cwd),
        workspace_status=status_for,
        daemon_status=lambda record: daemon_status(Path(record.local_path)),
        ensure_supervisor=ensure_supervisor,
        request_sync=request_sync,
        run_remote=run_remote,
        connect_workspace=connect_workspace,
        wait_initial_sync=wait_initial_sync,
        fetch_placeholders=fetch_registered,
        peek_placeholder=peek_registered,
        list_conflicts=list_conflicts,
        resolve_conflict=resolve_conflict,
        stop_local_supervisor=stop_local,
        stop_remote_watcher=stop_remote,
        delete_remote_workspace=delete_remote,
        prune_remote_agent=prune_remote,
        delete_local_workspace=delete_local,
        delete_registry_record=delete_registry,
    )


def _production_remote_components(
    record: BindingRecord,
) -> tuple[SubprocessSshRunner, RemoteWorkspaceClient, BatchTransport]:
    runner = _connected_runner(record.target)
    remote = RemoteWorkspaceClient(
        cast(Any, runner),
        target=record.target,
        workspace_id=record.workspace_id,
        agent_manager=RemoteAgentManager(runner),
    )
    remote.ensure_agent()
    transport = BatchTransport(
        Path(record.local_path),
        record.target,
        record.remote_path,
        remote,
        runner=runner,
    )
    return runner, remote, transport


def _supervisor_client(record: BindingRecord) -> SupervisorClient:
    paths = workspace_paths(record.workspace_id)
    return SupervisorClient(
        SupervisorRuntime(
            record.workspace_id,
            paths.root,
            runtime_dir() / "supervisors",
        )
    )


def _with_remote_client(
    record: BindingRecord,
    operation: Callable[[RemoteWorkspaceClient], object],
) -> object:
    _runner, remote, _transport = _production_remote_components(record)
    try:
        return operation(remote)
    finally:
        remote.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codex-rsb")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show a traceback when a command fails",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Write the default .rsbignore file")

    subparsers.add_parser("list", help="List SSH-configured servers")

    status = subparsers.add_parser("status", help="List local workspace bindings")
    status.add_argument("name", nargs="?", help="Connection name; defaults to all workspaces")
    status.add_argument("--watch", action="store_true", help="Refresh the status table")

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
    connect.add_argument("--name", default=None, help="Connection name for codex-rsb reconnect")
    connect.add_argument(
        "--no-shell",
        action="store_true",
        help="Bind/sync only; do not enter the remote shell",
    )

    reconnect = subparsers.add_parser("reconnect", help="Reconnect a named workspace")
    reconnect.add_argument("name", help="Connection name shown by codex-rsb list/status")
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
    forget.add_argument(
        "--local-only",
        action="store_true",
        help="Abandon unreachable remote workspace state",
    )

    conflicts = subparsers.add_parser("conflicts", help="List unresolved conflicts")
    conflicts.add_argument("name", nargs="?", help="Connection name; defaults to current workspace")

    resolve = subparsers.add_parser("resolve", help="Resolve a synchronization conflict")
    resolve.add_argument("path", help="Workspace-relative conflict path")
    winner = resolve.add_mutually_exclusive_group(required=True)
    winner.add_argument("--use-local", action="store_true", help="Keep the local version")
    winner.add_argument("--use-remote", action="store_true", help="Keep the remote version")

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


def open_wrapped_shell(name: str | None) -> int:
    return _open_wrapped_shell_for_record(_record_for_execution(name))


def _open_wrapped_shell_for_record(record: BindingRecord) -> int:
    local_root = _require_live_workspace(record)
    runner = _connected_runner(record.target)
    ensure_daemon(local_root)

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


def _poke_or_restart_daemon(local_root: Path, source: str) -> None:
    if poke_daemon(local_root, source):
        return
    ensure_daemon(local_root)
    if not poke_daemon(local_root, source):
        print(
            f"{_error_prefix()} warning: daemon did not accept sync notification after {source}",
            file=sys.stderr,
        )


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
            raise RegistryError(f"no connection named {name!r}; run codex-rsb status")
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


def _require_live_workspace(record: BindingRecord) -> Path:
    """Resolve a record's local root, failing with an actionable message if it is stale.

    A binding can outlive its local directory (for example a temporary workspace that
    was cleaned up). The low-level daemon only sees a missing marker and reports the
    cryptic "not a bound workspace"; surface the recovery steps instead.
    """
    local = Path(record.local_path)
    if not local.exists():
        raise RegistryError(
            f"connection {record.name!r} points to a missing local path: "
            f"{record.local_path}. "
            f"Run `codex-rsb reconnect {record.name} --local <new-path>` to repair it, "
            f"or `codex-rsb forget {record.name}` to remove it from {registry_path()}"
        )
    try:
        spec = read_workspace_spec(workspace_paths(record.workspace_id).workspace_file)
    except (OSError, ValueError) as exc:
        raise RegistryError(
            f"connection {record.name!r} has invalid external workspace state: {exc}"
        ) from exc
    if Path(spec.local_root).expanduser().resolve(strict=False) != local.resolve(strict=False):
        raise RegistryError(
            f"connection {record.name!r} local path disagrees with external workspace state"
        )
    return local


def enter_and_bind(*, target: str, remote: str, local: Path, open_shell: bool) -> int:
    runner = SubprocessSshRunner()

    def _initial_direction(
        selected_local: Path,
        selected_remote: str,
    ) -> InitialShellDirection:
        from remote_sandbox.policy import POLICY_FILE_NAME, StaticPolicyEngine

        policy = StaticPolicyEngine.from_file(selected_local / POLICY_FILE_NAME)
        try:
            local_has_content = any(
                not policy.is_ignored(entry.name) for entry in selected_local.iterdir()
            )
        except FileNotFoundError:
            local_has_content = False
        remote_has_content = any(
            not policy.is_ignored(name) for name in runner.listdir(target, selected_remote)
        )
        if local_has_content and not remote_has_content:
            return "local-to-remote"
        if not local_has_content and not remote_has_content:
            return "empty"
        return "remote-to-local"

    def _connect(event: ConnectRequestEvent) -> ConnectResponse:
        selected_local = Path(event.local) if event.local is not None else local
        result = bind_workspace(
            target=target,
            remote=event.remote,
            local=selected_local,
            runner=runner,
            confirm=confirm_prompt,
            connection_name=event.name,
        )
        bound_local = Path(result.connection.local_path)
        direction = _initial_direction(bound_local, result.connection.remote_path)
        ensure_daemon(bound_local, runner=runner)
        status = wait_for_daemon_control(bound_local, 5.0)
        if not status.running or status.pid is None:
            raise DaemonError("supervisor did not publish its process state")
        if status.phase.value in {"failed", "stopped"}:
            raise DaemonError(status.last_error or "workspace supervisor failed to start")

        def _ready_probe() -> ReadyProbeResult:
            try:
                current = daemon_control_status(bound_local)
            except DaemonError:
                current = daemon_status(bound_local)
                if (
                    not current.running
                    or current.pid is None
                    or current.phase.value in {"failed", "stopped"}
                    or current.conn_state == "disconnected"
                ):
                    return "stop"
                return "pending"
            if current.phase.value == "ready":
                return "ready"
            if current.phase.value in {"failed", "stopped"}:
                return "stop"
            if current.conn_state == "disconnected":
                return "stop"
            return "pending"

        _print_connection(result.connection)
        return ConnectResponse(
            ok=True,
            workspace_id=result.workspace.workspace_id,
            name=result.connection.name,
            remote_root=result.connection.remote_path,
            direction=direction,
            ready_probe=_ready_probe if direction == "local-to-remote" else None,
        )

    del open_shell
    result = enter_shell_loop(
        target,
        remote,
        nonce=secrets.token_hex(8),
        on_connect_request=_connect,
    )
    return result.exit_code


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    services = default_cli_services()
    try:
        service_commands = {
            "init",
            "status",
            "run",
            "forget",
            "fetch",
            "peek",
            "conflicts",
            "resolve",
        }
        if args.command in service_commands or (
            args.command in {"connect", "reconnect"} and args.no_shell
        ):
            return _dispatch_services(args, services)
        if args.command == "list":
            return list_servers()
        if args.command == "start":
            return start_binding_daemon(args.name)
        if args.command == "stop":
            return stop_binding_daemon(args.name)
        if args.command == "shell":
            if not _has_tty():
                raise ValueError("shell requires an interactive TTY")
            return open_wrapped_shell(args.name)
        if args.command == "set" and args.setting == "placeholder-limit":
            settings = set_placeholder_limit(args.value)
            print(
                "placeholder-limit: "
                f"{format_size_compact(settings.placeholder_limit)} "
                f"({settings_path()})"
            )
            return 0
        if args.command == "enter":
            if not _has_tty():
                raise ValueError("enter requires an interactive TTY")
            return enter_and_bind(
                target=args.target,
                remote=args.remote,
                local=Path(args.local),
                open_shell=True,
            )
        if args.command == "connect":
            if not _has_tty():
                raise ValueError(
                    "interactive shell requires a TTY; rerun in a terminal or pass --no-shell"
                )
            connection = services.connect_workspace(
                args.target,
                args.remote,
                Path(args.local),
                args.name,
            )
            record = connection.record
            _print_connection(record)
            return _open_wrapped_shell_for_record(record)
        if args.command == "reconnect":
            existing = services.find_record(args.name, services.registry)
            if existing is None:
                raise RegistryError(
                    f"no connection named {args.name!r}; run codex-rsb status"
                )
            local = Path(args.local) if args.local is not None else Path(existing.local_path)
            if not args.no_shell and not _has_tty():
                raise ValueError(
                    "interactive shell requires a TTY; rerun in a terminal or pass --no-shell"
                )
            connection = services.connect_workspace(
                existing.target,
                existing.remote_path,
                local,
                existing.name,
            )
            rebound = connection.record
            if args.no_shell:
                services.ensure_supervisor(rebound)
                _print_connection(rebound)
                _print_no_shell_status(
                    rebound,
                    services.workspace_status(rebound),
                    initial=False,
                )
                return 0
            _print_connection(rebound)
            return _open_wrapped_shell_for_record(rebound)
        parser.error(f"unknown command: {args.command}")
    except Exception as exc:
        if args.debug:
            traceback.print_exc()
        else:
            print(f"codex-rsb: {_one_line(str(exc), max_len=500)}", file=sys.stderr)
        return 2
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


def _print_connection(record: BindingRecord) -> None:
    print(
        "Connected "
        f"{record.name}: {record.target}:{record.remote_path} <-> {record.local_path}"
    )


def _print_no_shell_status(
    record: BindingRecord,
    status: WorkspaceStatus,
    *,
    initial: bool,
) -> None:
    if status.phase is WorkspacePhase.INITIAL_SYNCING:
        print("The initial sync continues in background.")
        print(f"Watch progress with `codex-rsb status {record.name} --watch`.")
    elif status.phase is WorkspacePhase.READY:
        print("Initial sync completed." if initial else "Workspace is ready.")
    else:
        print(f"Workspace supervisor phase: {status.phase.value}")


def _has_tty() -> bool:
    return os.isatty(0) and os.isatty(1)


def _error_prefix() -> str:
    return "codex-rsb:"


if __name__ == "__main__":
    raise SystemExit(main())
