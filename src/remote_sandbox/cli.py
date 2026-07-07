from __future__ import annotations

import argparse
import os
import secrets
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from remote_sandbox.bind import BindError, bind_workspace
from remote_sandbox.daemon import (
    DaemonError,
    StopResult,
    daemon_status,
    ensure_daemon,
    poke_daemon,
    stop_daemon_result,
)
from remote_sandbox.fetch import FetchError, fetch_placeholders
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
    format_size_compact,
    load_settings,
    set_placeholder_limit,
    settings_path,
)
from remote_sandbox.shell import enter_shell_loop
from remote_sandbox.ssh import SshError, SubprocessSshRunner
from remote_sandbox.ssh_config import SshHost, load_configured_hosts
from remote_sandbox.sync import SyncExecutionError

CLI_ERRORS = (
    BindError,
    FetchError,
    PeekError,
    RegistryError,
    SettingsError,
    SshError,
    SyncExecutionError,
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
        runner=SubprocessSshRunner(),
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
        runner=SubprocessSshRunner(),
        path=path,
        lines=lines,
        tail=tail,
    )
    sys.stdout.buffer.write(content)
    return 0


def list_servers() -> int:
    settings = load_settings()
    hosts = load_configured_hosts(require_identity=True)
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
    status = ensure_daemon(Path(record.local_path))
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
    local_root = Path(record.local_path)
    ensure_daemon(local_root)
    runner = SubprocessSshRunner()

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


def _ensure_daemon_for_record(record: BindingRecord) -> None:
    ensure_daemon(Path(record.local_path))


def run_remote_command(items: list[str]) -> int:
    record_name, command_argv = _parse_run_items(items)
    record = _record_for_execution(record_name)
    local_root = Path(record.local_path)
    ensure_daemon(local_root)
    result = SubprocessSshRunner().run_command(
        record.target,
        record.remote_path,
        tuple(command_argv),
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    _poke_or_restart_daemon(local_root, "run")
    return result.returncode


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


def enter_and_bind(*, target: str, remote: str, local: Path, open_shell: bool) -> int:
    enter_result = enter_shell_loop(target, remote, nonce=secrets.token_hex(8))
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
            runner=SubprocessSshRunner(),
            confirm=confirm_prompt,
            connection_name=enter_result.name,
        )
    except CLI_ERRORS as exc:
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
                runner=SubprocessSshRunner(),
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
                runner=SubprocessSshRunner(),
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
            removed = delete_binding_record(args.name)
        except CLI_ERRORS as exc:
            print(f"{_error_prefix()} {exc}", file=sys.stderr)
            return 2
        if not removed:
            print(f"{_error_prefix()} no connection named {args.name!r}", file=sys.stderr)
            return 2
        print(f"Forgot connection {args.name}")
        return 0

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
    lines = [
        f"placeholder-limit: {format_size_compact(placeholder_limit)}",
        "",
        "TARGET | BOUND | CONNECTIONS | CPU | MEM | GPU",
    ]
    for host in hosts:
        target_records = sorted(records_by_target.get(host.alias, ()), key=lambda item: item.name)
        bound = "yes" if target_records else "-"
        names = ", ".join(record.name for record in target_records) if target_records else "-"
        cpu, mem, gpu = (
            _list_resource_columns(probes[host.alias])
            if host.alias in probes
            else ("pending", "pending", "pending")
        )
        lines.append(
            " | ".join(
                [
                    host.alias,
                    bound,
                    names,
                    cpu,
                    mem,
                    gpu,
                ]
            )
        )
    return "\n".join(lines)


def _format_status_table(
    records: list[BindingRecord],
    *,
    current_workspace_id: str | None,
) -> str:
    sorted_records = sorted(
        records,
        key=lambda record: (record.target, record.name, record.local_path, record.remote_path),
    )
    lines = ["NAME | REMOTE | LOCAL | REMOTE_PATH | DAEMON | CURRENT"]
    for record in sorted_records:
        current = "*" if record.workspace_id == current_workspace_id else ""
        daemon = _daemon_column(record)
        lines.append(
            f"{record.name} | {record.target} | "
            f"{record.local_path} | {record.remote_path} | {daemon} | {current}"
        )
    return "\n".join(lines)


def _daemon_column(record: BindingRecord) -> str:
    status = daemon_status(Path(record.local_path))
    if not status.running:
        return "stopped"
    return f"running:{status.pid}" if status.pid is not None else "running"


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
