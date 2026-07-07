from __future__ import annotations

import base64
import json
import os
import pty
import re
import select
import shlex
import sys
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass

from remote_sandbox.registry import RegistryError, validate_connection_name
from remote_sandbox.ssh import validate_remote_path, validate_target

_SAFE_LABEL_CHARS = re.compile(r"[^A-Za-z0-9_.@:-]")


@dataclass(frozen=True, slots=True)
class BytesEvent:
    data: bytes


@dataclass(frozen=True, slots=True)
class BarrierEvent:
    status: int


@dataclass(frozen=True, slots=True)
class ConnectRequestEvent:
    remote: str
    local: str | None = None
    name: str | None = None


@dataclass(frozen=True, slots=True)
class EnterShellResult:
    exit_code: int
    remote: str | None
    local: str | None
    name: str | None = None


ShellEvent = BytesEvent | BarrierEvent | ConnectRequestEvent
ShellBackend = Callable[[list[str], str, Callable[[int], None]], int]
EnterShellBackend = Callable[[list[str], str, Callable[[ShellEvent], None]], int]


def display_label(target: str) -> str:
    return _SAFE_LABEL_CHARS.sub("_", target)


def build_managed_remote_shell_command(target: str, cwd: str, *, nonce: str) -> list[str]:
    validate_target(target)
    validate_remote_path(cwd)
    script = (
        "p=$1\n"
        "label=$2\n"
        "nonce=$3\n"
        'case "$p" in\n'
        '  "~") p=$HOME ;;\n'
        '  "~/"*) p=$HOME/${p#"~/"} ;;\n'
        "esac\n"
        'cd -- "$p" || exit\n'
        "export RSB_DISPLAY_LABEL=$label\n"
        "if command -v bash >/dev/null 2>&1; then\n"
        "  umask 077\n"
        "  rc=$(mktemp \"${TMPDIR:-/tmp}/remote-sandbox-rc.XXXXXX\") || exit 2\n"
        "  {\n"
        "    printf '__rsb_nonce=%s\\n' \"$nonce\"\n"
        "    cat <<'EOF'\n"
        "if [ -r ~/.bashrc ]; then\n"
        "  . ~/.bashrc\n"
        "fi\n"
        "__rsb_prompt() {\n"
        "  local s=$?\n"
        "  printf '\\033]777;remote-sandbox;cmd-done;%s;%s\\007' "
        "\"$__rsb_nonce\" \"$s\"\n"
        "  return \"$s\"\n"
        "}\n"
        "PROMPT_COMMAND=__rsb_prompt${PROMPT_COMMAND:+;$PROMPT_COMMAND}\n"
        "if [ -n \"${PS1:-}\" ]; then\n"
        "  PS1='[${RSB_DISPLAY_LABEL}] '\"$PS1\"\n"
        "else\n"
        "  PS1='[${RSB_DISPLAY_LABEL}] ${USER:-user}@\\h \\W % '\n"
        "fi\n"
        "trap 'rm -f \"${BASH_SOURCE[0]}\"' EXIT\n"
        "EOF\n"
        "  } > \"$rc\"\n"
        "  exec bash --noprofile --rcfile \"$rc\" -i\n"
        "fi\n"
        'printf "remote-sandbox shell requires bash on the remote host\\n" >&2\n'
        "exit 127\n"
    )
    remote_command = " ".join(
        [
            "sh",
            "-c",
            shlex.quote(script),
            "sh",
            shlex.quote(cwd),
            shlex.quote(display_label(target)),
            shlex.quote(nonce),
        ]
    )
    return ["ssh", "-tt", target, remote_command]


def build_enter_remote_shell_command(target: str, cwd: str, *, nonce: str) -> list[str]:
    validate_target(target)
    validate_remote_path(cwd)
    script = (
        "p=$1\n"
        "label=$2\n"
        "nonce=$3\n"
        'case "$p" in\n'
        '  "~") p=$HOME ;;\n'
        '  "~/"*) p=$HOME/${p#"~/"} ;;\n'
        "esac\n"
        'cd -- "$p" || exit\n'
        "export RSB_DISPLAY_LABEL=$label\n"
        "if command -v bash >/dev/null 2>&1; then\n"
        "  umask 077\n"
        "  rc=$(mktemp \"${TMPDIR:-/tmp}/remote-sandbox-enter-rc.XXXXXX\") || exit 2\n"
        "  {\n"
        "    printf '__rsb_nonce=%s\\n' \"$nonce\"\n"
        "    cat <<'EOF'\n"
        "if [ -r ~/.bashrc ]; then\n"
        "  . ~/.bashrc\n"
        "fi\n"
        "rsb() {\n"
        "  if [ \"${1:-}\" = connect ]; then\n"
        "    shift\n"
        "    local __rsb_remote=\"\"\n"
        "    local __rsb_local=\"\"\n"
        "    local __rsb_name=\"\"\n"
        "    while [ \"$#\" -gt 0 ]; do\n"
        "      case \"$1\" in\n"
        "        -r|--remote)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __rsb_remote=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        --name)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __rsb_name=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        -l|--local)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __rsb_local=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        --)\n"
        "          shift\n"
        "          break\n"
        "          ;;\n"
        "        -*)\n"
        "          printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "          return 2\n"
        "          ;;\n"
        "        *)\n"
        "          if [ -n \"$__rsb_remote\" ]; then\n"
        "            printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __rsb_remote=$1\n"
        "          shift\n"
        "          ;;\n"
        "      esac\n"
        "    done\n"
        "    if [ \"$#\" -gt 0 ]; then\n"
        "      printf 'usage: rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "      return 2\n"
        "    fi\n"
        "    if [ -z \"$__rsb_remote\" ]; then\n"
        "      __rsb_remote=$PWD\n"
        "    fi\n"
        "    case \"$__rsb_remote\" in\n"
        "      '~') __rsb_remote=$HOME ;;\n"
        "      '~/'*) __rsb_remote=$HOME/${__rsb_remote#'~/'} ;;\n"
        "      /*) ;;\n"
        "      *) __rsb_remote=$PWD/$__rsb_remote ;;\n"
        "    esac\n"
        "    if [ -d \"$__rsb_remote\" ]; then\n"
        "      __rsb_remote=$(cd -- \"$__rsb_remote\" && pwd -P) || return\n"
        "    fi\n"
        "    __rsb_payload=$(python3 -c 'import base64,json,sys; "
        "data={\"remote\":sys.argv[1],\"local\":sys.argv[2] or None,"
        "\"name\":sys.argv[3] or None}; "
        "print(base64.b64encode(json.dumps(data,separators=(\",\",\":\")).encode()).decode())' "
        "\"$__rsb_remote\" \"$__rsb_local\" \"$__rsb_name\") || return\n"
        "    printf '\\033]777;remote-sandbox;connect-request;%s;b64:%s\\007' "
        "\"$__rsb_nonce\" \"$__rsb_payload\"\n"
        "    exit 0\n"
        "  fi\n"
        "  command rsb \"$@\"\n"
        "}\n"
        "if [ -n \"${PS1:-}\" ]; then\n"
        "  PS1='[${RSB_DISPLAY_LABEL}] '\"$PS1\"\n"
        "else\n"
        "  PS1='[${RSB_DISPLAY_LABEL}] ${USER:-user}@\\h \\W % '\n"
        "fi\n"
        "trap 'rm -f \"${BASH_SOURCE[0]}\"' EXIT\n"
        "EOF\n"
        "  } > \"$rc\"\n"
        "  exec bash --noprofile --rcfile \"$rc\" -i\n"
        "fi\n"
        'printf "remote-sandbox enter requires bash on the remote host\\n" >&2\n'
        "exit 127\n"
    )
    remote_command = " ".join(
        [
            "sh",
            "-c",
            shlex.quote(script),
            "sh",
            shlex.quote(cwd),
            shlex.quote(display_label(target)),
            shlex.quote(nonce),
        ]
    )
    return ["ssh", "-tt", target, remote_command]


class ShellOutputParser:
    def __init__(self, nonce: str) -> None:
        self._cmd_done_prefix = f"\x1b]777;remote-sandbox;cmd-done;{nonce};".encode()
        self._connect_request_prefix = (
            f"\x1b]777;remote-sandbox;connect-request;{nonce};".encode()
        )
        self._prefixes = (self._cmd_done_prefix, self._connect_request_prefix)
        self._buffer = b""

    def feed(self, data: bytes) -> list[ShellEvent]:
        self._buffer += data
        events: list[ShellEvent] = []
        while self._buffer:
            start, prefix = self._find_next_marker(self._buffer)
            if start == -1:
                keep = max(
                    _longest_suffix_prefix_overlap(self._buffer, candidate)
                    for candidate in self._prefixes
                )
                emit_len = len(self._buffer) - keep
                if emit_len:
                    events.append(BytesEvent(self._buffer[:emit_len]))
                    self._buffer = self._buffer[emit_len:]
                break
            if start:
                events.append(BytesEvent(self._buffer[:start]))
                self._buffer = self._buffer[start:]
            assert prefix is not None
            end = self._buffer.find(b"\x07", len(prefix))
            if end == -1:
                break
            payload = self._buffer[len(prefix) : end]
            event = self._parse_marker(prefix, payload)
            if event is None:
                events.append(BytesEvent(self._buffer[: end + 1]))
            else:
                events.append(event)
            self._buffer = self._buffer[end + 1 :]
        return events

    def flush(self) -> list[ShellEvent]:
        if not self._buffer:
            return []
        data = self._buffer
        self._buffer = b""
        return [BytesEvent(data)]

    def _find_next_marker(self, data: bytes) -> tuple[int, bytes | None]:
        best_start = -1
        best_prefix: bytes | None = None
        for prefix in self._prefixes:
            start = data.find(prefix)
            if start != -1 and (best_start == -1 or start < best_start):
                best_start = start
                best_prefix = prefix
        return best_start, best_prefix

    def _parse_marker(self, prefix: bytes, payload: bytes) -> ShellEvent | None:
        if prefix == self._cmd_done_prefix:
            try:
                return BarrierEvent(status=int(payload.decode("ascii")))
            except ValueError:
                return None
        if prefix == self._connect_request_prefix:
            try:
                text = payload.decode("utf-8")
                remote, local, name = _split_connect_payload(text)
                validate_remote_path(remote)
                if local is not None and _has_control_char(local):
                    raise ValueError
                if name is not None:
                    validate_connection_name(name)
            except (ValueError, RegistryError):
                return None
            return ConnectRequestEvent(remote=remote, local=local, name=name)
        return None


def process_shell_output(
    chunks: Iterable[bytes],
    *,
    nonce: str,
    write_output: Callable[[bytes], None],
    on_barrier: Callable[[int], None],
    on_connect_request: Callable[[ConnectRequestEvent], None] | None = None,
) -> None:
    parser = ShellOutputParser(nonce)
    for chunk in chunks:
        for event in parser.feed(chunk):
            if isinstance(event, BytesEvent):
                write_output(event.data)
            elif isinstance(event, BarrierEvent):
                on_barrier(event.status)
            elif on_connect_request is not None:
                on_connect_request(event)
    for event in parser.flush():
        if isinstance(event, BytesEvent):
            write_output(event.data)
        elif isinstance(event, BarrierEvent):
            on_barrier(event.status)
        elif on_connect_request is not None:
            on_connect_request(event)


def managed_shell_loop(
    target: str,
    cwd: str,
    *,
    nonce: str,
    on_barrier: Callable[[int], None],
    backend: ShellBackend | None = None,
) -> int:
    argv = build_managed_remote_shell_command(target, cwd, nonce=nonce)
    shell_backend = backend or _pty_shell_backend
    return shell_backend(argv, nonce, on_barrier)


def enter_shell_loop(
    target: str,
    cwd: str,
    *,
    nonce: str,
    backend: EnterShellBackend | None = None,
) -> EnterShellResult:
    argv = build_enter_remote_shell_command(target, cwd, nonce=nonce)
    selected_remote: str | None = None
    selected_local: str | None = None
    selected_name: str | None = None

    def on_event(event: ShellEvent) -> None:
        nonlocal selected_remote, selected_local, selected_name
        if isinstance(event, BytesEvent):
            os.write(sys.stdout.fileno(), event.data)
        elif isinstance(event, ConnectRequestEvent):
            selected_remote = event.remote
            selected_local = event.local
            selected_name = event.name

    shell_backend = backend or _pty_enter_shell_backend
    exit_code = shell_backend(argv, nonce, on_event)
    return EnterShellResult(
        exit_code=exit_code,
        remote=selected_remote,
        local=selected_local,
        name=selected_name,
    )


def _pty_shell_backend(argv: list[str], nonce: str, on_barrier: Callable[[int], None]) -> int:
    pid, master_fd = pty.fork()
    if pid == 0:
        os.execvp(argv[0], argv)
    parser = ShellOutputParser(nonce)
    try:
        while True:
            readable, _, _ = select.select([master_fd, sys.stdin.fileno()], [], [])
            if master_fd in readable:
                try:
                    data = os.read(master_fd, 4096)
                except OSError:
                    break
                if not data:
                    break
                _handle_shell_events(parser.feed(data), on_barrier=on_barrier)
            if sys.stdin.fileno() in readable:
                data = os.read(sys.stdin.fileno(), 4096)
                if not data:
                    break
                os.write(master_fd, data)
    finally:
        _handle_shell_events(parser.flush(), on_barrier=on_barrier)
        with suppress(OSError):
            os.close(master_fd)
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return 1


def _pty_enter_shell_backend(
    argv: list[str],
    nonce: str,
    on_event: Callable[[ShellEvent], None],
) -> int:
    pid, master_fd = pty.fork()
    if pid == 0:
        os.execvp(argv[0], argv)
    parser = ShellOutputParser(nonce)
    try:
        while True:
            readable, _, _ = select.select([master_fd, sys.stdin.fileno()], [], [])
            if master_fd in readable:
                try:
                    data = os.read(master_fd, 4096)
                except OSError:
                    break
                if not data:
                    break
                for event in parser.feed(data):
                    on_event(event)
            if sys.stdin.fileno() in readable:
                data = os.read(sys.stdin.fileno(), 4096)
                if not data:
                    break
                os.write(master_fd, data)
    finally:
        for event in parser.flush():
            on_event(event)
        with suppress(OSError):
            os.close(master_fd)
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return 1


def _handle_shell_events(
    events: list[ShellEvent],
    *,
    on_barrier: Callable[[int], None],
) -> None:
    for event in events:
        if isinstance(event, BytesEvent):
            os.write(sys.stdout.fileno(), event.data)
        elif isinstance(event, BarrierEvent):
            on_barrier(event.status)


def _longest_suffix_prefix_overlap(data: bytes, prefix: bytes) -> int:
    max_len = min(len(data), len(prefix) - 1)
    for length in range(max_len, 0, -1):
        if data[-length:] == prefix[:length]:
            return length
    return 0


def _split_connect_payload(payload: str) -> tuple[str, str | None, str | None]:
    if payload.startswith("b64:"):
        try:
            raw = base64.b64decode(payload.removeprefix("b64:"), validate=True)
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ValueError("invalid connect payload") from exc
        if not isinstance(data, dict):
            raise ValueError("invalid connect payload")
        remote = data.get("remote")
        local = data.get("local")
        name = data.get("name")
        if not isinstance(remote, str):
            raise ValueError("invalid connect payload")
        if local is not None and not isinstance(local, str):
            raise ValueError("invalid connect payload")
        if name is not None and not isinstance(name, str):
            raise ValueError("invalid connect payload")
        return remote, local or None, name or None
    if "\t" not in payload:
        return payload, None, None
    parts = payload.split("\t")
    remote = parts[0]
    local = parts[1] if len(parts) > 1 and parts[1] else None
    name = parts[2] if len(parts) > 2 and parts[2] else None
    if len(parts) > 3:
        raise ValueError("invalid connect payload")
    return remote, local, name


def _has_control_char(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)
