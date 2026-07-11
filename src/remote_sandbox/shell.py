from __future__ import annotations

import base64
import fcntl
import json
import os
import posixpath
import pty
import re
import select
import shlex
import struct
import sys
import termios
import time
import tty
from collections.abc import Callable, Iterable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import Literal, Protocol

from remote_sandbox.registry import RegistryError, validate_connection_name
from remote_sandbox.ssh import ssh_control_opts, validate_remote_path, validate_target

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
class PromptEvent:
    pass


InitialShellDirection = Literal["local-to-remote", "remote-to-local", "empty"]
ReadyProbeResult = Literal["pending", "ready", "stop"]
_READY_PROBE_INTERVAL_S = 0.25


@dataclass(frozen=True, slots=True)
class ConnectResponse:
    ok: bool
    workspace_id: str | None = None
    name: str | None = None
    remote_root: str | None = None
    direction: InitialShellDirection | None = None
    error: str | None = None
    ready_probe: Callable[[], ReadyProbeResult | bool] | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    def encode(self) -> str:
        if not self.ok:
            return "\t".join(("error", _protocol_error_text(self.error)))
        fields = (self.workspace_id, self.name, self.remote_root, self.direction)
        if any(value is None or not value for value in fields):
            raise ValueError("successful connect response is incomplete")
        encoded = tuple(str(value) for value in fields)
        if any(_has_control_char(value) for value in encoded):
            raise ValueError("connect response contains a protocol control character")
        if self.direction not in {"local-to-remote", "remote-to-local", "empty"}:
            raise ValueError("connect response has an invalid initial direction")
        return "\t".join(("ok", *encoded))


class _ManagedSessionBackend(Protocol):
    remote_shell_pid: int


class ManagedShellSession:
    def __init__(self, *, backend: _ManagedSessionBackend, nonce: str) -> None:
        self.backend = backend
        self.nonce = nonce
        self.prompt_mode = "enter"
        self.remote_cwd = "/home/test"
        self._holding_cwd: str | None = None
        self._pending_workspace_cwd: str | None = None
        self._output: list[str] = []

    @property
    def remote_shell_pid(self) -> int:
        return self.backend.remote_shell_pid

    def feed_user_input(self, data: bytes) -> None:
        text = data.decode("utf-8", errors="replace")
        for line in text.splitlines():
            try:
                argv = shlex.split(line)
            except ValueError:
                continue
            if len(argv) == 2 and argv[0] == "cd":
                self.remote_cwd = _resolve_test_remote_cwd(self.remote_cwd, argv[1])

    def handle_connect_response(self, response: ConnectResponse) -> None:
        if not response.ok:
            self._output.append(_protocol_error_text(response.error) + "\n")
            return
        self.activate_workspace(
            response,
            direction=response.direction or "remote-to-local",
        )

    def activate_workspace(
        self,
        response: ConnectResponse,
        *,
        direction: str,
    ) -> None:
        if not response.ok or response.remote_root is None:
            raise ValueError("workspace activation requires a successful response")
        if direction not in {"local-to-remote", "remote-to-local", "empty"}:
            raise ValueError("invalid initial shell direction")
        self.prompt_mode = "managed"
        if direction == "local-to-remote":
            self._holding_cwd = self.remote_cwd
            self._pending_workspace_cwd = response.remote_root
            return
        self.remote_cwd = response.remote_root
        self._holding_cwd = None
        self._pending_workspace_cwd = None

    def publish_ready(self) -> None:
        pending = self._pending_workspace_cwd
        holding = self._holding_cwd
        if pending is not None and holding is not None and self.remote_cwd == holding:
            self.remote_cwd = pending
        self._holding_cwd = None
        self._pending_workspace_cwd = None

    def captured_output(self) -> str:
        return "".join(self._output)


@dataclass(frozen=True, slots=True)
class EnterShellResult:
    exit_code: int
    remote: str | None
    local: str | None
    name: str | None = None


ShellEvent = BytesEvent | BarrierEvent | ConnectRequestEvent | PromptEvent
ShellBackend = Callable[[list[str], str, Callable[[int], None]], int]
ConnectRequestHandler = Callable[[ConnectRequestEvent], ConnectResponse]
EnterShellBackend = Callable[[list[str], str, ConnectRequestHandler], int]


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
        "if [ -f /etc/bash.bashrc ]; then . /etc/bash.bashrc; fi\n"
        "if [ -f ~/.bashrc ]; then . ~/.bashrc; fi\n"
        "__rsb_prompt() {\n"
        "  local s=$?\n"
        "  printf '\\033]777;remote-sandbox;cmd-done;%s;%s\\007' "
        "\"$__rsb_nonce\" \"$s\"\n"
        "  PS1='\\[\\e[01;36m\\][${RSB_DISPLAY_LABEL}]\\[\\e[00m\\] "
        "${CONDA_PROMPT_MODIFIER}\\[\\e[01;32m\\]${USER:-user}@\\h\\[\\e[00m\\]:"
        "\\[\\e[01;34m\\]\\W\\[\\e[00m\\] % '\n"
        "}\n"
        "PROMPT_COMMAND=__rsb_prompt\n"
        "trap 'rm -f \"${BASH_SOURCE[0]}\"' EXIT\n"
        "EOF\n"
        "  } > \"$rc\"\n"
        "  exec bash --noprofile --rcfile \"$rc\" -i\n"
        "fi\n"
        'PS1="[${RSB_DISPLAY_LABEL}] ${USER:-user}@$(hostname) $(basename "$PWD") % "\n'
        'exec "${SHELL:-/bin/sh}" -i\n'
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
    return ["ssh", *ssh_control_opts(), "-tt", target, remote_command]


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
        "export CODEX_RSB_DISPLAY_LABEL=$label\n"
        "if command -v bash >/dev/null 2>&1; then\n"
        "  umask 077\n"
        "  rc=$(mktemp \"${TMPDIR:-/tmp}/codex-rsb-enter-rc.XXXXXX\") || exit 2\n"
        "  {\n"
        "    printf '__codex_nonce=%s\\n' \"$nonce\"\n"
        "    cat <<'EOF'\n"
        "if [ -f /etc/bash.bashrc ]; then . /etc/bash.bashrc; fi\n"
        "if [ -f ~/.bashrc ]; then . ~/.bashrc; fi\n"
        "__codex_prompt_mode=enter\n"
        "__codex_workspace_holding=\n"
        "__codex_workspace_pending_root=\n"
        "codex-rsb() {\n"
        "  if [ \"${1:-}\" = connect ]; then\n"
        "    shift\n"
        "    local __codex_remote=\"\"\n"
        "    local __codex_local=\"\"\n"
        "    local __codex_name=\"\"\n"
        "    local __codex_payload=\"\"\n"
        "    local __codex_response=\"\"\n"
        "    local __codex_status=\"\"\n"
        "    local __codex_workspace_id=\"\"\n"
        "    local __codex_workspace_name=\"\"\n"
        "    local __codex_workspace_root=\"\"\n"
        "    local __codex_direction=\"\"\n"
        "    while [ \"$#\" -gt 0 ]; do\n"
        "      case \"$1\" in\n"
        "        -r|--remote)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __codex_remote=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        --name)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __codex_name=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        -l|--local)\n"
        "          if [ \"$#\" -lt 2 ]; then\n"
        "            printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __codex_local=$2\n"
        "          shift 2\n"
        "          ;;\n"
        "        --)\n"
        "          shift\n"
        "          break\n"
        "          ;;\n"
        "        -*)\n"
        "          printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "          return 2\n"
        "          ;;\n"
        "        *)\n"
        "          if [ -n \"$__codex_remote\" ]; then\n"
        "            printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "            return 2\n"
        "          fi\n"
        "          __codex_remote=$1\n"
        "          shift\n"
        "          ;;\n"
        "      esac\n"
        "    done\n"
        "    if [ \"$#\" -gt 0 ]; then\n"
        "      printf 'usage: codex-rsb connect [--remote remote-path] "
        "[--local local-path] [--name name]\\n' >&2\n"
        "      return 2\n"
        "    fi\n"
        "    if [ -z \"$__codex_remote\" ]; then\n"
        "      __codex_remote=$PWD\n"
        "    fi\n"
        "    case \"$__codex_remote\" in\n"
        "      '~') __codex_remote=$HOME ;;\n"
        "      '~/'*) __codex_remote=$HOME/${__codex_remote#'~/'} ;;\n"
        "      /*) ;;\n"
        "      *) __codex_remote=$PWD/$__codex_remote ;;\n"
        "    esac\n"
        "    if [ -d \"$__codex_remote\" ]; then\n"
        "      __codex_remote=$(cd -- \"$__codex_remote\" && pwd -P) || return\n"
        "    fi\n"
        "    __codex_payload=$(python3 -c 'import base64,json,sys; "
        "data={\"remote\":sys.argv[1],\"local\":sys.argv[2] or None,"
        "\"name\":sys.argv[3] or None}; "
        "print(base64.b64encode(json.dumps(data,separators=(\",\",\":\")).encode()).decode())' "
        "\"$__codex_remote\" \"$__codex_local\" \"$__codex_name\") || return\n"
        "    __codex_response=$(\n"
        "      __codex_stty=$(stty -g) || exit 1\n"
        "      trap 'stty \"$__codex_stty\" >/dev/null 2>&1' EXIT\n"
        "      trap 'exit 130' HUP INT TERM\n"
        "      stty -echo || exit 1\n"
        "      printf '\\033]777;codex-rsb;connect-request;%s;b64:%s\\007' "
        "\"$__codex_nonce\" \"$__codex_payload\" > /dev/tty\n"
        "      IFS= read -r __codex_response\n"
        "      __codex_read_status=$?\n"
        "      printf '%s' \"$__codex_response\"\n"
        "      exit \"$__codex_read_status\"\n"
        "    )\n"
        "    if [ \"$?\" -ne 0 ]; then\n"
        "      printf 'codex-rsb: binding response cancelled\\n' >&2\n"
        "      return 1\n"
        "    fi\n"
        "    case \"$__codex_response\" in\n"
        "      error$'\\t'*)\n"
        "        printf 'codex-rsb: %s\\n' \"${__codex_response#*$'\\t'}\" >&2\n"
        "        return 1\n"
        "        ;;\n"
        "      ok$'\\t'*)\n"
        "        IFS=$'\\t' read -r __codex_status __codex_workspace_id "
        "__codex_workspace_name __codex_workspace_root __codex_direction "
        "<<< \"$__codex_response\"\n"
        "        ;;\n"
        "      *)\n"
        "        printf 'codex-rsb: invalid binding response\\n' >&2\n"
        "        return 1\n"
        "        ;;\n"
        "    esac\n"
        "    if [ \"$__codex_status\" != ok ] || "
        "[ -z \"$__codex_workspace_id\" ] || [ -z \"$__codex_workspace_name\" ] || "
        "[ -z \"$__codex_workspace_root\" ]; then\n"
        "      printf 'codex-rsb: invalid binding response\\n' >&2\n"
        "      return 1\n"
        "    fi\n"
        "    case \"$__codex_direction\" in\n"
        "      local-to-remote|remote-to-local|empty) ;;\n"
        "      *)\n"
        "        printf 'codex-rsb: invalid binding response\\n' >&2\n"
        "        return 1\n"
        "        ;;\n"
        "    esac\n"
        "    export CODEX_RSB_WORKSPACE_ID=$__codex_workspace_id\n"
        "    export CODEX_RSB_WORKSPACE_NAME=$__codex_workspace_name\n"
        "    export CODEX_RSB_WORKSPACE_ROOT=$__codex_workspace_root\n"
        "    __codex_prompt_mode=managed\n"
        "    if [ \"$__codex_direction\" = local-to-remote ]; then\n"
        "      cd -- \"$HOME\" || return 1\n"
        "      __codex_workspace_holding=$PWD\n"
        "      __codex_workspace_pending_root=$__codex_workspace_root\n"
        "    else\n"
        "      cd -- \"$__codex_workspace_root\" || return 1\n"
        "      __codex_workspace_holding=\n"
        "      __codex_workspace_pending_root=\n"
        "    fi\n"
        "    return 0\n"
        "  fi\n"
        "  command codex-rsb \"$@\"\n"
        "}\n"
        "__codex_rsb_publish_ready() {\n"
        "  if [ -n \"$__codex_workspace_pending_root\" ] && "
        "[ \"$PWD\" = \"$__codex_workspace_holding\" ]; then\n"
        "    cd -- \"$__codex_workspace_pending_root\" || return 1\n"
        "  fi\n"
        "  __codex_workspace_holding=\n"
        "  __codex_workspace_pending_root=\n"
        "}\n"
        "__codex_rsb_ready_key() {\n"
        "  local __codex_saved_line=$READLINE_LINE\n"
        "  local __codex_saved_point=$READLINE_POINT\n"
        "  __codex_rsb_publish_ready\n"
        "  READLINE_LINE=$__codex_saved_line\n"
        "  READLINE_POINT=$__codex_saved_point\n"
        "}\n"
        "bind -x '\"\\C-x\\C-]\": __codex_rsb_ready_key'\n"
        "__codex_enter_prompt() {\n"
        "  printf '\\033]777;codex-rsb;prompt;%s\\007' \"$__codex_nonce\"\n"
        "  if [ \"$__codex_prompt_mode\" = managed ]; then\n"
        "    PS1='\\[\\e[01;36m\\][${CODEX_RSB_DISPLAY_LABEL}:"
        "${CODEX_RSB_WORKSPACE_NAME}]\\[\\e[00m\\] ${CONDA_PROMPT_MODIFIER}"
        "\\[\\e[01;32m\\]${USER:-user}@\\h\\[\\e[00m\\]:"
        "\\[\\e[01;34m\\]\\W\\[\\e[00m\\] % '\n"
        "  else\n"
        "    PS1='\\[\\e[01;33m\\][${CODEX_RSB_DISPLAY_LABEL}:enter]\\[\\e[00m\\] "
        "${CONDA_PROMPT_MODIFIER}\\[\\e[01;32m\\]${USER:-user}@\\h\\[\\e[00m\\]:"
        "\\[\\e[01;34m\\]\\W\\[\\e[00m\\] % '\n"
        "  fi\n"
        "}\n"
        "PROMPT_COMMAND=__codex_enter_prompt\n"
        "trap 'rm -f \"${BASH_SOURCE[0]}\"' EXIT\n"
        "EOF\n"
        "  } > \"$rc\"\n"
        "  exec bash --noprofile --rcfile \"$rc\" -i\n"
        "fi\n"
        'printf "codex-rsb enter requires bash on the remote host\\n" >&2\n'
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
    return ["ssh", *ssh_control_opts(), "-tt", target, remote_command]


class ShellOutputParser:
    def __init__(self, nonce: str) -> None:
        self._cmd_done_prefix = f"\x1b]777;remote-sandbox;cmd-done;{nonce};".encode()
        self._connect_request_prefix = (
            f"\x1b]777;codex-rsb;connect-request;{nonce};".encode()
        )
        self._prompt_prefix = f"\x1b]777;codex-rsb;prompt;{nonce}".encode()
        self._prefixes = (
            self._cmd_done_prefix,
            self._connect_request_prefix,
            self._prompt_prefix,
        )
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
        if prefix == self._prompt_prefix and not payload:
            return PromptEvent()
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
            elif isinstance(event, ConnectRequestEvent) and on_connect_request is not None:
                on_connect_request(event)
    for event in parser.flush():
        if isinstance(event, BytesEvent):
            write_output(event.data)
        elif isinstance(event, BarrierEvent):
            on_barrier(event.status)
        elif isinstance(event, ConnectRequestEvent) and on_connect_request is not None:
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
    on_connect_request: ConnectRequestHandler | None = None,
    backend: EnterShellBackend | None = None,
) -> EnterShellResult:
    argv = build_enter_remote_shell_command(target, cwd, nonce=nonce)
    selected_remote: str | None = None
    selected_local: str | None = None
    selected_name: str | None = None

    def handle_request(event: ConnectRequestEvent) -> ConnectResponse:
        nonlocal selected_remote, selected_local, selected_name
        selected_remote = event.remote
        selected_local = event.local
        selected_name = event.name
        if on_connect_request is None:
            return ConnectResponse(ok=False, error="binding service unavailable")
        try:
            return on_connect_request(event)
        except KeyboardInterrupt:
            return ConnectResponse(ok=False, error="Binding cancelled")
        except Exception as exc:
            return ConnectResponse(ok=False, error=str(exc))

    shell_backend = backend or _pty_enter_shell_backend
    exit_code = shell_backend(argv, nonce, handle_request)
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
    stdin_fd = sys.stdin.fileno()
    with _raw_terminal(stdin_fd), _mirrored_window_size(master_fd):
        try:
            while True:
                readable, _, _ = select.select([master_fd, stdin_fd], [], [])
                if master_fd in readable:
                    try:
                        data = os.read(master_fd, 4096)
                    except OSError:
                        break
                    if not data:
                        break
                    _handle_shell_events(parser.feed(data), on_barrier=on_barrier)
                if stdin_fd in readable:
                    data = os.read(stdin_fd, 4096)
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
    on_connect_request: ConnectRequestHandler,
) -> int:
    pid, master_fd = pty.fork()
    if pid == 0:
        os.execvp(argv[0], argv)
    parser = ShellOutputParser(nonce)
    ready_probe: Callable[[], ReadyProbeResult | bool] | None = None
    ready_latched = False
    at_prompt = False
    next_ready_probe_at = 0.0
    stdin_fd = sys.stdin.fileno()
    try:
        original_terminal = termios.tcgetattr(stdin_fd)
    except termios.error:
        original_terminal = None
    if original_terminal is not None:
        tty.setraw(stdin_fd)
    with _mirrored_window_size(master_fd):
        try:
            while True:
                timeout: float | None = None
                if ready_probe is not None:
                    timeout = max(0.0, next_ready_probe_at - time.monotonic())
                readable, _, _ = select.select([master_fd, stdin_fd], [], [], timeout)
                if master_fd in readable:
                    try:
                        data = os.read(master_fd, 4096)
                    except OSError:
                        break
                    if not data:
                        break
                    for event in parser.feed(data):
                        if isinstance(event, BytesEvent):
                            os.write(sys.stdout.fileno(), event.data)
                        elif isinstance(event, PromptEvent):
                            at_prompt = True
                        elif isinstance(event, ConnectRequestEvent):
                            if original_terminal is not None:
                                termios.tcsetattr(
                                    stdin_fd,
                                    termios.TCSADRAIN,
                                    original_terminal,
                                )
                            try:
                                response = on_connect_request(event)
                                payload = response.encode().encode("utf-8") + b"\n"
                            except Exception as exc:
                                error_response = ConnectResponse(ok=False, error=str(exc))
                                payload = (
                                    error_response.encode().encode("utf-8") + b"\n"
                                )
                            finally:
                                if original_terminal is not None:
                                    tty.setraw(stdin_fd)
                            os.write(master_fd, payload)
                            if (
                                response.ok
                                and response.direction == "local-to-remote"
                                and response.ready_probe is not None
                            ):
                                ready_probe = response.ready_probe
                                ready_latched = False
                                next_ready_probe_at = time.monotonic()
                if stdin_fd in readable:
                    data = os.read(stdin_fd, 4096)
                    if not data:
                        break
                    if any(char in data for char in b"\r\n\x03\x04\x1a"):
                        at_prompt = False
                    os.write(master_fd, data)
                now = time.monotonic()
                if ready_probe is not None and now >= next_ready_probe_at:
                    next_ready_probe_at = now + _READY_PROBE_INTERVAL_S
                    try:
                        probe_result = ready_probe()
                    except Exception:
                        probe_result = "pending"
                    if probe_result is True or probe_result == "ready":
                        ready_probe = None
                        ready_latched = True
                    elif probe_result == "stop":
                        ready_probe = None
                if ready_latched and at_prompt:
                    os.write(master_fd, _ready_key_sequence())
                    ready_latched = False
                    at_prompt = False
        finally:
            for event in parser.flush():
                if isinstance(event, BytesEvent):
                    os.write(sys.stdout.fileno(), event.data)
            if original_terminal is not None:
                termios.tcsetattr(stdin_fd, termios.TCSADRAIN, original_terminal)
            with suppress(OSError):
                os.close(master_fd)
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return 1


@contextmanager
def _raw_terminal(fd: int):  # type: ignore[no-untyped-def]
    try:
        old_attrs = termios.tcgetattr(fd)
    except termios.error:
        yield
        return
    try:
        tty.setraw(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)


@contextmanager
def _mirrored_window_size(master_fd: int):  # type: ignore[no-untyped-def]
    _copy_window_size(master_fd)
    yield


def _copy_window_size(master_fd: int) -> None:
    try:
        size = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, b"\0" * 8)
        rows, cols, xpix, ypix = struct.unpack("HHHH", size)
        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, xpix, ypix))
    except OSError:
        return


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


def _protocol_error_text(error: str | None) -> str:
    value = error or "binding failed"
    printable = "".join(" " if _has_control_char(char) else char for char in value)
    return " ".join(printable.split()) or "binding failed"


def _ready_key_sequence() -> bytes:
    return b"\x18\x1d"


def _resolve_test_remote_cwd(current: str, requested: str) -> str:
    if requested == "~":
        return "/home/test"
    if requested.startswith("~/"):
        return posixpath.normpath(posixpath.join("/home/test", requested[2:]))
    if requested.startswith("/"):
        return posixpath.normpath(requested)
    return posixpath.normpath(posixpath.join(current, requested))
