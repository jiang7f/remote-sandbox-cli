from __future__ import annotations

import base64
import json

import pytest

import remote_sandbox.daemon as daemon_module
import remote_sandbox.shell as shell
from remote_sandbox.shell import BarrierEvent, BytesEvent, ConnectRequestEvent, ConnectResponse
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus


def test_managed_shell_command_and_backend_adapter() -> None:
    argv = shell.build_managed_remote_shell_command("user@host", "~/work dir", nonce="n1")
    calls: list[tuple[list[str], str]] = []
    barriers: list[int] = []

    def backend(command: list[str], nonce: str, on_barrier) -> int:
        calls.append((command, nonce))
        on_barrier(7)
        return 19

    result = shell.managed_shell_loop(
        "user@host",
        "~/work dir",
        nonce="n1",
        on_barrier=barriers.append,
        backend=backend,
    )

    assert argv[0] == "ssh"
    assert argv[-2] == "user@host"
    assert "cmd-done" in argv[-1]
    assert "printf '\\033]777;remote-sandbox;cmd-done" not in argv[-1]
    assert "cmd-done;${__rsb_nonce};${__rsb_last_status}" in argv[-1]
    assert "rsb;prompt;${__rsb_nonce};managed" in argv[-1]
    assert "bind -m emacs-standard" in argv[-1]
    assert "redraw-current-line" in argv[-1]
    assert shell._prompt_slot_sentinel("n1") in argv[-1]
    assert result == 19
    assert calls[0][1] == "n1"
    assert barriers == [7]
    assert shell.display_label("user host/$") == "user_host__"


def test_child_terminal_uses_xterm_only_for_missing_or_dumb_term(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for original in (None, "", "dumb"):
        if original is None:
            monkeypatch.delenv("TERM", raising=False)
        else:
            monkeypatch.setenv("TERM", original)
        shell._prepare_child_terminal_environment()
        assert shell.os.environ["TERM"] == "xterm"

    monkeypatch.setenv("TERM", "xterm-256color")
    shell._prepare_child_terminal_environment()
    assert shell.os.environ["TERM"] == "xterm-256color"


def test_enter_shell_loop_reports_selection_success_error_and_cancel() -> None:
    payload = ConnectRequestEvent("/work/dq", "/local", "dq")

    def success_backend(argv: list[str], nonce: str, handler) -> int:
        assert argv[0] == "ssh" and nonce == "nonce"
        assert handler(payload).ok is True
        return 3

    result = shell.enter_shell_loop(
        "host",
        "~",
        nonce="nonce",
        on_connect_request=lambda event: ConnectResponse(
            True,
            workspace_id="00000000-0000-4000-8000-000000000190",
            name=event.name,
            remote_root=event.remote,
            direction="empty",
        ),
        backend=success_backend,
    )
    assert result == shell.EnterShellResult(3, "/work/dq", "/local", "dq")

    def error_backend(argv: list[str], nonce: str, handler) -> int:
        del argv, nonce
        response = handler(payload)
        assert response.ok is False and response.error == "boom"
        return 2

    assert shell.enter_shell_loop(
        "host",
        "~",
        nonce="nonce",
        on_connect_request=lambda _event: (_ for _ in ()).throw(RuntimeError("boom")),
        backend=error_backend,
    ).exit_code == 2

    def cancel_backend(argv: list[str], nonce: str, handler) -> int:
        del argv, nonce
        response = handler(payload)
        assert response.ok is False and response.error == "Binding cancelled"
        return 130

    assert shell.enter_shell_loop(
        "host",
        "~",
        nonce="nonce",
        on_connect_request=lambda _event: (_ for _ in ()).throw(KeyboardInterrupt()),
        backend=cancel_backend,
    ).exit_code == 130


def test_process_shell_output_dispatches_bytes_barrier_and_connect() -> None:
    nonce = "expected"
    payload = base64.b64encode(
        json.dumps({"remote": "/work", "local": None, "name": "dq"}).encode()
    )
    connect = b"\x1b]777;rsb;connect-request;expected;b64:" + payload + b"\x07"
    barrier = b"\x1b]777;remote-sandbox;cmd-done;expected;9\x07"
    output: list[bytes] = []
    barriers: list[int] = []
    connections: list[ConnectRequestEvent] = []

    shell.process_shell_output(
        [b"plain", connect[:20], connect[20:] + barrier + b"tail"],
        nonce=nonce,
        write_output=output.append,
        on_barrier=barriers.append,
        on_connect_request=connections.append,
    )

    assert b"".join(output) == b"plaintail"
    assert barriers == [9]
    assert connections == [ConnectRequestEvent("/work", None, "dq")]


def test_shell_event_output_and_terminal_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    writes: list[tuple[int, bytes]] = []
    barriers: list[int] = []
    monkeypatch.setattr(shell.os, "write", lambda fd, data: writes.append((fd, data)))

    shell._handle_shell_events(
        [BytesEvent(b"data"), BarrierEvent(4)],
        on_barrier=barriers.append,
    )

    monkeypatch.setattr(
        shell.termios,
        "tcgetattr",
        lambda _fd: (_ for _ in ()).throw(shell.termios.error()),
    )
    with shell._raw_terminal(0):
        pass
    assert len(writes) == 1 and writes[0][1] == b"data"
    assert barriers == [4]


def test_shell_payload_probe_and_path_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    encoded = "b64:" + base64.b64encode(
        b'{"remote":"/work","local":"/local","name":"dq"}'
    ).decode()
    assert shell._split_connect_payload(encoded) == ("/work", "/local", "dq")
    assert shell._split_connect_payload("/work\t/local\tdq") == ("/work", "/local", "dq")
    with pytest.raises(ValueError, match="invalid"):
        shell._split_connect_payload("b64:not-base64")
    with pytest.raises(ValueError, match="invalid"):
        shell._split_connect_payload("a\tb\tc\td")
    assert shell._protocol_error_text("line\nbreak") == "line break"
    assert shell._protocol_error_text(None) == "binding failed"
    assert shell._resolve_test_remote_cwd("/work", "../tmp") == "/tmp"
    assert shell._resolve_test_remote_cwd("/work", "~/repo") == "/home/test/repo"

    status = WorkspaceStatus(WorkspacePhase.READY, SyncProgress("idle"))
    monkeypatch.setattr(daemon_module, "daemon_workspace_status", lambda _workspace_id: status)
    probe = shell._status_probe_for_workspace("00000000-0000-4000-8000-000000000191")
    assert probe is not None and probe() == status
    assert shell._status_probe_for_workspace("bad") is None
    assert shell._status_probe_for_workspace(None) is None

    result = shell._run_session_probe(
        lambda: (_ for _ in ()).throw(RuntimeError("ready")),
        lambda: (_ for _ in ()).throw(RuntimeError("status")),
        include_status=True,
    )
    assert result.ready == "pending" and result.status is None
    callback = shell._session_probe_callback(lambda: True, lambda: status, include_status=True)
    assert callback().ready is True
