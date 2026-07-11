import base64
import json
import shlex
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import remote_sandbox.cli as cli
import remote_sandbox.shell as shell_module
from remote_sandbox.bind import BindError
from remote_sandbox.registry import BindingRecord
from remote_sandbox.shell import (
    BytesEvent,
    ConnectRequestEvent,
    ConnectResponse,
    EnterShellResult,
    ShellOutputParser,
    build_enter_remote_shell_command,
    enter_shell_loop,
)


def test_connect_request_does_not_emit_exit_or_close_session() -> None:
    command = build_enter_remote_shell_command("host", "~", nonce="abc")
    script = command[-1]

    assert "connect-request" in script
    assert "exit 0" not in script
    assert "read -r __codex_response" in script
    assert "stty -echo" in script
    assert "codex-rsb()" in script


def test_connect_request_disables_echo_before_requesting_a_response() -> None:
    script = _enter_rcfile()

    assert script.index("stty -echo") < script.index("connect-request")
    marker_line = next(line for line in script.splitlines() if "connect-request" in line)
    assert marker_line.endswith('"$__codex_payload" > /dev/tty')


def test_response_read_restores_echo_for_success_eof_and_signals() -> None:
    script = _enter_rcfile()
    save = script.index("__codex_stty=$(stty -g)")
    restore = script.index("trap 'stty")
    signals = script.index("trap 'exit 130' HUP INT TERM")
    disable = script.index("stty -echo")
    marker = script.index("connect-request")
    read = script.index("read -r __codex_response")
    leave_subshell = script.index('exit "$__codex_read_status"')

    assert save < restore < signals < disable < marker < read < leave_subshell


def test_local_to_remote_uses_home_as_the_holding_directory() -> None:
    script = _enter_rcfile()
    branch = script.split('if [ "$__codex_direction" = local-to-remote ]; then', 1)[1]

    assert branch.index('cd -- "$HOME"') < branch.index("__codex_workspace_holding=$PWD")


def test_connect_request_parser_requires_the_session_nonce_across_chunks() -> None:
    payload = base64.b64encode(
        json.dumps(
            {"remote": "/work/量子 project", "local": "/tmp/local path", "name": "dq"},
            separators=(",", ":"),
        ).encode()
    )
    marker = b"\x1b]777;codex-rsb;connect-request;good;b64:" + payload + b"\x07"
    parser = ShellOutputParser("good")

    events = []
    for chunk in (marker[:11], marker[11:39], marker[39:]):
        events.extend(parser.feed(chunk))

    assert events == [
        ConnectRequestEvent(
            remote="/work/量子 project",
            local="/tmp/local path",
            name="dq",
        )
    ]

    forged = marker.replace(b";good;", b";wrong;")
    assert ShellOutputParser("good").feed(forged) == [BytesEvent(forged)]


def test_raw_terminal_restores_attributes_after_an_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    original = ["original"]
    monkeypatch.setattr(
        shell_module.termios,
        "tcgetattr",
        lambda fd: calls.append(("get", fd)) or original,
    )
    monkeypatch.setattr(
        shell_module.termios,
        "tcsetattr",
        lambda fd, when, attrs: calls.append(("restore", (fd, when, attrs))),
    )
    monkeypatch.setattr(
        shell_module.tty,
        "setraw",
        lambda fd: calls.append(("raw", fd)),
    )

    with pytest.raises(RuntimeError, match="stop"), shell_module._raw_terminal(7):
        raise RuntimeError("stop")

    assert calls == [
        ("get", 7),
        ("raw", 7),
        ("restore", (7, shell_module.termios.TCSADRAIN, original)),
    ]


def test_success_response_activates_managed_prompt() -> None:
    response = ConnectResponse(
        ok=True,
        workspace_id="w1",
        name="dq",
        remote_root="/work/dq",
        direction="remote-to-local",
    )

    assert response.encode() == "ok\tw1\tdq\t/work/dq\tremote-to-local"


def test_success_response_preserves_unicode_and_spaces() -> None:
    response = ConnectResponse(
        ok=True,
        workspace_id="w1",
        name="dq",
        remote_root="/work/量子 project",
        direction="local-to-remote",
    )

    assert response.encode() == "ok\tw1\tdq\t/work/量子 project\tlocal-to-remote"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workspace_id", "w1\tforged"),
        ("name", "dq\nerror"),
        ("remote_root", "/work/dq\rerror"),
    ],
)
def test_success_response_rejects_protocol_control_characters(
    field: str,
    value: str,
) -> None:
    values = {
        "workspace_id": "w1",
        "name": "dq",
        "remote_root": "/work/dq",
    }
    values[field] = value
    response = ConnectResponse(ok=True, direction="remote-to-local", **values)

    with pytest.raises(ValueError, match="protocol control"):
        response.encode()


def test_error_response_is_one_terminal_line() -> None:
    response = ConnectResponse(ok=False, error="cancelled\ntry again\tlater")

    assert response.encode() == "error\tcancelled try again later"


@pytest.mark.parametrize(
    "failure",
    [BindError("binding failed"), KeyboardInterrupt()],
)
def test_connect_failure_returns_error_without_closing_the_shell(
    failure: BaseException,
) -> None:
    captured: list[ConnectResponse] = []

    def backend(
        _argv: list[str],
        _nonce: str,
        request: Any,
    ) -> int:
        captured.append(
            request(ConnectRequestEvent(remote="/work/dq", name="dq"))
        )
        return 0

    def fail(_event: ConnectRequestEvent) -> ConnectResponse:
        raise failure

    result = enter_shell_loop(
        "host",
        "~",
        nonce="abc",
        on_connect_request=fail,
        backend=backend,
    )

    assert result.exit_code == 0
    assert captured == [
        ConnectResponse(
            ok=False,
            error="Binding cancelled" if isinstance(failure, KeyboardInterrupt) else str(failure),
        )
    ]


def test_enter_and_bind_responds_after_initial_sync_state_is_published(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected_local = tmp_path / "local project"
    selected_local.mkdir()
    connection = BindingRecord(
        name="dq",
        workspace_id="00000000-0000-4000-8000-000000000014",
        target="host",
        remote_path="/work/量子 project",
        local_path=str(selected_local),
        updated_at="2026-07-11T00:00:00+00:00",
    )
    calls: list[str] = []
    captured_response: list[ConnectResponse] = []

    class FakeRunner:
        def listdir(self, target: str, remote: str) -> list[str]:
            assert target == "host"
            assert remote == "/work/量子 project"
            return ["source.txt"]

    def fake_bind_workspace(**kwargs: object) -> SimpleNamespace:
        calls.append("metadata-committed")
        assert kwargs["remote"] == "/work/量子 project"
        assert kwargs["connection_name"] == "dq"
        return SimpleNamespace(
            workspace=SimpleNamespace(workspace_id=connection.workspace_id),
            connection=connection,
        )

    def fake_ensure_daemon(local_root: Path, *, runner: object) -> SimpleNamespace:
        calls.append("pid-control-published")
        assert local_root == selected_local
        assert isinstance(runner, FakeRunner)
        return SimpleNamespace(
            running=True,
            pid=1234,
            phase=SimpleNamespace(value="starting"),
            last_error=None,
        )

    def fake_daemon_status(local_root: Path) -> SimpleNamespace:
        calls.append("initial-syncing-published")
        assert local_root == selected_local
        return SimpleNamespace(
            running=True,
            pid=1234,
            phase=SimpleNamespace(value="initial-syncing"),
            last_error=None,
        )

    def fake_enter_shell_loop(
        target: str,
        cwd: str,
        *,
        nonce: str,
        on_connect_request: Any,
    ) -> EnterShellResult:
        assert target == "host"
        assert cwd == "~"
        assert nonce
        response = on_connect_request(
            ConnectRequestEvent(
                remote="/work/量子 project",
                local=str(selected_local),
                name="dq",
            )
        )
        calls.append("response-sent")
        captured_response.append(response)
        return EnterShellResult(0, "/work/量子 project", str(selected_local), "dq")

    monkeypatch.setattr(cli, "SubprocessSshRunner", FakeRunner)
    monkeypatch.setattr(cli, "bind_workspace", fake_bind_workspace)
    monkeypatch.setattr(cli, "ensure_daemon", fake_ensure_daemon)
    monkeypatch.setattr(cli, "daemon_status", fake_daemon_status)
    monkeypatch.setattr(cli, "enter_shell_loop", fake_enter_shell_loop)
    monkeypatch.setattr(cli, "_print_connection", lambda _record: None)
    monkeypatch.setattr(
        cli,
        "_open_wrapped_shell_for_record",
        lambda _record: pytest.fail("must not open a replacement SSH shell"),
    )

    result = cli.enter_and_bind(target="host", remote="~", local=tmp_path, open_shell=True)

    assert result == 0
    assert calls == [
        "metadata-committed",
        "pid-control-published",
        "initial-syncing-published",
        "response-sent",
    ]
    assert captured_response == [
        ConnectResponse(
            ok=True,
            workspace_id=connection.workspace_id,
            name="dq",
            remote_root="/work/量子 project",
            direction="remote-to-local",
        )
    ]


def _enter_rcfile() -> str:
    remote_command = build_enter_remote_shell_command("host", "~", nonce="abc")[-1]
    outer_script = shlex.split(remote_command)[2]
    return outer_script.split("cat <<'EOF'\n", 1)[1].split("\nEOF\n", 1)[0]
