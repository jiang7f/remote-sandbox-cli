import base64
import json
import shlex
import threading
import time
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
    assert "read -r __rsb_response" in script
    assert "stty -echo" in script
    assert "rsb()" in script


def test_connect_request_disables_echo_before_requesting_a_response() -> None:
    script = _enter_rcfile()

    assert script.index("stty -echo") < script.index("connect-request")
    marker_line = next(line for line in script.splitlines() if "connect-request" in line)
    assert marker_line.endswith('"$__rsb_payload" > /dev/tty')


def test_response_read_restores_echo_for_success_eof_and_signals() -> None:
    script = _enter_rcfile()
    save = script.index("__rsb_stty=$(stty -g)")
    restore = script.index("trap 'stty")
    signals = script.index("trap 'exit 130' HUP INT TERM")
    disable = script.index("stty -echo")
    marker = script.index("connect-request")
    read = script.index("read -r __rsb_response")
    leave_subshell = script.index('exit "$__rsb_read_status"')

    assert save < restore < signals < disable < marker < read < leave_subshell


def test_local_to_remote_uses_home_as_the_holding_directory() -> None:
    script = _enter_rcfile()
    branch = script.split('if [ "$__rsb_direction" = local-to-remote ] &&', 1)[1]

    assert branch.index('cd -- "$HOME"') < branch.index("__rsb_workspace_holding=$PWD")


def test_ready_transition_has_authenticated_prompt_and_private_readline_trigger() -> None:
    script = _enter_rcfile()

    assert "\\e]777;rsb;prompt;${__rsb_nonce};managed\\a" in script
    assert "\\e]777;rsb;prompt;${__rsb_nonce};enter\\a" in script
    prompt_function = script.split("__rsb_enter_prompt() {", 1)[1].split("}\n", 1)[0]
    assert "printf" not in prompt_function
    assert "bind -x" in script
    assert "__rsb_publish_ready" in script
    assert "READLINE_LINE" in script
    assert "READLINE_POINT" in script
    assert 'bind -x \'"\\C-x\\C-]": __rsb_ready_key\'' in script
    assert "\\e[778~" not in script
    assert 'bind -m emacs-standard \'"\\e[777~": redraw-current-line\'' in script
    assert 'bind -m vi-move \'"\\e[777~": redraw-current-line\'' in script
    assert 'bind -m vi-insertion -x \'"\\C-x\\C-]": __rsb_ready_key\'' in script
    assert 'bind -m vi-insertion \'"\\e[777~": redraw-current-line\'' in script
    assert shell_module._ready_key_sequence() == b"\x18\x1d"
    assert shell_module._redraw_key_sequence() == b"\x1b[777~"
    assert "__rsb_live_key" not in script
    assert shell_module._READY_PROBE_INTERVAL_S >= 0.25


def test_live_prompt_sentinel_has_the_same_fixed_display_width() -> None:
    sentinel = shell_module._prompt_slot_sentinel("abc")

    assert len(sentinel) == 34
    assert sentinel.startswith("[")
    assert sentinel.endswith("]")
    assert sentinel in _enter_rcfile()


@pytest.mark.parametrize(
    ("phase", "progress", "compact"),
    [
        (
            shell_module.WorkspacePhase.INITIAL_SYNCING,
            shell_module.SyncProgress("scanning"),
            "[ZJU_2:dq scanning]",
        ),
        (
            shell_module.WorkspacePhase.INITIAL_SYNCING,
            shell_module.SyncProgress(
                "transferring",
                files_done=40,
                files_total=100,
            ),
            "[ZJU_2:dq sync 40%]",
        ),
        (
            shell_module.WorkspacePhase.READY,
            shell_module.SyncProgress("idle"),
            "[ZJU_2:dq]",
        ),
    ],
)
def test_slot_keeps_readline_width_but_visually_returns_to_compact_width(
    phase: shell_module.WorkspacePhase,
    progress: shell_module.SyncProgress,
    compact: str,
) -> None:
    status = shell_module.WorkspaceStatus(
        phase,
        progress,
    )

    replacement = shell_module._render_prompt_slot("ZJU_2", "dq", status)
    padding = 34 - shell_module.display_width(compact)

    assert replacement == compact + " " * padding + f"\x1b[{padding}D"


def test_long_unicode_live_slot_cannot_overwrite_the_rest_of_the_prompt() -> None:
    status = shell_module.WorkspaceStatus(
        shell_module.WorkspacePhase.INITIAL_SYNCING,
        shell_module.SyncProgress("planning"),
    )

    replacement = shell_module._render_prompt_slot(
        "量子计算中心-long-target",
        "超长工作区-name",
        status,
    )

    prompt, cursor_back = replacement.rsplit("\x1b[", 1)
    assert shell_module.display_width(prompt) == 34
    assert prompt.rstrip().endswith("]")
    assert cursor_back.endswith("D")


def test_connect_request_parser_requires_the_session_nonce_across_chunks() -> None:
    payload = base64.b64encode(
        json.dumps(
            {"remote": "/work/量子 project", "local": "/tmp/local path", "name": "dq"},
            separators=(",", ":"),
        ).encode()
    )
    marker = b"\x1b]777;rsb;connect-request;good;b64:" + payload + b"\x07"
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


def test_prompt_marker_requires_the_session_nonce() -> None:
    enter = b"\x1b]777;rsb;prompt;good;enter\x07"
    managed = b"\x1b]777;rsb;prompt;good;managed\x07"

    assert ShellOutputParser("good").feed(enter) == [
        shell_module.PromptEvent(slot_authorized=False)
    ]
    assert ShellOutputParser("good").feed(managed) == [
        shell_module.PromptEvent(slot_authorized=True)
    ]
    forged = enter.replace(b";good;", b";wrong;")
    assert ShellOutputParser("good").feed(forged) == [BytesEvent(forged)]


def test_real_enter_prompt_does_not_authorize_bare_slot_bytes() -> None:
    marker = b"\x1b]777;rsb;prompt;good;enter\x07"
    visible_prompt = b"\x1b[01;33m[host:enter]\x1b[00m user@host:repo % "
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(marker + visible_prompt + slot)

    assert events[0] == shell_module.PromptEvent(slot_authorized=False)
    assert b"".join(event.data for event in events[1:] if isinstance(event, BytesEvent)) == (
        visible_prompt + slot
    )
    assert not any(isinstance(event, shell_module.PromptSlotEvent) for event in events)


def test_real_enter_prompt_preserves_fragmented_bare_slot_bytes() -> None:
    marker = b"\x1b]777;rsb;prompt;good;enter\x07"
    visible_prompt = b"\x1b[01;33m[host:enter]\x1b[00m % "
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(marker + visible_prompt + slot[:8])
    events.extend(parser.feed(slot[8:23]))
    events.extend(parser.feed(slot[23:]))

    assert events[0] == shell_module.PromptEvent(slot_authorized=False)
    assert b"".join(event.data for event in events[1:] if isinstance(event, BytesEvent)) == (
        visible_prompt + slot
    )


def test_real_enter_prompt_preserves_wrong_nonce_slot_bytes() -> None:
    marker = b"\x1b]777;rsb;prompt;good;enter\x07"
    visible_prompt = b"[host:enter] % "
    wrong_slot = shell_module._prompt_slot_sentinel("wrong").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(marker + visible_prompt + wrong_slot)

    assert events[0] == shell_module.PromptEvent(slot_authorized=False)
    assert b"".join(event.data for event in events[1:] if isinstance(event, BytesEvent)) == (
        visible_prompt + wrong_slot
    )


def test_prompt_slot_is_ordinary_output_without_authenticated_prompt_marker() -> None:
    slot = shell_module._prompt_slot_sentinel("good").encode()

    assert ShellOutputParser("good").feed(slot) == [BytesEvent(slot)]


def test_fragmented_prompt_slot_is_ordinary_output_without_authorization() -> None:
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = []
    for chunk in (slot[:5], slot[5:19], slot[19:]):
        events.extend(parser.feed(chunk))

    assert events == [BytesEvent(slot)]


def test_authenticated_prompt_allows_exactly_one_fragmented_slot() -> None:
    marker = b"\x1b]777;rsb;prompt;good;managed\x07"
    color = b"\x1b[01;36m"
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = []
    for chunk in (
        marker[:13],
        marker[13:] + color + slot[:7],
        slot[7:21],
        slot[21:] + slot,
    ):
        events.extend(parser.feed(chunk))

    assert events == [
        shell_module.PromptEvent(slot_authorized=True),
        BytesEvent(color),
        shell_module.PromptSlotEvent(),
        BytesEvent(slot),
    ]


def test_wrong_nonce_prompt_does_not_authorize_identical_slot_bytes() -> None:
    forged = b"\x1b]777;rsb;prompt;wrong\x07"
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(forged + slot)

    assert b"".join(event.data for event in events if isinstance(event, BytesEvent)) == (
        forged + slot
    )
    assert not any(isinstance(event, shell_module.PromptSlotEvent) for event in events)


def test_wrong_nonce_prompt_does_not_clear_valid_managed_authorization() -> None:
    managed = b"\x1b]777;rsb;prompt;good;managed\x07"
    forged = b"\x1b]777;rsb;prompt;wrong;enter\x07"
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(managed + forged + slot)

    assert events == [
        shell_module.PromptEvent(slot_authorized=True),
        BytesEvent(forged),
        shell_module.PromptSlotEvent(),
    ]


def test_connect_request_clears_unused_prompt_slot_authorization() -> None:
    marker = b"\x1b]777;rsb;prompt;good;managed\x07"
    payload = base64.b64encode(b'{"remote":"/work/dq","local":null,"name":"dq"}')
    connect = b"\x1b]777;rsb;connect-request;good;b64:" + payload + b"\x07"
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(marker + connect + slot)

    assert events == [
        shell_module.PromptEvent(slot_authorized=True),
        ConnectRequestEvent(remote="/work/dq", name="dq"),
        BytesEvent(slot),
    ]


def test_enter_connect_then_managed_prompt_authorizes_one_slot() -> None:
    enter = b"\x1b]777;rsb;prompt;good;enter\x07"
    payload = base64.b64encode(b'{"remote":"/work/dq","local":null,"name":"dq"}')
    connect = b"\x1b]777;rsb;connect-request;good;b64:" + payload + b"\x07"
    managed = b"\x1b]777;rsb;prompt;good;managed\x07"
    color = b"\x1b[01;36m"
    slot = shell_module._prompt_slot_sentinel("good").encode()
    parser = ShellOutputParser("good")

    events = parser.feed(enter + connect + managed + color + slot)

    assert events == [
        shell_module.PromptEvent(slot_authorized=False),
        ConnectRequestEvent(remote="/work/dq", name="dq"),
        shell_module.PromptEvent(slot_authorized=True),
        BytesEvent(color),
        shell_module.PromptSlotEvent(),
    ]


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


def test_success_response_can_request_immediate_workspace_entry() -> None:
    response = ConnectResponse(
        ok=True,
        workspace_id="w1",
        name="dq",
        remote_root="/work/dq",
        direction="local-to-remote",
        enter_immediately=True,
    )

    assert response.encode() == "ok\tw1\tdq\t/work/dq\tlocal-to-remote\t1"


def test_success_response_can_retain_a_local_ready_probe() -> None:
    def probe() -> bool:
        return True

    response = ConnectResponse(
        ok=True,
        workspace_id="w1",
        name="dq",
        remote_root="/work/dq",
        direction="local-to-remote",
        ready_probe=probe,
    )

    assert response.ready_probe is probe
    assert response.encode() == "ok\tw1\tdq\t/work/dq\tlocal-to-remote"


def test_success_response_can_retain_a_local_workspace_status_probe() -> None:
    expected = shell_module.WorkspaceStatus(
        shell_module.WorkspacePhase.INITIAL_SYNCING,
        shell_module.SyncProgress("scanning"),
    )

    def probe() -> shell_module.WorkspaceStatus:
        return expected

    response = ConnectResponse(
        ok=True,
        workspace_id="w1",
        name="dq",
        remote_root="/work/dq",
        direction="remote-to-local",
        status_probe=probe,
    )

    assert response.status_probe is probe
    assert response.encode() == "ok\tw1\tdq\t/work/dq\tremote-to-local"


def test_ready_probe_worker_allows_only_one_in_flight_callback() -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[int] = []
    worker = shell_module._ReadyProbeWorker()

    def blocked_probe() -> str:
        calls.append(1)
        started.set()
        release.wait(timeout=1.0)
        return "pending"

    assert worker.launch(blocked_probe, generation=7) is True
    assert started.wait(timeout=0.5)
    assert worker.launch(blocked_probe, generation=7) is False
    assert worker.launch(blocked_probe, generation=8) is False
    assert calls == [1]

    release.set()
    deadline = time.monotonic() + 0.5
    result = None
    while result is None and time.monotonic() < deadline:
        result = worker.take_result(generation=7)
        time.sleep(0.01)
    assert result == "pending"
    assert calls == [1]


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

    def fake_wait_for_daemon_control(
        local_root: Path,
        timeout: float,
    ) -> SimpleNamespace:
        assert timeout == 5.0
        return fake_daemon_status(local_root)

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
    monkeypatch.setattr(cli, "wait_for_daemon_control", fake_wait_for_daemon_control)
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
            enter_immediately=True,
        )
    ]


def test_enter_and_bind_rejects_durable_status_without_control_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected_local = tmp_path / "local"
    selected_local.mkdir()
    connection = BindingRecord(
        name="dq",
        workspace_id="00000000-0000-4000-8000-000000000014",
        target="host",
        remote_path="/work/dq",
        local_path=str(selected_local),
        updated_at="2026-07-11T00:00:00+00:00",
    )
    captured: list[ConnectResponse] = []

    class FakeRunner:
        def listdir(self, target: str, remote: str) -> list[str]:
            assert (target, remote) == ("host", "/work/dq")
            return ["source.txt"]

    fallback_status = SimpleNamespace(
        running=True,
        pid=1234,
        phase=SimpleNamespace(value="initial-syncing"),
        last_error=None,
    )

    monkeypatch.setattr(
        cli,
        "bind_workspace",
        lambda **_kwargs: SimpleNamespace(
            workspace=SimpleNamespace(workspace_id=connection.workspace_id),
            connection=connection,
        ),
    )
    monkeypatch.setattr(cli, "SubprocessSshRunner", FakeRunner)
    monkeypatch.setattr(cli, "ensure_daemon", lambda *_args, **_kwargs: fallback_status)
    monkeypatch.setattr(cli, "daemon_status", lambda _root: fallback_status)

    def control_unavailable(_root: Path, _timeout: float) -> object:
        raise cli.DaemonError("supervisor control endpoint is unresponsive")

    monkeypatch.setattr(
        cli,
        "wait_for_daemon_control",
        control_unavailable,
        raising=False,
    )

    def fake_enter_shell_loop(
        _target: str,
        _cwd: str,
        *,
        nonce: str,
        on_connect_request: Any,
    ) -> EnterShellResult:
        assert nonce
        try:
            response = on_connect_request(
                ConnectRequestEvent(remote="/work/dq", local=str(selected_local), name="dq")
            )
        except Exception as exc:
            response = ConnectResponse(ok=False, error=str(exc))
        captured.append(response)
        return EnterShellResult(0, "/work/dq", str(selected_local), "dq")

    monkeypatch.setattr(cli, "enter_shell_loop", fake_enter_shell_loop)
    monkeypatch.setattr(cli, "_print_connection", lambda _record: None)

    result = cli.enter_and_bind(target="host", remote="~", local=tmp_path, open_shell=True)

    assert result == 0
    assert captured == [
        ConnectResponse(ok=False, error="supervisor control endpoint is unresponsive")
    ]


def test_local_to_remote_production_connect_enters_immediately_without_ready_probe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected_local = tmp_path / "local"
    selected_local.mkdir()
    (selected_local / "source.txt").write_text("local", encoding="utf-8")
    connection = BindingRecord(
        name="dq",
        workspace_id="00000000-0000-4000-8000-000000000014",
        target="host",
        remote_path="/work/dq",
        local_path=str(selected_local),
        updated_at="2026-07-11T00:00:00+00:00",
    )
    captured: list[ConnectResponse] = []

    class FakeRunner:
        def listdir(self, target: str, remote: str) -> list[str]:
            assert (target, remote) == ("host", "/work/dq")
            return []

    running_status = SimpleNamespace(
        running=True,
        pid=1234,
        phase=SimpleNamespace(value="initial-syncing"),
        last_error=None,
        conn_state="ok",
    )
    monkeypatch.setattr(
        cli,
        "bind_workspace",
        lambda **_kwargs: SimpleNamespace(
            workspace=SimpleNamespace(workspace_id=connection.workspace_id),
            connection=connection,
        ),
    )
    monkeypatch.setattr(cli, "SubprocessSshRunner", FakeRunner)
    monkeypatch.setattr(cli, "ensure_daemon", lambda *_args, **_kwargs: running_status)
    monkeypatch.setattr(cli, "wait_for_daemon_control", lambda *_args: running_status)
    monkeypatch.setattr(cli, "_print_connection", lambda _record: None)

    def fake_enter_shell_loop(
        _target: str,
        _cwd: str,
        *,
        nonce: str,
        on_connect_request: Any,
    ) -> EnterShellResult:
        assert nonce
        captured.append(
            on_connect_request(
                ConnectRequestEvent(remote="/work/dq", local=str(selected_local), name="dq")
            )
        )
        return EnterShellResult(0, "/work/dq", str(selected_local), "dq")

    monkeypatch.setattr(cli, "enter_shell_loop", fake_enter_shell_loop)

    assert cli.enter_and_bind(target="host", remote="~", local=tmp_path, open_shell=True) == 0
    assert captured[0].ready_probe is None
    assert captured[0].enter_immediately is True


def _enter_rcfile() -> str:
    remote_command = build_enter_remote_shell_command("host", "~", nonce="abc")[-1]
    outer_script = shlex.split(remote_command)[2]
    return outer_script.split("cat <<'EOF'\n", 1)[1].split("\nEOF\n", 1)[0]
