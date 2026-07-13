from __future__ import annotations

import io
from dataclasses import fields
from pathlib import Path

import pytest

import remote_sandbox.cli as cli_module
from remote_sandbox.cli import build_parser


def test_parser_exposes_confirmed_commands_and_debug_flag() -> None:
    parser = build_parser()

    status = parser.parse_args(["--debug", "status", "dq", "--watch", "--paths"])
    assert status.debug is True
    assert status.name == "dq"
    assert status.watch is True
    assert status.paths is True
    assert parser.parse_args(["conflicts", "dq"]).command == "conflicts"
    skill = parser.parse_args(["skill", "install", "--force"])
    assert skill.skill_command == "install"
    assert skill.force is True
    uninstall = parser.parse_args(["skill", "uninstall"])
    assert uninstall.skill_command == "uninstall"
    assert uninstall.force is False
    resolved = parser.parse_args(["resolve", "model.py", "--use-local"])
    assert resolved.use_local is True
    forgotten = parser.parse_args(["forget", "dq", "--local-only"])
    assert forgotten.local_only is True
    no_shell = parser.parse_args(
        [
            "connect",
            "host",
            "--remote",
            "/work/dq",
            "--name",
            "dq",
            "--no-shell",
            "--yes",
        ]
    )
    assert no_shell.no_shell is True
    assert no_shell.yes is True
    automatic = parser.parse_args(
        ["connect", "host", "--auto-remote", "--local", "/work/project"]
    )
    assert automatic.auto_remote is True
    assert automatic.remote is None
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "connect",
                "host",
                "--remote",
                "/work/project",
                "--auto-remote",
            ]
        )


def test_version_flag_prints_the_installed_version(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        build_parser().parse_args(["--version"])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.startswith("rsb ")


def test_status_watch_screen_uses_alternate_screen_only_for_tty() -> None:
    class TtyBuffer(io.StringIO):
        def isatty(self) -> bool:
            return True

    terminal = TtyBuffer()
    with cli_module._status_watch_screen(terminal) as interactive:
        assert interactive is True
        terminal.write("frame")
    rendered = terminal.getvalue()
    assert rendered.startswith(cli_module._ALT_SCREEN_ENTER)
    assert rendered.endswith(cli_module._ALT_SCREEN_LEAVE)

    redirected = io.StringIO()
    with cli_module._status_watch_screen(redirected) as interactive:
        assert interactive is False
        redirected.write("frame")
    assert redirected.getvalue() == "frame"


def test_automatic_remote_workspace_path_is_stable_and_does_not_expose_local_path(
    tmp_path: Path,
) -> None:
    local = tmp_path / "Project Name"

    first = cli_module.automatic_remote_workspace_path(local, "Demo Project")
    second = cli_module.automatic_remote_workspace_path(local, "Demo Project")

    assert first == second
    assert first.startswith("~/rsb-workspaces/demo-project-")
    assert str(tmp_path) not in first


def test_resolve_requires_exactly_one_selected_source() -> None:
    parser = build_parser()

    assert parser.parse_args(["resolve", "model.py", "--use-local"]).use_local
    assert parser.parse_args(["resolve", "model.py", "--use-remote"]).use_remote
    with pytest.raises(SystemExit):
        parser.parse_args(["resolve", "model.py"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["resolve", "model.py", "--use-local", "--use-remote"]
        )


def test_run_preserves_arguments_after_double_dash() -> None:
    parsed = build_parser().parse_args(
        ["run", "dq", "--", "python", "-c", "print('--flag')", "--flag"]
    )

    assert parsed.items == [
        "dq",
        "--",
        "python",
        "-c",
        "print('--flag')",
        "--flag",
    ]


def test_cli_exposes_in_process_service_harness_types() -> None:
    service_fields = {field.name for field in fields(cli_module.CliServices)}
    result_fields = {field.name for field in fields(cli_module.CapturedCliResult)}

    assert {"registry", "cwd", "workspace_status", "run_remote"} <= service_fields
    assert result_fields == {"exit_code", "stdout", "stderr"}
    assert callable(cli_module.invoke_cli)


@pytest.mark.parametrize(
    "argv,command",
    [
        (["list"], "list"),
        (["set", "placeholder-limit", "10MB"], "set"),
        (["enter", "host"], "enter"),
        (["connect", "host", "--remote", "/work/dq"], "connect"),
        (["reconnect", "dq"], "reconnect"),
        (["start", "dq"], "start"),
        (["stop", "dq"], "stop"),
        (["shell", "dq"], "shell"),
        (["run", "dq", "--", "true"], "run"),
        (["fetch", "weights.bin"], "fetch"),
        (["peek", "weights.bin"], "peek"),
    ],
)
def test_parser_preserves_existing_command_surface(
    argv: list[str],
    command: str,
) -> None:
    assert build_parser().parse_args(argv).command == command
