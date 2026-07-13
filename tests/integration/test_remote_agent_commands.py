from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import remote_sandbox.remote_agent.__main__ as agent_main
from remote_sandbox.remote_agent.store import RemoteStore


def _configure_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, str]:
    home = tmp_path / "home"
    root = tmp_path / "workspace"
    home.mkdir()
    root.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("REMOTE_SANDBOX_HOME", str(tmp_path / "control"))
    monkeypatch.setenv("REMOTE_SANDBOX_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.delenv("REMOTE_SANDBOX_HOME", raising=False)
    monkeypatch.delenv("REMOTE_SANDBOX_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("REMOTE_SANDBOX_CONTROL_DIR", raising=False)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    return root, "00000000-0000-4000-8000-000000000170"


def test_in_process_agent_workspace_command_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, workspace_id = _configure_agent(tmp_path, monkeypatch)
    (root / "file.txt").write_bytes(b"content")
    (root / "link").symlink_to("file.txt")
    (root / "dir").mkdir()
    os.mkfifo(root / "pipe")

    registered = agent_main._handle_register(
        {"workspace_id": workspace_id, "root": str(root)}
    )
    status = agent_main._handle_status({"workspace_id": workspace_id})
    snapshot = agent_main._handle_snapshot({"workspace_id": workspace_id})
    metadata = agent_main._handle_metadata_paths(
        {"workspace_id": workspace_id, "paths": ["file.txt", "missing.txt"]}
    )
    hashed = agent_main._handle_hash_paths(
        {
            "workspace_id": workspace_id,
            "paths": ["file.txt", "link", "dir", "pipe", "missing.txt"],
        }
    )
    read_file = agent_main._handle_read_path(
        {"workspace_id": workspace_id, "path": "file.txt"}
    )
    read_link = agent_main._handle_read_path(
        {"workspace_id": workspace_id, "path": "link"}
    )
    read_dir = agent_main._handle_read_path(
        {"workspace_id": workspace_id, "path": "dir"}
    )
    read_missing = agent_main._handle_read_path(
        {"workspace_id": workspace_id, "path": "missing.txt"}
    )

    state_path = Path(str(registered["state_path"]))
    with RemoteStore(state_path) as store:
        event = store.append_event("modify", "file.txt", None)
    assert agent_main._handle_events(
        {"workspace_id": workspace_id, "after_sequence": 0, "follow": False}
    ) == 0
    event_output = capsys.readouterr().out
    acknowledged = agent_main._handle_ack(
        {"workspace_id": workspace_id, "sequence": event.sequence}
    )
    stopped = agent_main._handle_stop({"workspace_id": workspace_id})
    forgotten = agent_main._handle_forget({"workspace_id": workspace_id})
    stopped_again = agent_main._handle_stop({"workspace_id": workspace_id})
    forgotten_again = agent_main._handle_forget({"workspace_id": workspace_id})

    assert status["status"] == "stopped"
    assert status["latest_sequence"] == 0
    assert any(entry["path"] == "file.txt" for entry in snapshot["entries"])
    assert metadata["entries"][0]["content_hash"] is None
    assert [entry["kind"] for entry in hashed["entries"]] == [
        "file",
        "symlink",
        "dir",
        "special",
        "missing",
    ]
    assert read_file["missing"] is False
    assert read_link["missing"] is False
    assert read_dir == {"missing": True, "data": None}
    assert read_missing == {"missing": True, "data": None}
    assert json.loads(event_output)["path"] == "file.txt"
    assert acknowledged["acknowledged_sequence"] == event.sequence
    assert stopped["status"] == "stopped"
    assert forgotten["workspace_id"] == workspace_id
    assert stopped_again["already_forgotten"] is True
    assert forgotten_again["already_forgotten"] is True


def test_execution_environment_captures_interactive_exports_without_leaking_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, workspace_id = _configure_agent(tmp_path, monkeypatch)
    home = Path.home()
    custom_bin = home / "custom-bin"
    custom_bin.mkdir()
    python = custom_bin / "python"
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    python.chmod(0o755)
    secret_value = "remote-only-value"
    (home / ".bashrc").write_text(
        f'export PATH="$HOME/custom-bin:$PATH"\nexport CUSTOM_RUNTIME_TOKEN={secret_value}\n',
        encoding="utf-8",
    )
    agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})

    refreshed = agent_main._handle_execution_environment(
        {"workspace_id": workspace_id, "refresh": True}
    )
    cached = agent_main._handle_execution_environment(
        {"workspace_id": workspace_id, "refresh": False}
    )

    assert refreshed["available"] is True
    assert refreshed["refreshed"] is True
    assert refreshed["python"] == str(python)
    assert refreshed["path"].split(os.pathsep)[0] == str(custom_bin)
    assert refreshed["warning"] is None
    assert secret_value not in json.dumps(refreshed)
    export_file = Path(str(refreshed["export_file"]))
    assert export_file.stat().st_mode & 0o777 == 0o600
    assert f"export CUSTOM_RUNTIME_TOKEN={secret_value}" in export_file.read_text(
        encoding="utf-8"
    )
    assert cached["available"] is True
    assert cached["refreshed"] is False
    assert cached["export_file"] == str(export_file)

    second_bin = home / "second-bin"
    second_bin.mkdir()
    (home / ".bashrc").write_text(
        'export PATH="$HOME/second-bin:$PATH"\n',
        encoding="utf-8",
    )
    invalidated = agent_main._handle_execution_environment(
        {"workspace_id": workspace_id, "refresh": False}
    )

    assert invalidated["refreshed"] is True
    assert invalidated["path"].split(os.pathsep)[0] == str(second_bin)


def test_execution_environment_capture_failure_is_cached_as_a_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, workspace_id = _configure_agent(tmp_path, monkeypatch)
    agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})

    def fail_capture(*_args: object, **_kwargs: object) -> dict[str, str]:
        raise RuntimeError("interactive shell initialization timed out")

    monkeypatch.setattr(agent_main, "_capture_shell_environment", fail_capture)
    refreshed = agent_main._handle_execution_environment(
        {"workspace_id": workspace_id, "refresh": True}
    )
    cached = agent_main._handle_execution_environment(
        {"workspace_id": workspace_id, "refresh": False}
    )

    assert refreshed["available"] is False
    assert refreshed["refreshed"] is True
    assert refreshed["export_file"] is None
    assert "timed out" in str(refreshed["warning"])
    assert cached["available"] is False
    assert cached["refreshed"] is False
    assert cached["warning"] == refreshed["warning"]


@pytest.mark.parametrize(
    "raw_request,error",
    [
        (b"not-json\n", "Expecting value"),
        (b"[]\n", "JSON object"),
        (b'{"command":"unknown"}\n', "unsupported command"),
        (b'{"command":"status","payload":[]}\n', "payload must be a JSON object"),
    ],
)
def test_agent_main_reports_protocol_errors(
    raw_request: bytes,
    error: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdin = io.TextIOWrapper(io.BytesIO(raw_request), encoding="utf-8")
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    assert agent_main.main([]) == 2
    response = json.loads(stdout.getvalue())
    assert response["ok"] is False
    assert error in response["error"]


def test_agent_argument_validators_reject_wrong_types() -> None:
    with pytest.raises(ValueError, match="string"):
        agent_main._expect_string({"value": 1}, "value")
    with pytest.raises(ValueError, match="integer"):
        agent_main._expect_integer({"value": True}, "value")
    with pytest.raises(ValueError, match="boolean"):
        agent_main._expect_boolean({"value": 1}, "value", default=False)
    with pytest.raises(ValueError, match="list"):
        agent_main._expect_relative_paths({"paths": "value"}, "paths")
    with pytest.raises(ValueError, match="relative"):
        agent_main._expect_relative_paths({"paths": ["../escape"]}, "paths")
    with pytest.raises(ValueError, match="relative"):
        agent_main._expect_relative_paths(
            {"paths": ["nested/.remote-sandbox-new-abc/value.txt"]},
            "paths",
        )


def test_agent_start_status_stop_and_forget_state_machine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, workspace_id = _configure_agent(tmp_path, monkeypatch)
    agent_main._handle_register({"workspace_id": workspace_id, "root": str(root)})

    class FakeProcess:
        pid = 424242

        def terminate(self) -> None:
            raise AssertionError("successful start must not terminate the watcher")

        def wait(self, timeout: float) -> int:
            del timeout
            return 0

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    started = agent_main._handle_start({"workspace_id": workspace_id})
    monkeypatch.setattr(agent_main, "_watcher_identity", lambda *args: "current")
    status = agent_main._handle_status({"workspace_id": workspace_id})
    killed: list[tuple[int, int]] = []
    identities = iter(("current", "dead", "dead"))
    monkeypatch.setattr(agent_main, "_watcher_identity", lambda *args: next(identities, "dead"))
    monkeypatch.setattr(agent_main.os, "kill", lambda pid, sig: killed.append((pid, sig)))
    stopped = agent_main._handle_stop({"workspace_id": workspace_id})
    forgotten = agent_main._handle_forget({"workspace_id": workspace_id})

    assert started["pid"] == 424242
    assert started["status"] == "starting"
    assert status["status"] == "starting"
    assert killed and killed[0][0] == 424242
    assert stopped["status"] == "stopped"
    assert forgotten["workspace_id"] == workspace_id
