from __future__ import annotations

import base64

import remote_sandbox.shell as shell_module
from remote_sandbox.shell import BytesEvent, ConnectRequestEvent, ShellOutputParser


def test_connect_marker_with_wrong_nonce_is_printed_as_plain_output() -> None:
    data = b"\x1b]777;codex-rsb;connect-request;forged;b64:e30=\x07"

    assert ShellOutputParser("expected").feed(data) == [BytesEvent(data)]


def test_legacy_or_forged_protocol_marker_is_plain_output() -> None:
    data = b"\x1b]777;codex-remote-sandbox;connect-request;expected;b64:e30=\x07"

    assert ShellOutputParser("expected").feed(data) == [BytesEvent(data)]


def test_connect_marker_rejects_control_paths_without_consuming_output() -> None:
    payload = base64.b64encode(b'{"remote":"/work/line\\nbreak","name":"dq"}')
    data = b"\x1b]777;codex-rsb;connect-request;expected;b64:" + payload + b"\x07"

    assert ShellOutputParser("expected").feed(data) == [BytesEvent(data)]


def test_valid_connect_marker_requires_expected_nonce_and_payload() -> None:
    payload = base64.b64encode(b'{"remote":"/work/dq","name":"dq"}')
    data = b"\x1b]777;codex-rsb;connect-request;expected;b64:" + payload + b"\x07"

    assert ShellOutputParser("expected").feed(data) == [
        ConnectRequestEvent(remote="/work/dq", name="dq")
    ]


def test_prompt_sentinel_requires_managed_marker_and_is_single_use() -> None:
    slot = shell_module._prompt_slot_sentinel("expected").encode("ascii")
    managed = b"\x1b]777;codex-rsb;prompt;expected;managed\x07"
    parser = ShellOutputParser("expected")

    assert parser.feed(slot) == [BytesEvent(slot)]
    assert parser.feed(managed + slot + slot) == [
        shell_module.PromptEvent(slot_authorized=True),
        shell_module.PromptSlotEvent(),
        BytesEvent(slot),
    ]


def test_wrong_nonce_prompt_cannot_authorize_expected_sentinel() -> None:
    forged = b"\x1b]777;codex-rsb;prompt;wrong;managed\x07"
    slot = shell_module._prompt_slot_sentinel("expected").encode("ascii")

    events = ShellOutputParser("expected").feed(forged + slot)

    assert b"".join(event.data for event in events if isinstance(event, BytesEvent)) == (
        forged + slot
    )
    assert not any(isinstance(event, shell_module.PromptSlotEvent) for event in events)
