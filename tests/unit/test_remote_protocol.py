from remote_sandbox.remote_protocol import (
    AgentRequest,
    AgentResponse,
    decode_request,
    decode_response,
    encode_request,
)


def test_agent_protocol_round_trips_unicode_paths_without_shell_quoting() -> None:
    request = AgentRequest("register", {"workspace_id": "w1", "root": "/home/user/示例项目"})

    encoded = encode_request(request)

    assert encoded.endswith(b"\n")
    assert encoded.count(b"\n") == 1
    assert "示例项目".encode() in encoded
    assert decode_request(encoded) == request


def test_agent_protocol_decodes_success_and_error_responses() -> None:
    success = '{"ok":true,"payload":{"path":"/数据"}}\n'.encode()

    assert decode_response(success) == AgentResponse(
        ok=True,
        payload={"path": "/数据"},
    )
    assert decode_response(b'{"ok":false,"payload":{},"error":"unsupported"}\n') == (
        AgentResponse(ok=False, payload={}, error="unsupported")
    )
