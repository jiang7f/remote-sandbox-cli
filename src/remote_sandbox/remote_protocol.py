from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AgentRequest:
    command: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AgentResponse:
    ok: bool
    payload: dict[str, Any]
    error: str | None = None


def encode_request(request: AgentRequest) -> bytes:
    return (
        json.dumps(
            {"command": request.command, "payload": request.payload},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def decode_request(data: bytes) -> AgentRequest:
    raw = json.loads(data.decode("utf-8"))
    return AgentRequest(str(raw["command"]), dict(raw["payload"]))


def decode_response(data: bytes) -> AgentResponse:
    raw = json.loads(data.decode("utf-8"))
    return AgentResponse(bool(raw["ok"]), dict(raw.get("payload", {})), raw.get("error"))
