from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from . import AGENT_VERSION


def _archive_sha256() -> str:
    digest = hashlib.sha256()
    with Path(sys.argv[0]).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str]) -> int:
    if argv == ["self-check"]:
        print("codex-remote-sandbox-agent " + AGENT_VERSION + " " + _archive_sha256())
        return 0

    request = json.loads(sys.stdin.buffer.readline().decode("utf-8"))
    response = {
        "ok": False,
        "payload": {},
        "error": "unsupported command: " + str(request.get("command")),
    }
    print(json.dumps(response, ensure_ascii=False, separators=(",", ":")))
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
