from __future__ import annotations

import json
from dataclasses import dataclass

from remote_sandbox.manifest import normalize_relative_path

PLACEHOLDER_MAGIC = b"REMOTE-SANDBOX PLACEHOLDER\n"
PLACEHOLDER_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class PlaceholderMetadata:
    path: str
    size: int
    mtime_ns: int
    content_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_relative_path(self.path))
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ValueError("placeholder size must be a non-negative integer")
        if isinstance(self.mtime_ns, bool) or not isinstance(self.mtime_ns, int):
            raise ValueError("placeholder mtime_ns must be an integer")
        if not isinstance(self.content_hash, str) or not self.content_hash:
            raise ValueError("placeholder content hash must not be empty")
        if any(ord(char) < 32 or ord(char) == 127 for char in self.content_hash):
            raise ValueError("placeholder content hash contains a control character")


def encode_placeholder(metadata: PlaceholderMetadata) -> bytes:
    payload = {
        "schema_version": PLACEHOLDER_SCHEMA_VERSION,
        "path": metadata.path,
        "size": metadata.size,
        "mtime_ns": metadata.mtime_ns,
        "content_hash": metadata.content_hash,
    }
    body = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return PLACEHOLDER_MAGIC + body + b"\n"


def decode_placeholder(data: bytes, *, expected_path: str) -> PlaceholderMetadata | None:
    if not data.startswith(PLACEHOLDER_MAGIC):
        return None

    try:
        decoded = data[len(PLACEHOLDER_MAGIC) :].decode("utf-8")
        payload = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("placeholder metadata is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError("placeholder metadata is invalid: expected an object")

    expected_keys = {"schema_version", "path", "size", "mtime_ns", "content_hash"}
    if set(payload) != expected_keys:
        raise ValueError("placeholder metadata is invalid: unexpected fields")
    schema_version = payload["schema_version"]
    if type(schema_version) is not int or schema_version != PLACEHOLDER_SCHEMA_VERSION:
        raise ValueError("placeholder metadata is invalid: unsupported schema version")
    metadata_path = payload["path"]
    if not isinstance(metadata_path, str):
        raise ValueError("placeholder metadata is invalid: path must be a string")

    try:
        metadata = PlaceholderMetadata(
            path=metadata_path,
            size=payload["size"],
            mtime_ns=payload["mtime_ns"],
            content_hash=payload["content_hash"],
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"placeholder metadata is invalid: {exc}") from exc

    normalized_expected = normalize_relative_path(expected_path)
    if metadata.path != normalized_expected:
        raise ValueError(
            "placeholder path mismatch: "
            f"file is {normalized_expected}, metadata says {metadata.path}"
        )
    return metadata
