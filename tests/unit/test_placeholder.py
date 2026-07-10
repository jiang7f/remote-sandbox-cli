import json

import pytest

from remote_sandbox.placeholder import (
    PLACEHOLDER_MAGIC,
    PlaceholderMetadata,
    decode_placeholder,
    encode_placeholder,
)


def test_placeholder_round_trip_requires_the_expected_path() -> None:
    metadata = PlaceholderMetadata("weights.bin", 50_000_000, 123, "abc123")
    encoded = encode_placeholder(metadata)

    assert encoded.startswith(PLACEHOLDER_MAGIC)
    assert decode_placeholder(encoded, expected_path="weights.bin") == metadata
    with pytest.raises(ValueError, match="path mismatch"):
        decode_placeholder(encoded, expected_path="other.bin")


def test_ordinary_small_text_is_not_a_placeholder() -> None:
    assert decode_placeholder(b"ordinary content\n", expected_path="notes.txt") is None


@pytest.mark.parametrize(
    "payload",
    [
        b"not json\n",
        json.dumps(
            {
                "schema_version": 999,
                "path": "weights.bin",
                "size": 1,
                "mtime_ns": 2,
                "content_hash": "abc123",
            }
        ).encode(),
        json.dumps(
            {
                "schema_version": True,
                "path": "weights.bin",
                "size": 1,
                "mtime_ns": 2,
                "content_hash": "abc123",
            }
        ).encode(),
        json.dumps(
            {
                "schema_version": 1,
                "path": "../escape.bin",
                "size": 1,
                "mtime_ns": 2,
                "content_hash": "abc123",
            }
        ).encode(),
        json.dumps(
            {
                "schema_version": 1,
                "path": "weights.bin",
                "size": "large",
                "mtime_ns": 2,
                "content_hash": "abc123",
            }
        ).encode(),
    ],
)
def test_magic_header_with_malformed_metadata_is_rejected(payload: bytes) -> None:
    with pytest.raises(ValueError, match="placeholder metadata"):
        decode_placeholder(PLACEHOLDER_MAGIC + payload, expected_path="weights.bin")


def test_magic_header_with_non_string_path_is_rejected() -> None:
    payload = json.dumps(
        {
            "schema_version": 1,
            "path": ["w", "e"],
            "size": 1,
            "mtime_ns": 2,
            "content_hash": "abc123",
        }
    ).encode()

    with pytest.raises(ValueError, match="placeholder metadata"):
        decode_placeholder(PLACEHOLDER_MAGIC + payload, expected_path="weights.bin")


def test_placeholder_metadata_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="size"):
        PlaceholderMetadata("weights.bin", -1, 123, "abc123")
    with pytest.raises(ValueError, match="content hash"):
        PlaceholderMetadata("weights.bin", 1, 123, "")
