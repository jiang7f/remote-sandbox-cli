from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.state import WorkspaceStore


def _fingerprint(path: str, digest: str) -> EntryFingerprint:
    return EntryFingerprint(
        path,
        EntryKind.FILE,
        4,
        456,
        0o100644,
        content_hash=digest,
    )


def test_replace_base_round_trips_fingerprints_and_removes_stale_paths(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    first = _fingerprint("model.py", "local")
    link = EntryFingerprint(
        "current",
        EntryKind.SYMLINK,
        None,
        789,
        0o120777,
        link_target="model.py",
        content_hash="link-hash",
    )
    with WorkspaceStore.open(db) as store:
        store.replace_base({first.path: first, link.path: link})
        store.replace_base({link.path: link})

    with WorkspaceStore.open(db) as store:
        assert store.get_base("model.py") == MissingEntry("model.py")
        assert store.list_base() == {"current": link}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("size", 4.5),
        ("mtime_ns", True),
        ("mode", float(0o100644)),
        ("link_target", 123),
        ("content_hash", b"abc"),
        ("is_placeholder", 1),
    ],
)
def test_invalid_fingerprint_fields_are_rejected_before_base_changes(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    db = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(db) as store:
        with pytest.raises(ValueError, match=field):
            fingerprint = EntryFingerprint(
                "model.py",
                EntryKind.FILE,
                value if field == "size" else 4,  # type: ignore[arg-type]
                value if field == "mtime_ns" else 456,  # type: ignore[arg-type]
                value if field == "mode" else 0o100644,  # type: ignore[arg-type]
                link_target=value if field == "link_target" else None,  # type: ignore[arg-type]
                content_hash=value if field == "content_hash" else "abc",  # type: ignore[arg-type]
                is_placeholder=value if field == "is_placeholder" else False,  # type: ignore[arg-type]
            )
            store.upsert_base(fingerprint)

        assert store.list_base() == {}


def test_conflict_keeps_both_versions_and_remains_unresolved(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    local_fingerprint = _fingerprint("model.py", "local")
    remote_fingerprint = _fingerprint("model.py", "remote")
    with WorkspaceStore.open(db) as store:
        record = store.create_conflict(
            path="model.py",
            reason="both-modified",
            local_blob=b"local\n",
            remote_blob=b"remote\n",
            local_fingerprint=local_fingerprint,
            remote_fingerprint=remote_fingerprint,
        )
        assert record.resolved_at is None

    with WorkspaceStore.open(db) as store:
        restored = store.get_conflict(record.conflict_id)
        assert restored.local_blob == b"local\n"
        assert restored.remote_blob == b"remote\n"
        assert restored.local_fingerprint == local_fingerprint
        assert restored.remote_fingerprint == remote_fingerprint
        assert store.list_conflicts() == [restored]


def test_resolving_conflict_preserves_its_stored_versions(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        record = store.create_conflict(
            path="model.py",
            reason="both-modified",
            local_blob=b"local\n",
            remote_blob=b"remote\n",
        )
        resolved = store.resolve_conflict(record.conflict_id, resolved_at=999.0)

        assert resolved.resolved_at == 999.0
        assert resolved.local_blob == b"local\n"
        assert resolved.remote_blob == b"remote\n"
        assert store.list_conflicts(unresolved_only=True) == []


def test_corrupt_fingerprint_json_is_rejected_on_read(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(db) as store:
        store.replace_base({"model.py": _fingerprint("model.py", "abc")})

    connection = sqlite3.connect(db)
    connection.execute(
        "UPDATE base_entries SET fingerprint_json = ? WHERE path = ?",
        ('{"schema_version":true}', "model.py"),
    )
    connection.commit()
    connection.close()

    with (
        WorkspaceStore.open(db) as store,
        pytest.raises(ValueError, match="fingerprint JSON schema"),
    ):
        store.get_base("model.py")
