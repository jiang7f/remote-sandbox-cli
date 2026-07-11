from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from remote_sandbox.manifest import MISSING, EntryKind, FileEntry
from remote_sandbox.state import StateStore, WorkspaceStore
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus, format_progress


def test_starting_status_is_durable_before_sync(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    status = WorkspaceStatus(
        WorkspacePhase.STARTING,
        SyncProgress("starting"),
        pending=2,
        conflicts=1,
        last_error="waiting for remote",
        last_sync_at=123.5,
    )

    with WorkspaceStore.open(db) as store:
        store.set_status(status)

    with WorkspaceStore.open(db) as store:
        assert store.get_status() == status


def test_new_store_has_stopped_status(tmp_path: Path) -> None:
    with WorkspaceStore.open(tmp_path / "state.sqlite3") as store:
        assert store.get_status() == WorkspaceStatus(
            WorkspacePhase.STOPPED,
            SyncProgress("stopped"),
        )


def test_scanning_progress_is_informative_before_a_total_exists() -> None:
    progress = SyncProgress("scanning", files_done=1_843, bytes_done=31_000_000)

    assert format_progress(progress) == "scanning 1843 files 31.0 MB"


def test_sync_progress_formats_totals_and_current_path() -> None:
    progress = SyncProgress(
        "syncing",
        files_done=421,
        files_total=3_626,
        bytes_done=8_400_000,
        bytes_total=47_000_000,
        current_path="src/model.py",
        elapsed_seconds=1.25,
    )

    assert format_progress(progress) == (
        "syncing 421/3626 files 8.4/47.0 MB src/model.py 1.2s"
    )


def test_progress_rejects_negative_and_impossible_counts() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        SyncProgress("syncing", files_done=-1)
    with pytest.raises(ValueError, match="files_done"):
        SyncProgress("syncing", files_done=2, files_total=1)
    with pytest.raises(ValueError, match="bytes_done"):
        SyncProgress("syncing", bytes_done=2, bytes_total=1)


def test_progress_and_status_counts_require_integers() -> None:
    with pytest.raises(ValueError, match="integers"):
        SyncProgress("syncing", files_done=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="integers"):
        WorkspaceStatus(
            WorkspacePhase.SYNCING,
            SyncProgress("syncing"),
            pending=1.5,  # type: ignore[arg-type]
        )


def test_invalid_current_path_is_rejected_before_status_changes(
    tmp_path: Path,
) -> None:
    db = tmp_path / "state.sqlite3"
    with WorkspaceStore.open(db) as store:
        original = store.get_status()

        with pytest.raises(ValueError, match="current_path"):
            store.set_status(
                WorkspaceStatus(
                    WorkspacePhase.SYNCING,
                    SyncProgress(
                        "syncing",
                        current_path=123,  # type: ignore[arg-type]
                    ),
                )
            )

        assert store.get_status() == original


def test_workspace_store_configures_durable_sqlite_pragmas(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"

    with WorkspaceStore.open(db):
        pass

    connection = sqlite3.connect(db)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        assert connection.execute("PRAGMA user_version").fetchone()[0] >= 2
    finally:
        connection.close()

    with WorkspaceStore.open(db) as store:
        assert store._connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert store._connection.execute("PRAGMA busy_timeout").fetchone()[0] == 5_000
        tables = {
            row[0]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert {
            "base_entries",
            "events",
            "watermarks",
            "workspace_status",
            "expected_echoes",
            "conflicts",
        } <= tables


def test_legacy_v1_database_is_migrated_without_losing_base_entries(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(db)
    connection.executescript(
        """
        CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');
        CREATE TABLE base_entries (
            path TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            size INTEGER,
            mtime REAL,
            hash TEXT,
            is_placeholder INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO base_entries(path, kind, size, mtime, hash, is_placeholder)
        VALUES ('legacy.txt', 'file', 3, 1.5, 'abc', 0);
        """
    )
    connection.commit()
    connection.close()

    with WorkspaceStore.open(db) as store:
        legacy = store.get_base("legacy.txt")
        assert legacy.path == "legacy.txt"
        assert legacy.size == 3
        assert legacy.content_hash == "abc"
        assert store.get_status().phase is WorkspacePhase.STOPPED


def test_legacy_state_store_adapter_keeps_file_entry_behavior(tmp_path: Path) -> None:
    entry = FileEntry(EntryKind.FILE, "legacy.txt", 3, 1.5, "abc")

    with StateStore.open(tmp_path / "state.sqlite3") as store:
        store.upsert_base(entry)
        assert store.get_base("legacy.txt") == entry
        assert store.get_base("missing.txt") is MISSING
        assert store.list_base() == {"legacy.txt": entry}


def test_newer_database_schema_is_rejected(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(db)
    connection.execute("PRAGMA user_version=999")
    connection.close()

    with pytest.raises(RuntimeError, match="newer than supported"):
        WorkspaceStore.open(db)
