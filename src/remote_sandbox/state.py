from __future__ import annotations

import sqlite3
from pathlib import Path
from types import TracebackType

from remote_sandbox.manifest import MISSING, EntryKind, EntryState, FileEntry

SCHEMA_VERSION = 1


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    @classmethod
    def open(cls, path: Path) -> StateStore:
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(path)

    def __enter__(self) -> StateStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        self.close()

    def close(self) -> None:
        self._conn.close()

    def get_base(self, path: str) -> EntryState:
        row = self._conn.execute(
            """
            SELECT path, kind, size, mtime, hash, is_placeholder
            FROM base_entries
            WHERE path = ?
            """,
            (path,),
        ).fetchone()
        if row is None:
            return MISSING
        return _entry_from_row(row)

    def upsert_base(self, entry: FileEntry) -> None:
        self._conn.execute(
            """
            INSERT INTO base_entries(path, kind, size, mtime, hash, is_placeholder)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                kind = excluded.kind,
                size = excluded.size,
                mtime = excluded.mtime,
                hash = excluded.hash,
                is_placeholder = excluded.is_placeholder
            """,
            (
                entry.path,
                entry.kind.value,
                entry.size,
                entry.mtime,
                entry.hash,
                int(entry.is_placeholder),
            ),
        )
        self._conn.commit()

    def delete_base(self, path: str) -> None:
        self._conn.execute("DELETE FROM base_entries WHERE path = ?", (path,))
        self._conn.commit()

    def list_base(self) -> dict[str, FileEntry]:
        rows = self._conn.execute(
            """
            SELECT path, kind, size, mtime, hash, is_placeholder
            FROM base_entries
            ORDER BY path
            """
        ).fetchall()
        return {str(row["path"]): _entry_from_row(row) for row in rows}

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS base_entries (
                path TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                size INTEGER,
                mtime REAL,
                hash TEXT,
                is_placeholder INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        self._conn.commit()


def _entry_from_row(row: sqlite3.Row) -> FileEntry:
    return FileEntry(
        kind=EntryKind(str(row["kind"])),
        path=str(row["path"]),
        size=row["size"],
        mtime=row["mtime"],
        hash=row["hash"],
        is_placeholder=bool(row["is_placeholder"]),
    )
