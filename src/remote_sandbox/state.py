from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any

from remote_sandbox.journal import EventKind, JournalEvent
from remote_sandbox.manifest import (
    MISSING,
    EntryFingerprint,
    EntryKind,
    EntryState,
    FileEntry,
    MissingEntry,
    normalize_relative_path,
)
from remote_sandbox.status import SyncProgress, WorkspacePhase, WorkspaceStatus

SCHEMA_VERSION = 5
_JSON_SCHEMA_VERSION = 1
_SIDES = frozenset({"local", "remote"})


@dataclass(frozen=True, slots=True)
class ConflictRecord:
    conflict_id: str
    path: str
    reason: str
    local_blob: bytes | None
    remote_blob: bytes | None
    local_fingerprint: EntryFingerprint | None
    remote_fingerprint: EntryFingerprint | None
    created_at: float
    resolved_at: float | None = None


@dataclass(frozen=True, slots=True)
class AuditSignature:
    path: str
    kind: EntryKind
    ctime_ns: int
    device: int
    inode: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_relative_path(self.path))
        if type(self.kind) is not EntryKind:
            raise ValueError("audit signature kind must be an EntryKind")
        for value in (self.ctime_ns, self.device, self.inode):
            if type(value) is not int or value < 0:
                raise ValueError("audit signature identity values must be non-negative integers")


class WorkspaceStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._transaction_depth = 0
        self._connection = sqlite3.connect(
            path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._configure_connection()
            self._migrate_schema()
            self.path.chmod(0o600)
        except BaseException:
            self._connection.close()
            raise

    @classmethod
    def open(cls, path: Path) -> WorkspaceStore:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        return cls(path)

    def __enter__(self) -> WorkspaceStore:
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
        with self._lock:
            if self._transaction_depth:
                raise RuntimeError("cannot close workspace store during a transaction")
            self._connection.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            outermost = self._transaction_depth == 0
            savepoint = f"workspace_store_{self._transaction_depth}"
            if outermost:
                self._connection.execute("BEGIN IMMEDIATE")
            else:
                self._connection.execute(f"SAVEPOINT {savepoint}")
            self._transaction_depth += 1
            try:
                yield
            except BaseException:
                self._transaction_depth -= 1
                if outermost:
                    self._connection.rollback()
                else:
                    self._connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                raise
            else:
                self._transaction_depth -= 1
                if outermost:
                    self._connection.commit()
                else:
                    self._connection.execute(f"RELEASE SAVEPOINT {savepoint}")

    def get_base(self, path: str) -> EntryFingerprint | MissingEntry:
        normalized = normalize_relative_path(path)
        with self._lock:
            row = self._connection.execute(
                "SELECT fingerprint_json FROM base_entries WHERE path = ?",
                (normalized,),
            ).fetchone()
        if row is None:
            return MissingEntry(normalized)
        return _decode_fingerprint(row["fingerprint_json"], expected_path=normalized)

    def list_base(self) -> dict[str, EntryFingerprint]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT path, fingerprint_json FROM base_entries ORDER BY path"
            ).fetchall()
        return {
            _expect_str(row["path"], "base path"): _decode_fingerprint(
                row["fingerprint_json"],
                expected_path=_expect_str(row["path"], "base path"),
            )
            for row in rows
        }

    def replace_base(self, entries: Mapping[str, EntryFingerprint]) -> None:
        encoded: list[tuple[str, str]] = []
        for path, entry in entries.items():
            normalized = normalize_relative_path(path)
            if normalized != entry.path:
                raise ValueError(f"base key does not match fingerprint path: {path}")
            encoded.append((normalized, _encode_fingerprint(entry)))
        with self.transaction():
            self._connection.execute("DELETE FROM base_entries")
            self._connection.executemany(
                "INSERT INTO base_entries(path, fingerprint_json) VALUES (?, ?)",
                encoded,
            )

    def upsert_base(self, entry: EntryFingerprint) -> None:
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO base_entries(path, fingerprint_json) VALUES (?, ?)
                ON CONFLICT(path) DO UPDATE SET fingerprint_json = excluded.fingerprint_json
                """,
                (entry.path, _encode_fingerprint(entry)),
            )

    def delete_base(self, path: str) -> None:
        normalized = normalize_relative_path(path)
        with self.transaction():
            self._connection.execute("DELETE FROM base_entries WHERE path = ?", (normalized,))

    def append_event(
        self,
        side: str,
        kind: EventKind,
        path: str,
        destination_path: str | None = None,
    ) -> JournalEvent:
        _validate_side(side)
        with self.transaction():
            sequence = self._last_allocated_sequence(side) + 1
            event = JournalEvent(side, sequence, EventKind(kind), path, destination_path)
            self._connection.execute(
                """
                INSERT INTO events(side, sequence, kind, path, destination_path, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.side,
                    event.sequence,
                    event.kind.value,
                    event.path,
                    event.destination_path,
                    time.time(),
                ),
            )
        return event

    def record_events(self, events: Iterable[JournalEvent]) -> None:
        if isinstance(events, (str, bytes)):
            raise ValueError("events must be an iterable of JournalEvent values")
        recorded = tuple(events)
        if any(type(event) is not JournalEvent for event in recorded):
            raise ValueError("events must contain JournalEvent values")
        with self.transaction():
            for event in recorded:
                existing = self._connection.execute(
                    """
                    SELECT side, sequence, kind, path, destination_path
                    FROM events WHERE side = ? AND sequence = ?
                    """,
                    (event.side, event.sequence),
                ).fetchone()
                if existing is not None:
                    if _journal_event_from_row(existing) != event:
                        raise RuntimeError(
                            f"journal sequence {event.sequence} for {event.side} changed"
                        )
                    continue
                self._connection.execute(
                    """
                    INSERT INTO events(side, sequence, kind, path, destination_path, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.side,
                        event.sequence,
                        event.kind.value,
                        event.path,
                        event.destination_path,
                        time.time(),
                    ),
                )

    def pending_events(self, side: str, after_sequence: int) -> list[JournalEvent]:
        _validate_side(side)
        _validate_non_negative_int(after_sequence, "after_sequence")
        with self._lock:
            effective_sequence = max(after_sequence, self.acknowledged_sequence(side))
            rows = self._connection.execute(
                """
                SELECT side, sequence, kind, path, destination_path
                FROM events
                WHERE side = ? AND sequence > ?
                ORDER BY sequence
                """,
                (side, effective_sequence),
            ).fetchall()
        return [_journal_event_from_row(row) for row in rows]

    def acknowledge(self, side: str, through_sequence: int) -> None:
        _validate_side(side)
        _validate_non_negative_int(through_sequence, "through_sequence")
        with self.transaction():
            last_allocated = self._last_allocated_sequence(side)
            if through_sequence > last_allocated:
                raise ValueError(
                    f"cannot acknowledge unallocated journal sequence {through_sequence} "
                    f"for {side} side"
                )
            self._connection.execute(
                """
                INSERT INTO watermarks(side, acknowledged_sequence) VALUES (?, ?)
                ON CONFLICT(side) DO UPDATE SET acknowledged_sequence = MAX(
                    watermarks.acknowledged_sequence,
                    excluded.acknowledged_sequence
                )
                """,
                (side, through_sequence),
            )

    def _last_allocated_sequence(self, side: str) -> int:
        row = self._connection.execute(
            """
            SELECT MAX(
                COALESCE((SELECT MAX(sequence) FROM events WHERE side = ?), 0),
                COALESCE(
                    (SELECT acknowledged_sequence FROM watermarks WHERE side = ?),
                    0
                )
            ) AS last_sequence
            """,
            (side, side),
        ).fetchone()
        if row is None:
            raise RuntimeError("could not read the last allocated journal sequence")
        return _expect_int(row["last_sequence"], "last allocated event sequence")

    def latest_sequence(self, side: str) -> int:
        _validate_side(side)
        with self._lock:
            return self._last_allocated_sequence(side)

    def get_initial_sync_watermarks(self) -> tuple[int, int] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT local_start, remote_start
                FROM initial_sync_checkpoint WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        return (
            _expect_int(row["local_start"], "initial local watermark"),
            _expect_int(row["remote_start"], "initial remote watermark"),
        )

    def set_initial_sync_watermarks(self, local_start: int, remote_start: int) -> None:
        _validate_non_negative_int(local_start, "local_start")
        _validate_non_negative_int(remote_start, "remote_start")
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO initial_sync_checkpoint(singleton, local_start, remote_start)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton) DO NOTHING
                """,
                (local_start, remote_start),
            )

    def clear_initial_sync_watermarks(self) -> None:
        with self.transaction():
            self._connection.execute(
                "DELETE FROM initial_sync_checkpoint WHERE singleton = 1"
            )

    def acknowledged_sequence(self, side: str) -> int:
        _validate_side(side)
        with self._lock:
            row = self._connection.execute(
                "SELECT acknowledged_sequence FROM watermarks WHERE side = ?",
                (side,),
            ).fetchone()
        if row is None:
            return 0
        return _expect_int(row["acknowledged_sequence"], "acknowledged sequence")

    def set_status(self, status: WorkspaceStatus) -> None:
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO workspace_status(
                    singleton, phase, progress_json, pending, conflicts, last_error, last_sync_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    phase = excluded.phase,
                    progress_json = excluded.progress_json,
                    pending = excluded.pending,
                    conflicts = excluded.conflicts,
                    last_error = excluded.last_error,
                    last_sync_at = excluded.last_sync_at
                """,
                (
                    status.phase.value,
                    _encode_progress(status.progress),
                    status.pending,
                    status.conflicts,
                    status.last_error,
                    status.last_sync_at,
                ),
            )

    def get_status(self) -> WorkspaceStatus:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT phase, progress_json, pending, conflicts, last_error, last_sync_at
                FROM workspace_status WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            raise RuntimeError("workspace status row is missing")
        last_error = row["last_error"]
        if last_error is not None:
            last_error = _expect_str(last_error, "last_error")
        last_sync_at = _optional_float(row["last_sync_at"], "last_sync_at")
        return WorkspaceStatus(
            WorkspacePhase(_expect_str(row["phase"], "workspace phase")),
            _decode_progress(row["progress_json"]),
            pending=_expect_int(row["pending"], "pending count"),
            conflicts=_expect_int(row["conflicts"], "conflict count"),
            last_error=last_error,
            last_sync_at=last_sync_at,
        )

    def set_expected_echo(
        self,
        side: str,
        fingerprint: EntryFingerprint | MissingEntry,
    ) -> None:
        _validate_side(side)
        if fingerprint.path is None:
            raise ValueError("expected echo entries require a path")
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO expected_echoes(side, path, fingerprint_json, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(side, path) DO UPDATE SET
                    fingerprint_json = excluded.fingerprint_json,
                    created_at = excluded.created_at
                """,
                (side, fingerprint.path, _encode_expected_echo(fingerprint), time.time()),
            )

    def get_expected_echo(
        self,
        side: str,
        path: str,
    ) -> EntryFingerprint | MissingEntry | None:
        _validate_side(side)
        normalized = normalize_relative_path(path)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT fingerprint_json FROM expected_echoes
                WHERE side = ? AND path = ?
                """,
                (side, normalized),
            ).fetchone()
        if row is None:
            return None
        return _decode_expected_echo(row["fingerprint_json"], expected_path=normalized)

    def consume_expected_echo(
        self,
        side: str,
        fingerprint: EntryFingerprint | MissingEntry,
    ) -> bool:
        _validate_side(side)
        if fingerprint.path is None:
            raise ValueError("expected echo entries require a path")
        with self.transaction():
            row = self._connection.execute(
                """
                SELECT fingerprint_json FROM expected_echoes
                WHERE side = ? AND path = ?
                """,
                (side, fingerprint.path),
            ).fetchone()
            if row is None:
                return False
            expected = _decode_expected_echo(
                row["fingerprint_json"],
                expected_path=fingerprint.path,
            )
            if expected != fingerprint:
                return False
            self._connection.execute(
                "DELETE FROM expected_echoes WHERE side = ? AND path = ?",
                (side, fingerprint.path),
            )
            return True

    def requeue_paths(self, paths: Iterable[str], reason: str) -> None:
        if isinstance(paths, (str, bytes)):
            raise ValueError("requeue paths must be an iterable of relative paths")
        if type(reason) is not str or not reason:
            raise ValueError("requeue reason must not be empty")
        normalized = tuple(sorted({normalize_relative_path(path) for path in paths}))
        with self.transaction():
            self._connection.executemany(
                """
                INSERT INTO requeued_paths(path, reason, created_at) VALUES (?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    reason = excluded.reason,
                    created_at = excluded.created_at
                """,
                ((path, reason, time.time()) for path in normalized),
            )

    def list_requeued_paths(self) -> tuple[str, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT path FROM requeued_paths ORDER BY path"
            ).fetchall()
        return tuple(_expect_str(row["path"], "requeued path") for row in rows)

    def clear_requeued_paths(self, paths: Iterable[str]) -> None:
        if isinstance(paths, (str, bytes)):
            raise ValueError("requeue paths must be an iterable of relative paths")
        normalized = tuple(sorted({normalize_relative_path(path) for path in paths}))
        with self.transaction():
            self._connection.executemany(
                "DELETE FROM requeued_paths WHERE path = ?",
                ((path,) for path in normalized),
            )

    def list_audit_signatures(self, side: str) -> dict[str, AuditSignature]:
        _validate_side(side)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT path, kind, ctime_ns, device, inode
                FROM audit_signatures WHERE side = ? ORDER BY path
                """,
                (side,),
            ).fetchall()
        return {
            _expect_str(row["path"], "audit signature path"): AuditSignature(
                _expect_str(row["path"], "audit signature path"),
                EntryKind(_expect_str(row["kind"], "audit signature kind")),
                _expect_int(row["ctime_ns"], "audit signature ctime"),
                _expect_int(row["device"], "audit signature device"),
                _expect_int(row["inode"], "audit signature inode"),
            )
            for row in rows
        }

    def replace_audit_signatures(
        self,
        side: str,
        signatures: Mapping[str, AuditSignature],
    ) -> None:
        _validate_side(side)
        rows = []
        for path, signature in signatures.items():
            normalized = normalize_relative_path(path)
            if signature.path != normalized:
                raise ValueError("audit signature key does not match path")
            rows.append(
                (
                    side,
                    normalized,
                    signature.kind.value,
                    signature.ctime_ns,
                    signature.device,
                    signature.inode,
                )
            )
        with self.transaction():
            self._connection.execute("DELETE FROM audit_signatures WHERE side = ?", (side,))
            self._connection.executemany(
                """
                INSERT INTO audit_signatures(side, path, kind, ctime_ns, device, inode)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def update_audit_signatures(
        self,
        side: str,
        signatures: Mapping[str, AuditSignature | None],
    ) -> None:
        _validate_side(side)
        with self.transaction():
            for path, signature in signatures.items():
                normalized = normalize_relative_path(path)
                if signature is None:
                    self._connection.execute(
                        "DELETE FROM audit_signatures WHERE side = ? AND path = ?",
                        (side, normalized),
                    )
                    continue
                if signature.path != normalized:
                    raise ValueError("audit signature key does not match path")
                self._connection.execute(
                    """
                    INSERT INTO audit_signatures(side, path, kind, ctime_ns, device, inode)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(side, path) DO UPDATE SET
                        kind = excluded.kind,
                        ctime_ns = excluded.ctime_ns,
                        device = excluded.device,
                        inode = excluded.inode
                    """,
                    (
                        side,
                        normalized,
                        signature.kind.value,
                        signature.ctime_ns,
                        signature.device,
                        signature.inode,
                    ),
                )

    def create_conflict(
        self,
        *,
        path: str,
        reason: str,
        local_blob: bytes | None,
        remote_blob: bytes | None,
        local_fingerprint: EntryFingerprint | None = None,
        remote_fingerprint: EntryFingerprint | None = None,
    ) -> ConflictRecord:
        normalized = normalize_relative_path(path)
        if not reason:
            raise ValueError("conflict reason must not be empty")
        if local_blob is None and remote_blob is None:
            raise ValueError("a conflict must preserve at least one version")
        for fingerprint in (local_fingerprint, remote_fingerprint):
            if fingerprint is not None and fingerprint.path != normalized:
                raise ValueError("conflict fingerprint path does not match conflict path")
        record = ConflictRecord(
            conflict_id=uuid.uuid4().hex,
            path=normalized,
            reason=reason,
            local_blob=local_blob,
            remote_blob=remote_blob,
            local_fingerprint=local_fingerprint,
            remote_fingerprint=remote_fingerprint,
            created_at=time.time(),
        )
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO conflicts(
                    conflict_id, path, reason, local_blob, remote_blob,
                    local_fingerprint_json, remote_fingerprint_json, created_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    record.conflict_id,
                    record.path,
                    record.reason,
                    record.local_blob,
                    record.remote_blob,
                    _encode_optional_fingerprint(record.local_fingerprint),
                    _encode_optional_fingerprint(record.remote_fingerprint),
                    record.created_at,
                ),
            )
        return record

    def get_conflict(self, conflict_id: str) -> ConflictRecord:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM conflicts WHERE conflict_id = ?",
                (conflict_id,),
            ).fetchone()
        if row is None:
            raise KeyError(conflict_id)
        return _conflict_from_row(row)

    def list_conflicts(self, *, unresolved_only: bool = False) -> list[ConflictRecord]:
        where = "WHERE resolved_at IS NULL" if unresolved_only else ""
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM conflicts {where} ORDER BY created_at, conflict_id"
            ).fetchall()
        return [_conflict_from_row(row) for row in rows]

    def resolve_conflict(
        self,
        conflict_id: str,
        *,
        resolved_at: float | None = None,
    ) -> ConflictRecord:
        timestamp = time.time() if resolved_at is None else resolved_at
        with self.transaction():
            cursor = self._connection.execute(
                """
                UPDATE conflicts SET resolved_at = COALESCE(resolved_at, ?)
                WHERE conflict_id = ?
                """,
                (timestamp, conflict_id),
            )
            if cursor.rowcount == 0:
                raise KeyError(conflict_id)
        return self.get_conflict(conflict_id)

    def _configure_connection(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=5000")

    def _migrate_schema(self) -> None:
        version = self._read_schema_version()
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"workspace database schema {version} is newer than supported {SCHEMA_VERSION}"
            )
        with self.transaction():
            if version == 0:
                self._create_current_schema()
            elif version == 1:
                self._migrate_legacy_base_entries()
                self._create_current_schema()
            elif version in {2, 3, 4}:
                self._create_current_schema()
            self._connection.execute(
                """
                INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(SCHEMA_VERSION),),
            )
            self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._ensure_default_status()

    def _read_schema_version(self) -> int:
        user_version = _expect_int(
            self._connection.execute("PRAGMA user_version").fetchone()[0],
            "SQLite user_version",
        )
        if user_version:
            return user_version
        if not self._table_exists("schema_meta"):
            return 0
        row = self._connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return 0
        try:
            return int(_expect_str(row["value"], "schema version"))
        except ValueError as exc:
            raise RuntimeError("invalid workspace schema version") from exc

    def _table_exists(self, name: str) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def _migrate_legacy_base_entries(self) -> None:
        if not self._table_exists("base_entries"):
            return
        columns = {
            _expect_str(row["name"], "column name")
            for row in self._connection.execute("PRAGMA table_info(base_entries)").fetchall()
        }
        if "fingerprint_json" in columns:
            return
        rows = self._connection.execute(
            "SELECT path, kind, size, mtime, hash, is_placeholder FROM base_entries"
        ).fetchall()
        self._connection.execute("ALTER TABLE base_entries RENAME TO base_entries_legacy")
        self._connection.execute(
            """
            CREATE TABLE base_entries (
                path TEXT PRIMARY KEY,
                fingerprint_json TEXT NOT NULL
            )
            """
        )
        migrated = []
        for row in rows:
            entry = _legacy_entry_from_row(row)
            fingerprint = _legacy_entry_to_fingerprint(entry)
            migrated.append((fingerprint.path, _encode_fingerprint(fingerprint)))
        self._connection.executemany(
            "INSERT INTO base_entries(path, fingerprint_json) VALUES (?, ?)",
            migrated,
        )
        self._connection.execute("DROP TABLE base_entries_legacy")

    def _create_current_schema(self) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS base_entries (
                path TEXT PRIMARY KEY,
                fingerprint_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS events (
                side TEXT NOT NULL CHECK(side IN ('local', 'remote')),
                sequence INTEGER NOT NULL CHECK(sequence > 0),
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                destination_path TEXT,
                created_at REAL NOT NULL,
                PRIMARY KEY(side, sequence)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS watermarks (
                side TEXT PRIMARY KEY CHECK(side IN ('local', 'remote')),
                acknowledged_sequence INTEGER NOT NULL CHECK(acknowledged_sequence >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS workspace_status (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                phase TEXT NOT NULL,
                progress_json TEXT NOT NULL,
                pending INTEGER NOT NULL CHECK(pending >= 0),
                conflicts INTEGER NOT NULL CHECK(conflicts >= 0),
                last_error TEXT,
                last_sync_at REAL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS initial_sync_checkpoint (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                local_start INTEGER NOT NULL CHECK(local_start >= 0),
                remote_start INTEGER NOT NULL CHECK(remote_start >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS expected_echoes (
                side TEXT NOT NULL CHECK(side IN ('local', 'remote')),
                path TEXT NOT NULL,
                fingerprint_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY(side, path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS conflicts (
                conflict_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                reason TEXT NOT NULL,
                local_blob BLOB,
                remote_blob BLOB,
                local_fingerprint_json TEXT,
                remote_fingerprint_json TEXT,
                created_at REAL NOT NULL,
                resolved_at REAL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS conflicts_path_unresolved
            ON conflicts(path, resolved_at)
            """,
            """
            CREATE TABLE IF NOT EXISTS requeued_paths (
                path TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_signatures (
                side TEXT NOT NULL CHECK(side IN ('local', 'remote')),
                path TEXT NOT NULL,
                kind TEXT NOT NULL,
                ctime_ns INTEGER NOT NULL CHECK(ctime_ns >= 0),
                device INTEGER NOT NULL CHECK(device >= 0),
                inode INTEGER NOT NULL CHECK(inode >= 0),
                PRIMARY KEY(side, path)
            )
            """,
        )
        for statement in statements:
            self._connection.execute(statement)

    def _ensure_default_status(self) -> None:
        progress = SyncProgress("stopped")
        self._connection.execute(
            """
            INSERT OR IGNORE INTO workspace_status(
                singleton, phase, progress_json, pending, conflicts, last_error, last_sync_at
            ) VALUES (1, ?, ?, 0, 0, NULL, NULL)
            """,
            (WorkspacePhase.STOPPED.value, _encode_progress(progress)),
        )


class StateStore:
    """Compatibility adapter for the legacy full-scan synchronizer."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._store = WorkspaceStore.open(path)

    @classmethod
    def open(cls, path: Path) -> StateStore:
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
        self._store.close()

    def get_base(self, path: str) -> EntryState:
        entry = self._store.get_base(path)
        if isinstance(entry, MissingEntry):
            return MISSING
        return _fingerprint_to_legacy_entry(entry)

    def upsert_base(self, entry: FileEntry) -> None:
        self._store.upsert_base(_legacy_entry_to_fingerprint(entry))

    def delete_base(self, path: str) -> None:
        self._store.delete_base(path)

    def list_base(self) -> dict[str, FileEntry]:
        return {
            path: _fingerprint_to_legacy_entry(entry)
            for path, entry in self._store.list_base().items()
        }


def _journal_event_from_row(row: sqlite3.Row) -> JournalEvent:
    destination = _optional_str(row["destination_path"], "event destination")
    return JournalEvent(
        _expect_str(row["side"], "event side"),
        _expect_int(row["sequence"], "event sequence"),
        EventKind(_expect_str(row["kind"], "event kind")),
        _expect_str(row["path"], "event path"),
        destination,
    )


def _conflict_from_row(row: sqlite3.Row) -> ConflictRecord:
    path = _expect_str(row["path"], "conflict path")
    return ConflictRecord(
        conflict_id=_expect_str(row["conflict_id"], "conflict id"),
        path=path,
        reason=_expect_str(row["reason"], "conflict reason"),
        local_blob=_optional_bytes(row["local_blob"], "local conflict blob"),
        remote_blob=_optional_bytes(row["remote_blob"], "remote conflict blob"),
        local_fingerprint=_decode_optional_fingerprint(
            row["local_fingerprint_json"],
            expected_path=path,
        ),
        remote_fingerprint=_decode_optional_fingerprint(
            row["remote_fingerprint_json"],
            expected_path=path,
        ),
        created_at=_expect_float(row["created_at"], "conflict timestamp"),
        resolved_at=_optional_float(row["resolved_at"], "conflict resolution timestamp"),
    )


def _encode_fingerprint(entry: EntryFingerprint) -> str:
    payload = {
        "schema_version": _JSON_SCHEMA_VERSION,
        "path": entry.path,
        "kind": entry.kind.value,
        "size": entry.size,
        "mtime_ns": entry.mtime_ns,
        "mode": entry.mode,
        "link_target": entry.link_target,
        "content_hash": entry.content_hash,
        "is_placeholder": entry.is_placeholder,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _encode_optional_fingerprint(entry: EntryFingerprint | None) -> str | None:
    return None if entry is None else _encode_fingerprint(entry)


def _encode_expected_echo(entry: EntryFingerprint | MissingEntry) -> str:
    if isinstance(entry, EntryFingerprint):
        return _encode_fingerprint(entry)
    if entry.path is None:
        raise ValueError("expected echo entries require a path")
    payload = {
        "schema_version": _JSON_SCHEMA_VERSION,
        "path": entry.path,
        "missing": True,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _decode_expected_echo(
    raw: object,
    *,
    expected_path: str,
) -> EntryFingerprint | MissingEntry:
    payload = _decode_json_object(raw, "expected echo")
    if payload.get("missing") is not True:
        return _decode_fingerprint(raw, expected_path=expected_path)
    _require_json_schema(payload, "expected echo")
    path = _required_string(payload, "path", "expected echo")
    if path != expected_path:
        raise ValueError("stored expected echo path does not match its database key")
    return MissingEntry(path)


def _decode_fingerprint(raw: object, *, expected_path: str) -> EntryFingerprint:
    payload = _decode_json_object(raw, "fingerprint")
    _require_json_schema(payload, "fingerprint")
    path = _required_string(payload, "path", "fingerprint")
    if path != expected_path:
        raise ValueError("stored fingerprint path does not match its database key")
    kind = EntryKind(_required_string(payload, "kind", "fingerprint"))
    size = _optional_json_int(payload, "size", "fingerprint")
    mtime_ns = _optional_json_int(payload, "mtime_ns", "fingerprint")
    mode = _optional_json_int(payload, "mode", "fingerprint")
    link_target = _optional_json_string(payload, "link_target", "fingerprint")
    content_hash = _optional_json_string(payload, "content_hash", "fingerprint")
    is_placeholder = payload.get("is_placeholder")
    if type(is_placeholder) is not bool:
        raise ValueError("invalid fingerprint JSON field: is_placeholder")
    if size is not None and size < 0:
        raise ValueError("invalid fingerprint JSON field: size")
    return EntryFingerprint(
        path,
        kind,
        size,
        mtime_ns,
        mode,
        link_target=link_target,
        content_hash=content_hash,
        is_placeholder=is_placeholder,
    )


def _decode_optional_fingerprint(
    raw: object,
    *,
    expected_path: str,
) -> EntryFingerprint | None:
    if raw is None:
        return None
    return _decode_fingerprint(raw, expected_path=expected_path)


def _encode_progress(progress: SyncProgress) -> str:
    payload = {
        "schema_version": _JSON_SCHEMA_VERSION,
        "stage": progress.stage,
        "files_done": progress.files_done,
        "files_total": progress.files_total,
        "bytes_done": progress.bytes_done,
        "bytes_total": progress.bytes_total,
        "current_path": progress.current_path,
        "elapsed_seconds": progress.elapsed_seconds,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _decode_progress(raw: object) -> SyncProgress:
    payload = _decode_json_object(raw, "progress")
    _require_json_schema(payload, "progress")
    return SyncProgress(
        _required_string(payload, "stage", "progress"),
        files_done=_required_json_int(payload, "files_done", "progress"),
        files_total=_required_json_int(payload, "files_total", "progress"),
        bytes_done=_required_json_int(payload, "bytes_done", "progress"),
        bytes_total=_required_json_int(payload, "bytes_total", "progress"),
        current_path=_optional_json_string(payload, "current_path", "progress"),
        elapsed_seconds=_progress_elapsed(payload),
    )


def _progress_elapsed(payload: Mapping[str, Any]) -> float:
    value = payload.get("elapsed_seconds", 0.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError("invalid progress JSON field: elapsed_seconds")
    return float(value)


def _decode_json_object(raw: object, label: str) -> dict[str, Any]:
    if not isinstance(raw, str):
        raise ValueError(f"invalid {label} JSON")
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid {label} JSON") from exc
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise ValueError(f"invalid {label} JSON")
    return decoded


def _require_json_schema(payload: Mapping[str, Any], label: str) -> None:
    if type(payload.get("schema_version")) is not int:
        raise ValueError(f"invalid {label} JSON schema version")
    if payload["schema_version"] != _JSON_SCHEMA_VERSION:
        raise ValueError(f"unsupported {label} JSON schema version")


def _required_string(payload: Mapping[str, Any], key: str, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"invalid {label} JSON field: {key}")
    return value


def _optional_json_string(payload: Mapping[str, Any], key: str, label: str) -> str | None:
    value = payload.get(key)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"invalid {label} JSON field: {key}")
    return value


def _required_json_int(payload: Mapping[str, Any], key: str, label: str) -> int:
    value = payload.get(key)
    if type(value) is not int:
        raise ValueError(f"invalid {label} JSON field: {key}")
    return value


def _optional_json_int(payload: Mapping[str, Any], key: str, label: str) -> int | None:
    value = payload.get(key)
    if value is not None and type(value) is not int:
        raise ValueError(f"invalid {label} JSON field: {key}")
    return value


def _validate_side(side: str) -> None:
    if side not in _SIDES:
        raise ValueError(f"invalid journal side: {side}")


def _validate_non_negative_int(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")


def _expect_str(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"invalid stored {label}")
    return value


def _optional_str(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _expect_str(value, label)


def _expect_int(value: object, label: str) -> int:
    if type(value) is not int:
        raise ValueError(f"invalid stored {label}")
    return value


def _expect_float(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid stored {label}")
    return float(value)


def _optional_float(value: object, label: str) -> float | None:
    if value is None:
        return None
    return _expect_float(value, label)


def _optional_bytes(value: object, label: str) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise ValueError(f"invalid stored {label}")
    return value


def _legacy_entry_from_row(row: sqlite3.Row) -> FileEntry:
    mtime = row["mtime"]
    if mtime is not None:
        mtime = _expect_float(mtime, "legacy mtime")
    return FileEntry(
        kind=EntryKind(_expect_str(row["kind"], "legacy entry kind")),
        path=_expect_str(row["path"], "legacy entry path"),
        size=row["size"],
        mtime=mtime,
        hash=row["hash"],
        is_placeholder=bool(row["is_placeholder"]),
    )


def _legacy_entry_to_fingerprint(entry: FileEntry) -> EntryFingerprint:
    modes = {
        EntryKind.FILE: 0o100644,
        EntryKind.DIR: 0o040755,
        EntryKind.SYMLINK: 0o120777,
        EntryKind.SPECIAL: None,
    }
    mtime_ns = None if entry.mtime is None else round(entry.mtime * 1_000_000_000)
    return EntryFingerprint(
        entry.path,
        entry.kind,
        entry.size,
        mtime_ns,
        modes[entry.kind],
        content_hash=entry.hash,
        is_placeholder=entry.is_placeholder,
    )


def _fingerprint_to_legacy_entry(entry: EntryFingerprint) -> FileEntry:
    mtime = None if entry.mtime_ns is None else entry.mtime_ns / 1_000_000_000
    return FileEntry(
        kind=entry.kind,
        path=entry.path,
        size=entry.size,
        mtime=mtime,
        hash=entry.content_hash,
        is_placeholder=entry.is_placeholder,
    )
