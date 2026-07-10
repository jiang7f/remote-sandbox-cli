from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

_WORKSPACE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_EVENT_KINDS = {"create", "modify", "delete", "move", "rescan-required"}


@dataclass(frozen=True, slots=True)
class RemoteEvent:
    sequence: int
    kind: str
    path: str
    destination_path: str | None
    created_at: float


@dataclass(frozen=True, slots=True)
class RemoteWorkspace:
    workspace_id: str
    root: Path


@dataclass(frozen=True, slots=True)
class RemoteIndexEntry:
    workspace_id: str
    root: Path
    state_path: Path


@dataclass(frozen=True, slots=True)
class WatcherState:
    pid: int | None
    status: str
    backend: str | None
    started_at: float | None
    heartbeat_at: float | None
    error: str | None


class RemoteStore:
    """Thread-safe durable journal and protected remote workspace registry."""

    def __init__(self, database: Path) -> None:
        self.database = database.expanduser().resolve(strict=False)
        self.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.database.parent.chmod(0o700)
        self._connection = sqlite3.connect(
            self.database,
            isolation_level=None,
            check_same_thread=False,
            timeout=30.0,
        )
        self.database.chmod(0o600)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.execute("PRAGMA busy_timeout = 30000")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._initialize_schema()

    def __enter__(self) -> RemoteStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        self.close()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def register_workspace(
        self,
        workspace_id: str,
        root: Path,
        *,
        home: Path | None = None,
    ) -> RemoteWorkspace:
        safe_id = validate_workspace_id(workspace_id)
        canonical_root = validate_workspace_root(root, home=home)
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT workspace_id, root FROM workspace WHERE singleton = 1"
            ).fetchone()
            if row is not None:
                existing = RemoteWorkspace(str(row["workspace_id"]), Path(str(row["root"])))
                if existing == RemoteWorkspace(safe_id, canonical_root):
                    return existing
                raise ValueError("workspace state is already registered to another root")
            self._connection.execute(
                "INSERT INTO workspace(singleton, workspace_id, root) VALUES(1, ?, ?)",
                (safe_id, str(canonical_root)),
            )
        return RemoteWorkspace(safe_id, canonical_root)

    def workspace(self) -> RemoteWorkspace:
        with self._lock:
            row = self._connection.execute(
                "SELECT workspace_id, root FROM workspace WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise LookupError("remote workspace is not registered")
        return RemoteWorkspace(str(row["workspace_id"]), Path(str(row["root"])))

    def register_index(
        self,
        workspace_id: str,
        root: Path,
        state_path: Path,
        *,
        home: Path | None = None,
    ) -> RemoteIndexEntry:
        safe_id = validate_workspace_id(workspace_id)
        canonical_root = validate_workspace_root(root, home=home)
        canonical_state = _validate_absolute_path(state_path, label="state path")
        entry = RemoteIndexEntry(safe_id, canonical_root, canonical_state)
        with self._lock, self._connection:
            by_id = self._connection.execute(
                "SELECT workspace_id, root, state_path FROM remote_index WHERE workspace_id = ?",
                (safe_id,),
            ).fetchone()
            by_root = self._connection.execute(
                "SELECT workspace_id, root, state_path FROM remote_index WHERE root = ?",
                (str(canonical_root),),
            ).fetchone()
            existing_by_id = _index_entry(by_id)
            existing_by_root = _index_entry(by_root)
            if existing_by_id == entry and existing_by_root == entry:
                return entry
            if existing_by_id is not None or existing_by_root is not None:
                raise ValueError("remote root or workspace is already registered")
            self._connection.execute(
                "INSERT INTO remote_index(workspace_id, root, state_path) VALUES(?, ?, ?)",
                (safe_id, str(canonical_root), str(canonical_state)),
            )
        return entry

    def index_entry(self, workspace_id: str) -> RemoteIndexEntry | None:
        safe_id = validate_workspace_id(workspace_id)
        with self._lock:
            row = self._connection.execute(
                "SELECT workspace_id, root, state_path FROM remote_index WHERE workspace_id = ?",
                (safe_id,),
            ).fetchone()
        return _index_entry(row)

    def workspace_for_root(self, root: Path) -> RemoteIndexEntry | None:
        canonical_root = validate_workspace_root(root)
        with self._lock:
            row = self._connection.execute(
                "SELECT workspace_id, root, state_path FROM remote_index WHERE root = ?",
                (str(canonical_root),),
            ).fetchone()
        return _index_entry(row)

    def remove_index(self, workspace_id: str) -> None:
        safe_id = validate_workspace_id(workspace_id)
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM remote_index WHERE workspace_id = ?",
                (safe_id,),
            )

    def append_event(
        self,
        kind: str,
        path: str,
        destination_path: str | None,
    ) -> RemoteEvent:
        safe_kind, safe_path, safe_destination = _validate_event(kind, path, destination_path)
        created_at = time.time()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO events(kind, path, destination_path, created_at) VALUES(?, ?, ?, ?)",
                (safe_kind, safe_path, safe_destination, created_at),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("failed to allocate remote event sequence")
            sequence = cursor.lastrowid
        return RemoteEvent(sequence, safe_kind, safe_path, safe_destination, created_at)

    def events_after(self, sequence: int) -> list[RemoteEvent]:
        safe_sequence = _validate_sequence(sequence)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT sequence, kind, path, destination_path, created_at
                FROM events
                WHERE sequence > ?
                ORDER BY sequence
                """,
                (safe_sequence,),
            ).fetchall()
        return [_event(row) for row in rows]

    def latest_sequence(self) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS latest FROM events"
            ).fetchone()
        return int(row["latest"])

    def acknowledge(self, sequence: int) -> None:
        safe_sequence = _validate_sequence(sequence)
        with self._lock, self._connection:
            latest_row = self._connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS latest FROM events"
            ).fetchone()
            latest = int(latest_row["latest"])
            if safe_sequence > latest:
                raise ValueError("acknowledgement exceeds latest event sequence")
            self._connection.execute(
                """
                UPDATE watermark
                SET acknowledged_sequence = MAX(acknowledged_sequence, ?)
                WHERE singleton = 1
                """,
                (safe_sequence,),
            )

    def acknowledged_sequence(self) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT acknowledged_sequence FROM watermark WHERE singleton = 1"
            ).fetchone()
        return int(row["acknowledged_sequence"])

    def record_watcher(
        self,
        pid: int | None,
        status: str,
        *,
        backend: str | None,
        error: str | None = None,
    ) -> WatcherState:
        if pid is not None and (type(pid) is not int or pid <= 0):
            raise ValueError("watcher pid must be a positive integer")
        if not status or _has_control_character(status):
            raise ValueError("watcher status must be non-empty")
        now = time.time()
        with self._lock, self._connection:
            previous = self._connection.execute(
                "SELECT started_at FROM watcher WHERE singleton = 1"
            ).fetchone()
            previous_started = (
                None
                if previous is None or previous["started_at"] is None
                else float(previous["started_at"])
            )
            started_at: float | None
            if status == "starting" or (status == "running" and previous_started is None):
                started_at = now
            else:
                started_at = previous_started
            self._connection.execute(
                """
                INSERT INTO watcher(
                    singleton, pid, status, backend, started_at, heartbeat_at, error
                ) VALUES(1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    pid = excluded.pid,
                    status = excluded.status,
                    backend = excluded.backend,
                    started_at = excluded.started_at,
                    heartbeat_at = excluded.heartbeat_at,
                    error = excluded.error
                """,
                (pid, status, backend, started_at, now, error),
            )
        return self.watcher_state()

    def heartbeat(self) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE watcher SET heartbeat_at = ? WHERE singleton = 1",
                (time.time(),),
            )

    def watcher_state(self) -> WatcherState:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT pid, status, backend, started_at, heartbeat_at, error
                FROM watcher
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return WatcherState(None, "stopped", None, None, None, None)
        return WatcherState(
            None if row["pid"] is None else int(row["pid"]),
            str(row["status"]),
            None if row["backend"] is None else str(row["backend"]),
            None if row["started_at"] is None else float(row["started_at"]),
            None if row["heartbeat_at"] is None else float(row["heartbeat_at"]),
            None if row["error"] is None else str(row["error"]),
        )

    def _initialize_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workspace (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                workspace_id TEXT NOT NULL UNIQUE,
                root TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                destination_path TEXT,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS watermark (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                acknowledged_sequence INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS watcher (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                pid INTEGER,
                status TEXT NOT NULL,
                backend TEXT,
                started_at REAL,
                heartbeat_at REAL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS remote_index (
                workspace_id TEXT PRIMARY KEY,
                root TEXT NOT NULL UNIQUE,
                state_path TEXT NOT NULL UNIQUE
            );

            INSERT OR IGNORE INTO watermark(singleton, acknowledged_sequence) VALUES(1, 0);
            INSERT OR IGNORE INTO watcher(singleton, status) VALUES(1, 'stopped');
            """
        )


def validate_workspace_id(value: str) -> str:
    if not isinstance(value, str) or _WORKSPACE_ID.fullmatch(value) is None:
        raise ValueError("invalid workspace id")
    return value


def validate_workspace_root(root: Path, *, home: Path | None = None) -> Path:
    raw = root.expanduser()
    if not raw.is_absolute():
        raise ValueError("workspace root must be absolute")
    if _has_control_character(str(raw)):
        raise ValueError("workspace root contains control characters")
    try:
        canonical = raw.resolve(strict=True)
    except (FileNotFoundError, RuntimeError) as exc:
        raise ValueError("workspace root must be an existing directory") from exc
    if not canonical.is_dir():
        raise ValueError("workspace root must be an existing directory")
    if str(raw) != str(canonical):
        raise ValueError("workspace root must be canonical")
    if canonical == Path(canonical.anchor):
        raise ValueError("workspace root cannot be the filesystem root")
    canonical_home = (Path.home() if home is None else home).expanduser().resolve(strict=False)
    if canonical == canonical_home:
        raise ValueError("workspace root cannot be the remote home directory")
    return canonical


def _validate_absolute_path(path: Path, *, label: str) -> Path:
    raw = path.expanduser()
    if not raw.is_absolute():
        raise ValueError(f"{label} must be absolute")
    canonical = raw.resolve(strict=False)
    if str(raw) != str(canonical):
        raise ValueError(f"{label} must be canonical")
    return canonical


def _validate_event(
    kind: str,
    path: str,
    destination_path: str | None,
) -> tuple[str, str, str | None]:
    if kind not in _EVENT_KINDS:
        raise ValueError(f"invalid remote event kind: {kind}")
    if kind == "rescan-required":
        if path != "*" or destination_path is not None:
            raise ValueError("rescan event must use '*' without a destination")
        return kind, path, None
    safe_path = _validate_relative_path(path)
    if kind == "move":
        if destination_path is None:
            raise ValueError("move event requires a destination")
        return kind, safe_path, _validate_relative_path(destination_path)
    if destination_path is not None:
        raise ValueError("only move events may have a destination")
    return kind, safe_path, None


def _validate_relative_path(value: str) -> str:
    if not value or value == "." or _has_control_character(value) or "\\" in value:
        raise ValueError("event path must be a safe relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("event path must be a safe relative path")
    normalized = path.as_posix()
    if normalized != value:
        raise ValueError("event path must be a canonical relative path")
    return normalized


def _validate_sequence(value: int) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("sequence must be a non-negative integer")
    return value


def _event(row: sqlite3.Row) -> RemoteEvent:
    return RemoteEvent(
        int(row["sequence"]),
        str(row["kind"]),
        str(row["path"]),
        None if row["destination_path"] is None else str(row["destination_path"]),
        float(row["created_at"]),
    )


def _index_entry(row: sqlite3.Row | None) -> RemoteIndexEntry | None:
    if row is None:
        return None
    return RemoteIndexEntry(
        str(row["workspace_id"]),
        Path(str(row["root"])),
        Path(str(row["state_path"])),
    )


def _has_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def process_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
