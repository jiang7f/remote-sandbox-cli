from __future__ import annotations

import fcntl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from remote_sandbox.marker import local_meta_dir


class WorkspaceLockError(RuntimeError):
    pass


def _lock_path(local_root: Path) -> Path:
    metadata_dir = local_meta_dir(local_root)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    return metadata_dir / "sync.lock"


def acquire_workspace_lock(local_root: Path) -> BinaryIO:
    """Acquire the workspace lock and return the open handle.

    The caller owns the returned handle and must close it to release the lock
    (closing the fd drops the flock). Used by the daemon to hold the lock for its
    whole lifetime as a single-instance / exclusive-syncer guarantee.
    """
    handle = _lock_path(local_root).open("a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise WorkspaceLockError(
            f"workspace is already syncing: {local_root}. "
            "Wait for the current rsb process to finish, then retry."
        ) from exc
    return handle


@contextmanager
def workspace_lock(local_root: Path) -> Iterator[None]:
    handle = acquire_workspace_lock(local_root)
    try:
        yield
    finally:
        with handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
