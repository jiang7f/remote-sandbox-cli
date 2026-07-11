import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from remote_sandbox.daemon import _configure_daemon_logging


def test_daemon_log_rotates_and_is_user_only(tmp_path: Path) -> None:
    log_path = tmp_path / "metadata" / "daemon.log"
    _configure_daemon_logging(log_path)
    handlers = logging.getLogger("remote_sandbox.daemon").handlers
    handler = next(item for item in handlers if isinstance(item, RotatingFileHandler))
    assert handler.maxBytes == 5 * 1024 * 1024
    assert handler.backupCount == 3
    assert handler.encoding.lower().replace("-", "") == "utf8"
    assert log_path.stat().st_mode & 0o777 == 0o600
    assert log_path.parent.stat().st_mode & 0o777 == 0o700
