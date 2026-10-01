"""Cold-start retries are bounded and limited to SQLite lock conditions."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Barrier
from unittest.mock import Mock

import pytest

from laneq import cli, sqlite_init


def test_transient_startup_lock_retries_before_queue_work(monkeypatch):
    conn = Mock()
    conn.execute.side_effect = [sqlite3.OperationalError("database is locked"), None]
    sleeper = Mock()
    monkeypatch.setattr(sqlite_init.time, "sleep", sleeper)
    sqlite_init.enable_wal(conn)
    assert conn.execute.call_count == 2
    sleeper.assert_called_once_with(0.01)


@pytest.mark.parametrize("message", ["database disk image is malformed", "permission denied", "disk I/O error"])
def test_nonlock_errors_do_not_retry_or_sleep(monkeypatch, message):
    conn = Mock()
    conn.execute.side_effect = sqlite3.OperationalError(message)
    sleeper = Mock()
    monkeypatch.setattr(sqlite_init.time, "sleep", sleeper)
    with pytest.raises(sqlite3.OperationalError, match=message):
        sqlite_init.enable_wal(conn)
    sleeper.assert_not_called()
    assert conn.execute.call_count == 1


def test_unchanged_lock_stops_at_deadline(monkeypatch):
    conn = Mock()
    conn.execute.side_effect = sqlite3.OperationalError("database is locked")
    clock = iter([0, 0.2, 1])
    monkeypatch.setattr(sqlite_init.time, "monotonic", lambda: next(clock))
    sleeper = Mock()
    monkeypatch.setattr(sqlite_init.time, "sleep", sleeper)
    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        sqlite_init.enable_wal(conn, timeout_seconds=1)
    assert conn.execute.call_count == 2
    sleeper.assert_called_once_with(0.01)


@pytest.mark.parametrize("code", [5, 6, 261])
def test_primary_and_extended_lock_codes_are_recognized(code):
    error = sqlite3.OperationalError("opaque native message")
    error.sqlite_errorcode = code
    assert sqlite_init.transient_lock(error)


def test_failed_queue_initialization_closes_connection(tmp_path, monkeypatch):
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "failed.db"))
    conn = Mock()
    monkeypatch.setattr(cli.sqlite3, "connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(sqlite_init, "enable_wal", lambda *args: (_ for _ in ()).throw(RuntimeError("failed init")))
    with pytest.raises(RuntimeError, match="failed init"):
        cli.connect()
    conn.close.assert_called_once()


def test_simultaneous_cold_start_connections_preserve_schema_and_integrity(tmp_path, monkeypatch):
    path = tmp_path / "cold.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    barrier = Barrier(6)

    def connect(_):
        barrier.wait(timeout=10)
        with closing(cli.connect()) as conn:
            return conn.execute("PRAGMA journal_mode").fetchone()[0]

    with ThreadPoolExecutor(max_workers=6) as pool:
        modes = list(pool.map(connect, range(6)))
    assert modes == ["wal"] * 6
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM directives").fetchone() == (0,)
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
