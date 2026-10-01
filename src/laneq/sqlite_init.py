"""Bounded cold-start lock handling before any queue operation is dispatched."""

import sqlite3
import time


def transient_lock(error: sqlite3.OperationalError) -> bool:
    code = getattr(error, "sqlite_errorcode", 0) & 255
    return code in (5, 6) or str(error) in (  # Primary SQLITE_BUSY/LOCKED; named constants need Python 3.11.
        "database is locked",
        "database table is locked",
    )


def enable_wal(conn: sqlite3.Connection, *, timeout_seconds: float = 10) -> None:
    """PRAGMA mode changes may return BUSY without using the connection busy handler."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as error:
            if not transient_lock(error) or time.monotonic() >= deadline:
                raise
            time.sleep(0.01)
