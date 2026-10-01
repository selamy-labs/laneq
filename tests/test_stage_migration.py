"""Protected-stage migration preserves legacy data, backup and rollback."""

from contextlib import closing

import pytest

from laneq import cli, core


def legacy_database(path):
    import sqlite3

    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript(
            """CREATE TABLE directives(
            id INTEGER PRIMARY KEY AUTOINCREMENT, priority INTEGER NOT NULL DEFAULT 1,
            body TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT, taken_at TEXT, done_at TEXT, taken_by TEXT,
            claim_token TEXT, lease_until TEXT, requeue_count INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER REFERENCES directives(id), lane TEXT NOT NULL DEFAULT 'default',
            not_before TEXT, blocked_by TEXT);
            INSERT INTO directives(body,created_at) VALUES('preserve','2026-01-01T00:00:00Z');"""
        )


def schema(path):
    import sqlite3

    with closing(sqlite3.connect(path)) as conn:
        return conn.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()


def test_protected_migration_keeps_verified_prechange_backup(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    legacy_database(path)
    before = schema(path)
    monkeypatch.setenv("LANEQ_DB", str(path))
    assert core.show(1)["recovery_policy"] == "requeue"
    backups = list(tmp_path.glob("legacy.db.backup-*"))
    assert len(backups) == 1 and schema(backups[0]) == before
    cli.verify_sqlite_integrity(backups[0])
    assert {name for name, _ in schema(path)} >= {"directives", "handoff_receipts", "stage_admissions"}
    with closing(cli.connect()) as conn:
        assert cli.migration_plan(conn) == []
    assert core.take()["body"] == "preserve"


@pytest.mark.parametrize("failure_at", ["add_recovery_policy", "create_handoff_receipts", "create_stage_admissions"])
def test_stage_migration_failure_rolls_back_entire_addition(tmp_path, monkeypatch, failure_at):
    path = tmp_path / "legacy.db"
    legacy_database(path)
    before = schema(path)
    monkeypatch.setenv("LANEQ_DB", str(path))

    def injected(_step, name):
        if name == failure_at:
            raise RuntimeError("stage migration fault")

    monkeypatch.setattr(cli, "_MIGRATION_TEST_HOOK", injected)
    with pytest.raises(RuntimeError, match="stage migration fault"):
        cli.connect()
    assert schema(path) == before
    backups = list(tmp_path.glob("legacy.db.backup-*"))
    assert len(backups) == 1 and schema(backups[0]) == before
    cli.verify_sqlite_integrity(backups[0])
