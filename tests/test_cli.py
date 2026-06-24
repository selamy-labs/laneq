from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from laneq import cli


class CliResult:
    def __init__(self, returncode: int, stdout: str, stderr: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def run_q(db: Path, *args: str, input_text: str | None = None) -> CliResult:
    stdout = StringIO()
    stderr = StringIO()
    old_db = os.environ.get("LANEQ_DB")
    os.environ["LANEQ_DB"] = str(db)
    old_stdin = sys.stdin
    if input_text is not None:
        sys.stdin = StringIO(input_text)
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            returncode = cli.main(list(args))
    finally:
        sys.stdin = old_stdin
        if old_db is None:
            os.environ.pop("LANEQ_DB", None)
        else:
            os.environ["LANEQ_DB"] = old_db
    return CliResult(returncode, stdout.getvalue(), stderr.getvalue())


def run_q_subprocess(db: Path, *args: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["LANEQ_DB"] = str(db)
    return subprocess.run(
        [sys.executable, "-m", "laneq.cli", *args],
        input=input_text,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def rows(db: Path, query: str):
    con = sqlite3.connect(db)
    try:
        return con.execute(query).fetchall()
    finally:
        con.close()


def create_legacy_v1_database(db: Path) -> None:
    con = sqlite3.connect(db)
    con.execute(
        """CREATE TABLE directives(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            priority INTEGER NOT NULL DEFAULT 1,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT,
            taken_at TEXT,
            done_at TEXT
        )"""
    )
    con.execute(
        "INSERT INTO directives(priority, body, status, created_at) "
        "VALUES(1, 'legacy', 'pending', '2026-01-01T00:00:00Z')"
    )
    con.commit()
    con.close()


def backup_files(db: Path) -> list[Path]:
    return sorted(db.parent.glob(f"{db.name}.backup-*"))


def test_push_next_done_stats(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    assert run_q(db, "push", "-p", "P1", "-b", "build a small thing").returncode == 0

    taken = run_q(db, "next", "--id")

    assert taken.returncode == 0
    assert taken.stdout == "build a small thing"
    assert taken.stderr == "#1\n"
    assert run_q(db, "done", "1").stdout == "#1 -> done\n"
    stats = run_q(db, "stats")
    assert "P1  done     1" in stats.stdout


def test_priority_ordering_and_fifo_within_priority(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P2", "-b", "third")
    run_q(db, "push", "-p", "P0", "-b", "first")
    run_q(db, "push", "-p", "P0", "-b", "second")
    run_q(db, "push", "-p", "P1", "-b", "middle")

    assert run_q(db, "next").stdout == "first"
    assert run_q(db, "next").stdout == "second"
    assert run_q(db, "next").stdout == "middle"
    assert run_q(db, "next").stdout == "third"


def test_peek_show_list_reprioritize_requeue_and_drop(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P2", "-b", "low")
    run_q(db, "push", "-p", "P1", "-b", "normal")

    peek = run_q(db, "peek")
    assert peek.returncode == 0
    assert peek.stdout == "#2 [P1]\nnormal\n"

    assert run_q(db, "reprioritize", "1", "P0").stdout == "#1 -> P0\n"
    assert run_q(db, "list").stdout.splitlines()[0].startswith("#1")
    assert "P0  low" in run_q(db, "list").stdout.splitlines()[0]
    assert "low" in run_q(db, "show", "1").stdout

    assert run_q(db, "next").stdout == "low"
    assert run_q(db, "requeue", "1").stdout == "#1 -> pending\n"
    assert run_q(db, "drop", "1").stdout == "#1 -> dropped\n"
    all_rows = run_q(db, "list", "--all").stdout
    assert "#1" in all_rows
    # After requeue (setting to pending), requeue_count is incremented to 1
    assert "P0 <dropped> requeues=1  low" in all_rows


def test_push_from_file_and_stdin(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    body_file = tmp_path / "directive.txt"
    body_file.write_text("from file\nsecond line")

    assert run_q(db, "push", "-f", str(body_file)).returncode == 0
    assert run_q(db, "push", input_text="from stdin").returncode == 0

    assert run_q(db, "next").stdout == "from file\nsecond line"
    assert run_q(db, "next").stdout == "from stdin"


def test_empty_next_and_peek_exit_3(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    assert run_q(db, "next").returncode == 3
    assert run_q(db, "peek").returncode == 3


def test_legacy_codex_db_env_still_selects_database(tmp_path: Path) -> None:
    db = tmp_path / "legacy-env.db"
    old_laneq_db = os.environ.pop("LANEQ_DB", None)
    old_codex_db = os.environ.get("CODEX_Q_DB")
    os.environ["CODEX_Q_DB"] = str(db)
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            assert cli.main(["push", "-b", "legacy env"]) == 0
        assert db.exists()
        assert rows(db, "SELECT body,status FROM directives") == [("legacy env", "pending")]
    finally:
        os.environ.pop("CODEX_Q_DB", None)
        if old_codex_db is not None:
            os.environ["CODEX_Q_DB"] = old_codex_db
        if old_laneq_db is not None:
            os.environ["LANEQ_DB"] = old_laneq_db


def test_reap_requeues_stale_taken_item(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "stale")
    assert run_q(db, "next").stdout == "stale"
    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET taken_at='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    result = run_q(db, "reap", "--stale-seconds", "1")

    assert result.returncode == 0
    assert "#1 -> pending" in result.stdout
    assert rows(db, "SELECT status,taken_at FROM directives WHERE id=1") == [("pending", None)]


def test_next_can_reap_before_taking(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P1", "-b", "old")
    run_q(db, "next")
    run_q(db, "push", "-p", "P2", "-b", "new")
    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET taken_at='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    assert run_q(db, "next", "--reap-stale-seconds", "1").stdout == "old"
    assert rows(db, "SELECT id,status FROM directives ORDER BY id") == [(1, "taken"), (2, "pending")]


def test_concurrent_next_only_one_reader_wins_each_item(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "only")
    results: list[subprocess.CompletedProcess[str]] = []

    def take() -> None:
        results.append(run_q_subprocess(db, "next"))

    threads = [threading.Thread(target=take), threading.Thread(target=take)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(result.returncode for result in results) == [0, 3]
    assert [result.stdout for result in results].count("only") == 1
    assert rows(db, "SELECT status, COUNT(*) FROM directives GROUP BY status") == [("taken", 1)]


def test_missing_item_mutations_fail(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    for command in ("done", "requeue", "drop"):
        result = run_q(db, command, "404")
        assert result.returncode == 1
        assert "no item #404" in result.stderr


def test_empty_inputs_and_missing_items_report_errors(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    empty_push = run_q(db, "push", "-b", "   ")
    assert empty_push.returncode == 1
    assert "empty body" in empty_push.stderr

    missing_show = run_q(db, "show", "404")
    assert missing_show.returncode == 1
    assert "no item #404" in missing_show.stderr

    missing_repriority = run_q(db, "reprioritize", "404", "P0")
    assert missing_repriority.returncode == 1
    assert "no item #404" in missing_repriority.stderr


def test_empty_list_stats_and_no_stale_reap(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    assert run_q(db, "list").stdout == "(queue empty)\n"
    assert run_q(db, "stats").stdout == "(empty)\n"
    assert run_q(db, "reap", "--stale-seconds", "1").stdout == "laneq: no stale taken items\n"


def test_reap_ignores_fresh_taken_and_handles_unknown_age(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "fresh")
    run_q(db, "next")
    run_q(db, "push", "-p", "P0", "-b", "unknown")
    run_q(db, "next")

    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET taken_at=? WHERE id=1", (cli.utc_now(),))
    con.execute("UPDATE directives SET taken_at='not-a-time' WHERE id=2")
    con.commit()
    con.close()

    result = run_q(db, "reap", "--stale-seconds", "999999")

    assert result.returncode == 0
    assert "#2 -> pending (unknown-age, taken_at=not-a-time)" in result.stdout
    assert rows(db, "SELECT id,status FROM directives ORDER BY id") == [(1, "taken"), (2, "pending")]


def test_migration_adds_v2_columns_to_legacy_database(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    create_legacy_v1_database(db)

    assert run_q(db, "list").stdout == "#1    P1  legacy\n"
    cols = {row[1] for row in rows(db, "PRAGMA table_info(directives)")}
    assert {"taken_by", "lease_until", "requeue_count", "parent_id", "lane", "not_before", "blocked_by"} <= cols
    assert rows(db, "SELECT lane,requeue_count FROM directives WHERE id=1") == [("default", 0)]
    backups = backup_files(db)
    assert len(backups) == 1
    assert rows(backups[0], "PRAGMA integrity_check") == [("ok",)]


def test_migration_dry_run_shows_plan_without_touching_database(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    create_legacy_v1_database(db)

    result = run_q(db, "migrate", "--dry-run")

    assert result.returncode == 0
    assert "laneq migration plan:" in result.stdout
    assert "- add_taken_by" in result.stdout
    assert not backup_files(db)
    cols = {row[1] for row in rows(db, "PRAGMA table_info(directives)")}
    assert "lane" not in cols


def test_explicit_migration_prints_changes_and_verified_backup(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    create_legacy_v1_database(db)

    result = run_q(db, "migrate")

    assert result.returncode == 0
    assert "laneq migration complete: changes=" in result.stdout
    assert "backup=" in result.stdout
    backups = backup_files(db)
    assert len(backups) == 1
    assert rows(backups[0], "PRAGMA integrity_check") == [("ok",)]


def test_migration_failure_rolls_back_original_and_keeps_backup(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    create_legacy_v1_database(db)

    def fail_after_second_step(step: int, _name: str) -> None:
        if step == 2:
            raise RuntimeError("injected migration failure")

    old_hook = cli._MIGRATION_TEST_HOOK
    cli._MIGRATION_TEST_HOOK = fail_after_second_step
    try:
        result = run_q(db, "migrate")
    finally:
        cli._MIGRATION_TEST_HOOK = old_hook

    assert result.returncode == 1
    assert "injected migration failure" in result.stderr
    cols = {row[1] for row in rows(db, "PRAGMA table_info(directives)")}
    assert "taken_by" not in cols
    assert rows(db, "SELECT id, body, status FROM directives") == [(1, "legacy", "pending")]
    backups = backup_files(db)
    assert len(backups) == 1
    assert rows(backups[0], "PRAGMA integrity_check") == [("ok",)]


def test_migration_retention_prunes_old_backups(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    create_legacy_v1_database(db)
    for index in range(4):
        backup = tmp_path / f"legacy.db.backup-2026010{index}T000000Z"
        backup.write_bytes(db.read_bytes())

    result = run_q(db, "migrate", "--keep-backups", "2")

    assert result.returncode == 0
    assert len(backup_files(db)) == 2
    assert "pruned backups:" in result.stdout


def test_migration_from_intermediate_schema_adds_remaining_columns(tmp_path: Path) -> None:
    db = tmp_path / "legacy-v2.db"
    create_legacy_v1_database(db)
    con = sqlite3.connect(db)
    con.execute("ALTER TABLE directives ADD COLUMN taken_by TEXT")
    con.execute("ALTER TABLE directives ADD COLUMN lease_until TEXT")
    con.execute("ALTER TABLE directives ADD COLUMN requeue_count INTEGER NOT NULL DEFAULT 0")
    con.commit()
    con.close()

    assert run_q(db, "migrate").returncode == 0

    cols = {row[1] for row in rows(db, "PRAGMA table_info(directives)")}
    assert {"taken_by", "lease_until", "requeue_count", "parent_id", "lane", "not_before", "blocked_by"} <= cols
    assert rows(db, "SELECT lane,requeue_count FROM directives WHERE id=1") == [("default", 0)]


def test_next_records_consumer_lease_and_touch_extends(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "lease me")

    taken = run_q(db, "next", "--consumer", "worker-a", "--lease", "10m", "--id")

    assert taken.returncode == 0
    assert taken.stdout == "lease me"
    assert taken.stderr == "#1\n"
    stored = rows(db, "SELECT status,taken_by,lease_until,requeue_count FROM directives WHERE id=1")[0]
    assert stored[0] == "taken"
    assert stored[1] == "worker-a"
    assert stored[2] is not None
    assert stored[3] == 0
    assert "taken_by=worker-a" in run_q(db, "show", "1").stdout
    assert "consumers:\n  worker-a: 1" in run_q(db, "stats").stdout

    old_lease = stored[2]
    touched = run_q(db, "touch", "1", "--lease", "1h")

    assert touched.returncode == 0
    new_lease = rows(db, "SELECT lease_until FROM directives WHERE id=1")[0][0]
    assert new_lease > old_lease
    assert f"#1 lease_until={new_lease}" in touched.stdout


def test_expired_lease_lazy_reclaim_requeues_and_tracks_count(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "expired")
    assert run_q(db, "next", "--consumer", "dead-worker", "--lease", "1").stdout == "expired"
    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET lease_until='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    listed = run_q(db, "list")

    assert "#1" in listed.stdout
    assert "requeues=1" in listed.stdout
    assert rows(db, "SELECT status,taken_by,lease_until,requeue_count FROM directives WHERE id=1") == [
        ("pending", None, None, 1)
    ]
    assert run_q(db, "next", "--consumer", "worker-b").stdout == "expired"
    assert rows(db, "SELECT status,taken_by,requeue_count FROM directives WHERE id=1") == [("taken", "worker-b", 1)]


def test_expired_lease_reap_command_reports_reclaimed_items(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-b", "lease")
    run_q(db, "next", "--consumer", "worker")
    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET lease_until='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    result = run_q(db, "reap", "--expired-leases")

    assert result.returncode == 0
    assert "#1 -> pending (expired lease_until=2026-01-01T00:00:00Z, taken_by=worker)" in result.stdout


def test_lanes_isolate_next_peek_and_list(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "default work")
    run_q(db, "push", "-p", "P0", "--lane", "matchpoint", "-b", "matchpoint work")

    assert run_q(db, "peek", "--lane", "matchpoint").stdout == "#2 [P0 lane=matchpoint]\nmatchpoint work\n"
    assert "matchpoint work" in run_q(db, "list", "--lane", "matchpoint").stdout
    assert "default work" not in run_q(db, "list", "--lane", "matchpoint").stdout
    assert run_q(db, "next", "--lane", "matchpoint").stdout == "matchpoint work"
    assert run_q(db, "next").stdout == "default work"


def test_defer_until_keeps_future_work_out_of_next(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "future")
    run_q(db, "push", "-p", "P1", "-b", "ready")

    deferred = run_q(db, "defer", "1", "--until", "2999-01-01T00:00:00Z")

    assert deferred.returncode == 0
    assert "#1 -> deferred not_before=2999-01-01T00:00:00Z" in deferred.stdout
    assert run_q(db, "peek").stdout == "#2 [P1]\nready\n"
    assert run_q(db, "next").stdout == "ready"
    all_rows = run_q(db, "list", "--all").stdout
    assert "#1" in all_rows
    assert "P0 <deferred> not_before=2999-01-01T00:00:00Z" in all_rows
    assert rows(db, "SELECT status,not_before FROM directives WHERE id=1") == [("deferred", "2999-01-01T00:00:00Z")]


def test_defer_for_past_duration_reclaims_on_next_operation(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "soon")

    assert run_q(db, "defer", "1", "--for", "1").returncode == 0
    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET not_before='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    assert run_q(db, "next").stdout == "soon"
    assert rows(db, "SELECT status,not_before FROM directives WHERE id=1") == [("taken", None)]


def test_defer_blocked_by_dependency_releases_after_terminal_status(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P0", "-b", "dependency")
    run_q(db, "push", "-p", "P0", "-b", "blocked child")
    run_q(db, "push", "-p", "P1", "-b", "fallback")

    deferred = run_q(db, "defer", "2", "--blocked-by", "1")

    assert deferred.returncode == 0
    assert "#2 -> deferred blocked_by=1" in deferred.stdout
    assert run_q(db, "next").stdout == "dependency"
    assert run_q(db, "next").stdout == "fallback"

    run_q(db, "done", "1")
    assert run_q(db, "next").stdout == "blocked child"
    assert rows(db, "SELECT id,status,blocked_by FROM directives ORDER BY id") == [
        (1, "done", None),
        (2, "taken", None),
        (3, "taken", None),
    ]


def test_defer_rejects_invalid_or_missing_dependencies(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-b", "body")

    self_dep = run_q(db, "defer", "1", "--blocked-by", "1")
    assert self_dep.returncode == 1
    assert "cannot be blocked by itself" in self_dep.stderr

    missing = run_q(db, "defer", "1", "--blocked-by", "99")
    assert missing.returncode == 1
    assert "no dependency #99" in missing.stderr

    no_gate = run_q(db, "defer", "1")
    assert no_gate.returncode == 1
    assert "defer requires --until, --for, or --blocked-by" in no_gate.stderr


def test_threading_parent_show_list_and_status(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    run_q(db, "push", "-p", "P1", "-b", "root")
    run_q(db, "push", "-p", "P0", "--parent", "1", "-b", "child")
    run_q(db, "push", "-p", "P2", "--parent", "2", "-b", "grandchild")

    shown = run_q(db, "show", "2")
    assert "parent=#1" in shown.stdout
    assert "Thread:" in shown.stdout
    assert "#1 [P1] pending parent=-  root" in shown.stdout
    assert "#3 [P2] pending parent=#2  grandchild" in shown.stdout

    thread_list = run_q(db, "list", "--thread", "1").stdout
    assert "#1" in thread_list and "#2" in thread_list and "#3" in thread_list
    open_status = run_q(db, "thread-status", "2").stdout
    assert "thread #1 open total=3 open=3" in open_status

    run_q(db, "done", "1")
    run_q(db, "done", "2")
    run_q(db, "drop", "3")

    assert run_q(db, "thread-status", "1").stdout == "thread #1 done total=3 open=0\n"


def test_touch_missing_item_reports_error(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    result = run_q(db, "touch", "404")

    assert result.returncode == 1
    assert "no item #404" in result.stderr


def test_touch_not_taken_item_reports_error(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    # Push an item but don't take it
    push_result = run_q(db, "push", "-b", "test item")
    assert push_result.returncode == 0

    # Extract the item ID from the output: "queued #<id> [P1]: test item"
    lines = push_result.stdout.strip().split("\n")
    # The last line is the output from push
    output_line = lines[-1]
    # Format: "queued #1 [P1]: test item"
    item_id = output_line.split("#")[1].split()[0]

    # Try to touch it without taking it first
    result = run_q(db, "touch", item_id)

    assert result.returncode == 1
    assert f"no taken item #{item_id}" in result.stderr


def test_push_rejects_missing_parent_and_thread_status_missing_item(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    missing_parent = run_q(db, "push", "--parent", "99", "-b", "orphan")
    assert missing_parent.returncode == 1
    assert "no parent #99" in missing_parent.stderr

    missing_thread = run_q(db, "thread-status", "99")
    assert missing_thread.returncode == 1
    assert "no item #99" in missing_thread.stderr
