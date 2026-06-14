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
    assert "P0 <dropped>  low" in all_rows


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

    assert run_q(db, "list").stdout == "#1    P1  legacy\n"
    cols = {row[1] for row in rows(db, "PRAGMA table_info(directives)")}
    assert {"taken_by", "lease_until", "requeue_count", "parent_id", "lane"} <= cols
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


def test_push_rejects_missing_parent_and_thread_status_missing_item(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"

    missing_parent = run_q(db, "push", "--parent", "99", "-b", "orphan")
    assert missing_parent.returncode == 1
    assert "no parent #99" in missing_parent.stderr

    missing_thread = run_q(db, "thread-status", "99")
    assert missing_thread.returncode == 1
    assert "no item #99" in missing_thread.stderr
