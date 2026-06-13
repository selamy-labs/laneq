from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from codex_q import cli


class CliResult:
    def __init__(self, returncode: int, stdout: str, stderr: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def run_q(db: Path, *args: str, input_text: str | None = None) -> CliResult:
    stdout = StringIO()
    stderr = StringIO()
    old_db = os.environ.get("CODEX_Q_DB")
    os.environ["CODEX_Q_DB"] = str(db)
    old_stdin = sys.stdin
    if input_text is not None:
        sys.stdin = StringIO(input_text)
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            returncode = cli.main(list(args))
    finally:
        sys.stdin = old_stdin
        if old_db is None:
            os.environ.pop("CODEX_Q_DB", None)
        else:
            os.environ["CODEX_Q_DB"] = old_db
    return CliResult(returncode, stdout.getvalue(), stderr.getvalue())


def run_q_subprocess(db: Path, *args: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["CODEX_Q_DB"] = str(db)
    return subprocess.run(
        [sys.executable, "-m", "codex_q.cli", *args],
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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
    assert run_q(db, "reap", "--stale-seconds", "1").stdout == "codex-q: no stale taken items\n"


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
