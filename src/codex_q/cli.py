"""SQLite-backed priority queue CLI for local directive handoff."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

DEFAULT_DB = "~/.claude/codex-queue.db"
DB_ENV = "CODEX_Q_DB"
DEFAULT_REAP_STALE_SECONDS = int(os.environ.get("CODEX_Q_REAP_STALE_SECONDS", os.environ.get("LANEQ_REAP_STALE_SECONDS", "21600")))
PRIORITIES = {"P0": 0, "P1": 1, "P2": 2}
PRIORITY_NAMES = {value: key for key, value in PRIORITIES.items()}
EMPTY_EXIT_CODE = 3


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def db_path() -> Path:
    return Path(os.path.expanduser(os.environ.get(DB_ENV, DEFAULT_DB)))


def connect() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS directives(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            priority INTEGER NOT NULL DEFAULT 1,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT,
            taken_at TEXT,
            done_at TEXT
        )"""
    )
    return conn


def first_line(body: str, limit: int = 70) -> str:
    stripped = body.strip()
    text = stripped.splitlines()[0] if stripped else ""
    return f"{text[:limit]}..." if len(text) > limit else text


def read_body(args: argparse.Namespace) -> str:
    if args.body is not None:
        return args.body
    if args.file is not None:
        return Path(args.file).read_text()
    return sys.stdin.read()


def cmd_push(args: argparse.Namespace) -> int:
    body = read_body(args)
    if not body.strip():
        print("codex-q: empty body", file=sys.stderr)
        return 1
    conn = connect()
    cur = conn.execute(
        "INSERT INTO directives(priority, body, status, created_at) VALUES(?, ?, 'pending', ?)",
        (PRIORITIES[args.priority], body, utc_now()),
    )
    conn.commit()
    print(f"queued #{cur.lastrowid} [{args.priority}]: {first_line(body)}")
    return 0


def reap_stale(stale_seconds: int, *, quiet: bool = False) -> int:
    cutoff = dt.timedelta(seconds=stale_seconds)
    now = dt.datetime.now(dt.timezone.utc)
    conn = connect()
    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute("SELECT id, taken_at FROM directives WHERE status='taken' ORDER BY priority ASC, id ASC").fetchall()
    expired: list[tuple[int, str | None, int | None]] = []
    for item_id, taken_at in rows:
        taken = parse_time(taken_at)
        if taken is None or now - taken >= cutoff:
            expired.append((item_id, taken_at, None if taken is None else int((now - taken).total_seconds())))
    for item_id, _, _ in expired:
        conn.execute("UPDATE directives SET status='pending', taken_at=NULL WHERE id=?", (item_id,))
    conn.execute("COMMIT")
    if not quiet:
        if not expired:
            print("codex-q: no stale taken items")
        for item_id, taken_at, age in expired:
            detail = "unknown-age" if age is None else f"age_seconds={age}"
            print(f"#{item_id} -> pending ({detail}, taken_at={taken_at or '-'})")
    return len(expired)


def cmd_next(args: argparse.Namespace) -> int:
    if args.reap_stale_seconds is not None:
        reap_stale(args.reap_stale_seconds, quiet=True)
    conn = connect()
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT id, body FROM directives WHERE status='pending' ORDER BY priority ASC, id ASC LIMIT 1"
    ).fetchone()
    if row is None:
        conn.execute("COMMIT")
        return EMPTY_EXIT_CODE
    conn.execute("UPDATE directives SET status='taken', taken_at=? WHERE id=?", (utc_now(), row[0]))
    conn.execute("COMMIT")
    if args.id:
        print(f"#{row[0]}", file=sys.stderr)
    sys.stdout.write(row[1])
    return 0


def cmd_peek(args: argparse.Namespace) -> int:
    row = connect().execute(
        "SELECT id, priority, body FROM directives WHERE status='pending' ORDER BY priority ASC, id ASC LIMIT 1"
    ).fetchone()
    if row is None:
        return EMPTY_EXIT_CODE
    print(f"#{row[0]} [{PRIORITY_NAMES[row[1]]}]\n{row[2]}")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    row = connect().execute(
        "SELECT id, priority, status, created_at, taken_at, done_at, body FROM directives WHERE id=?",
        (args.id,),
    ).fetchone()
    if row is None:
        print(f"codex-q: no item #{args.id}", file=sys.stderr)
        return 1
    print(
        f"#{row[0]} [{PRIORITY_NAMES[row[1]]}] {row[2]}  "
        f"created={row[3] or '-'} taken={row[4] or '-'} done={row[5] or '-'}"
    )
    print(row[6])
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    query = "SELECT id, priority, status, created_at, body FROM directives"
    if not args.all:
        query += " WHERE status='pending'"
    query += " ORDER BY priority ASC, id ASC"
    rows = connect().execute(query).fetchall()
    if not rows:
        print("(queue empty)")
        return 0
    for item_id, priority, status, _, body in rows:
        flag = "" if status == "pending" else f" <{status}>"
        print(f"#{item_id:<4} {PRIORITY_NAMES[priority]}{flag}  {first_line(body)}")
    return 0


def cmd_reprioritize(args: argparse.Namespace) -> int:
    conn = connect()
    cur = conn.execute("UPDATE directives SET priority=? WHERE id=?", (PRIORITIES[args.priority], args.id))
    conn.commit()
    if cur.rowcount == 0:
        print(f"codex-q: no item #{args.id}", file=sys.stderr)
        return 1
    print(f"#{args.id} -> {args.priority}")
    return 0


def set_status(item_id: int, status: str) -> int:
    conn = connect()
    if status == "pending":
        cur = conn.execute("UPDATE directives SET status='pending', taken_at=NULL WHERE id=?", (item_id,))
    elif status == "done":
        cur = conn.execute("UPDATE directives SET status='done', done_at=? WHERE id=?", (utc_now(), item_id))
    else:
        cur = conn.execute("UPDATE directives SET status=?, taken_at=? WHERE id=?", (status, utc_now(), item_id))
    conn.commit()
    if cur.rowcount == 0:
        print(f"codex-q: no item #{item_id}", file=sys.stderr)
        return 1
    print(f"#{item_id} -> {status}")
    return 0


def cmd_done(args: argparse.Namespace) -> int:
    return set_status(args.id, "done")


def cmd_requeue(args: argparse.Namespace) -> int:
    return set_status(args.id, "pending")


def cmd_drop(args: argparse.Namespace) -> int:
    return set_status(args.id, "dropped")


def cmd_reap(args: argparse.Namespace) -> int:
    reap_stale(args.stale_seconds)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    rows = connect().execute(
        "SELECT priority, status, COUNT(*) FROM directives GROUP BY priority, status ORDER BY priority, status"
    ).fetchall()
    if not rows:
        print("(empty)")
        return 0
    for priority, status, count in rows:
        print(f"{PRIORITY_NAMES[priority]:<3} {status:<8} {count}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codex-q")
    sub = parser.add_subparsers(dest="cmd", required=True)

    push = sub.add_parser("push")
    push.add_argument("-p", "--priority", choices=PRIORITIES, default="P1")
    push.add_argument("-b", "--body")
    push.add_argument("-f", "--file")
    push.set_defaults(fn=cmd_push)

    next_parser = sub.add_parser("next")
    next_parser.add_argument("--id", action="store_true")
    next_parser.add_argument("--reap-stale-seconds", type=int, nargs="?", const=DEFAULT_REAP_STALE_SECONDS)
    next_parser.set_defaults(fn=cmd_next)

    sub.add_parser("peek").set_defaults(fn=cmd_peek)

    show = sub.add_parser("show")
    show.add_argument("id", type=int)
    show.set_defaults(fn=cmd_show)

    list_parser = sub.add_parser("list")
    list_parser.add_argument("--all", action="store_true")
    list_parser.set_defaults(fn=cmd_list)

    reprioritize = sub.add_parser("reprioritize")
    reprioritize.add_argument("id", type=int)
    reprioritize.add_argument("priority", choices=PRIORITIES)
    reprioritize.set_defaults(fn=cmd_reprioritize)

    done = sub.add_parser("done")
    done.add_argument("id", type=int)
    done.set_defaults(fn=cmd_done)

    requeue = sub.add_parser("requeue")
    requeue.add_argument("id", type=int)
    requeue.set_defaults(fn=cmd_requeue)

    drop = sub.add_parser("drop")
    drop.add_argument("id", type=int)
    drop.set_defaults(fn=cmd_drop)

    reap = sub.add_parser("reap")
    reap.add_argument("--stale-seconds", type=int, default=DEFAULT_REAP_STALE_SECONDS)
    reap.set_defaults(fn=cmd_reap)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
