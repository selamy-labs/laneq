"""SQLite-backed priority queue CLI for local directive handoff."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

DEFAULT_DB = "~/.claude/laneq.db"
DB_ENV = "LANEQ_DB"
LEGACY_DB_ENV = "CODEX_Q_DB"
DEFAULT_REAP_STALE_SECONDS = int(
    os.environ.get("LANEQ_REAP_STALE_SECONDS", os.environ.get("CODEX_Q_REAP_STALE_SECONDS", "21600"))
)
DEFAULT_LEASE_SECONDS = int(os.environ.get("LANEQ_LEASE_SECONDS", os.environ.get("CODEX_Q_LEASE_SECONDS", "1800")))
DEFAULT_LANE = "default"
PRIORITIES = {"P0": 0, "P1": 1, "P2": 2}
PRIORITY_NAMES = {value: key for key, value in PRIORITIES.items()}
EMPTY_EXIT_CODE = 3
TERMINAL_STATUSES = {"done", "dropped"}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_after(seconds: int) -> str:
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def db_path() -> Path:
    return Path(os.path.expanduser(os.environ.get(DB_ENV, os.environ.get(LEGACY_DB_ENV, DEFAULT_DB))))


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
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(directives)").fetchall()}
    migrations = {
        "taken_by": "ALTER TABLE directives ADD COLUMN taken_by TEXT",
        "lease_until": "ALTER TABLE directives ADD COLUMN lease_until TEXT",
        "requeue_count": "ALTER TABLE directives ADD COLUMN requeue_count INTEGER NOT NULL DEFAULT 0",
        "parent_id": "ALTER TABLE directives ADD COLUMN parent_id INTEGER REFERENCES directives(id)",
        "lane": f"ALTER TABLE directives ADD COLUMN lane TEXT NOT NULL DEFAULT '{DEFAULT_LANE}'",
    }
    for column, sql in migrations.items():
        if column not in columns:
            conn.execute(sql)
    conn.execute("UPDATE directives SET lane=? WHERE lane IS NULL OR lane=''", (DEFAULT_LANE,))
    conn.execute("UPDATE directives SET requeue_count=0 WHERE requeue_count IS NULL")
    conn.commit()


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


def parse_duration(value: str | int | None, *, default: int = DEFAULT_LEASE_SECONDS) -> int:
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    suffix = text[-1]
    try:
        if suffix in multipliers:
            return max(1, int(float(text[:-1]) * multipliers[suffix]))
        return max(1, int(float(text)))
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid duration: {value}") from None


def reclaim_expired_leases(conn: sqlite3.Connection | None = None, *, quiet: bool = True) -> int:
    own_conn = conn is None
    conn = conn or connect()
    caller_had_transaction = conn.in_transaction
    now = utc_now()
    if own_conn:
        conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute(
        "SELECT id, taken_by, lease_until FROM directives "
        "WHERE status='taken' AND lease_until IS NOT NULL AND lease_until <= ? "
        "ORDER BY priority ASC, id ASC",
        (now,),
    ).fetchall()
    for item_id, _, _ in rows:
        conn.execute(
            "UPDATE directives SET status='pending', taken_at=NULL, taken_by=NULL, lease_until=NULL, "
            "requeue_count=COALESCE(requeue_count,0)+1 WHERE id=?",
            (item_id,),
        )
    if own_conn:
        conn.execute("COMMIT")
    elif rows and not caller_had_transaction:
        conn.commit()
    if not quiet:
        if not rows:
            print("laneq: no expired leases")
        for item_id, taken_by, lease_until in rows:
            print(f"#{item_id} -> pending (expired lease_until={lease_until or '-'}, taken_by={taken_by or '-'})")
    return len(rows)


def parent_exists(conn: sqlite3.Connection, parent_id: int | None) -> bool:
    if parent_id is None:
        return True
    return conn.execute("SELECT 1 FROM directives WHERE id=?", (parent_id,)).fetchone() is not None


def cmd_push(args: argparse.Namespace) -> int:
    body = read_body(args)
    if not body.strip():
        print("laneq: empty body", file=sys.stderr)
        return 1
    conn = connect()
    reclaim_expired_leases(conn)
    if not parent_exists(conn, args.parent):
        print(f"laneq: no parent #{args.parent}", file=sys.stderr)
        return 1
    cur = conn.execute(
        "INSERT INTO directives(priority, body, status, created_at, parent_id, lane) VALUES(?, ?, 'pending', ?, ?, ?)",
        (PRIORITIES[args.priority], body, utc_now(), args.parent, args.lane),
    )
    conn.commit()
    parent = f" parent=#{args.parent}" if args.parent is not None else ""
    lane = "" if args.lane == DEFAULT_LANE else f" lane={args.lane}"
    print(f"queued #{cur.lastrowid} [{args.priority}{lane}{parent}]: {first_line(body)}")
    return 0


def reap_stale(stale_seconds: int, *, quiet: bool = False) -> int:
    cutoff = dt.timedelta(seconds=stale_seconds)
    now = dt.datetime.now(dt.timezone.utc)
    conn = connect()
    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute(
        "SELECT id, taken_at FROM directives WHERE status='taken' ORDER BY priority ASC, id ASC"
    ).fetchall()
    expired: list[tuple[int, str | None, int | None]] = []
    for item_id, taken_at in rows:
        taken = parse_time(taken_at)
        if taken is None or now - taken >= cutoff:
            expired.append((item_id, taken_at, None if taken is None else int((now - taken).total_seconds())))
    for item_id, _, _ in expired:
        conn.execute(
            "UPDATE directives SET status='pending', taken_at=NULL, taken_by=NULL, lease_until=NULL, "
            "requeue_count=COALESCE(requeue_count,0)+1 WHERE id=?",
            (item_id,),
        )
    conn.execute("COMMIT")
    if not quiet:
        if not expired:
            print("laneq: no stale taken items")
        for item_id, taken_at, age in expired:
            detail = "unknown-age" if age is None else f"age_seconds={age}"
            print(f"#{item_id} -> pending ({detail}, taken_at={taken_at or '-'})")
    return len(expired)


def cmd_next(args: argparse.Namespace) -> int:
    if args.reap_stale_seconds is not None:
        reap_stale(args.reap_stale_seconds, quiet=True)
    conn = connect()
    conn.execute("BEGIN IMMEDIATE")
    reclaim_expired_leases(conn)
    row = conn.execute(
        "SELECT id, body FROM directives WHERE status='pending' AND lane=? ORDER BY priority ASC, id ASC LIMIT 1",
        (args.lane,),
    ).fetchone()
    if row is None:
        conn.execute("COMMIT")
        return EMPTY_EXIT_CODE
    lease_seconds = parse_duration(args.lease)
    conn.execute(
        "UPDATE directives SET status='taken', taken_at=?, taken_by=?, lease_until=? WHERE id=?",
        (utc_now(), args.consumer, utc_after(lease_seconds), row[0]),
    )
    conn.execute("COMMIT")
    if args.id:
        print(f"#{row[0]}", file=sys.stderr)
    sys.stdout.write(row[1])
    return 0


def cmd_peek(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    row = conn.execute(
        "SELECT id, priority, body, lane FROM directives "
        "WHERE status='pending' AND lane=? "
        "ORDER BY priority ASC, id ASC LIMIT 1",
        (args.lane,),
    ).fetchone()
    if row is None:
        return EMPTY_EXIT_CODE
    lane = "" if row[3] == DEFAULT_LANE else f" lane={row[3]}"
    print(f"#{row[0]} [{PRIORITY_NAMES[row[1]]}{lane}]\n{row[2]}")
    return 0


def ancestors(conn: sqlite3.Connection, item_id: int) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    seen: set[int] = set()
    current = item_id
    while True:
        row = conn.execute(
            "SELECT id, priority, status, parent_id, body FROM directives WHERE id=?",
            (current,),
        ).fetchone()
        if row is None or row[3] is None or row[3] in seen:
            return list(reversed(out))
        seen.add(row[3])
        parent = conn.execute(
            "SELECT id, priority, status, parent_id, body FROM directives WHERE id=?",
            (row[3],),
        ).fetchone()
        if parent is None:
            return list(reversed(out))
        out.append(parent)
        current = int(parent[0])


def descendants(conn: sqlite3.Connection, item_id: int) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    stack = [item_id]
    while stack:
        current = stack.pop()
        children = conn.execute(
            "SELECT id, priority, status, parent_id, body FROM directives "
            "WHERE parent_id=? ORDER BY priority ASC, id ASC",
            (current,),
        ).fetchall()
        out.extend(children)
        stack.extend(int(row[0]) for row in reversed(children))
    return out


def thread_rows(conn: sqlite3.Connection, item_id: int) -> list[tuple[Any, ...]]:
    root = item_id
    while True:
        row = conn.execute("SELECT parent_id FROM directives WHERE id=?", (root,)).fetchone()
        if row is None or row[0] is None:
            break
        root = int(row[0])
    root_row = conn.execute(
        "SELECT id, priority, status, parent_id, body FROM directives WHERE id=?",
        (root,),
    ).fetchone()
    return ([] if root_row is None else [root_row]) + descendants(conn, root)


def print_thread(rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    print("Thread:")
    for item_id, priority, status, parent_id, body in rows:
        parent = "-" if parent_id is None else f"#{parent_id}"
        print(f"  #{item_id} [{PRIORITY_NAMES[priority]}] {status} parent={parent}  {first_line(body)}")


def cmd_show(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    row = conn.execute(
        "SELECT id, priority, status, created_at, taken_at, done_at, body, taken_by, "
        "lease_until, requeue_count, parent_id, lane FROM directives WHERE id=?",
        (args.id,),
    ).fetchone()
    if row is None:
        print(f"laneq: no item #{args.id}", file=sys.stderr)
        return 1
    print(
        f"#{row[0]} [{PRIORITY_NAMES[row[1]]}] {row[2]} lane={row[11] or DEFAULT_LANE} "
        f"parent={('-' if row[10] is None else '#' + str(row[10]))} taken_by={row[7] or '-'} "
        f"lease_until={row[8] or '-'} requeues={row[9] or 0} "
        f"created={row[3] or '-'} taken={row[4] or '-'} done={row[5] or '-'}"
    )
    print(row[6])
    thread = ancestors(conn, args.id) + descendants(conn, args.id)
    if thread:
        print_thread(thread_rows(conn, args.id))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    params: list[Any] = []
    if args.thread is not None:
        rows = thread_rows(conn, args.thread)
    else:
        query = (
            "SELECT id, priority, status, parent_id, body, taken_by, lease_until, requeue_count, lane FROM directives"
        )
        where = []
        if not args.all:
            where.append("status='pending'")
        if args.lane is not None:
            where.append("lane=?")
            params.append(args.lane)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY priority ASC, id ASC"
        rows = conn.execute(query, params).fetchall()
    if not rows:
        print("(queue empty)")
        return 0
    for row in rows:
        item_id, priority, status = row[0], row[1], row[2]
        body = row[4]
        flag = "" if status == "pending" else f" <{status}>"
        detail = ""
        if len(row) >= 9:
            taken_by, lease_until, requeues, lane = row[5], row[6], row[7], row[8]
            lane_detail = "" if lane == DEFAULT_LANE else f" lane={lane}"
            consumer = f" by={taken_by}" if taken_by else ""
            lease = f" lease={lease_until}" if lease_until else ""
            requeue = f" requeues={requeues}" if requeues else ""
            detail = f"{lane_detail}{consumer}{lease}{requeue}"
        print(f"#{item_id:<4} {PRIORITY_NAMES[priority]}{flag}{detail}  {first_line(body)}")
    return 0


def cmd_reprioritize(args: argparse.Namespace) -> int:
    conn = connect()
    cur = conn.execute("UPDATE directives SET priority=? WHERE id=?", (PRIORITIES[args.priority], args.id))
    conn.commit()
    if cur.rowcount == 0:
        print(f"laneq: no item #{args.id}", file=sys.stderr)
        return 1
    print(f"#{args.id} -> {args.priority}")
    return 0


def set_status(item_id: int, status: str) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    if status == "pending":
        cur = conn.execute(
            "UPDATE directives SET status='pending', taken_at=NULL, taken_by=NULL, lease_until=NULL WHERE id=?",
            (item_id,),
        )
    elif status == "done":
        cur = conn.execute(
            "UPDATE directives SET status='done', done_at=?, taken_by=NULL, lease_until=NULL WHERE id=?",
            (utc_now(), item_id),
        )
    else:
        cur = conn.execute(
            "UPDATE directives SET status=?, taken_at=?, taken_by=NULL, lease_until=NULL WHERE id=?",
            (status, utc_now(), item_id),
        )
    conn.commit()
    if cur.rowcount == 0:
        print(f"laneq: no item #{item_id}", file=sys.stderr)
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
    if args.expired_leases:
        reclaim_expired_leases(quiet=False)
    else:
        reap_stale(args.stale_seconds)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    rows = conn.execute(
        "SELECT priority, status, COUNT(*) FROM directives GROUP BY priority, status ORDER BY priority, status"
    ).fetchall()
    if not rows:
        print("(empty)")
        return 0
    for priority, status, count in rows:
        print(f"{PRIORITY_NAMES[priority]:<3} {status:<8} {count}")
    consumers = conn.execute(
        "SELECT COALESCE(taken_by, '-'), COUNT(*) FROM directives "
        "WHERE status='taken' GROUP BY COALESCE(taken_by, '-') ORDER BY 1"
    ).fetchall()
    if consumers:
        print("consumers:")
        for consumer, count in consumers:
            print(f"  {consumer}: {count}")
    return 0


def cmd_touch(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    cur = conn.execute(
        "UPDATE directives SET lease_until=? WHERE id=? AND status='taken'",
        (utc_after(parse_duration(args.lease)), args.id),
    )
    conn.commit()
    if cur.rowcount == 0:
        print(f"laneq: no taken item #{args.id}", file=sys.stderr)
        return 1
    lease_until = conn.execute("SELECT lease_until FROM directives WHERE id=?", (args.id,)).fetchone()[0]
    print(f"#{args.id} lease_until={lease_until}")
    return 0


def cmd_thread_status(args: argparse.Namespace) -> int:
    conn = connect()
    reclaim_expired_leases(conn)
    rows = thread_rows(conn, args.id)
    if not rows:
        print(f"laneq: no item #{args.id}", file=sys.stderr)
        return 1
    open_rows = [row for row in rows if row[2] not in TERMINAL_STATUSES]
    status = "done" if not open_rows else "open"
    print(f"thread #{rows[0][0]} {status} total={len(rows)} open={len(open_rows)}")
    for item_id, priority, item_status, parent_id, body in open_rows:
        parent = "-" if parent_id is None else f"#{parent_id}"
        print(f"  #{item_id} [{PRIORITY_NAMES[priority]}] {item_status} parent={parent}  {first_line(body)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="laneq")
    sub = parser.add_subparsers(dest="cmd", required=True)

    push = sub.add_parser("push")
    push.add_argument("-p", "--priority", choices=PRIORITIES, default="P1")
    push.add_argument("-b", "--body")
    push.add_argument("-f", "--file")
    push.add_argument("--parent", type=int)
    push.add_argument("--lane", default=DEFAULT_LANE)
    push.set_defaults(fn=cmd_push)

    next_parser = sub.add_parser("next")
    next_parser.add_argument("--id", action="store_true")
    next_parser.add_argument("--reap-stale-seconds", type=int, nargs="?", const=DEFAULT_REAP_STALE_SECONDS)
    next_parser.add_argument(
        "--consumer", default=os.environ.get("LANEQ_CONSUMER", os.environ.get("CODEX_Q_CONSUMER", "-"))
    )
    next_parser.add_argument("--lease", default=str(DEFAULT_LEASE_SECONDS))
    next_parser.add_argument("--lane", default=DEFAULT_LANE)
    next_parser.set_defaults(fn=cmd_next)

    peek = sub.add_parser("peek")
    peek.add_argument("--lane", default=DEFAULT_LANE)
    peek.set_defaults(fn=cmd_peek)

    show = sub.add_parser("show")
    show.add_argument("id", type=int)
    show.set_defaults(fn=cmd_show)

    list_parser = sub.add_parser("list")
    list_parser.add_argument("--all", action="store_true")
    list_parser.add_argument("--lane")
    list_parser.add_argument("--thread", type=int)
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
    reap.add_argument("--expired-leases", action="store_true")
    reap.set_defaults(fn=cmd_reap)

    touch = sub.add_parser("touch")
    touch.add_argument("id", type=int)
    touch.add_argument("--lease", default=str(DEFAULT_LEASE_SECONDS))
    touch.set_defaults(fn=cmd_touch)

    thread_status = sub.add_parser("thread-status")
    thread_status.add_argument("id", type=int)
    thread_status.set_defaults(fn=cmd_thread_status)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
