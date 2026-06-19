"""SQLite-backed priority queue CLI for local directive handoff."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
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
DEFAULT_BACKUP_RETENTION = int(os.environ.get("LANEQ_BACKUP_RETENTION", "5"))
BASE_SCHEMA_SQL = f"""CREATE TABLE directives(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            priority INTEGER NOT NULL DEFAULT 1,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT,
            taken_at TEXT,
            done_at TEXT,
            taken_by TEXT,
            lease_until TEXT,
            requeue_count INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER REFERENCES directives(id),
            lane TEXT NOT NULL DEFAULT '{DEFAULT_LANE}'
        )"""
SCHEMA_MIGRATIONS = {
    "taken_by": "ALTER TABLE directives ADD COLUMN taken_by TEXT",
    "lease_until": "ALTER TABLE directives ADD COLUMN lease_until TEXT",
    "requeue_count": "ALTER TABLE directives ADD COLUMN requeue_count INTEGER NOT NULL DEFAULT 0",
    "parent_id": "ALTER TABLE directives ADD COLUMN parent_id INTEGER REFERENCES directives(id)",
    "lane": f"ALTER TABLE directives ADD COLUMN lane TEXT NOT NULL DEFAULT '{DEFAULT_LANE}'",
}
DATA_MIGRATIONS = [
    ("normalize_lane", "UPDATE directives SET lane=? WHERE lane IS NULL OR lane=''", (DEFAULT_LANE,)),
    ("normalize_requeue_count", "UPDATE directives SET requeue_count=0 WHERE requeue_count IS NULL", ()),
]
_MIGRATION_TEST_HOOK = None


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
    existed_before_open = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    migrate(
        conn,
        path=path,
        existed_before_open=existed_before_open,
        keep_backups=DEFAULT_BACKUP_RETENTION,
        report=sys.stderr,
    )
    return conn


class MigrationResult:
    def __init__(self, changes: list[str], backup_path: Path | None, pruned: list[Path]) -> None:
        self.changes = changes
        self.backup_path = backup_path
        self.pruned = pruned


def directives_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='directives'").fetchone()
    return row is not None


def migration_plan(conn: sqlite3.Connection) -> list[tuple[str, str, tuple[Any, ...]]]:
    if not directives_table_exists(conn):
        return [("create_directives", BASE_SCHEMA_SQL, ())]

    columns = {row[1] for row in conn.execute("PRAGMA table_info(directives)").fetchall()}
    plan: list[tuple[str, str, tuple[Any, ...]]] = []
    for column, sql in SCHEMA_MIGRATIONS.items():
        if column not in columns:
            plan.append((f"add_{column}", sql, ()))
    lane_needs_default = "lane" in columns and conn.execute(
        "SELECT COUNT(*) FROM directives WHERE lane IS NULL OR lane=''"
    ).fetchone()[0]
    if lane_needs_default:
        plan.append(DATA_MIGRATIONS[0])
    requeue_needs_default = "requeue_count" in columns and conn.execute(
        "SELECT COUNT(*) FROM directives WHERE requeue_count IS NULL"
    ).fetchone()[0]
    if requeue_needs_default:
        plan.append(DATA_MIGRATIONS[1])
    return plan


def verify_sqlite_integrity(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
    finally:
        conn.close()
    if result is None or result[0] != "ok":
        detail = "<no result>" if result is None else str(result[0])
        raise RuntimeError(f"backup integrity_check failed for {path}: {detail}")


def backup_candidates(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f"{path.name}.backup-*"), key=lambda item: item.name, reverse=True)


def prune_old_backups(path: Path, keep: int) -> list[Path]:
    if keep < 1:
        raise ValueError("--keep-backups must be at least 1")
    pruned: list[Path] = []
    for candidate in backup_candidates(path)[keep:]:
        candidate.unlink()
        pruned.append(candidate)
    return pruned


def make_backup_path(path: Path) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    candidate = path.with_name(f"{path.name}.backup-{stamp}")
    suffix = 1
    while candidate.exists():
        suffix += 1
        candidate = path.with_name(f"{path.name}.backup-{stamp}-{suffix}")
    return candidate


def backup_database(conn: sqlite3.Connection, path: Path, keep_backups: int) -> tuple[Path, list[Path]]:
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    backup_path = make_backup_path(path)
    shutil.copy2(path, backup_path)
    verify_sqlite_integrity(backup_path)
    pruned = prune_old_backups(path, keep_backups)
    return backup_path, pruned


def migrate(
    conn: sqlite3.Connection,
    *,
    path: Path | None = None,
    existed_before_open: bool = True,
    dry_run: bool = False,
    keep_backups: int = DEFAULT_BACKUP_RETENTION,
    report=None,
) -> MigrationResult:
    plan = migration_plan(conn)
    if dry_run:
        return MigrationResult([name for name, _, _ in plan], None, [])
    if not plan:
        return MigrationResult([], None, [])

    backup_path = None
    pruned: list[Path] = []
    if path is not None and existed_before_open:
        backup_path, pruned = backup_database(conn, path, keep_backups)

    changes: list[str] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        for step, (name, sql, params) in enumerate(plan, start=1):
            conn.execute(sql, params)
            changes.append(name)
            if _MIGRATION_TEST_HOOK is not None:
                _MIGRATION_TEST_HOOK(step, name)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    if report is not None and changes:
        backup = str(backup_path) if backup_path is not None else "<new database>"
        print(f"laneq: migrated database changes={','.join(changes)} backup={backup}", file=report)
    return MigrationResult(changes, backup_path, pruned)


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
    from laneq import core

    body = read_body(args)
    try:
        result = core.push(body, priority=args.priority, parent=args.parent, lane=args.lane)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
        return 1
    parent = f" parent=#{result['parent']}" if result["parent"] is not None else ""
    lane = "" if result["lane"] == DEFAULT_LANE else f" lane={result['lane']}"
    print(f"queued #{result['id']} [{result['priority']}{lane}{parent}]: {result['summary']}")
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
    from laneq import core

    result = core.take(
        consumer=args.consumer,
        lease=args.lease,
        lane=args.lane,
        reap_stale_seconds=args.reap_stale_seconds,
    )
    if result is None:
        return EMPTY_EXIT_CODE
    if args.id:
        print(f"#{result['id']}", file=sys.stderr)
    sys.stdout.write(result["body"])
    return 0


def cmd_peek(args: argparse.Namespace) -> int:
    from laneq import core

    result = core.peek(lane=args.lane)
    if result is None:
        return EMPTY_EXIT_CODE
    lane = "" if result["lane"] == DEFAULT_LANE else f" lane={result['lane']}"
    print(f"#{result['id']} [{result['priority']}{lane}]\n{result['body']}")
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
    from laneq import core

    try:
        result = core.show(args.id)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
        return 1
    parent = "-" if result["parent"] is None else f"#{result['parent']}"
    print(
        f"#{result['id']} [{result['priority']}] {result['status']} lane={result['lane']} "
        f"parent={parent} taken_by={result['taken_by'] or '-'} "
        f"lease_until={result['lease_until'] or '-'} requeues={result['requeue_count']} "
        f"created={result['created_at'] or '-'} taken={result['taken_at'] or '-'} done={result['done_at'] or '-'}"
    )
    print(result["body"])
    if result["thread"]:
        conn = connect()
        print_thread(thread_rows(conn, args.id))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    from laneq import core

    rows = core.listing(all_statuses=args.all, lane=args.lane, thread=args.thread)
    if not rows:
        print("(queue empty)")
        return 0
    for row in rows:
        flag = "" if row["status"] == "pending" else f" <{row['status']}>"
        detail = ""
        if "lane" in row:
            lane_detail = "" if row["lane"] == DEFAULT_LANE else f" lane={row['lane']}"
            consumer = f" by={row['taken_by']}" if row["taken_by"] else ""
            lease = f" lease={row['lease_until']}" if row["lease_until"] else ""
            requeue = f" requeues={row['requeue_count']}" if row["requeue_count"] else ""
            detail = f"{lane_detail}{consumer}{lease}{requeue}"
        print(f"#{row['id']:<4} {row['priority']}{flag}{detail}  {row['summary']}")
    return 0


def cmd_reprioritize(args: argparse.Namespace) -> int:
    from laneq import core

    try:
        core.reprioritize(args.id, args.priority)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
        return 1
    print(f"#{args.id} -> {args.priority}")
    return 0


def set_status(item_id: int, status: str) -> int:
    from laneq import core

    try:
        core.set_status(item_id, status)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
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
    from laneq import core

    result = core.stats()
    if not result["by_status"]:
        print("(empty)")
        return 0
    for entry in result["by_status"]:
        print(f"{entry['priority']:<3} {entry['status']:<8} {entry['count']}")
    if result["consumers"]:
        print("consumers:")
        for entry in result["consumers"]:
            print(f"  {entry['consumer']}: {entry['count']}")
    return 0


def cmd_touch(args: argparse.Namespace) -> int:
    from laneq import core

    try:
        result = core.touch(args.id, lease=args.lease)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
        return 1
    print(f"#{result['id']} lease_until={result['lease_until']}")
    return 0


def cmd_thread_status(args: argparse.Namespace) -> int:
    from laneq import core

    try:
        result = core.thread_status(args.id)
    except core.QueueError as error:
        print(f"laneq: {error}", file=sys.stderr)
        return 1
    print(f"thread #{result['root']} {result['status']} total={result['total']} open={result['open']}")
    for item in result["open_items"]:
        parent = "-" if item["parent"] is None else f"#{item['parent']}"
        print(f"  #{item['id']} [{item['priority']}] {item['status']} parent={parent}  {item['summary']}")
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    path = db_path()
    existed_before_open = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        try:
            result = migrate(
                conn,
                path=path,
                existed_before_open=existed_before_open,
                dry_run=args.dry_run,
                keep_backups=args.keep_backups,
            )
        except Exception as error:
            print(f"laneq: migration failed: {error}", file=sys.stderr)
            return 1
    finally:
        conn.close()

    if args.dry_run:
        if not result.changes:
            print("laneq migration plan: no changes")
        else:
            print("laneq migration plan:")
            for change in result.changes:
                print(f"- {change}")
        return 0

    if not result.changes:
        print("laneq migration complete: no changes")
        return 0
    backup = str(result.backup_path) if result.backup_path is not None else "<new database>"
    print(f"laneq migration complete: changes={','.join(result.changes)} backup={backup}")
    if result.pruned:
        print("pruned backups:")
        for backup_path in result.pruned:
            print(f"- {backup_path}")
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

    migrate_parser = sub.add_parser("migrate")
    migrate_parser.add_argument("--dry-run", action="store_true")
    migrate_parser.add_argument("--keep-backups", type=int, default=DEFAULT_BACKUP_RETENTION)
    migrate_parser.set_defaults(fn=cmd_migrate)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
