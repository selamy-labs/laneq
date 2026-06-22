"""Pure queue operations shared by the CLI and the MCP server.

These functions hold the queue logic exactly once. They accept primitive
arguments, mutate the SQLite database through the shared helpers in
:mod:`laneq.cli`, and return structured results (dicts) instead of printing.
The CLI formats these results for humans; the MCP server serialises them to
JSON. Neither layer reimplements any SQL.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from laneq import cli

PRIORITIES = cli.PRIORITIES
PRIORITY_NAMES = cli.PRIORITY_NAMES
DEFAULT_LANE = cli.DEFAULT_LANE
DEFAULT_LEASE_SECONDS = cli.DEFAULT_LEASE_SECONDS
DEFAULT_REAP_STALE_SECONDS = cli.DEFAULT_REAP_STALE_SECONDS
TERMINAL_STATUSES = cli.TERMINAL_STATUSES
PARKED_STATUS = cli.PARKED_STATUS


class QueueError(Exception):
    """A queue operation failed for an expected, user-facing reason."""


def _priority_value(priority: str) -> int:
    if priority not in PRIORITIES:
        raise QueueError(f"invalid priority {priority!r}; expected one of {sorted(PRIORITIES)}")
    return PRIORITIES[priority]


def push(
    body: str,
    *,
    priority: str = "P1",
    parent: int | None = None,
    lane: str = DEFAULT_LANE,
) -> dict[str, Any]:
    """Enqueue a directive and return its stored representation."""
    if not body.strip():
        raise QueueError("empty body")
    priority_value = _priority_value(priority)
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    if not cli.parent_exists(conn, parent):
        raise QueueError(f"no parent #{parent}")
    cur = conn.execute(
        "INSERT INTO directives(priority, body, status, created_at, parent_id, lane) VALUES(?, ?, 'pending', ?, ?, ?)",
        (priority_value, body, cli.utc_now(), parent, lane),
    )
    conn.commit()
    return {
        "id": int(cur.lastrowid),
        "priority": priority,
        "lane": lane,
        "parent": parent,
        "status": "pending",
        "summary": cli.first_line(body),
    }


def take(
    *,
    consumer: str = "-",
    lease: str | int | None = None,
    lane: str = DEFAULT_LANE,
    reap_stale_seconds: int | None = None,
) -> dict[str, Any] | None:
    """Atomically take the highest-priority pending directive in ``lane``.

    Returns ``None`` when the lane has no pending work.
    """
    if reap_stale_seconds is not None:
        cli.reap_stale(reap_stale_seconds, quiet=True)
    conn = cli.connect()
    conn.execute("BEGIN IMMEDIATE")
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    row = conn.execute(
        "SELECT id, body FROM directives WHERE status='pending' AND lane=? ORDER BY priority ASC, id ASC LIMIT 1",
        (lane,),
    ).fetchone()
    if row is None:
        conn.execute("COMMIT")
        return None
    lease_seconds = cli.parse_duration(lease)
    conn.execute(
        "UPDATE directives SET status='taken', taken_at=?, taken_by=?, lease_until=? WHERE id=?",
        (cli.utc_now(), consumer, cli.utc_after(lease_seconds), row[0]),
    )
    conn.execute("COMMIT")
    return {"id": int(row[0]), "body": row[1], "consumer": consumer, "lane": lane}


def peek(*, lane: str = DEFAULT_LANE) -> dict[str, Any] | None:
    """Return the next pending directive in ``lane`` without taking it."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    row = conn.execute(
        "SELECT id, priority, body, lane FROM directives "
        "WHERE status='pending' AND lane=? ORDER BY priority ASC, id ASC LIMIT 1",
        (lane,),
    ).fetchone()
    if row is None:
        return None
    return {"id": int(row[0]), "priority": PRIORITY_NAMES[row[1]], "lane": row[3], "body": row[2]}


def _thread_payload(conn: sqlite3.Connection, item_id: int) -> list[dict[str, Any]]:
    return [
        {
            "id": int(r[0]),
            "priority": PRIORITY_NAMES[r[1]],
            "status": r[2],
            "parent": None if r[3] is None else int(r[3]),
            "summary": cli.first_line(r[4]),
        }
        for r in cli.thread_rows(conn, item_id)
    ]


def show(item_id: int) -> dict[str, Any]:
    """Return the full record for ``item_id`` including its thread."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    row = conn.execute(
        "SELECT id, priority, status, created_at, taken_at, done_at, body, taken_by, "
        "lease_until, requeue_count, parent_id, lane, not_before, blocked_by FROM directives WHERE id=?",
        (item_id,),
    ).fetchone()
    if row is None:
        raise QueueError(f"no item #{item_id}")
    has_thread = bool(cli.ancestors(conn, item_id) or cli.descendants(conn, item_id))
    return {
        "id": int(row[0]),
        "priority": PRIORITY_NAMES[row[1]],
        "status": row[2],
        "lane": row[11] or DEFAULT_LANE,
        "parent": None if row[10] is None else int(row[10]),
        "taken_by": row[7],
        "lease_until": row[8],
        "requeue_count": row[9] or 0,
        "created_at": row[3],
        "taken_at": row[4],
        "done_at": row[5],
        "not_before": row[12],
        "blocked_by": row[13],
        "body": row[6],
        "thread": _thread_payload(conn, item_id) if has_thread else [],
    }


def listing(
    *,
    all_statuses: bool = False,
    lane: str | None = None,
    thread: int | None = None,
) -> list[dict[str, Any]]:
    """List directives. Mirrors ``laneq list`` filtering semantics."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    if thread is not None:
        rows = cli.thread_rows(conn, thread)
        return [
            {
                "id": int(r[0]),
                "priority": PRIORITY_NAMES[r[1]],
                "status": r[2],
                "parent": None if r[3] is None else int(r[3]),
                "summary": cli.first_line(r[4]),
            }
            for r in rows
        ]
    params: list[Any] = []
    query = (
        "SELECT id, priority, status, parent_id, body, taken_by, lease_until, requeue_count, lane, "
        "not_before, blocked_by FROM directives"
    )
    where = []
    if not all_statuses:
        where.append("status='pending'")
    if lane is not None:
        where.append("lane=?")
        params.append(lane)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY priority ASC, id ASC"
    rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": int(r[0]),
            "priority": PRIORITY_NAMES[r[1]],
            "status": r[2],
            "parent": None if r[3] is None else int(r[3]),
            "taken_by": r[5],
            "lease_until": r[6],
            "requeue_count": r[7] or 0,
            "lane": r[8],
            "not_before": r[9],
            "blocked_by": r[10],
            "summary": cli.first_line(r[4]),
        }
        for r in rows
    ]


def reprioritize(item_id: int, priority: str) -> dict[str, Any]:
    """Change a directive's priority. Does not silently demote anything else."""
    priority_value = _priority_value(priority)
    conn = cli.connect()
    cur = conn.execute("UPDATE directives SET priority=? WHERE id=?", (priority_value, item_id))
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no item #{item_id}")
    return {"id": item_id, "priority": priority}


def set_status(item_id: int, status: str) -> dict[str, Any]:
    """Set a directive's status (``done``, ``pending``/requeue, ``dropped``)."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    if status == "pending":
        cur = conn.execute(
            "UPDATE directives SET status='pending', taken_at=NULL, taken_by=NULL, lease_until=NULL, "
            "not_before=NULL, blocked_by=NULL, requeue_count=COALESCE(requeue_count,0)+1 WHERE id=?",
            (item_id,),
        )
    elif status == "done":
        cur = conn.execute(
            "UPDATE directives SET status='done', done_at=?, taken_by=NULL, lease_until=NULL WHERE id=?",
            (cli.utc_now(), item_id),
        )
    else:
        cur = conn.execute(
            "UPDATE directives SET status=?, taken_at=?, taken_by=NULL, lease_until=NULL WHERE id=?",
            (status, cli.utc_now(), item_id),
        )
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no item #{item_id}")
    return {"id": item_id, "status": status}


def defer(
    item_id: int,
    *,
    until: str | None = None,
    delay: str | int | None = None,
    blocked_by: list[str] | None = None,
) -> dict[str, Any]:
    """Defer a directive until a time and/or dependency items are terminal."""
    if until and delay:
        raise QueueError("use either --until or --for, not both")
    not_before = None
    if until:
        if cli.parse_time(until) is None:
            raise QueueError("invalid --until; expected UTC timestamp like 2026-06-19T19:00:00Z")
        not_before = until
    elif delay:
        not_before = cli.utc_after(cli.parse_duration(delay))
    dep_ids: list[int] = []
    for value in blocked_by or []:
        dep_ids.extend(cli.parse_dependency_ids(value))
    dep_ids = sorted(set(dep_ids))
    if not not_before and not dep_ids:
        raise QueueError("defer requires --until, --for, or --blocked-by")
    if item_id in dep_ids:
        raise QueueError("an item cannot be blocked by itself")
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    for dep_id in dep_ids:
        if not cli.parent_exists(conn, dep_id):
            raise QueueError(f"no dependency #{dep_id}")
    blocked_text = cli.format_dependency_ids(dep_ids)
    cur = conn.execute(
        "UPDATE directives SET status='deferred', taken_at=NULL, taken_by=NULL, lease_until=NULL, "
        "not_before=?, blocked_by=? WHERE id=?",
        (not_before, blocked_text, item_id),
    )
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no item #{item_id}")
    return {"id": item_id, "status": "deferred", "not_before": not_before, "blocked_by": blocked_text}


def touch(item_id: int, *, lease: str | int | None = None) -> dict[str, Any]:
    """Extend the lease on a taken directive."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    cur = conn.execute(
        "UPDATE directives SET lease_until=? WHERE id=? AND status='taken'",
        (cli.utc_after(cli.parse_duration(lease)), item_id),
    )
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no taken item #{item_id}")
    lease_until = conn.execute("SELECT lease_until FROM directives WHERE id=?", (item_id,)).fetchone()[0]
    return {"id": item_id, "lease_until": lease_until}


def reap(*, expired_leases: bool = False, stale_seconds: int = DEFAULT_REAP_STALE_SECONDS) -> dict[str, Any]:
    """Reclaim stale taken directives or expired leases."""
    if expired_leases:
        count = cli.reclaim_expired_leases(quiet=True)
        return {"mode": "expired-leases", "reclaimed": count}
    count = cli.reap_stale(stale_seconds, quiet=True)
    return {"mode": "stale", "reclaimed": count, "stale_seconds": stale_seconds}


def stats() -> dict[str, Any]:
    """Return counts by priority/status and taken counts by consumer."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    by_status = [
        {"priority": PRIORITY_NAMES[priority], "status": status, "count": count}
        for priority, status, count in conn.execute(
            "SELECT priority, status, COUNT(*) FROM directives GROUP BY priority, status ORDER BY priority, status"
        ).fetchall()
    ]
    consumers = [
        {"consumer": consumer, "count": count}
        for consumer, count in conn.execute(
            "SELECT COALESCE(taken_by, '-'), COUNT(*) FROM directives "
            "WHERE status='taken' GROUP BY COALESCE(taken_by, '-') ORDER BY 1"
        ).fetchall()
    ]
    return {"by_status": by_status, "consumers": consumers}


def thread_status(item_id: int) -> dict[str, Any]:
    """Summarise whether the directive thread rooted at ``item_id`` has open work."""
    conn = cli.connect()
    cli.reclaim_expired_leases(conn)
    cli.reclaim_deferred(conn)
    rows = cli.thread_rows(conn, item_id)
    if not rows:
        raise QueueError(f"no item #{item_id}")
    open_rows = [r for r in rows if r[2] not in TERMINAL_STATUSES]
    return {
        "root": int(rows[0][0]),
        "status": "done" if not open_rows else "open",
        "total": len(rows),
        "open": len(open_rows),
        "open_items": [
            {
                "id": int(r[0]),
                "priority": PRIORITY_NAMES[r[1]],
                "status": r[2],
                "parent": None if r[3] is None else int(r[3]),
                "summary": cli.first_line(r[4]),
            }
            for r in open_rows
        ],
    }


def park(item_id: int) -> dict[str, Any]:
    """Move a taken directive into parked status (durable hold, excluded from claim/peek/reap)."""
    conn = cli.connect()
    cur = conn.execute(
        "UPDATE directives SET status=? WHERE id=? AND status='taken'",
        (PARKED_STATUS, item_id),
    )
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no taken item #{item_id}")
    return {"id": item_id, "status": PARKED_STATUS}


def unpark(item_id: int) -> dict[str, Any]:
    """Remove a directive from parked status (returns to pending)."""
    conn = cli.connect()
    cur = conn.execute(
        "UPDATE directives SET status='pending', taken_at=NULL, taken_by=NULL, lease_until=NULL WHERE id=? AND status=?",
        (item_id, PARKED_STATUS),
    )
    conn.commit()
    if cur.rowcount == 0:
        raise QueueError(f"no parked item #{item_id}")
    return {"id": item_id, "status": "pending"}
