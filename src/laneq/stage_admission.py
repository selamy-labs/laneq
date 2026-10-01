"""Stable admission identity shared by spreadsheet roots and stage successors."""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from typing import Any

from laneq import cli, core
from laneq.handoff_contract import Successor, parse_successor


def admit_in_transaction(conn: sqlite3.Connection, task: Successor, parent: int | None) -> int:
    fingerprint = (hashlib.sha256(task.body.encode()).hexdigest(), task.lane, core.PRIORITIES[task.priority])
    existing = conn.execute(
        "SELECT directive_id,body_digest,lane,priority FROM stage_admissions WHERE admission_key=?",
        (task.admission_key,),
    ).fetchone()
    if existing is not None:
        if tuple(existing[1:]) != fingerprint:
            raise core.PreconditionError("conflicting stage admission identity")
        return int(existing[0])
    cursor = conn.execute(
        "INSERT INTO directives(priority,body,status,created_at,parent_id,lane,recovery_policy) "
        "VALUES(?,?,'pending',?,?,?,'hold')",
        (fingerprint[2], task.body, cli.utc_now(), parent, task.lane),
    )
    item_id = int(cursor.lastrowid)
    conn.execute(
        "INSERT INTO stage_admissions(admission_key,directive_id,body_digest,lane,priority) VALUES(?,?,?,?,?)",
        (task.admission_key, item_id, *fingerprint),
    )
    return item_id


def admit(*, admission_key: str, body: str, lane: str, priority: str = "P1") -> dict[str, Any]:
    task = parse_successor({"admission_key": admission_key, "body": body, "lane": lane, "priority": priority})
    with closing(cli.connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        with conn:
            item_id = admit_in_transaction(conn, task, None)
            status, parent = conn.execute("SELECT status,parent_id FROM directives WHERE id=?", (item_id,)).fetchone()
            return {
                "id": item_id,
                "status": status,
                "lane": task.lane,
                "priority": task.priority,
                "parent": parent,
                "summary": cli.first_line(task.body),
            }
