"""Atomic resource/dependency constraints; trusted producer/publisher only.

This does not authenticate spreadsheet sources, approval or external CI/main.
The owner must verify those facts before calling; model processes get no database
or producer/publisher API. An expired lease never releases a writer reservation.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from typing import Any

from laneq import cli, core
from laneq.handoff_contract import bounded_text, decode_manifest, digest_text, parse_handoff
from laneq.reservation_contract import Reservation, parse_reservation
from laneq.stage_admission import admit_in_transaction

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS reserved_roots(
        admission_key TEXT PRIMARY KEY,
        work_id TEXT NOT NULL,
        namespace TEXT NOT NULL,
        qualification_digest TEXT NOT NULL,
        reservation_digest TEXT NOT NULL,
        directive_id INTEGER NOT NULL UNIQUE REFERENCES directives(id),
        final_key TEXT,
        delivery_digest TEXT
    )""",
    """CREATE UNIQUE INDEX IF NOT EXISTS reserved_active_work
        ON reserved_roots(work_id) WHERE delivery_digest IS NULL""",
    """CREATE TABLE IF NOT EXISTS reserved_paths(
        admission_key TEXT NOT NULL REFERENCES reserved_roots(admission_key),
        path TEXT NOT NULL,
        PRIMARY KEY(admission_key,path)
    )""",
)


def initialize(conn: sqlite3.Connection) -> None:
    # execute each DDL statement inside the caller's transaction; executescript
    # would commit before resource checks/admission and break crash atomicity.
    for statement in SCHEMA:
        conn.execute(statement)


def root(conn: sqlite3.Connection, key: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM reserved_roots WHERE admission_key=?", (key,)).fetchone()


def receipt(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    state = conn.execute("SELECT status FROM directives WHERE id=?", (row["directive_id"],)).fetchone()
    if state is None:
        raise core.PreconditionError("reserved directive is missing")
    return {
        "id": row["directive_id"],
        "work_id": row["work_id"],
        "reservation_digest": row["reservation_digest"],
        "status": state[0],
        "delivery_digest": row["delivery_digest"],
    }


def require_dependencies(conn: sqlite3.Connection, reservation: Reservation) -> None:
    for dependency in reservation.dependencies:
        found = conn.execute(
            "SELECT 1 FROM reserved_roots WHERE work_id=? AND delivery_digest=?",
            (dependency.work_id, dependency.delivery_digest),
        ).fetchone()
        if found is None:
            raise core.PreconditionError("dependency lacks the registered delivery receipt")


def require_resources(conn: sqlite3.Connection, reservation: Reservation) -> None:
    for path in reservation.paths:
        active = conn.execute(
            "SELECT 1 FROM reserved_paths p JOIN reserved_roots r USING(admission_key) "
            "WHERE r.namespace=? AND r.delivery_digest IS NULL AND "
            "(p.path='.' OR ?='.' OR instr(?||'/',p.path||'/')=1 OR instr(p.path||'/',?||'/')=1) LIMIT 1",
            (reservation.namespace, path, path, path),
        ).fetchone()
        if active is not None:
            raise core.PreconditionError("source paths overlap an unreleased writer reservation")
    if conn.execute(
        "SELECT 1 FROM reserved_roots WHERE work_id=? AND delivery_digest IS NULL", (reservation.work_id,)
    ).fetchone():
        raise core.PreconditionError("work identity already has an unreleased reservation")


def insert_root(conn: sqlite3.Connection, reservation: Reservation) -> sqlite3.Row:
    if conn.execute(
        "SELECT 1 FROM stage_admissions WHERE admission_key=?", (reservation.task.admission_key,)
    ).fetchone():
        raise core.PreconditionError("unreserved admission identity already exists")
    require_dependencies(conn, reservation)
    require_resources(conn, reservation)
    item_id = admit_in_transaction(conn, reservation.task, None)
    conn.execute(
        "INSERT INTO reserved_roots VALUES(?,?,?,?,?,?,NULL,NULL)",
        (
            reservation.task.admission_key,
            reservation.work_id,
            reservation.namespace,
            reservation.qualification_digest,
            reservation.digest,
            item_id,
        ),
    )
    conn.executemany(
        "INSERT INTO reserved_paths VALUES(?,?)",
        ((reservation.task.admission_key, path) for path in reservation.paths),
    )
    return root(conn, reservation.task.admission_key)


def admit(value: object) -> dict[str, Any]:
    reservation = parse_reservation(value)
    with closing(cli.connect()) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        with conn:
            initialize(conn)
            existing = root(conn, reservation.task.admission_key)
            if existing is not None and existing["reservation_digest"] != reservation.digest:
                raise core.PreconditionError("conflicting immutable reservation")
            return receipt(conn, existing if existing is not None else insert_root(conn, reservation))


def stage_node(conn: sqlite3.Connection, key: str) -> sqlite3.Row:
    node = conn.execute(
        "SELECT d.id,d.parent_id,d.status,d.body,d.lane,d.priority,d.recovery_policy,a.admission_key,"
        "a.body_digest,a.lane AS admitted_lane,h.manifest_json,h.manifest_digest "
        "FROM stage_admissions a JOIN directives d ON d.id=a.directive_id "
        "LEFT JOIN handoff_receipts h ON h.directive_id=d.id WHERE a.admission_key=?",
        (key,),
    ).fetchone()
    if node is None or node["status"] != "done" or node["manifest_json"] is None:
        raise core.PreconditionError("delivery requires completed fenced stages")
    if node["recovery_policy"] != "hold" or node["lane"] != node["admitted_lane"]:
        raise core.PreconditionError("delivery stage ownership changed")
    return node


def stage_manifest(node: sqlite3.Row):
    manifest = parse_handoff(decode_manifest(node["manifest_json"].encode()))
    if (
        manifest.digest != node["manifest_digest"]
        or hashlib.sha256(node["body"].encode()).hexdigest() != manifest.input_digest
        or node["body_digest"] != manifest.input_digest
    ):
        raise core.PreconditionError("delivery stage identity changed")
    return manifest


def previous_stage(conn: sqlite3.Connection, current: sqlite3.Row, account: str) -> sqlite3.Row:
    previous = {
        "publication": "validation",
        "validation": "review",
        "review": "implementation",
        "implementation": "review",
    }
    parent = conn.execute(
        "SELECT admission_key FROM stage_admissions WHERE directive_id=?", (current["parent_id"],)
    ).fetchone()
    if parent is None:
        raise core.PreconditionError("publication is outside the reserved lineage")
    ancestor = stage_node(conn, parent[0])
    expected = account + ":" + previous[current["lane"].split(":")[1]]
    sealed = stage_manifest(ancestor)
    linked = any(
        child.admission_key == current["admission_key"]
        and child.body == current["body"]
        and child.lane == current["lane"]
        and core.PRIORITIES[child.priority] == current["priority"]
        for child in sealed.successors
    )
    if ancestor["lane"] != expected or len(sealed.successors) != 1 or not linked:
        raise core.PreconditionError("delivery stages skipped or changed owning account")
    return ancestor


def require_lineage(conn: sqlite3.Connection, row: sqlite3.Row, final_key: str, delivery: str) -> None:
    current = stage_node(conn, final_key)
    account = stage_node(conn, row["admission_key"])["lane"].split(":")[0]
    manifest = stage_manifest(current)
    if current["lane"] != account + ":publication" or manifest.successors or manifest.receipt_digest != delivery:
        raise core.PreconditionError("delivery requires the exact final publication receipt")
    for _ in range(64):
        if current["id"] == row["directive_id"]:
            return
        current = previous_stage(conn, current, account)
    raise core.PreconditionError("delivery lineage exceeds bounded depth")


def record_delivery(
    *, admission_key: str, reservation_digest: str, final_key: str, delivery_digest: str
) -> dict[str, Any]:
    """Publisher calls only AFTER independent review and external CI/main proof.

    A native stage acknowledgement alone never calls this operation. No CLI or
    consumer bridge exposure is added. The exact four-stage fenced lineage is a
    necessary local check, not authentication of external publication evidence.
    """
    key = bounded_text(admission_key, 256)
    final = bounded_text(final_key, 256)
    grant = digest_text(reservation_digest)
    delivery = digest_text(delivery_digest)
    with closing(cli.connect()) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        with conn:
            initialize(conn)
            existing = root(conn, key)
            if existing is None or existing["reservation_digest"] != grant:
                raise core.PreconditionError("foreign delivery reservation")
            if existing["delivery_digest"] is not None:
                if existing["delivery_digest"] != delivery or existing["final_key"] != final:
                    raise core.PreconditionError("conflicting registered delivery")
                return receipt(conn, existing)
            require_lineage(conn, existing, final, delivery)
            conn.execute(
                "UPDATE reserved_roots SET final_key=?,delivery_digest=? WHERE admission_key=?", (final, delivery, key)
            )
            return receipt(conn, root(conn, key))
