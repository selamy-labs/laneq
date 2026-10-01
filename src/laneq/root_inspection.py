"""Read an exact admission without creation, migration, replay or release.

An absent receipt is only an observation. A concurrent admission may commit
after this snapshot; absence never authorizes replay or releasing writer holds.
The trusted launcher must retain the same database identity used for admission.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Any

from laneq import cli, core
from laneq.reservation_contract import Reservation
from laneq.root_reservations import receipt, root


def require_schema(conn: sqlite3.Connection) -> None:
    # Compile every required projection even for an empty database. A missing or
    # partial schema is an unavailable backend, never an absent admission.
    for query in (
        "SELECT admission_key,work_id,namespace,qualification_digest,reservation_digest,"
        "directive_id,final_key,delivery_digest FROM reserved_roots LIMIT 0",
        "SELECT admission_key,path FROM reserved_paths LIMIT 0",
        "SELECT admission_key,directive_id,body_digest,lane,priority FROM stage_admissions LIMIT 0",
        "SELECT id,status FROM directives LIMIT 0",
    ):
        conn.execute(query)


def inspect(reservation: Reservation) -> dict[str, Any] | None:
    uri = cli.db_path().resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=10)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        require_schema(conn)
        existing = root(conn, reservation.task.admission_key)
        if existing is None:
            orphan = conn.execute(
                "SELECT 1 FROM stage_admissions WHERE admission_key=?", (reservation.task.admission_key,)
            ).fetchone()
            if orphan is not None:
                raise core.PreconditionError("unreserved admission identity already exists")
            return None
        if existing["reservation_digest"] != reservation.digest:
            raise core.PreconditionError("conflicting immutable reservation")
        return receipt(conn, existing)
