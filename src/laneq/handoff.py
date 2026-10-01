"""Atomic fenced completion and successor publication in one queue service."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from typing import Any

from laneq import cli, core
from laneq.handoff_contract import Handoff, bounded_text, parse_handoff
from laneq.stage_admission import admit_in_transaction


def sealed_receipt(conn: sqlite3.Connection, item_id: int, token: str, manifest: Handoff) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT claim_token,manifest_digest,receipt_json FROM handoff_receipts WHERE directive_id=?", (item_id,)
    ).fetchone()
    if row is None:
        return None
    if row[0] != token or row[1] != manifest.digest:
        raise core.PreconditionError("conflicting sealed handoff")
    return dict(json.loads(row[2]))


def require_input(conn: sqlite3.Connection, item_id: int, manifest: Handoff) -> None:
    row = conn.execute("SELECT body,recovery_policy FROM directives WHERE id=?", (item_id,)).fetchone()
    if row is None:
        raise core.NotFoundError(f"no item #{item_id}")
    if row[1] != "hold" or hashlib.sha256(row[0].encode()).hexdigest() != manifest.input_digest:
        raise core.PreconditionError("handoff requires an exact protected input")


def require_active(conn: sqlite3.Connection, item_id: int, token: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM directives WHERE id=? AND status='taken' AND claim_token=? "
        "AND lease_until>strftime('%Y-%m-%dT%H:%M:%SZ','now')",
        (item_id, token),
    ).fetchone()
    if row is None:
        raise core.PreconditionError("handoff claim is absent, stale or expired")


def insert_successors(conn: sqlite3.Connection, item_id: int, manifest: Handoff) -> list[int]:
    children: list[int] = []
    for child in manifest.successors:
        children.append(admit_in_transaction(conn, child, item_id))
    return children


def finish_parent(conn: sqlite3.Connection, item_id: int, token: str) -> None:
    cursor = conn.execute(
        "UPDATE directives SET status='done',done_at=?,taken_by=NULL,claim_token=NULL,lease_until=NULL "
        "WHERE id=? AND status='taken' AND claim_token=? AND lease_until>strftime('%Y-%m-%dT%H:%M:%SZ','now')",
        (cli.utc_now(), item_id, token),
    )
    if cursor.rowcount != 1:
        raise core.PreconditionError("handoff claim expired during transition")


def commit_handoff(conn: sqlite3.Connection, item_id: int, token: str, manifest: Handoff) -> dict[str, Any]:
    require_input(conn, item_id, manifest)
    require_active(conn, item_id, token)
    receipt = {
        "id": item_id,
        "status": "done",
        "input_digest": manifest.input_digest,
        "artifact_digest": manifest.artifact_digest,
        "receipt_ref": manifest.receipt_ref,
        "receipt_digest": manifest.receipt_digest,
        "manifest_digest": manifest.digest,
        "successor_ids": insert_successors(conn, item_id, manifest),
    }
    conn.execute(
        "INSERT INTO handoff_receipts(directive_id,claim_token,manifest_digest,manifest_json,receipt_json) "
        "VALUES(?,?,?,?,?)",
        (item_id, token, manifest.digest, manifest.encoded, json.dumps(receipt, sort_keys=True)),
    )
    finish_parent(conn, item_id, token)
    return receipt


def require_item_id(item_id: int) -> None:
    if isinstance(item_id, bool) or not isinstance(item_id, int) or not 1 <= item_id <= 9223372036854775807:
        raise core.QueueError("handoff ID must be a positive SQLite integer")


def complete(item_id: int, *, claim_token: str, manifest: object) -> dict[str, Any]:
    """Exact replay is safe; another token/content cannot overwrite a sealed result."""
    require_item_id(item_id)
    token = bounded_text(claim_token, 256)
    parsed = parse_handoff(manifest)
    with closing(cli.connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        with conn:
            existing = sealed_receipt(conn, item_id, token, parsed)
            return existing if existing is not None else commit_handoff(conn, item_id, token, parsed)


def get_receipt(item_id: int) -> dict[str, Any] | None:
    """Observe durable settlement without changing task ownership or retrying work."""
    require_item_id(item_id)
    with closing(cli.connect()) as conn:
        row = conn.execute("SELECT receipt_json FROM handoff_receipts WHERE directive_id=?", (item_id,)).fetchone()
        return None if row is None else dict(json.loads(row[0]))
