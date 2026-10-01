"""Bounded JSON transport for a trusted, fixed-lane queue owner.

This is not a network capability service. The launcher pins the entire client
cohort and owns database access; model workloads receive neither this CLI nor
its credentials. Admission stays with the separate trusted producer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass

from laneq import cli, core, handoff
from laneq.handoff_contract import bounded_text, decode_manifest, object_fields, parse_handoff

FIELDS = {
    "claim": {"operation"},
    "inspect": {"operation", "task_id", "claim_token"},
    "renew": {"operation", "task_id", "claim_token"},
    "complete": {"operation", "task_id", "claim_token", "manifest"},
}


@dataclass(frozen=True)
class Owner:
    lane: str
    consumer: str
    lease_seconds: int = 90
    successor_lanes: tuple[str, ...] = ()

    def validate(self) -> None:
        bounded_text(self.lane, 128)
        bounded_text(self.consumer, 256)
        if isinstance(self.lease_seconds, bool) or not isinstance(self.lease_seconds, int):
            raise core.QueueError("invalid bridge lease")
        if not 1 <= self.lease_seconds <= 3600:
            raise core.QueueError("invalid bridge lease")
        for lane in self.successor_lanes:
            bounded_text(lane, 128)


def request(value: object) -> dict[str, object]:
    operation = value.get("operation") if isinstance(value, dict) else None
    if not isinstance(operation, str) or operation not in FIELDS:
        raise core.QueueError("invalid bridge operation")
    fields = object_fields(value, FIELDS[operation])
    if operation != "claim":
        bounded_text(fields["task_id"], 256)
        bounded_text(fields["claim_token"], 256)
    return fields


def lookup(conn: sqlite3.Connection, task_id: str, owner: Owner) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT d.id,d.body,d.claim_token,d.lease_until,a.body_digest,a.admission_key "
        "FROM stage_admissions a JOIN directives d ON d.id=a.directive_id "
        "WHERE a.admission_key=? AND a.lane=? AND d.lane=? AND d.recovery_policy='hold'",
        (task_id, owner.lane, owner.lane),
    ).fetchone()
    if row is None:
        raise core.PreconditionError("bridge task is outside the admitted lane")
    if hashlib.sha256(row["body"].encode()).hexdigest() != row["body_digest"]:
        raise core.PreconditionError("bridge input identity changed")
    return row


def snapshot(row: sqlite3.Row) -> dict[str, object]:
    return {
        "task_id": row["admission_key"],
        "claim_token": row["claim_token"],
        "input_digest": row["body_digest"],
        "lease_until": row["lease_until"],
        "body": row["body"],
    }


def inspect(task_id: str, token: str, owner: Owner) -> dict[str, object]:
    with closing(cli.connect()) as conn:
        conn.execute("BEGIN")
        row = lookup(conn, task_id, owner)
        handoff.require_active(conn, row["id"], token)
        return snapshot(row)


def claim(owner: Owner) -> dict[str, object] | None:
    taken = core.take(consumer=owner.consumer, lease=owner.lease_seconds, lane=owner.lane, recovery_policy="hold")
    if taken is None:
        return None
    with closing(cli.connect()) as conn:
        identity = conn.execute(
            "SELECT admission_key FROM stage_admissions WHERE directive_id=?", (taken["id"],)
        ).fetchone()
    if identity is None:
        # Retain the protected claim for reconciliation, never release or execute it.
        raise core.PreconditionError("bridge claim has no admission identity")
    return inspect(identity[0], taken["claim_token"], owner)


def renew(task_id: str, token: str, owner: Owner) -> dict[str, object]:
    with closing(cli.connect()) as conn:
        row = lookup(conn, task_id, owner)
        handoff.require_active(conn, row["id"], token)
        item_id = row["id"]
    core.touch(item_id, lease=owner.lease_seconds, claim_token=token)
    return inspect(task_id, token, owner)


def complete(task_id: str, token: str, value: object, owner: Owner) -> dict[str, object]:
    manifest = parse_handoff(value)
    with closing(cli.connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        with conn:
            item_id = lookup(conn, task_id, owner)["id"]
            existing = handoff.sealed_receipt(conn, item_id, token, manifest)
            if existing is not None:
                return existing
            if any(child.lane not in owner.successor_lanes for child in manifest.successors):
                raise core.PreconditionError("bridge successor lane is not authorized")
            # Reuse native fencing/outbox helpers under the same authorization lock.
            return handoff.commit_handoff(conn, item_id, token, manifest)


def operate(value: object, owner: Owner) -> dict[str, object] | None:
    owner.validate()
    fields = request(value)
    operation = fields["operation"]
    if operation == "claim":
        return claim(owner)
    task_id = str(fields["task_id"])
    token = str(fields["claim_token"])
    if operation == "complete":
        return complete(task_id, token, fields["manifest"], owner)
    action = {"inspect": inspect, "renew": renew}[str(operation)]
    return action(task_id, token, owner)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--consumer", required=True)
    parser.add_argument("--lease-seconds", type=int, default=90)
    parser.add_argument("--successor-lane", action="append", default=[])
    args = parser.parse_args(argv)
    owner = Owner(args.lane, args.consumer, args.lease_seconds, tuple(args.successor_lane))
    try:
        value = decode_manifest(sys.stdin.buffer.read(131073))
        result = operate(value, owner)
    except (core.QueueError, ValueError, OSError, sqlite3.Error) as error:
        # Never echo source bodies, arbitrary fields or backend error prose.
        print(json.dumps({"protocol": 1, "error": type(error).__name__}))
        return 1
    print(json.dumps({"protocol": 1, "result": result}, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
