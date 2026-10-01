"""Real SQLite transaction, fencing and recovery tests; no model calls."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from laneq import cli, core, handoff


@pytest.fixture
def prepared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db = tmp_path / "stages.db"
    monkeypatch.setenv("LANEQ_DB", str(db))
    body = json.dumps({"sheet_id": "561", "source_revision": "immutable", "account": "dev"})
    task = core.push(body, lane="dev:implementation", recovery_policy="hold")
    claim = core.take(consumer="dev-muse-1", lane="dev:implementation", lease="10m", recovery_policy="hold")
    manifest = {
        "input_digest": hashlib.sha256(body.encode()).hexdigest(),
        "artifact_digest": "a" * 64,
        "receipt_ref": "artifact://dev/561/provider-receipt.json",
        "receipt_digest": "b" * 64,
        "successors": [
            {
                "admission_key": "dev:561:review:artifact-a",
                "body": "independent exact-artifact review",
                "lane": "dev:review",
                "priority": "P1",
            }
        ],
    }
    return db, task["id"], claim["claim_token"], manifest


def rows(db: Path, sql: str, args=()):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(sql, args).fetchall()


def execute_sql(db: Path, sql: str, args=()):
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute(sql, args)


def test_atomic_completion_seals_output_and_publishes_protected_review(prepared):
    db, item, token, manifest = prepared
    result = handoff.complete(item, claim_token=token, manifest=manifest)
    assert result["successor_ids"] == [2]
    assert result["artifact_digest"] == manifest["artifact_digest"]
    assert rows(db, "SELECT status,claim_token FROM directives WHERE id=?", (item,)) == [("done", None)]
    assert rows(db, "SELECT parent_id,lane,recovery_policy,status FROM directives WHERE id=2") == [
        (item, "dev:review", "hold", "pending")
    ]
    sealed = rows(db, "SELECT manifest_json,receipt_json FROM handoff_receipts WHERE directive_id=?", (item,))[0]
    assert json.loads(sealed[0]) == manifest
    assert json.loads(sealed[1]) == result
    assert core.take(lane="dev:review", recovery_policy="hold")["id"] == 2


def test_duplicate_acknowledgement_returns_receipt_without_reexecuting_or_duplicating(prepared):
    db, item, token, manifest = prepared
    result = handoff.complete(item, claim_token=token, manifest=manifest)
    assert handoff.complete(item, claim_token=token, manifest=manifest) == result
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(1,)]


@pytest.mark.parametrize("change", ["artifact", "source", "receipt", "children", "token"])
def test_sealed_handoff_rejects_changed_manifest_or_owner(prepared, change):
    db, item, token, manifest = prepared
    handoff.complete(item, claim_token=token, manifest=manifest)
    mutated = json.loads(json.dumps(manifest))
    if change == "token":
        token = "another-fence"
    elif change == "children":
        mutated["successors"][0]["body"] = "different scope"
    else:
        field = {"artifact": "artifact_digest", "source": "input_digest", "receipt": "receipt_digest"}[change]
        mutated[field] = "c" * 64
    with pytest.raises(core.PreconditionError, match="conflicting sealed handoff"):
        handoff.complete(item, claim_token=token, manifest=mutated)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]


@pytest.mark.parametrize("failure", ["wrong-token", "expired", "wrong-source", "missing"])
def test_rejects_stale_authority_before_creating_children(prepared, failure):
    db, item, token, manifest = prepared
    if failure == "expired":
        execute_sql(db, "UPDATE directives SET lease_until='2000-01-01T00:00:00Z'")
    elif failure == "wrong-token":
        token = "stale-fence"
    elif failure == "wrong-source":
        manifest["input_digest"] = "c" * 64
    else:
        item = 999
    with pytest.raises(core.QueueError):
        handoff.complete(item, claim_token=token, manifest=manifest)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(0,)]


def test_child_insert_failure_rolls_back_prior_child_and_entire_handoff(prepared):
    db, item, token, manifest = prepared
    execute_sql(
        db,
        """CREATE TRIGGER reject_child BEFORE INSERT ON directives
        WHEN NEW.body='blocked-child' BEGIN SELECT RAISE(ABORT,'forced child failure'); END""",
    )
    manifest["successors"].append(
        {"admission_key": "blocked", "body": "blocked-child", "lane": "dev:review", "priority": "P2"}
    )
    with pytest.raises(sqlite3.IntegrityError, match="forced child failure"):
        handoff.complete(item, claim_token=token, manifest=manifest)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]
    assert rows(db, "SELECT status,claim_token FROM directives") == [("taken", token)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(0,)]


def test_expiry_during_transition_rolls_back_children_and_receipt(prepared):
    db, item, token, manifest = prepared
    execute_sql(
        db,
        """CREATE TRIGGER expire_owner AFTER INSERT ON directives
        WHEN NEW.parent_id=1 BEGIN UPDATE directives SET lease_until='2000-01-01T00:00:00Z' WHERE id=1; END""",
    )
    with pytest.raises(core.PreconditionError, match="expired during transition"):
        handoff.complete(item, claim_token=token, manifest=manifest)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]
    assert rows(db, "SELECT status,claim_token FROM directives") == [("taken", token)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(0,)]


def test_racing_identical_completions_publish_exactly_once(prepared):
    db, item, token, manifest = prepared
    with ThreadPoolExecutor(max_workers=6) as pool:
        receipts = list(pool.map(lambda _: handoff.complete(item, claim_token=token, manifest=manifest), range(12)))
    assert all(receipt == receipts[0] for receipt in receipts)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(1,)]


@pytest.mark.parametrize("death_point", ["INSERT INTO handoff_receipts", "COMMIT", "after-commit"])
def test_process_death_has_an_unambiguous_durable_recovery_boundary(prepared, death_point):
    db, item, token, manifest = prepared
    script = """
import json,os,sys
from laneq import cli,handoff
point=sys.argv[4]
connect=cli.connect
def fault_connection():
    conn=connect()
    conn.set_trace_callback(lambda sql: os._exit(86) if sql.startswith(point) else None)
    return conn
if point!='after-commit': cli.connect=fault_connection
handoff.complete(int(sys.argv[1]),claim_token=sys.argv[2],manifest=json.loads(sys.argv[3]))
os._exit(86)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(item), token, json.dumps(manifest), death_point],
        env={**os.environ, "LANEQ_DB": str(db)},
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 86, result.stderr
    if death_point == "after-commit":
        assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]
        receipt = handoff.complete(item, claim_token=token, manifest=manifest)
        assert receipt["successor_ids"] == [2]
    else:
        assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]
        assert rows(db, "SELECT status,claim_token FROM directives") == [("taken", token)]
        assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(0,)]
    assert rows(db, "PRAGMA integrity_check") == [("ok",)]


@pytest.mark.parametrize("operation", ["show", "peek", "take", "stats", "list", "reap", "push", "stale-reap"])
def test_expired_protected_work_is_held_with_its_previous_owner_preserved(prepared, operation):
    db, item, token, _manifest = prepared
    execute_sql(db, "UPDATE directives SET lease_until='2000-01-01T00:00:00Z',taken_at='2000-01-01T00:00:00Z'")
    operations = {
        "show": lambda: core.show(item),
        "peek": lambda: core.peek(lane="dev:implementation"),
        "take": lambda: core.take(lane="dev:implementation", recovery_policy="hold"),
        "stats": core.stats,
        "list": lambda: core.listing(all_statuses=True),
        "reap": lambda: core.reap(expired_leases=True),
        "push": lambda: core.push("unrelated normal work"),
        "stale-reap": lambda: cli.reap_stale(1),
    }
    operations[operation]()
    assert rows(db, "SELECT status,claim_token,taken_by,requeue_count FROM directives WHERE id=?", (item,)) == [
        ("parked", token, "dev-muse-1", 0)
    ]
    assert core.take(lane="dev:implementation", recovery_policy="hold") is None
    assert core.peek(lane="dev:implementation") is None
    with pytest.raises(core.PreconditionError, match="verified recovery"):
        core.unpark(item)


@pytest.mark.parametrize("operation", ["done", "pending", "dropped", "defer"])
@pytest.mark.parametrize("administrative", [False, True])
def test_generic_mutation_cannot_release_protected_paid_work(prepared, operation, administrative):
    db, item, token, _manifest = prepared
    authority = {"force": True} if administrative else {"claim_token": token}
    with pytest.raises(core.PreconditionError, match="verified recovery"):
        if operation == "defer":
            core.defer(item, delay="1m", **authority)
        else:
            core.set_status(item, operation, **authority)
    assert rows(db, "SELECT status,claim_token FROM directives") == [("taken", token)]
    # The rejected call must not leave a live write lock behind.
    assert core.touch(item, claim_token=token)["id"] == item


def test_explicit_pause_preserves_protected_fence_until_reconciliation(prepared):
    db, item, token, _manifest = prepared
    core.park(item, claim_token=token)
    assert rows(db, "SELECT status,claim_token FROM directives") == [("parked", token)]
    assert core.take(lane="dev:implementation", recovery_policy="hold") is None
    with pytest.raises(core.PreconditionError):
        core.unpark(item)


def test_ordinary_work_retains_legacy_expiry_and_mutation_behavior(prepared):
    db, _item, _token, _manifest = prepared
    job = core.push("ordinary", lane="legacy")
    core.take(lane="legacy")
    execute_sql(db, "UPDATE directives SET lease_until='2000-01-01T00:00:00Z' WHERE id=?", (job["id"],))
    assert core.take(lane="legacy")["id"] == job["id"]
    assert core.set_status(job["id"], "done", force=True)["status"] == "done"
