from __future__ import annotations

import copy
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from laneq import cli, core, stage_admission, stage_bridge
from laneq import root_reservations as reservations


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "queue.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    return path


def grant(key="one", *, paths=None, namespace="repo:example/project", dependencies=None):
    return {
        "task": {"admission_key": key, "body": "immutable " + key, "lane": "dev:implementation", "priority": "P1"},
        "work_id": "sheet:" + key,
        "namespace": namespace,
        "qualification_digest": "a" * 64,
        "paths": ["src/a.py"] if paths is None else paths,
        "dependencies": [] if dependencies is None else dependencies,
    }


def sql(db, query, values=()):
    with closing(sqlite3.connect(db)) as conn, conn:
        return conn.execute(query, values).fetchall()


def complete_stage(task, successor=None):
    owner = stage_bridge.Owner(
        task["lane"], "test-owner", successor_lanes=(() if successor is None else (successor["lane"],))
    )
    claim = stage_bridge.claim(owner)
    assert claim["task_id"] == task["admission_key"]
    manifest = {
        "input_digest": claim["input_digest"],
        "artifact_digest": "b" * 64,
        "receipt_ref": "test:receipt",
        "receipt_digest": "c" * 64,
        "successors": [] if successor is None else [successor],
    }
    stage_bridge.complete(claim["task_id"], claim["claim_token"], manifest, owner)


def finish(value, stages=("review", "validation", "publication")):
    task = value["task"]
    account = task["lane"].split(":")[0]
    for index, stage in enumerate(stages):
        child = {
            "admission_key": task["admission_key"] + f":{index}",
            "body": "stage " + str(index),
            "lane": account + ":" + stage,
            "priority": "P1",
        }
        complete_stage(task, child)
        task = child
    complete_stage(task)
    return task["admission_key"]


def deliver(value, admitted, final):
    return reservations.record_delivery(
        admission_key=value["task"]["admission_key"],
        reservation_digest=admitted["reservation_digest"],
        final_key=final,
        delivery_digest="c" * 64,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("work_id", "white space"),
        ("work_id", "bad\x7f"),
        ("namespace", "UPPER"),
        ("paths", []),
        ("paths", ["a"] * 2),
        ("paths", ["a"] * 65),
        ("paths", "a"),
        ("paths", ["../a"]),
        ("paths", ["/a"]),
        ("paths", ["a//b"]),
        ("paths", ["a/./b"]),
        ("paths", ["a\\b"]),
        ("paths", ["a\n"]),
        ("dependencies", "bad"),
        ("dependencies", [{"work_id": "sheet:one", "delivery_digest": "c" * 64}]),
        ("dependencies", [{"work_id": "other", "delivery_digest": "c" * 64}] * 2),
        ("dependencies", [{"work_id": "other", "delivery_digest": "c" * 64}] * 65),
        ("qualification_digest", "wrong"),
    ],
)
def test_invalid_input_does_not_create_database(db, field, value):
    invalid = grant()
    invalid[field] = value
    with pytest.raises(core.QueueError):
        reservations.admit(invalid)
    assert not db.exists()


def test_invalid_root_lane_and_oversized_manifest_fail_before_connect(db):
    value = grant()
    value["task"]["lane"] = "dev:review"
    with pytest.raises(core.QueueError):
        reservations.admit(value)
    value = grant(paths=[f"{index}/" + "a" * 4000 for index in range(64)])
    with pytest.raises(core.QueueError, match="128 KiB"):
        reservations.admit(value)
    assert not db.exists()


def test_concurrent_exact_replay_creates_one_root(db):
    with closing(cli.connect()):
        pass
    value = grant(paths=["z", "a"])
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: reservations.admit(copy.deepcopy(value)), range(12)))
    assert all(result == results[0] for result in results)
    assert sql(db, "SELECT count(*) FROM directives") == [(1,)]
    assert sql(db, "SELECT count(*) FROM reserved_paths") == [(2,)]
    value["paths"].reverse()
    assert reservations.admit(value) == results[0]


@pytest.mark.parametrize(
    "field,value",
    [
        ("work_id", "different"),
        ("namespace", "repo:different"),
        ("qualification_digest", "d" * 64),
        ("paths", ["different"]),
        ("dependencies", [{"work_id": "other", "delivery_digest": "c" * 64}]),
    ],
)
def test_grant_replay_cannot_change_immutable_authority(db, field, value):
    original = grant()
    reservations.admit(original)
    original[field] = value
    with pytest.raises(core.PreconditionError, match="immutable"):
        reservations.admit(original)


@pytest.mark.parametrize(
    "held,new",
    [
        ("src", "src/a"),
        ("src/a", "src"),
        ("src/a", "src/a"),
        (".", "anything"),
        ("anything", "."),
        ("a_%", "a_%/b"),
        ("café", "café/a"),
    ],
)
def test_overlapping_writers_fail_atomically(db, held, new):
    reservations.admit(grant(paths=[held]))
    with pytest.raises(core.PreconditionError, match="overlap"):
        reservations.admit(grant("two", paths=[new]))
    assert sql(db, "SELECT count(*) FROM directives") == [(1,)]


def test_disjoint_names_and_namespaces_do_not_collide(db):
    reservations.admit(grant(paths=["src/a", "a_%"]))
    reservations.admit(grant("two", paths=["src/ab", "a_X"]))
    reservations.admit(grant("three", namespace="repo:another", paths=["."]))
    assert sql(db, "SELECT count(*) FROM directives") == [(3,)]


def test_concurrent_conflicting_admission_has_exactly_one_winner(db):
    with closing(cli.connect()):
        pass

    def attempt(index):
        try:
            reservations.admit(grant(str(index)))
            return True
        except core.PreconditionError:
            return False

    with ThreadPoolExecutor(max_workers=16) as pool:
        assert sum(pool.map(attempt, range(16))) == 1
    assert sql(db, "SELECT count(*) FROM stage_admissions") == [(1,)]


def test_fifteen_distinct_writers_can_admit_concurrently(db):
    with closing(cli.connect()):
        pass
    with ThreadPoolExecutor(max_workers=15) as pool:
        receipts = list(pool.map(lambda i: reservations.admit(grant(str(i), paths=[f"src/{i}"])), range(15)))
    assert len({receipt["id"] for receipt in receipts}) == 15


def test_work_identity_cannot_be_duplicated_across_accounts_or_namespaces(db):
    reservations.admit(grant())
    value = grant("two", paths=["different"], namespace="repo:other")
    value["work_id"] = "sheet:one"
    value["task"]["lane"] = "matchpoint:implementation"
    with pytest.raises(core.PreconditionError, match="identity"):
        reservations.admit(value)


def test_existing_unreserved_root_cannot_be_retrofitted(db):
    stage_admission.admit(**grant()["task"])
    with pytest.raises(core.PreconditionError, match="unreserved"):
        reservations.admit(grant())


def test_expired_lease_keeps_paths_held(db):
    reservations.admit(grant())
    owner = stage_bridge.Owner("dev:implementation", "owner")
    stage_bridge.claim(owner)
    sql(db, "UPDATE directives SET lease_until='2000-01-01T00:00:00Z'")
    assert stage_bridge.claim(owner) is None
    with pytest.raises(core.PreconditionError, match="overlap"):
        reservations.admit(grant("two"))
    assert sql(db, "SELECT status FROM directives") == [("parked",)]


def test_implementation_ack_is_not_delivery_or_dependency_proof(db):
    value = grant()
    admitted = reservations.admit(value)
    complete_stage(value["task"])
    with pytest.raises(core.PreconditionError, match="publication"):
        deliver(value, admitted, "one")
    with pytest.raises(core.PreconditionError, match="overlap"):
        reservations.admit(grant("two"))
    dependent = grant(
        "dependency", paths=["other"], dependencies=[{"work_id": "sheet:one", "delivery_digest": "c" * 64}]
    )
    with pytest.raises(core.PreconditionError, match="dependency"):
        reservations.admit(dependent)


@pytest.mark.parametrize(
    "stages",
    [("review", "validation", "publication"), ("review", "implementation", "review", "validation", "publication")],
)
def test_publisher_exact_lineage_releases_paths_and_satisfies_dependencies(db, stages):
    value = grant()
    admitted = reservations.admit(value)
    final = finish(value, stages)
    delivered = deliver(value, admitted, final)
    assert delivered["delivery_digest"] == "c" * 64
    assert deliver(value, admitted, final) == delivered
    assert reservations.admit(value) == delivered
    dependent = grant("two", dependencies=[{"work_id": "sheet:one", "delivery_digest": "c" * 64}])
    reservations.admit(dependent)
    # Replaying the released root does not reacquire the writer from its successor.
    assert reservations.admit(value) == delivered
    with pytest.raises(core.PreconditionError, match="overlap"):
        reservations.admit(grant("three"))
    with pytest.raises(core.PreconditionError, match="conflicting"):
        reservations.record_delivery(
            admission_key="one",
            reservation_digest=admitted["reservation_digest"],
            final_key=final,
            delivery_digest="d" * 64,
        )


@pytest.mark.parametrize("stages", [("publication",), ("review", "publication"), ("validation", "publication")])
def test_skipped_review_or_validation_cannot_release_writer(db, stages):
    value = grant()
    admitted = reservations.admit(value)
    with pytest.raises(core.PreconditionError, match="skipped"):
        deliver(value, admitted, finish(value, stages))
    assert sql(db, "SELECT delivery_digest FROM reserved_roots") == [(None,)]


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE directives SET body='changed' WHERE id=1",
        "UPDATE directives SET recovery_policy='requeue' WHERE id=1",
        "UPDATE stage_admissions SET body_digest='changed' WHERE directive_id=1",
        "UPDATE directives SET lane='matchpoint:implementation' WHERE id=1",
        "UPDATE handoff_receipts SET manifest_digest='changed' WHERE directive_id=1",
    ],
)
def test_changed_frozen_stage_cannot_release_writer(db, mutation):
    value = grant()
    admitted = reservations.admit(value)
    final = finish(value)
    sql(db, mutation)
    with pytest.raises(core.PreconditionError, match="changed"):
        deliver(value, admitted, final)


def test_sql_failure_rolls_back_directive_admission_and_all_paths(db):
    reservations.admit(grant())
    sql(
        db,
        "CREATE TRIGGER fail_path BEFORE INSERT ON reserved_paths WHEN NEW.path='z' "
        "BEGIN SELECT RAISE(ABORT,'test'); END",
    )
    with pytest.raises(sqlite3.IntegrityError):
        reservations.admit(grant("two", paths=["other", "z"]))
    assert sql(db, "SELECT count(*) FROM directives") == [(1,)]
    assert sql(db, "SELECT count(*) FROM reserved_roots") == [(1,)]
    assert sql(db, "SELECT count(*) FROM reserved_paths") == [(1,)]


def test_process_death_during_path_insert_has_no_partial_admission(db):
    with closing(cli.connect()):
        pass
    script = """
import json, os, sqlite3, sys
from laneq import cli, root_reservations
class Crash(sqlite3.Connection):
    def executemany(self, query, values):
        self.execute(query, next(iter(values)))
        os._exit(23)
cli.connect = lambda: sqlite3.connect(os.environ["LANEQ_DB"], factory=Crash)
root_reservations.admit(json.loads(sys.argv[1]))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(grant(paths=["a", "z"]))],
        env=os.environ.copy(),
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 23, result.stderr
    assert sql(db, "SELECT count(*) FROM directives") == [(0,)]
    assert sql(db, "SELECT count(*) FROM stage_admissions") == [(0,)]
    assert sql(db, "SELECT count(*) FROM sqlite_master WHERE name='reserved_roots'") == [(0,)]


def test_missing_directive_replay_does_not_claim_success(db):
    reservations.admit(grant())
    sql(db, "DELETE FROM directives")
    with pytest.raises(core.PreconditionError, match="missing"):
        reservations.admit(grant())


@pytest.mark.parametrize(
    "field,value", [("body", "changed"), ("priority", "P0"), ("lane", "matchpoint:implementation")]
)
def test_task_content_replay_cannot_change_grant(db, field, value):
    value_grant = grant()
    reservations.admit(value_grant)
    value_grant["task"][field] = value
    with pytest.raises(core.PreconditionError, match="immutable"):
        reservations.admit(value_grant)


def test_unknown_or_incomplete_publication_keeps_reservation(db):
    value = grant()
    admitted = reservations.admit(value)
    for final in ("missing", "one"):
        with pytest.raises(core.PreconditionError, match="completed"):
            deliver(value, admitted, final)
    with pytest.raises(core.PreconditionError, match="foreign"):
        reservations.record_delivery(
            admission_key="one", reservation_digest="d" * 64, final_key="one", delivery_digest="c" * 64
        )
    with pytest.raises(core.PreconditionError, match="foreign"):
        reservations.record_delivery(
            admission_key="missing", reservation_digest="d" * 64, final_key="one", delivery_digest="c" * 64
        )


def test_other_complete_lineage_is_not_this_roots_delivery(db):
    value = grant()
    admitted = reservations.admit(value)
    finish(value)
    other = grant("other", paths=["different"])
    reservations.admit(other)
    final = finish(other)
    with pytest.raises(core.PreconditionError, match="outside"):
        deliver(value, admitted, final)


def test_terminal_publication_must_have_no_successor_and_exact_receipt(db):
    value = grant()
    admitted = reservations.admit(value)
    final = finish(value)
    with pytest.raises(core.PreconditionError, match="exact final"):
        reservations.record_delivery(
            admission_key="one",
            reservation_digest=admitted["reservation_digest"],
            final_key=final,
            delivery_digest="d" * 64,
        )
    deliver(value, admitted, final)
    with pytest.raises(core.PreconditionError, match="conflicting"):
        reservations.record_delivery(
            admission_key="one",
            reservation_digest=admitted["reservation_digest"],
            final_key="other",
            delivery_digest="c" * 64,
        )


def test_changed_child_priority_cannot_match_sealed_parent(db):
    value = grant()
    admitted = reservations.admit(value)
    final = finish(value)
    sql(db, "UPDATE directives SET priority='P0' WHERE id=2")
    with pytest.raises(core.PreconditionError, match="skipped"):
        deliver(value, admitted, final)


def test_bounded_lineage_does_not_walk_unlimited_repair_cycles(db):
    value = grant()
    admitted = reservations.admit(value)
    stages = ("review", "implementation") * 32 + ("review", "validation", "publication")
    final = finish(value, stages)
    with pytest.raises(core.PreconditionError, match="bounded depth"):
        deliver(value, admitted, final)


def test_completed_branch_cannot_release_reservation_with_pending_sibling(db):
    value = grant()
    admitted = reservations.admit(value)
    owner = stage_bridge.Owner("dev:implementation", "owner", successor_lanes=("dev:review",))
    claim = stage_bridge.claim(owner)
    first = {"admission_key": "first", "body": "first branch", "lane": "dev:review", "priority": "P0"}
    sibling = {**first, "admission_key": "sibling", "body": "pending sibling", "priority": "P2"}
    stage_bridge.complete(
        claim["task_id"],
        claim["claim_token"],
        {
            "input_digest": claim["input_digest"],
            "artifact_digest": "b" * 64,
            "receipt_ref": "test:branch",
            "receipt_digest": "c" * 64,
            "successors": [first, sibling],
        },
        owner,
    )
    final = finish({"task": first}, stages=("validation", "publication"))
    with pytest.raises(core.PreconditionError, match="skipped"):
        deliver(value, admitted, final)
    assert sql(db, "SELECT status FROM directives WHERE body='pending sibling'") == [("pending",)]
    with pytest.raises(core.PreconditionError, match="overlap"):
        reservations.admit(grant("two"))


def test_nonterminal_publication_cannot_release_writer(db):
    value = grant()
    admitted = reservations.admit(value)
    # A publication with a successor is not a terminal delivery receipt.
    final = finish(value, stages=("review", "validation", "publication", "review"))
    publication = sql(db, "SELECT admission_key FROM stage_admissions WHERE lane='dev:publication'")[0][0]
    with pytest.raises(core.PreconditionError, match="exact final"):
        deliver(value, admitted, publication)
    assert final != publication
