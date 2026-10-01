"""Stable identities deduplicate work across admission and stage handoffs."""

from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from laneq import cli, core, handoff, stage_admission
from laneq.handoff_contract import parse_handoff


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "stage.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    return path


def rows(db, query):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(query).fetchall()


def test_ordinary_claim_and_peek_cannot_accidentally_take_protected_work(db):
    protected = stage_admission.admit(admission_key="protected", body="stage work", lane="same")
    assert core.take(lane="same") is None
    assert core.peek(lane="same") is None
    ordinary = core.push("legacy work", lane="same")
    assert core.peek(lane="same")["id"] == ordinary["id"]
    assert core.peek(lane="same", recovery_policy="hold")["id"] == protected["id"]
    assert core.take(lane="same", recovery_policy="hold")["id"] == protected["id"]
    assert core.take(lane="same")["id"] == ordinary["id"]


@pytest.mark.parametrize("operation", [core.push, core.take, core.peek])
def test_invalid_claim_policy_cannot_open_database(db, operation):
    arguments = {"recovery_policy": "accidental"}
    if operation is core.push:
        arguments["body"] = "bad policy"
    with pytest.raises(core.QueueError, match="recovery policy"):
        operation(**arguments)
    assert not db.exists()


def test_concurrent_feed_delivery_creates_one_protected_task(db):
    def admit(_):
        return stage_admission.admit(
            admission_key="dev:row561:revision1:implementation", body="immutable job", lane="dev:implementation"
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        receipts = list(pool.map(admit, range(12)))
    assert all(receipt["id"] == receipts[0]["id"] for receipt in receipts)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]
    assert rows(db, "SELECT COUNT(*) FROM stage_admissions") == [(1,)]
    claim = core.take(lane="dev:implementation", recovery_policy="hold")
    assert (
        stage_admission.admit(
            admission_key="dev:row561:revision1:implementation", body="immutable job", lane="dev:implementation"
        )["status"]
        == "taken"
    )
    assert core.take(lane="dev:implementation", recovery_policy="hold") is None
    assert claim["input_digest"] == hashlib.sha256(b"immutable job").hexdigest()


@pytest.mark.parametrize("change", [{"body": "changed"}, {"lane": "matchpoint:implementation"}, {"priority": "P0"}])
def test_existing_identity_cannot_change_immutable_work_or_scope(db, change):
    original = {
        "admission_key": "qualified-work",
        "body": "exact assignment",
        "lane": "dev:implementation",
        "priority": "P1",
    }
    stage_admission.admit(**original)
    with pytest.raises(core.PreconditionError, match="conflicting stage admission"):
        stage_admission.admit(**{**original, **change})
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]


def test_handoff_reuses_exact_existing_successor_identity_without_duplicate_work(db):
    task = stage_admission.admit(admission_key="implementation", body="implementation", lane="dev:implementation")
    review = stage_admission.admit(admission_key="review:artifact-a", body="exact artifact review", lane="dev:review")
    claim = core.take(lane="dev:implementation", recovery_policy="hold")
    manifest = {
        "input_digest": claim["input_digest"],
        "artifact_digest": "a" * 64,
        "receipt_ref": "artifact://receipt",
        "receipt_digest": "b" * 64,
        "successors": [
            {
                "admission_key": "review:artifact-a",
                "body": "exact artifact review",
                "lane": "dev:review",
                "priority": "P1",
            }
        ],
    }
    receipt = handoff.complete(task["id"], claim_token=claim["claim_token"], manifest=manifest)
    assert receipt["successor_ids"] == [review["id"]]
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]


def test_conflicting_successor_rolls_back_entire_transition(db):
    stage_admission.admit(admission_key="implementation", body="implementation", lane="dev:implementation")
    stage_admission.admit(admission_key="review-existing", body="original", lane="dev:review")
    claim = core.take(lane="dev:implementation", recovery_policy="hold")
    manifest = {
        "input_digest": claim["input_digest"],
        "artifact_digest": "a" * 64,
        "receipt_ref": "artifact://receipt",
        "receipt_digest": "b" * 64,
        "successors": [
            {"admission_key": "first-child", "body": "fresh", "lane": "dev:review", "priority": "P1"},
            {"admission_key": "review-existing", "body": "changed scope", "lane": "dev:review", "priority": "P1"},
        ],
    }
    with pytest.raises(core.PreconditionError, match="conflicting stage admission"):
        handoff.complete(claim["id"], claim_token=claim["claim_token"], manifest=manifest)
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(2,)]
    assert rows(db, "SELECT COUNT(*) FROM stage_admissions") == [(2,)]
    assert rows(db, "SELECT COUNT(*) FROM handoff_receipts") == [(0,)]
    assert core.show(claim["id"])["status"] == "taken"


def test_duplicate_successor_keys_are_rejected_before_admission(db):
    child = {"admission_key": "one-key", "body": "review", "lane": "dev:review", "priority": "P1"}
    data = {
        "input_digest": "a" * 64,
        "artifact_digest": "b" * 64,
        "receipt_digest": "c" * 64,
        "receipt_ref": "artifact://receipt",
        "successors": [child, child],
    }
    with pytest.raises(core.QueueError, match="duplicate successor admission key"):
        parse_handoff(data)
    assert not db.exists()


def test_stable_root_admission_cli_replays_existing_job(db, capsys):
    command = [
        "push",
        "-b",
        "job",
        "--lane",
        "dev:implementation",
        "--recovery-policy",
        "hold",
        "--admission-key",
        "sheet:561:revision:implementation",
    ]
    assert cli.main(command) == 0
    capsys.readouterr()
    assert cli.main(command) == 0
    assert rows(db, "SELECT COUNT(*) FROM directives") == [(1,)]


@pytest.mark.parametrize("extra", [[], ["--recovery-policy", "hold", "--parent", "1"]])
def test_cli_prevents_ordinary_or_parented_admission_key_bypass(db, capsys, extra):
    assert cli.main(["push", "-b", "job", "--admission-key", "key", *extra]) == 1
    assert "stable root admission requires hold policy" in capsys.readouterr().err
    assert not db.exists()


@pytest.mark.parametrize("key", ["", " ", "é" * 129])
def test_admission_key_is_bounded_and_nonempty(db, key):
    with pytest.raises(core.QueueError):
        stage_admission.admit(admission_key=key, body="job", lane="dev:implementation")
    assert not db.exists()
