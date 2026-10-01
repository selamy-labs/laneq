"""Real native queue behavior behind the fixed-lane machine interface."""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from laneq import core, stage_admission, stage_bridge

OWNER = stage_bridge.Owner("dev:implementation", "test-consumer", successor_lanes=("dev:review",))


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "bridge.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    return path


def admit(key="sheet:task:revision:implementation", lane=OWNER.lane):
    return stage_admission.admit(admission_key=key, body="immutable canonical task", lane=lane)


def take(owner=OWNER):
    return stage_bridge.operate({"operation": "claim"}, owner)


def frame(claim, operation):
    return {"operation": operation, "task_id": claim["task_id"], "claim_token": claim["claim_token"]}


def manifest(claim):
    return {
        "input_digest": claim["input_digest"],
        "artifact_digest": "a" * 64,
        "receipt_ref": "account-bound:receipt-1",
        "receipt_digest": "b" * 64,
        "successors": [
            {
                "admission_key": "sheet:task:artifact-a:review",
                "body": "immutable review",
                "lane": "dev:review",
                "priority": "P1",
            }
        ],
    }


def sql(db, query, values=()):
    with closing(sqlite3.connect(db)) as conn, conn:
        return conn.execute(query, values).fetchall()


def test_real_machine_claim_inspect_renew_complete_and_exact_replay(db):
    admit()
    claim = take()
    assert claim["task_id"] == "sheet:task:revision:implementation"
    assert claim["input_digest"] == hashlib.sha256(claim["body"].encode()).hexdigest()
    assert len(claim["claim_token"]) == 32
    assert stage_bridge.operate(frame(claim, "inspect"), OWNER) == claim
    renewed = stage_bridge.operate(frame(claim, "renew"), OWNER)
    assert {key: value for key, value in renewed.items() if key != "lease_until"} == {
        key: value for key, value in claim.items() if key != "lease_until"
    }
    assert renewed["lease_until"] >= claim["lease_until"]
    completed = {**frame(claim, "complete"), "manifest": manifest(claim)}
    receipt = stage_bridge.operate(completed, OWNER)
    assert receipt["status"] == "done" and len(receipt["successor_ids"]) == 1
    assert stage_bridge.operate(completed, OWNER) == receipt
    assert take() is None
    assert sql(db, "SELECT lane,status,recovery_policy FROM directives ORDER BY id") == [
        ("dev:implementation", "done", "hold"),
        ("dev:review", "pending", "hold"),
    ]
    with pytest.raises(core.PreconditionError):
        stage_bridge.operate(frame(claim, "inspect"), OWNER)


def test_twenty_clients_claim_distinct_real_tasks_and_continue_immediately(db):
    for index in range(20):
        admit(f"canonical:{index}")
    with ThreadPoolExecutor(max_workers=20) as pool:
        claims = list(pool.map(lambda _: take(), range(20)))
    assert len({claim["task_id"] for claim in claims}) == 20
    assert len({claim["claim_token"] for claim in claims}) == 20
    assert take() is None
    admit("next-ready-task")
    assert take()["task_id"] == "next-ready-task"


@pytest.mark.parametrize("operation", ["inspect", "renew", "complete"])
def test_other_lane_cannot_inspect_renew_or_complete_even_with_exact_fence(db, operation):
    admit()
    claim = take()
    value = frame(claim, operation)
    if operation == "complete":
        value["manifest"] = manifest(claim)
    foreign = stage_bridge.Owner("matchpoint:implementation", "foreign", successor_lanes=("dev:review",))
    with pytest.raises(core.PreconditionError, match="outside"):
        stage_bridge.operate(value, foreign)
    assert sql(db, "SELECT status FROM directives") == [("taken",)]


@pytest.mark.parametrize("operation", ["inspect", "renew", "complete"])
def test_stale_token_rejected_by_native_transaction(db, operation):
    admit()
    claim = take()
    value = {**frame(claim, operation), "claim_token": "foreign-token"}
    if operation == "complete":
        value["manifest"] = manifest(claim)
    with pytest.raises(core.PreconditionError):
        stage_bridge.operate(value, OWNER)
    assert sql(db, "SELECT status FROM directives") == [("taken",)]


def test_expired_protected_claim_is_held_and_cannot_repeat_work(db):
    admit()
    claim = take()
    sql(db, "UPDATE directives SET lease_until='2000-01-01T00:00:00Z'")
    for operation in ("inspect", "renew"):
        with pytest.raises(core.PreconditionError):
            stage_bridge.operate(frame(claim, operation), OWNER)
    assert take() is None
    assert sql(db, "SELECT status,claim_token FROM directives") == [("parked", claim["claim_token"])]


def test_unauthorized_successor_and_conflicting_receipt_preserve_parent(db):
    admit()
    claim = take()
    value = {**frame(claim, "complete"), "manifest": manifest(claim)}
    value["manifest"]["successors"][0]["lane"] = "matchpoint:review"
    with pytest.raises(core.PreconditionError, match="successor"):
        stage_bridge.operate(value, OWNER)
    assert sql(db, "SELECT status FROM directives") == [("taken",)]
    value["manifest"]["successors"][0]["lane"] = "dev:review"
    stage_bridge.operate(value, OWNER)
    value["manifest"]["artifact_digest"] = "c" * 64
    with pytest.raises(core.PreconditionError, match="conflicting"):
        stage_bridge.operate(value, OWNER)


def test_unregistered_protected_work_is_quarantined_without_execution(db):
    core.push("unqualified protected work", lane=OWNER.lane, recovery_policy="hold")
    with pytest.raises(core.PreconditionError, match="admission identity"):
        take()
    assert sql(db, "SELECT status,recovery_policy FROM directives") == [("taken", "hold")]


def test_sealed_replay_survives_narrower_policy_without_authorizing_new_work(db):
    admit()
    claim = take()
    value = {**frame(claim, "complete"), "manifest": manifest(claim)}
    receipt = stage_bridge.operate(value, OWNER)
    restricted = stage_bridge.Owner(OWNER.lane, OWNER.consumer)
    assert stage_bridge.operate(value, restricted) == receipt
    assert sql(db, "SELECT COUNT(*) FROM directives") == [(2,)]
    admit("another-source-task")
    next_claim = take()
    with pytest.raises(core.PreconditionError, match="successor"):
        stage_bridge.operate({**frame(next_claim, "complete"), "manifest": manifest(next_claim)}, restricted)
    assert sql(db, "SELECT status FROM directives ORDER BY id DESC LIMIT 1") == [("taken",)]


def test_changed_input_is_rejected(db):
    admit()
    claim = take()
    sql(db, "UPDATE directives SET body='tampered'")
    with pytest.raises(core.PreconditionError, match="identity changed"):
        stage_bridge.operate(frame(claim, "inspect"), OWNER)


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"operation": []},
        {"operation": "force"},
        {"operation": "claim", "lane": "foreign"},
        {"operation": "inspect", "task_id": "task", "claim_token": ""},
    ],
)
def test_malformed_frames_never_open_database(db, value):
    with pytest.raises(core.QueueError):
        stage_bridge.operate(value, OWNER)
    assert not db.exists()


@pytest.mark.parametrize("lease", [True, 1.5, 0, 3601])
def test_invalid_owner_config_never_opens_database(db, lease):
    with pytest.raises(core.QueueError):
        stage_bridge.operate({"operation": "claim"}, stage_bridge.Owner("dev:implementation", "owner", lease))
    assert not db.exists()


def run(content):
    return subprocess.run(
        [sys.executable, "-m", "laneq.stage_bridge", "--lane", OWNER.lane, "--consumer", OWNER.consumer],
        input=content,
        capture_output=True,
        timeout=15,
        env=dict(os.environ),
        check=False,
    )


def test_actual_process_protocol_and_bounded_invalid_frames(db):
    admit()
    result = run(b'{"operation":"claim"}')
    assert result.returncode == 0
    response = json.loads(result.stdout)
    assert response["protocol"] == 1 and response["result"]["task_id"] == "sheet:task:revision:implementation"
    assert json.loads(run(b'{"operation":"claim"}').stdout) == {"protocol": 1, "result": None}
    for content in (b"bad-json", b"\xff", b" " * 131073, b'{"operation":"claim","operation":"claim"}'):
        invalid = run(content)
        assert invalid.returncode == 1
        assert "error" in json.loads(invalid.stdout)
        assert b"bad-json" not in invalid.stdout


@pytest.mark.parametrize(
    "content", [b'{"operation":"claim"}', b"bad-json", b'{"operation":"claim","source":"private-body"}']
)
def test_main_wire_contract_and_redacted_errors(db, content, monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(content), encoding="utf-8"))
    args = ["--lane", OWNER.lane, "--consumer", OWNER.consumer, "--successor-lane", "dev:review"]
    exit_code = stage_bridge.main(args)
    wire = capsys.readouterr().out
    parsed = json.loads(wire)
    assert parsed["protocol"] == 1
    assert exit_code == (0 if "result" in parsed else 1)
    assert "private-body" not in wire
