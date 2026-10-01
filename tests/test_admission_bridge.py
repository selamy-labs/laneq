"""Pinned producer grants reach the actual atomic reservation transaction."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import select
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from laneq import admission_bridge, cli, core, root_reservations, stage_bridge
from laneq.reservation_contract import parse_reservation


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "admission.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    return path


def grant(account="dev"):
    return {
        "task": {
            "admission_key": "sheet:42:revision:implementation",
            "body": "Exact qualified source — no shell or model authority",
            "lane": account + ":implementation",
            "priority": "P1",
        },
        "work_id": "sheet:42",
        "namespace": "repo:github.com/selamy-labs/project",
        "qualification_digest": "a" * 64,
        "paths": ["src/owned.ts"],
        "dependencies": [],
    }


def owner(value):
    return admission_bridge.Owner(
        value["task"]["lane"].split(":")[0], value["namespace"], parse_reservation(value).digest
    )


def request(value, operation="admit"):
    return {"operation": operation, "reservation": value}


@pytest.mark.parametrize("account", ["dev", "matchpoint"])
def test_pinned_grant_admits_real_reserved_root_then_consumer_claims(db, account):
    value = grant(account)
    receipt = admission_bridge.operate(request(value), owner(value))
    assert receipt["work_id"] == "sheet:42"
    assert receipt["reservation_digest"] == parse_reservation(value).digest
    assert receipt["status"] == "pending" and receipt["delivery_digest"] is None
    claim = stage_bridge.operate({"operation": "claim"}, stage_bridge.Owner(value["task"]["lane"], "consumer"))
    assert claim["body"] == value["task"]["body"]
    assert claim["input_digest"] == hashlib.sha256(claim["body"].encode()).hexdigest()
    assert claim["task_id"] == value["task"]["admission_key"]
    replay = admission_bridge.operate(request(value), owner(value))
    assert replay["id"] == receipt["id"] and replay["status"] == "taken"
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute("SELECT count(*) FROM directives").fetchone()[0] == 1
        assert conn.execute("SELECT namespace FROM reserved_roots").fetchone()[0] == value["namespace"]
        assert conn.execute("SELECT path FROM reserved_paths").fetchone()[0] == "src/owned.ts"


@pytest.mark.parametrize(
    "section,key,changed",
    [
        ("task", "admission_key", "other:identity"),
        ("task", "body", "changed source"),
        ("task", "lane", "matchpoint:implementation"),
        ("task", "priority", "P0"),
        (None, "work_id", "different:work"),
        (None, "namespace", "repo:foreign"),
        (None, "qualification_digest", "b" * 64),
        (None, "paths", ["."]),
        (None, "dependencies", [{"work_id": "prerequisite", "delivery_digest": "c" * 64}]),
    ],
)
@pytest.mark.parametrize("operation", ["admit", "inspect"])
def test_any_grant_change_fails_before_database_access(db, section, key, changed, operation):
    value = grant()
    pinned = owner(value)
    modified = copy.deepcopy(value)
    target = modified if section is None else modified[section]
    target[key] = changed
    with pytest.raises(core.PreconditionError, match="pinned grant"):
        admission_bridge.operate(request(modified, operation), pinned)
    assert not db.exists()


@pytest.mark.parametrize(
    "pinned",
    [
        admission_bridge.Owner("root", "repo:project", "a" * 64),
        admission_bridge.Owner("dev", "UPPERCASE", "a" * 64),
        admission_bridge.Owner("dev", "repo:project", "a" * 63),
        admission_bridge.Owner("dev", "repo:project", "A" * 64),
    ],
)
def test_invalid_launcher_owner_rejected_before_database_access(db, pinned):
    with pytest.raises(core.QueueError):
        admission_bridge.operate(request(grant()), pinned)
    assert not db.exists()


@pytest.mark.parametrize("field,changed", [("account", "matchpoint"), ("namespace", "repo:foreign")])
def test_owning_account_and_namespace_checked_even_with_matching_digest(db, field, changed):
    value = grant()
    facts = {"account": "dev", "namespace": value["namespace"], "reservation_digest": parse_reservation(value).digest}
    facts[field] = changed
    with pytest.raises(core.PreconditionError, match="pinned grant"):
        admission_bridge.operate(request(value), admission_bridge.Owner(**facts))
    assert not db.exists()


@pytest.mark.parametrize("operation", ["claim", "complete", "record_delivery", "release", None, {}])
def test_transport_cannot_be_used_for_consumer_or_publisher_operations(db, operation):
    with pytest.raises(core.QueueError):
        admission_bridge.operate({"operation": operation, "reservation": grant()}, owner(grant()))
    assert not db.exists()


@pytest.mark.parametrize("value", [None, [], {}, {"operation": "admit"}, {**request(grant()), "force": True}])
def test_exact_frame_fields_required_before_database_access(db, value):
    with pytest.raises(core.QueueError):
        admission_bridge.operate(value, owner(grant()))
    assert not db.exists()


def arguments(value):
    pinned = owner(value)
    return [
        "--account",
        pinned.account,
        "--namespace",
        pinned.namespace,
        "--reservation-digest",
        pinned.reservation_digest,
    ]


def test_real_cli_and_twelve_concurrent_replays_return_one_admission(db):
    value = grant()
    frame = json.dumps(request(value))
    result = subprocess.run(
        [sys.executable, "-m", "laneq.admission_bridge", *arguments(value)],
        input=frame,
        text=True,
        capture_output=True,
        env=os.environ.copy(),
        check=True,
    )
    receipt = json.loads(result.stdout)
    assert receipt["protocol"] == 1 and receipt["result"]["status"] == "pending"
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: admission_bridge.operate(request(value), owner(value)), range(12)))
    assert {item["id"] for item in results} == {receipt["result"]["id"]}
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute("SELECT count(*) FROM directives").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM reserved_roots").fetchone()[0] == 1


@pytest.mark.parametrize(
    "payload",
    [
        b"not json secret-source",
        b'{"operation":"admit","operation":"release","reservation":{}}',
        b'{"operation":"admit","reservation":NaN}',
        b'{"operation":"admit","reservation":"private-source"}',
        b"x" * 131073,
    ],
)
def test_cli_errors_are_bounded_and_do_not_echo_private_input(db, monkeypatch, capsys, payload):
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(payload)))
    assert admission_bridge.main(arguments(grant())) == 1
    output = capsys.readouterr().out
    assert len(output) < 80 and "source" not in output
    assert json.loads(output)["protocol"] == 1
    assert not db.exists()


def test_cli_backend_failure_reports_type_without_leaking_source(db, monkeypatch, capsys):
    value = grant()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(json.dumps(request(value)).encode())))

    def unavailable(_):
        raise sqlite3.OperationalError("private database path and source details")

    monkeypatch.setattr(admission_bridge.root_reservations, "admit", unavailable)
    assert admission_bridge.main(arguments(value)) == 1
    assert json.loads(capsys.readouterr().out) == {"protocol": 1, "error": "OperationalError"}


def test_cli_success_frame(db, monkeypatch, capsys):
    value = grant()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(json.dumps(request(value)).encode())))
    assert admission_bridge.main(arguments(value)) == 0
    assert json.loads(capsys.readouterr().out)["result"]["work_id"] == value["work_id"]


def test_inspection_missing_database_fails_without_creating_anything(db):
    with pytest.raises(sqlite3.OperationalError):
        admission_bridge.operate(request(grant(), "inspect"), owner(grant()))
    assert not db.exists() and not list(db.parent.iterdir())


@pytest.mark.parametrize("table", ["reserved_roots", "reserved_paths", "stage_admissions", "directives"])
def test_inspection_partial_schema_fails_without_repair(db, table):
    value = grant()
    admission_bridge.operate(request(value), owner(value))
    with closing(sqlite3.connect(db)) as conn:
        conn.execute(f"DROP TABLE {table}")
        conn.commit()
        before = tuple(conn.iterdump())
    with pytest.raises(sqlite3.OperationalError):
        admission_bridge.operate(request(value, "inspect"), owner(value))
    with closing(sqlite3.connect(db)) as conn:
        assert tuple(conn.iterdump()) == before


def test_inspection_absent_root_is_non_authorizing_snapshot_during_live_writer(db):
    value = grant()
    # Provisioning is explicit and belongs to the trusted owner, never inspect.
    with closing(cli.connect()) as provisioner:
        root_reservations.initialize(provisioner)
        provisioner.commit()
    with closing(cli.connect()) as writer:
        writer.row_factory = sqlite3.Row
        writer.execute("BEGIN IMMEDIATE")
        root_reservations.insert_root(writer, parse_reservation(value))
        assert admission_bridge.operate(request(value, "inspect"), owner(value)) is None
        writer.commit()
    receipt = admission_bridge.operate(request(value, "inspect"), owner(value))
    assert receipt["id"] == 1 and receipt["status"] == "pending"


def test_inspection_orphan_identity_fails_closed(db):
    value = grant()
    admission_bridge.operate(request(value), owner(value))
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("DELETE FROM reserved_paths")
        conn.execute("DELETE FROM reserved_roots")
        conn.commit()
    with pytest.raises(core.PreconditionError, match="unreserved admission identity"):
        admission_bridge.operate(request(value, "inspect"), owner(value))


def test_inspection_conflicting_root_fails_closed(db):
    value = grant()
    admission_bridge.operate(request(value), owner(value))
    changed = copy.deepcopy(value)
    changed["task"]["body"] = "fresh immutable revision"
    with pytest.raises(core.PreconditionError, match="conflicting immutable"):
        admission_bridge.operate(request(changed, "inspect"), owner(changed))


@pytest.mark.parametrize("account", ["dev", "matchpoint"])
def test_inspection_observes_claim_without_reaping_expired_lease_or_modifying_database(db, account):
    value = grant(account)
    first = admission_bridge.operate(request(value), owner(value))
    stage_bridge.operate({"operation": "claim"}, stage_bridge.Owner(value["task"]["lane"], "consumer"))
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("UPDATE directives SET lease_until='2000-01-01T00:00:00Z'")
        conn.commit()
        before = tuple(conn.iterdump())
    observed = admission_bridge.operate(request(value, "inspect"), owner(value))
    assert observed == {**first, "status": "taken"}
    with closing(sqlite3.connect(db)) as conn:
        assert tuple(conn.iterdump()) == before


def test_inspection_missing_reserved_directive_fails_closed(db):
    value = grant()
    admission_bridge.operate(request(value), owner(value))
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("DELETE FROM directives")
        conn.commit()
    with pytest.raises(core.PreconditionError, match="reserved directive is missing"):
        admission_bridge.operate(request(value, "inspect"), owner(value))


def test_killed_after_committed_admission_before_response_recovers_by_read_only_cli(db):
    value = grant()
    child = """
import os, signal, sys
from laneq import admission_bridge
class LostResponse:
    def write(self, value):
        os.write(1, b'committed-before-response\\n')
        signal.pause()
    def flush(self):
        pass
sys.stdout = LostResponse()
admission_bridge.main(sys.argv[1:])
"""
    process = subprocess.Popen(
        [sys.executable, "-c", child, *arguments(value)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=os.environ.copy(),
    )
    try:
        process.stdin.write(json.dumps(request(value)))
        process.stdin.close()
        assert select.select([process.stdout], [], [], 10)[0], "admission did not commit"
        assert process.stdout.readline() == "committed-before-response\n"
        process.kill()
        assert process.wait(timeout=5) < 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
    recovered = subprocess.run(
        [sys.executable, "-m", "laneq.admission_bridge", *arguments(value)],
        input=json.dumps(request(value, "inspect")),
        text=True,
        capture_output=True,
        env=os.environ.copy(),
        check=True,
        timeout=10,
    )
    receipt = json.loads(recovered.stdout)["result"]
    assert receipt["id"] == 1 and receipt["status"] == "pending"
    assert receipt["reservation_digest"] == parse_reservation(value).digest
    competing = copy.deepcopy(value)
    competing["task"]["admission_key"] = "other:implementation"
    competing["work_id"] = "other:work"
    with pytest.raises(core.PreconditionError, match="source paths overlap"):
        admission_bridge.operate(request(competing), owner(competing))
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute("SELECT count(*) FROM directives").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM reserved_paths").fetchone()[0] == 1
