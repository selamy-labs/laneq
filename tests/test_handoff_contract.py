"""Untrusted handoff data cannot enter a transaction or widen its bounds."""

from __future__ import annotations

import json

import pytest

from laneq import cli, core, handoff
from laneq.handoff_contract import bounded_text, parse_handoff


def manifest():
    return {
        "input_digest": "a" * 64,
        "artifact_digest": "b" * 64,
        "receipt_digest": "c" * 64,
        "receipt_ref": "artifact://receipt",
        "successors": [{"admission_key": "review", "body": "qualified review", "lane": "dev:review", "priority": "P1"}],
    }


@pytest.mark.parametrize("invalid", [None, [], "raw text", {}, {**manifest(), "unexpected": "authority"}])
def test_rejects_invalid_manifest_envelopes(invalid):
    with pytest.raises(core.QueueError):
        parse_handoff(invalid)


@pytest.mark.parametrize("field", ["input_digest", "artifact_digest", "receipt_digest"])
@pytest.mark.parametrize("invalid", ["", "a" * 63, "a" * 65, "A" * 64, "x" + "a" * 64, 0])
def test_requires_exact_hashes(field, invalid):
    data = manifest()
    data[field] = invalid
    with pytest.raises(core.QueueError):
        parse_handoff(data)


@pytest.mark.parametrize("invalid", [None, True, 1, "", " ", "é" * 2049, "\ud800"])
def test_receipt_reference_is_bounded_utf8_text(invalid):
    data = manifest()
    data["receipt_ref"] = invalid
    with pytest.raises(core.QueueError):
        parse_handoff(data)


@pytest.mark.parametrize("invalid", [None, {}, (), [manifest()["successors"][0]] * 17])
def test_bounds_successor_count(invalid):
    data = manifest()
    data["successors"] = invalid
    with pytest.raises(core.QueueError):
        parse_handoff(data)


@pytest.mark.parametrize(
    "child",
    [
        None,
        {},
        {"admission_key": "review", "body": "task", "lane": "dev", "priority": "P9"},
        {"admission_key": "review", "body": "task", "lane": "dev", "priority": "P00"},
        {"admission_key": "review", "body": " ", "lane": "dev", "priority": "P1"},
        {"admission_key": "review", "body": "x" * 65537, "lane": "dev", "priority": "P1"},
        {"admission_key": "review", "body": "task", "lane": "x" * 129, "priority": "P1"},
        {"admission_key": "review", "body": "task", "lane": "dev", "priority": "P1", "budget_override": True},
    ],
)
def test_rejects_malformed_successor_scopes(child):
    data = manifest()
    data["successors"] = [child]
    with pytest.raises(core.QueueError):
        parse_handoff(data)


def test_bounds_aggregate_manifest_even_when_individual_children_fit():
    data = manifest()
    data["successors"] = [
        {"admission_key": str(i), "body": "x" * 65536, "lane": "dev", "priority": "P1"} for i in range(2)
    ]
    with pytest.raises(core.QueueError, match="128 KiB"):
        parse_handoff(data)


def test_canonical_manifest_is_detached_from_mutable_input_and_key_order():
    data = manifest()
    data["successors"][0]["body"] = "review é"
    reverse = {key: data[key] for key in reversed(data)}
    parsed = parse_handoff(data)
    assert parse_handoff(reverse).digest == parsed.digest
    data["successors"][0]["body"] = "changed after parsing"
    assert parsed.successors[0].body == "review é"
    assert json.loads(parsed.encoded)["successors"][0]["body"] == "review é"


def test_terminal_stage_can_complete_without_followup_work():
    data = manifest()
    data["successors"] = []
    assert parse_handoff(data).successors == ()


@pytest.mark.parametrize("value", ["x", "é"])
def test_accepts_exact_utf8_byte_boundary(value):
    assert bounded_text(value, len(value.encode())) == value


@pytest.mark.parametrize("item_id", [False, True, "1", -1, 0, 9223372036854775808])
def test_invalid_identity_never_opens_queue(item_id, monkeypatch):
    monkeypatch.setattr(cli, "connect", lambda: pytest.fail("invalid handoff touched durable state"))
    with pytest.raises(core.QueueError, match="positive SQLite integer"):
        handoff.complete(item_id, claim_token="token", manifest=manifest())


@pytest.mark.parametrize("token", [None, "", " ", "x" * 257])
def test_invalid_fence_never_opens_queue(token, monkeypatch):
    monkeypatch.setattr(cli, "connect", lambda: pytest.fail("invalid handoff touched durable state"))
    with pytest.raises(core.QueueError):
        handoff.complete(1, claim_token=token, manifest=manifest())
