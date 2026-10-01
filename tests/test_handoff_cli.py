"""Native CLI and stored receipt use the same fenced transaction."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from laneq import cli, core, handoff
from laneq.handoff_cli import read_manifest


def test_native_cli_handoff_and_readback(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "queue.db"))
    body = '{"task":"immutable sheet row"}'
    assert cli.main(["push", "--recovery-policy", "hold", "--lane", "dev:implementation", "-b", body]) == 0
    capsys.readouterr()
    assert (
        cli.main(["next", "--json", "--lane", "dev:implementation", "--consumer", "dev-1", "--recovery-policy", "hold"])
        == 0
    )
    claim = json.loads(capsys.readouterr().out)
    assert claim["input_digest"] == hashlib.sha256(body.encode()).hexdigest()
    assert claim["lease_until"] == core.show(claim["id"])["lease_until"]
    manifest = {
        "input_digest": claim["input_digest"],
        "artifact_digest": "a" * 64,
        "receipt_ref": "artifact://receipt",
        "receipt_digest": "b" * 64,
        "successors": [],
    }
    path = tmp_path / "result.json"
    path.write_text(json.dumps(manifest))
    command = ["handoff", str(claim["id"]), "--claim-token", claim["claim_token"], "--manifest-file", str(path)]
    assert cli.main(command) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert cli.main(command) == 0
    assert json.loads(capsys.readouterr().out) == receipt
    assert cli.main(["handoff-receipt", str(claim["id"])]) == 0
    assert json.loads(capsys.readouterr().out) == receipt
    assert handoff.get_receipt(999) is None


@pytest.mark.parametrize(
    "content",
    [
        b'{"input_digest":"first","input_digest":"second"}',
        b'{"children":{"key":1,"key":2}}',
        b"not json",
        b"\xff",
        b" " * 131073,
        b"[" * 1100 + b"]" * 1100,
        b"[" * 10000 + b"]" * 10000,
    ],
)
def test_cli_rejects_invalid_bytes_or_duplicate_keys(tmp_path, monkeypatch, capsys, content):
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "unused.db"))
    path = tmp_path / "manifest.json"
    path.write_bytes(content)
    assert cli.main(["handoff", "1", "--claim-token", "fence", "--manifest-file", str(path)]) == 1
    assert "handoff rejected" in capsys.readouterr().err
    assert not (tmp_path / "unused.db").exists()


def test_missing_manifest_is_not_a_queue_or_execution_retry(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "unused.db"))
    assert cli.main(["handoff", "1", "--claim-token", "fence", "--manifest-file", str(tmp_path / "missing")]) == 1
    assert "handoff rejected" in capsys.readouterr().err
    assert not (tmp_path / "unused.db").exists()


def test_file_reader_keeps_exact_text_values_and_allows_boundary(tmp_path):
    path = tmp_path / "exact.json"
    value = {"body": " qualified é ", "priority": "P1"}
    data = json.dumps(value).encode()
    path.write_bytes(data + b" " * (131072 - len(data)))
    assert read_manifest(path) == value
