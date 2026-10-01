"""Bounded immutable handoff input, parsed before touching durable state."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from laneq.core import PRIORITIES, QueueError


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise QueueError("duplicate handoff JSON key")
        result[key] = value
    return result


def decode_manifest(content: bytes) -> object:
    if len(content) > 131072:
        raise QueueError("handoff manifest exceeds 128 KiB")
    try:
        return json.loads(content.decode("utf-8"), object_pairs_hook=unique_object)
    except RecursionError as error:
        raise QueueError("handoff JSON nesting exceeds parser limit") from error


@dataclass(frozen=True)
class Successor:
    admission_key: str
    body: str
    lane: str
    priority: str


@dataclass(frozen=True)
class Handoff:
    input_digest: str
    artifact_digest: str
    receipt_ref: str
    receipt_digest: str
    successors: tuple[Successor, ...]
    encoded: str
    digest: str


def object_fields(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise QueueError("handoff object has missing or unexpected fields")
    return {str(key): item for key, item in value.items()}


def bounded_text(value: object, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueueError("handoff text is empty, invalid or oversized")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as error:
        raise QueueError("handoff text is not valid UTF-8") from error
    if size > maximum:
        raise QueueError("handoff text is empty, invalid or oversized")
    return value


def digest_text(value: object) -> str:
    result = bounded_text(value, 64)
    if re.fullmatch(r"[0-9a-f]{64}", result) is None:
        raise QueueError("handoff digest must be 64 lowercase hex characters")
    return result


def parse_successor(value: object) -> Successor:
    fields = object_fields(value, {"admission_key", "body", "lane", "priority"})
    priority = bounded_text(fields["priority"], 2)
    if priority not in PRIORITIES:
        raise QueueError("invalid successor priority")
    return Successor(
        bounded_text(fields["admission_key"], 256),
        bounded_text(fields["body"], 65536),
        bounded_text(fields["lane"], 128),
        priority,
    )


def parse_successors(value: object) -> tuple[Successor, ...]:
    if not isinstance(value, list) or len(value) > 16:
        raise QueueError("handoff requires a list of at most 16 successors")
    successors = tuple(parse_successor(item) for item in value)
    if len({child.admission_key for child in successors}) != len(successors):
        raise QueueError("duplicate successor admission key")
    return successors


def parse_handoff(value: object) -> Handoff:
    fields = object_fields(value, {"input_digest", "artifact_digest", "receipt_ref", "receipt_digest", "successors"})
    source = digest_text(fields["input_digest"])
    artifact = digest_text(fields["artifact_digest"])
    receipt = digest_text(fields["receipt_digest"])
    reference = bounded_text(fields["receipt_ref"], 4096)
    successors = parse_successors(fields["successors"])
    frozen = {
        "input_digest": source,
        "artifact_digest": artifact,
        "receipt_digest": receipt,
        "receipt_ref": reference,
        "successors": [asdict(child) for child in successors],
    }
    encoded = json.dumps(frozen, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > 131072:
        raise QueueError("handoff manifest exceeds 128 KiB")
    return Handoff(
        source, artifact, reference, receipt, successors, encoded, hashlib.sha256(encoded.encode()).hexdigest()
    )
