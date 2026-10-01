"""Immutable, bounded inputs for the trusted producer's resource reservations."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from laneq.core import QueueError
from laneq.handoff_contract import Successor, bounded_text, digest_text, object_fields, parse_successor


@dataclass(frozen=True)
class Dependency:
    work_id: str
    delivery_digest: str


@dataclass(frozen=True)
class Reservation:
    task: Successor
    work_id: str
    namespace: str
    qualification_digest: str
    paths: tuple[str, ...]
    dependencies: tuple[Dependency, ...]
    digest: str


def identity(value: object) -> str:
    result = bounded_text(value, 256)
    if any(ord(character) < 33 or ord(character) == 127 for character in result):
        raise QueueError("reservation identity contains whitespace or controls")
    return result


def namespace_text(value: object) -> str:
    result = bounded_text(value, 256)
    if re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*", result) is None:
        raise QueueError("reservation namespace is not canonical")
    return result


def path_text(value: object) -> str:
    result = bounded_text(value, 4096)
    if result == ".":
        return result
    if "\\" in result or any(ord(character) < 32 or ord(character) == 127 for character in result):
        raise QueueError("reservation path contains controls or separators")
    if any(part in ("", ".", "..") for part in result.split("/")):
        raise QueueError("reservation path must be canonical and relative")
    return result


def paths_value(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= 64:
        raise QueueError("reservation requires 1..64 paths")
    result = tuple(sorted(path_text(item) for item in value))
    if len(set(result)) != len(result):
        raise QueueError("duplicate reservation path")
    return result


def dependencies_value(value: object, work_id: str) -> tuple[Dependency, ...]:
    if not isinstance(value, list) or len(value) > 64:
        raise QueueError("reservation requires at most 64 dependencies")
    result = []
    for item in value:
        fields = object_fields(item, {"work_id", "delivery_digest"})
        result.append(Dependency(identity(fields["work_id"]), digest_text(fields["delivery_digest"])))
    identifiers = {item.work_id for item in result}
    if len(identifiers) != len(result) or work_id in identifiers:
        raise QueueError("duplicate or self reservation dependency")
    return tuple(sorted(result, key=lambda item: item.work_id))


def parse_reservation(value: object) -> Reservation:
    fields = object_fields(value, {"task", "work_id", "namespace", "qualification_digest", "paths", "dependencies"})
    task = parse_successor(fields["task"])
    if task.lane not in ("dev:implementation", "matchpoint:implementation"):
        raise QueueError("reserved root requires an owning implementation lane")
    work_id = identity(fields["work_id"])
    namespace = namespace_text(fields["namespace"])
    qualification = digest_text(fields["qualification_digest"])
    paths = paths_value(fields["paths"])
    dependencies = dependencies_value(fields["dependencies"], work_id)
    frozen = {
        "task": asdict(task),
        "work_id": work_id,
        "namespace": namespace,
        "qualification_digest": qualification,
        "paths": paths,
        "dependencies": [asdict(item) for item in dependencies],
    }
    encoded = json.dumps(frozen, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 131072:
        raise QueueError("reservation manifest exceeds 128 KiB")
    return Reservation(
        task, work_id, namespace, qualification, paths, dependencies, hashlib.sha256(encoded.encode()).hexdigest()
    )
