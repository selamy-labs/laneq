"""Single-grant admission transport for the trusted feed producer.

The launcher pins reviewed code and an externally qualified reservation digest.
Models receive neither this interface nor database access. This exposes no
publisher, release, arbitrary queue, subprocess or network operation.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass

from laneq import core, root_inspection, root_reservations
from laneq.handoff_contract import decode_manifest, digest_text, object_fields
from laneq.reservation_contract import namespace_text, parse_reservation


@dataclass(frozen=True)
class Owner:
    account: str
    namespace: str
    reservation_digest: str

    def validate(self) -> None:
        if self.account not in ("dev", "matchpoint"):
            raise core.QueueError("invalid admission account")
        namespace_text(self.namespace)
        digest_text(self.reservation_digest)


def operate(value: object, owner: Owner) -> dict[str, object] | None:
    owner.validate()
    fields = object_fields(value, {"operation", "reservation"})
    if fields["operation"] not in ("admit", "inspect"):
        raise core.QueueError("invalid admission operation")
    # Own the bounded snapshot: a caller cannot change its mutable input after
    # the digest check but before the transaction reparses the reservation.
    frozen = decode_manifest(json.dumps(fields["reservation"], ensure_ascii=False, allow_nan=False).encode())
    reservation = parse_reservation(frozen)
    if (
        reservation.task.lane != owner.account + ":implementation"
        or reservation.namespace != owner.namespace
        or reservation.digest != owner.reservation_digest
    ):
        raise core.PreconditionError("reservation is outside the pinned grant")
    if fields["operation"] == "inspect":
        return root_inspection.inspect(reservation)
    return root_reservations.admit(frozen)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", choices=("dev", "matchpoint"), required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--reservation-digest", required=True)
    args = parser.parse_args(argv)
    owner = Owner(args.account, args.namespace, args.reservation_digest)
    try:
        value = decode_manifest(sys.stdin.buffer.read(131073))
        result = operate(value, owner)
    except (core.QueueError, ValueError, TypeError, OSError, sqlite3.Error) as error:
        # Never echo source bodies, paths, arbitrary input or backend error prose.
        print(json.dumps({"protocol": 1, "error": type(error).__name__}))
        return 1
    print(json.dumps({"protocol": 1, "result": result}, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
