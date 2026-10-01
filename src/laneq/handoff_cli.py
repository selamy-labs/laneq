"""JSON transport for the same durable handoff transaction used by adapters."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from laneq import core, handoff
from laneq.handoff_contract import decode_manifest


def read_manifest(path: Path) -> object:
    with path.open("rb") as stream:
        content = stream.read(131073)
    return decode_manifest(content)


def cmd_handoff(args: argparse.Namespace) -> int:
    try:
        manifest = read_manifest(Path(args.manifest_file))
        receipt = handoff.complete(args.id, claim_token=args.claim_token, manifest=manifest)
    except (OSError, UnicodeError, ValueError, core.QueueError) as error:
        print(f"laneq: handoff rejected: {error}", file=sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


def cmd_receipt(args: argparse.Namespace) -> int:
    print(json.dumps(handoff.get_receipt(args.id), sort_keys=True))
    return 0


def register(subparsers: argparse._SubParsersAction) -> None:
    completion = subparsers.add_parser("handoff")
    completion.add_argument("id", type=int)
    completion.add_argument("--claim-token", required=True)
    completion.add_argument("--manifest-file", required=True)
    completion.set_defaults(fn=cmd_handoff)
    observation = subparsers.add_parser("handoff-receipt")
    observation.add_argument("id", type=int)
    observation.set_defaults(fn=cmd_receipt)
