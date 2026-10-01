# Fenced stage handoffs and recovery holds

Authority: deterministic pull-worker goal, 2026-09-30, thread
01a0ea93-c4e2-70b1-8b16-1ecc219f927e. This feature adds queue primitives for
the existing GCP migration; it does not resume portfolio timers or authorize
paid models, deployments, publication or additional infrastructure.

Protected directives opt into `recovery_policy=hold`. Expired or stale leases
move them into parked status, preserving the former consumer, token and lease
for reconciliation. They cannot be automatically reclaimed, generically
requeued, deferred, completed or unparked. Replacement admission will require
an independently validated writer-death/source/spend recovery receipt; that
supervisor integration remains separate work. Ordinary directives retain
their existing automatic-requeue behavior.

Fenced completion validates a bounded immutable result and at most 16 qualified
successors. Each successor has an immutable admission key. Root feed replay
and successor replay resolve to the same directive; an existing key with a
different body, lane or initial priority is rejected. Repeated keys within
one manifest are invalid. The same SQLite write transaction verifies source-body identity,
creates protected successor directives, seals the result/manifest/child IDs,
and marks the parent done. The final parent update checks an unexpired token.
Any error or process death before commit leaves no successor or completion.
An exact duplicate request returns the durable receipt without reinserting
children; changed content or another fencing token is rejected. Death after
commit and before acknowledgement can therefore be reconciled without another
model invocation. This is a transaction within one queue service, not a shared
SQLite filesystem across worker pods.

The trusted stage adapter must validate account, stage, source/spec/approval
envelopes, disjoint source ownership and spend admission before calling these
primitives. Model-generated follow-ups do not themselves authorize enqueueing.
Result hashes refer to immutable persisted artifacts and provider receipts;
queue sealing is not a claim that those external artifacts are authentic.
Independent review, CI and authorized publication remain separate stages.

Acceptance includes migration backup/rollback, legacy compatibility, stale
tokens/expiry, duplicate/conflicting requests, concurrent completions, crash
before and after commit, malformed and oversized manifests, and protected
expiry/stale-reaping through all currently available queue operations.
CLI and service transports must route to the same core transaction.

Production cutover additionally requires a complete pinned-client cohort;
older clients do not understand protected recovery behavior. No production
database is changed until that proof and independent review are complete.

## Native CLI contract

`push --recovery-policy hold --admission-key <account/task/revision/stage-key>`
admits a protected root idempotently. Stable root admission cannot supply a
parent; stage successors use the fenced handoff operation instead.

`next --json --lane <account:stage> --recovery-policy hold` returns the original claim fields plus
`input_digest` (SHA256 of the exact UTF-8 body) and `lease_until` for protected
work. Existing ordinary claim JSON remains unchanged. Claims and peeks default
to ordinary work; protected-stage consumers must explicitly select hold policy.

`handoff <id> --claim-token <token> --manifest-file <path>` validates and commits
the stage atomically, returning a JSON receipt. The UTF-8 manifest has exactly
`input_digest`, `artifact_digest`, `receipt_ref`, `receipt_digest` and `successors`.
Each successor has exactly `admission_key`, `body`, `lane`, and `priority`.
Reject duplicate JSON keys, invalid hashes, excessive count/bytes and unknown
fields before opening the queue. `handoff-receipt <id>` observes a sealed result
without changing ownership or invoking a model.

## Service contract and remaining integration

The additive gRPC service exposes `AdmitStage`, `CompleteHandoff` and
`GetHandoffReceipt`. `Take` and `Peek` require `recovery_hold=true` for protected
work. Claims carry the exact input digest and existing lease timestamp.
The directive's hold flag has explicit presence; existing partial thread
listings omit it, meaning unknown rather than ordinary recovery behavior.
Handoff JSON is decoded by the same bounded duplicate-key-rejecting parser
as the CLI; all transports call the same core transaction. Ordinary requests
retain their previous behavior.

Generated bindings use grpcio-tools 1.81.1 and protobuf 6.33.5, matching the
previous committed generator versions. Declared optional runtime minima now
match those bindings rather than advertising versions that cannot import them.
Regenerate with `python -m grpc_tools.protoc -I proto
--python_out=src/laneq/grpc --pyi_out=src/laneq/grpc
--grpc_python_out=src/laneq/grpc proto/laneq.proto`, then make the generated
`laneq_pb2` import relative in `laneq_pb2_grpc.py`.

These methods inherit the existing sender-bound grant/proof interceptor.
That interceptor authenticates identity, audience and request integrity;
it does NOT enforce lane/account or producer/recovery method permissions.
The trusted deployment adapter must add those capability restrictions and
enforce authentication before production admission. The GCP consumer adapter,
writer-death recovery supervisor, artifact validation and spend ledger remain
separate integration work. A transport roundtrip is not live fleet proof.
