# Trusted root reservations

`laneq.root_reservations` is a local Python library for a trusted feed producer
and publisher. A separate `laneq.admission_bridge` provides a bounded, single-
grant producer invocation. Neither interface adds MCP, gRPC, network capability,
model permission or deployment. Existing generic queues and consumers remain unchanged.
This increment does not establish production source qualification or a live
worker saturation result.

The producer calls `admit(manifest)` with these exact fields:

```json
{
  "task": {
    "admission_key": "sheet:task:revision:implementation",
    "body": "Immutable authorized task envelope",
    "lane": "dev:implementation",
    "priority": "P1"
  },
  "work_id": "sheet:task",
  "namespace": "repo:github.com/organization/project",
  "qualification_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "paths": ["src/component.py"],
  "dependencies": []
}
```

Root lanes are `dev:implementation` and `matchpoint:implementation`. Each
dependency contains `work_id` and the exact `delivery_digest` registered by the
publisher. A generic `done` status or implementation handoff does not satisfy a
dependency. Qualification hashes reference externally verified evidence; they
do not authenticate it.

Admission checks dependencies, active work identity and overlapping path
prefixes, then writes the protected directive, immutable stage admission, root
reservation and paths in one `BEGIN IMMEDIATE` transaction. Concurrent duplicate
admissions return one receipt. Conflicting immutable content fails. Exact
replay after delivery returns the old receipt without acquiring paths again.
An existing unreserved admission cannot be retrofitted by this operation.

Paths use POSIX relative syntax; `.` reserves the whole namespace. Prefix
matching is literal, case-sensitive and segment-aware. The trusted producer
must resolve repository aliases, case folding, symlinks and other filesystem
aliases into consistent resource identities **before admission**. It must also
verify current source revisions, owning account, user approvals, legacy writer
suppression and all source-specific gates. The library alone guarantees logical
namespace/path exclusion, not physical filesystem identity.

Reservations persist across implementation acknowledgements, review repairs,
provider outages, process death, parked claims and lease expiry. The trusted
launcher must enforce one successor per nonterminal reserved stage, appropriate
account/stage routing, independent reviewers, scoped repairs within reserved
paths and the pinned client cohort. The generic handoff API still supports
branching; this library does not make that API a reserved-work authorizer.
Do not route arbitrary model-generated successors directly into it.

After independently verifying actual review, required CI and authorized
publication/main evidence, the publisher calls:

```python
record_delivery(
    admission_key=root_key,
    reservation_digest=admission_receipt["reservation_digest"],
    final_key=publication_key,
    delivery_digest=verified_receipt_digest,
)
```

The operation requires completed fenced handoffs with unchanged admitted bodies
and lanes, exact parent/successor content and a singleton lineage:
implementation → review → validation → publication. Review → implementation
repair loops are permitted and must pass fresh review. The final publication
has no successors and must carry the exact delivery receipt digest. Traversal
is bounded to 64 stages. Branched, skipped, foreign, incomplete or altered
lineages cannot release paths; uncertainty remains held for reconciliation.
There is no force-release or expired-lease release operation.

Recording delivery atomically releases the logical reservation and enables
dependencies carrying that exact receipt. This necessary local lineage check
does not prove external CI, reviewer independence or publication by itself.
Models must not receive database access or either producer/publisher function.

## Pinned producer invocation

The trusted producer may invoke `python -m laneq.admission_bridge` with fixed
`--account`, `--namespace` and `--reservation-digest` launcher arguments. The
digest is `parse_reservation(qualified_manifest).digest`; it binds the complete
normalized reservation, including exact task body, identity, lane, priority,
qualification reference, paths and dependencies. It is supplied by the trusted
qualifier, never accepted from model output. The entire launcher, runtime and
native module bytes still require owner custody/attestation before use.

Stdin accepts `{"operation":"admit","reservation":qualified_manifest}` or
`{"operation":"inspect","reservation":qualified_manifest}`, bounded to
128 KiB. Both operations snapshot the input and check the complete pinned
digest, owning implementation lane and namespace before touching the database,
and admission calls the existing atomic reservation operation. Exact repeated invocation
returns the original reservation; a changed grant is not a retry. Output is a
protocol-1 JSON receipt or a bounded error type without input/backend details.

Inspection opens the same trusted `LANEQ_DB` path in SQLite read-only mode and
reads one consistent snapshot. It neither creates/migrates the database nor
reaps leases, claims work or releases reservations. A matching root returns its
current receipt; an absent root in a complete initialized schema returns
`{"protocol":1,"result":null}`. Missing database/schema, an unreserved admission
with the same key, conflicting root content or a missing directive fail closed.
The launcher must retain the exact original database identity across recovery.

After an uncertain admission response, inspect the persisted immutable grant.
An absent result **never authorizes a retry or releasing source/capacity holds**:
the original writer may still commit after the read snapshot. The owner must
retain uncertainty until it has independently established the writer outcome.

```mermaid
sequenceDiagram
    participant Owner as Trusted producer
    participant Bridge as Pinned admission bridge
    participant DB as Native queue host
    Owner->>Bridge: admit immutable reservation
    Bridge->>DB: atomic admission and writer holds
    DB-->>Bridge: committed receipt
    Note over Owner,Bridge: Response lost; durable owner retains uncertainty
    Owner->>Bridge: inspect same immutable reservation
    Bridge->>DB: read-only consistent snapshot
    DB-->>Owner: matching receipt, absence, or fail-closed error
    Note over Owner,DB: Absence does not authorize replay or release
```

This bridge has no claim, execution, successor, publication or release operation.
It does not qualify spreadsheet prose, verify approval, canonicalize physical
aliases or make old qualification evidence current. The producer must recheck
those conditions and its live authority before each first admission. Keep the
bridge and database unavailable to model processes. No publisher CLI is added.

Use the database on one supported queue host, with native SQLite transactions.
Do not share this database across arbitrary pods or network filesystems. A GCP
deployment still needs the existing owner's backend and capability design,
source qualification and consumer integration before real admissions.
