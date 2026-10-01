# Trusted root reservations

`laneq.root_reservations` is a local Python library for a trusted feed producer
and publisher. It adds no CLI, MCP, gRPC, network capability, model permission,
or deployment. Existing generic queues and their consumers remain unchanged.
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

Use the database on one supported queue host, with native SQLite transactions.
Do not share this database across arbitrary pods or network filesystems. A GCP
deployment still needs the existing owner's backend and capability design,
source qualification and consumer integration before real admissions.
