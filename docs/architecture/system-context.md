# System context

`laneq` is a local priority queue with a shared SQLite-backed core and three
runtime interfaces, plus a fixed-lane machine bridge for a trusted queue owner.
The MCP, gRPC, authentication, and telemetry dependencies
are optional extras; the core package and CLI have no third-party runtime
dependencies.

```mermaid
flowchart LR
    operator["Operator or automation"]
    mcp_client["MCP client"]
    grpc_client["gRPC client"]
    stage_owner["Trusted stage owner"]
    laneq["laneq<br/>Local priority queue"]
    sqlite[("SQLite database<br/>LANEQ_DB")]
    otlp["OTLP collector<br/>optional"]

    operator -->|"CLI commands"| laneq
    mcp_client <-->|"MCP JSON-RPC over stdio"| laneq
    grpc_client -->|"laneq.v1 unary RPCs"| laneq
    stage_owner <-->|"Bounded JSON frames<br/>fixed lane and successor policy"| laneq
    laneq -->|"Directives, migrations, and backups"| sqlite
    laneq -.->|"Auto-instrumented telemetry when configured"| otlp
```

The interfaces use shared Python queue operations (`laneq.core` and the
protected admission/handoff modules). The CLI and
MCP server select the same local database through `LANEQ_DB` (or the legacy
`CODEX_Q_DB` fallback). The gRPC server maps the protobuf service to those same
operations and can optionally enforce PASETO grants plus per-request proofs;
authentication is off by default.

This view documents runtime clients, persistence, and optional telemetry. The
repository does not define a deployment topology.

## Protected stage directives

Trusted qualifiers can idempotently admit protected root work through the
CLI or `AdmitStage` RPC. Consumers explicitly select hold recovery policy;
ordinary claims and peeks exclude protected work. `CompleteHandoff` and
the CLI handoff command share one SQLite transaction: verify the exact input
digest and live token, deduplicate successor admissions, seal the immutable
manifest and receipt, then mark the parent done. An exact replay reads the
sealed receipt without publishing duplicate successors.

```mermaid
stateDiagram-v2
    [*] --> pending: Stable protected admission
    pending --> taken: Explicit protected claim
    taken --> taken: Lease renewal
    taken --> done: Receipt and successors committed atomically
    taken --> parked: Expiry or explicit hold
    done --> [*]
    note right of parked
        Ownership evidence retained.
        No generic requeue or unpark.
        Verified recovery integration required.
    end note
```

The queue validates hashes and immutable references; the external adapter
must prove artifact authenticity, source/spec/approval qualification, account
and method authorization, writer death and spend reconciliation. Existing
sender-bound authentication does not implement these capability boundaries.
Legacy administrative force paths must also be confined by deployment policy.
One queue service owns its local SQLite database; worker pods use the service
interface. Production use requires the remaining integrations and a pinned
compatible client cohort. See [STAGE-HANDOFF.md](../../STAGE-HANDOFF.md).

## Fixed-lane machine bridge

The trusted local owner can invoke `python -m laneq.stage_bridge --lane LANE
--consumer OWNER --successor-lane REVIEW_LANE`. One bounded JSON request on
stdin performs `claim`, `inspect`, `renew`, or `complete`; stdout contains a
versioned result or a redacted error class. Task IDs are immutable admission
keys. Each operation checks the registered lane and body digest, and native
fencing applies to inspection, renewal and completion. New successors must
match the launcher's allowlist. An exact sealed completion replay remains
readable after that allowlist narrows, without authorizing new work.

This CLI is a local transport, not a public capability boundary. Its trusted
launcher owns the database and pins every client. Model workloads must receive
neither database/CLI access nor provider credentials. Source qualification,
authentic provider receipts and account/method authorization remain external
requirements; the bridge does not establish them by accepting digest strings.
