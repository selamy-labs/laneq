# laneq

`laneq` is a tiny local SQLite priority queue for feeding directives to
autonomous agents and orchestrators. It never talks to the network and stores
only the queue data you put into its local database.

Priorities sort as `P0 < P1 < P2`, with FIFO ordering inside each priority.
`next` atomically takes one pending item so concurrent workers do not receive
the same directive.

## Install

Run directly from GitHub:

```bash
uvx --from git+https://github.com/selamy-labs/laneq@v0.3.0 laneq --help
```

Or install with pipx:

```bash
pipx install git+https://github.com/selamy-labs/laneq@v0.3.0
```

The `laneq` name is already occupied on PyPI by an unrelated lane-line
detection package, so releases are GitHub-tag based until the distribution
name is resolved.

## Usage

```bash
laneq push -p P0 -b "ship the smallest verified fix"
laneq peek
laneq next --id --consumer worker-a
laneq done 1
laneq stats
```

Read a directive body from a file:

```bash
laneq push -p P1 -f directive.txt
```

Use a specific database path:

```bash
LANEQ_DB=/tmp/laneq.db laneq list --all
```

The default database is `~/.claude/laneq.db`.

## Coordination

Consumers can identify themselves when taking work:

```bash
laneq next --id --consumer worker-a
laneq list --all
laneq stats
```

Taken directives receive a lease. The default lease is 30 minutes, configurable
with `LANEQ_LEASE_SECONDS`. Use `--lease` on `next`
or `touch` to set or extend it:

```bash
laneq next --consumer claude --lease 45m
laneq touch 7 --lease 10m
laneq reap --expired-leases
```

Expired leases are reclaimed lazily on queue operations and increment the
directive's `requeue_count`.

Use lanes to isolate independent work streams inside the same SQLite database:

```bash
laneq push --lane release -p P0 -b "verify release candidate"
laneq next --lane release --consumer worker-a
laneq list --lane release
```

Use parent links to create directive threads:

```bash
laneq push -p P0 -b "investigate incident"
laneq push --parent 1 -p P0 -b "collect deployment evidence"
laneq list --thread 1
laneq thread-status 1
```

## Commands

- `push`: enqueue a directive from `--body`, `--file`, or stdin; add `--lane`
  and `--parent` to route and thread it.
- `next`: atomically take the highest-priority pending directive and print its
  body; add `--consumer`, `--lease`, and `--lane` for multi-worker coordination.
- `peek`: print the next pending directive without taking it; add `--lane` to
  inspect a specific lane.
- `show`: print any directive by id, including lane, thread, consumer, lease,
  and requeue details.
- `list`: list pending directives; add `--all` to include non-pending items,
  `--lane` to filter a lane, or `--thread` to render a thread.
- `reprioritize`: change a directive priority.
- `done`, `requeue`, `drop`: update directive status.
- `touch`: extend the lease for a taken directive.
- `thread-status`: summarize whether a directive thread still has open work.
- `reap`: requeue stale taken directives or expired leases.
- `stats`: print counts by priority/status and taken counts by consumer.

`next` and `peek` exit with status code `3` when the queue is empty.

Existing v0.1 databases migrate in place on first open. New columns are added
for consumers, leases, lane names, parent links, and requeue counts while
preserving existing directive ids and statuses.

`codex-q` remains as a compatibility command alias for existing local
automation. Prefer `laneq` for new docs, scripts, and integrations.

## Development

```bash
python -m pip install -e ".[test]"
coverage run -m pytest
coverage report --fail-under=95
```

The runtime package intentionally has zero third-party dependencies.
