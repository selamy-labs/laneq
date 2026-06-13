# codex-q

`codex-q` is a tiny local SQLite priority queue for feeding directives to
autonomous agents and orchestrators. It never talks to the network and stores
only the queue data you put into its local database.

Priorities sort as `P0 < P1 < P2`, with FIFO ordering inside each priority.
`next` atomically takes one pending item so concurrent workers do not receive
the same directive.

## Install

Run directly from GitHub:

```bash
uvx --from git+https://github.com/selamy-labs/codex-q@v0.1.0 codex-q --help
```

Or install with pipx:

```bash
pipx install git+https://github.com/selamy-labs/codex-q@v0.1.0
```

## Usage

```bash
codex-q push -p P0 -b "ship the smallest verified fix"
codex-q peek
codex-q next --id
codex-q done 1
codex-q stats
```

Read a directive body from a file:

```bash
codex-q push -p P1 -f directive.txt
```

Use a specific database path:

```bash
CODEX_Q_DB=/tmp/codex-q.db codex-q list --all
```

The default database is `~/.claude/codex-queue.db`.

## Commands

- `push`: enqueue a directive from `--body`, `--file`, or stdin.
- `next`: atomically take the highest-priority pending directive and print its body.
- `peek`: print the next pending directive without taking it.
- `show`: print any directive by id, including non-pending items.
- `list`: list pending directives; add `--all` to include non-pending items.
- `reprioritize`: change a directive priority.
- `done`, `requeue`, `drop`: update directive status.
- `reap`: requeue stale taken directives.
- `stats`: print counts by priority and status.

`next` and `peek` exit with status code `3` when the queue is empty.

## Development

```bash
python -m pip install -e ".[test]"
coverage run -m pytest
coverage report --fail-under=95
```

The runtime package intentionally has zero third-party dependencies.
