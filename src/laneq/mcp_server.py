"""MCP server exposing the laneq priority queue as typed tools.

This is an optional integration: install it with ``pip install laneq[mcp]``.
The core package keeps zero runtime dependencies; the ``mcp`` SDK is only
required to run this server.

Every tool is a thin wrapper over :mod:`laneq.core`, so the queue logic and
SQL live in exactly one place. Tools take structured inputs and return JSON
objects. The selected database follows the usual ``LANEQ_DB`` / ``CODEX_Q_DB``
environment resolution, so the MCP server and the CLI share one queue.
"""

from __future__ import annotations

from typing import Any, Literal

try:
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError as error:  # pragma: no cover - import guard
    raise SystemExit(
        "laneq MCP server requires the 'mcp' package. Install it with: pip install 'laneq[mcp]'"
    ) from error

from laneq import core

Priority = Literal["P0", "P1", "P2"]

INSTRUCTIONS = (
    "Local SQLite priority queue for handing directives between orchestrators and "
    "autonomous workers. Priorities sort P0 < P1 < P2 with FIFO inside a priority. "
    "Use laneq_next to atomically take the highest-priority pending item, laneq_done "
    "to complete it, and laneq_push to enqueue work. Lanes isolate independent "
    "streams; parent links form threads. Priority is owned by the enqueuer: do not "
    "demote items you cannot start -- skip and surface them instead."
)


def laneq_push(
    body: str,
    priority: Priority = "P1",
    parent: int | None = None,
    lane: str = core.DEFAULT_LANE,
) -> dict[str, Any]:
    """Enqueue a directive. Returns the new item id and its stored fields.

    Set ``parent`` to thread the item under an existing directive and ``lane``
    to route it to an isolated work stream.
    """
    return core.push(body, priority=priority, parent=parent, lane=lane)


def laneq_next(
    consumer: str = "-",
    lease: str = str(core.DEFAULT_LEASE_SECONDS),
    lane: str = core.DEFAULT_LANE,
) -> dict[str, Any]:
    """Atomically take the highest-priority pending directive in a lane.

    Records ``consumer`` as the taker and sets a lease (e.g. "45m", "1h", or
    seconds). Returns ``{"empty": true}`` when the lane has no work.
    """
    result = core.take(consumer=consumer, lease=lease, lane=lane)
    return {"empty": True} if result is None else result


def laneq_peek(lane: str = core.DEFAULT_LANE) -> dict[str, Any]:
    """Show the next pending directive in a lane without taking it.

    Returns ``{"empty": true}`` when the lane has no pending work.
    """
    result = core.peek(lane=lane)
    return {"empty": True} if result is None else result


def laneq_show(id: int) -> dict[str, Any]:
    """Return the full record for a directive, including its thread."""
    return core.show(id)


def laneq_list(
    all_statuses: bool = False,
    lane: str | None = None,
    thread: int | None = None,
) -> dict[str, Any]:
    """List directives.

    By default lists only pending items. Set ``all_statuses`` to include
    taken/done/dropped, ``lane`` to filter one lane, or ``thread`` to render the
    full thread rooted at that id.
    """
    return {"items": core.listing(all_statuses=all_statuses, lane=lane, thread=thread)}


def laneq_reprioritize(id: int, priority: Priority) -> dict[str, Any]:
    """Change a directive's priority. Priority is enqueuer-owned."""
    return core.reprioritize(id, priority)


def laneq_done(id: int) -> dict[str, Any]:
    """Mark a directive done (completed)."""
    return core.set_status(id, "done")


def laneq_requeue(id: int) -> dict[str, Any]:
    """Return a directive to pending so it can be taken again."""
    return core.set_status(id, "pending")


def laneq_drop(id: int) -> dict[str, Any]:
    """Drop a directive (terminal, not done)."""
    return core.set_status(id, "dropped")


def laneq_touch(id: int, lease: str = str(core.DEFAULT_LEASE_SECONDS)) -> dict[str, Any]:
    """Extend the lease on a taken directive while still working it."""
    return core.touch(id, lease=lease)


def laneq_reap(
    expired_leases: bool = False,
    stale_seconds: int = core.DEFAULT_REAP_STALE_SECONDS,
) -> dict[str, Any]:
    """Reclaim work back to pending.

    With ``expired_leases`` true, reclaim items whose lease has lapsed;
    otherwise reclaim items taken longer ago than ``stale_seconds``.
    """
    return core.reap(expired_leases=expired_leases, stale_seconds=stale_seconds)


def laneq_stats() -> dict[str, Any]:
    """Return counts by priority/status and taken counts by consumer."""
    return core.stats()


def laneq_thread_status(id: int) -> dict[str, Any]:
    """Summarise whether the directive thread rooted at ``id`` has open work."""
    return core.thread_status(id)


TOOLS = (
    laneq_push,
    laneq_next,
    laneq_peek,
    laneq_show,
    laneq_list,
    laneq_reprioritize,
    laneq_done,
    laneq_requeue,
    laneq_drop,
    laneq_touch,
    laneq_reap,
    laneq_stats,
    laneq_thread_status,
)


def build_server() -> FastMCP:
    """Build the laneq MCP server with every queue tool registered."""
    server = FastMCP("laneq", instructions=INSTRUCTIONS)
    for tool in TOOLS:
        server.add_tool(tool)
    return server


def main() -> None:
    """Run the laneq MCP server over stdio."""
    build_server().run()


if __name__ == "__main__":  # pragma: no cover
    main()
