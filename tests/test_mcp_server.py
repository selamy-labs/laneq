"""End-to-end tests for the laneq MCP server.

Each test drives the tools through ``FastMCP.call_tool`` against a temporary
database, exercising the real MCP path (tool registration, argument coercion,
structured output, and error surfacing) rather than calling ``core`` directly.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from laneq.mcp_server import build_server


def call(db: Path, name: str, arguments: dict[str, Any] | None = None) -> Any:
    """Invoke an MCP tool against ``db`` and return its structured result."""
    old_db = os.environ.get("LANEQ_DB")
    os.environ["LANEQ_DB"] = str(db)
    try:
        _, structured = asyncio.run(build_server().call_tool(name, arguments or {}))
    finally:
        if old_db is None:
            os.environ.pop("LANEQ_DB", None)
        else:
            os.environ["LANEQ_DB"] = old_db
    return structured


def test_server_registers_every_queue_tool() -> None:
    tools = asyncio.run(build_server().list_tools())
    names = {tool.name for tool in tools}
    assert names == {
        "laneq_push",
        "laneq_next",
        "laneq_peek",
        "laneq_show",
        "laneq_list",
        "laneq_reprioritize",
        "laneq_done",
        "laneq_requeue",
        "laneq_defer",
        "laneq_drop",
        "laneq_touch",
        "laneq_reap",
        "laneq_stats",
        "laneq_thread_status",
    }
    push = next(tool for tool in tools if tool.name == "laneq_push")
    assert "body" in push.inputSchema["properties"]


def test_push_next_done_round_trip(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    pushed = call(db, "laneq_push", {"body": "build a small thing", "priority": "P0"})
    assert pushed == {
        "id": 1,
        "priority": "P0",
        "lane": "default",
        "parent": None,
        "status": "pending",
        "summary": "build a small thing",
    }

    taken = call(db, "laneq_next", {"consumer": "worker-a", "lease": "10m"})
    assert taken == {"id": 1, "body": "build a small thing", "consumer": "worker-a", "lane": "default"}

    done = call(db, "laneq_done", {"id": 1})
    assert done == {"id": 1, "status": "done"}

    stats = call(db, "laneq_stats")
    assert {"priority": "P0", "status": "done", "count": 1} in stats["by_status"]


def test_priority_ordering_through_tools(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "third", "priority": "P2"})
    call(db, "laneq_push", {"body": "first", "priority": "P0"})
    call(db, "laneq_push", {"body": "middle", "priority": "P1"})

    assert call(db, "laneq_next")["body"] == "first"
    assert call(db, "laneq_next")["body"] == "middle"
    assert call(db, "laneq_next")["body"] == "third"


def test_peek_does_not_take_and_reports_empty(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    assert call(db, "laneq_peek") == {"empty": True}
    assert call(db, "laneq_next") == {"empty": True}

    call(db, "laneq_push", {"body": "peek me", "priority": "P1"})
    peeked = call(db, "laneq_peek")
    assert peeked == {"id": 1, "priority": "P1", "lane": "default", "body": "peek me"}
    # peek left it pending, so next still gets it
    assert call(db, "laneq_next")["body"] == "peek me"


def test_show_includes_thread(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "root", "priority": "P1"})
    call(db, "laneq_push", {"body": "child", "priority": "P0", "parent": 1})

    shown = call(db, "laneq_show", {"id": 1})
    assert shown["id"] == 1
    assert shown["status"] == "pending"
    assert [item["summary"] for item in shown["thread"]] == ["root", "child"]


def test_list_filters_by_status_lane_and_thread(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "default work", "priority": "P0"})
    call(db, "laneq_push", {"body": "release work", "priority": "P0", "lane": "release"})

    pending = call(db, "laneq_list")
    assert {item["summary"] for item in pending["items"]} == {"default work", "release work"}

    only_release = call(db, "laneq_list", {"lane": "release"})
    assert [item["summary"] for item in only_release["items"]] == ["release work"]

    call(db, "laneq_next", {"lane": "release"})
    call(db, "laneq_done", {"id": 2})
    assert call(db, "laneq_list", {"lane": "release"})["items"] == []
    all_release = call(db, "laneq_list", {"all_statuses": True, "lane": "release"})
    assert [item["status"] for item in all_release["items"]] == ["done"]

    call(db, "laneq_push", {"body": "child", "priority": "P0", "parent": 1})
    threaded = call(db, "laneq_list", {"thread": 1})
    assert [item["summary"] for item in threaded["items"]] == ["default work", "child"]


def test_reprioritize_requeue_and_drop(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "low", "priority": "P2"})

    assert call(db, "laneq_reprioritize", {"id": 1, "priority": "P0"}) == {"id": 1, "priority": "P0"}
    assert call(db, "laneq_next")["body"] == "low"
    assert call(db, "laneq_requeue", {"id": 1}) == {"id": 1, "status": "pending"}
    assert call(db, "laneq_drop", {"id": 1}) == {"id": 1, "status": "dropped"}


def test_defer_tool_blocks_next_until_dependency_done(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "dependency", "priority": "P0"})
    call(db, "laneq_push", {"body": "blocked", "priority": "P0"})
    call(db, "laneq_push", {"body": "fallback", "priority": "P1"})

    deferred = call(db, "laneq_defer", {"id": 2, "blocked_by": ["1"]})

    assert deferred == {"id": 2, "status": "deferred", "not_before": None, "blocked_by": "1"}
    assert call(db, "laneq_next")["body"] == "dependency"
    assert call(db, "laneq_next")["body"] == "fallback"
    call(db, "laneq_done", {"id": 1})
    assert call(db, "laneq_next")["body"] == "blocked"


def test_touch_extends_lease(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "lease me", "priority": "P0"})
    call(db, "laneq_next", {"consumer": "worker", "lease": "10m"})

    touched = call(db, "laneq_touch", {"id": 1, "lease": "1h"})
    assert touched["id"] == 1
    assert touched["lease_until"] is not None


def test_reap_expired_leases_and_stale(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "work", "priority": "P0"})
    call(db, "laneq_next", {"consumer": "worker", "lease": "1"})

    # No reclaim yet because lease has not been forced into the past.
    fresh = call(db, "laneq_reap", {"stale_seconds": 999999})
    assert fresh == {"mode": "stale", "reclaimed": 0, "stale_seconds": 999999}

    import sqlite3

    con = sqlite3.connect(db)
    con.execute("UPDATE directives SET lease_until='2026-01-01T00:00:00Z' WHERE id=1")
    con.commit()
    con.close()

    reaped = call(db, "laneq_reap", {"expired_leases": True})
    assert reaped == {"mode": "expired-leases", "reclaimed": 1}
    assert call(db, "laneq_next")["body"] == "work"


def test_thread_status_tracks_open_and_done(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    call(db, "laneq_push", {"body": "root", "priority": "P1"})
    call(db, "laneq_push", {"body": "child", "priority": "P0", "parent": 1})

    open_status = call(db, "laneq_thread_status", {"id": 2})
    assert open_status["root"] == 1
    assert open_status["status"] == "open"
    assert open_status["total"] == 2
    assert open_status["open"] == 2

    call(db, "laneq_done", {"id": 1})
    call(db, "laneq_done", {"id": 2})
    closed = call(db, "laneq_thread_status", {"id": 1})
    assert closed["status"] == "done"
    assert closed["open"] == 0
    assert closed["open_items"] == []


def test_missing_item_surfaces_tool_error(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    with pytest.raises(ToolError, match="no item #404"):
        call(db, "laneq_show", {"id": 404})
    with pytest.raises(ToolError, match="no item #404"):
        call(db, "laneq_done", {"id": 404})


def test_push_rejects_empty_body_and_missing_parent(tmp_path: Path) -> None:
    db = tmp_path / "queue.db"
    with pytest.raises(ToolError, match="empty body"):
        call(db, "laneq_push", {"body": "   "})
    with pytest.raises(ToolError, match="no parent #99"):
        call(db, "laneq_push", {"body": "orphan", "parent": 99})


def test_main_runs_server_over_stdio(monkeypatch: pytest.MonkeyPatch) -> None:
    from laneq import mcp_server

    transports: list[str] = []
    monkeypatch.setattr(
        "mcp.server.fastmcp.FastMCP.run",
        lambda self, transport="stdio", **_: transports.append(transport),
    )
    mcp_server.main()
    assert transports == ["stdio"]
