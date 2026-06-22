"""Tests for the gRPC server implementation."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from laneq import core


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        os.environ["LANEQ_DB"] = str(db_path)
        yield db_path


def test_core_push_and_show(temp_db):
    """Test pushing and showing a directive."""
    result = core.push(json.dumps({"intent": "test", "origin": "pytest"}), priority="P1")
    assert result["id"]
    assert result["status"] == "pending"
    assert result["priority"] == "P1"

    show = core.show(result["id"])
    assert show["id"] == result["id"]
    assert show["status"] == "pending"


def test_core_take_and_claim(temp_db):
    """Test taking a directive and verifying lease."""
    # Push a directive
    push_result = core.push(json.dumps({"task": "work"}), priority="P0")
    item_id = push_result["id"]

    # Take it
    take_result = core.take(consumer="test-worker", lease=30)
    assert take_result["id"] == item_id
    assert take_result["body"]
    assert take_result["consumer"] == "test-worker"

    # Peek should return nothing (already taken)
    peek_result = core.peek()
    assert peek_result is None


def test_core_empty_take(temp_db):
    """Test take when queue is empty."""
    take_result = core.take(consumer="test-worker")
    assert take_result is None


def test_core_reprioritize(temp_db):
    """Test changing priority of a directive."""
    # Push two directives
    push1 = core.push("first", priority="P1")
    push2 = core.push("second", priority="P2")

    # Reprioritize second to P0
    repr_result = core.reprioritize(push2["id"], "P0")
    assert repr_result["priority"] == "P0"

    # Take should get the reprioritized one first
    take_result = core.take(consumer="worker")
    assert take_result["id"] == push2["id"]


def test_core_defer_and_promotion(temp_db):
    """Test deferring a directive and checking auto-promotion."""
    # Push and take
    push_result = core.push("work", priority="P1")
    item_id = push_result["id"]

    take_result = core.take(consumer="worker")
    assert take_result["id"] == item_id

    # Set status to done
    set_status_result = core.set_status(item_id, "done")
    assert set_status_result["status"] == "done"

    # Defer a new directive
    push2 = core.push("deferred_work")
    defer_result = core.defer(push2["id"], delay="1s")
    assert defer_result["status"] == "deferred"
    assert defer_result["not_before"]


def test_core_parked_status(temp_db):
    """Test parking and unparking directives."""
    # Push and take
    push_result = core.push("parkable_work", priority="P1")
    item_id = push_result["id"]

    take_result = core.take(consumer="worker")
    assert take_result["id"] == item_id

    # Park it
    park_result = core.park(item_id)
    assert park_result["status"] == "parked"

    # Peek should return nothing (parked is not pending)
    peek_result = core.peek()
    assert peek_result is None

    # Show should show parked status
    show = core.show(item_id)
    assert show["status"] == "parked"

    # Unpark it
    unpark_result = core.unpark(item_id)
    assert unpark_result["status"] == "pending"

    # Peek should now return it
    peek_result = core.peek()
    assert peek_result is not None
    assert peek_result["id"] == item_id


def test_core_requeue_count_on_requeue(temp_db):
    """Test that requeue_count increments when setting status to pending."""
    # Push and take
    push_result = core.push("work")
    item_id = push_result["id"]

    take_result = core.take(consumer="worker")

    # Show before requeue
    show = core.show(item_id)
    assert show["requeue_count"] == 0

    # Requeue (set status to pending)
    core.set_status(item_id, "pending")

    # Show after requeue
    show = core.show(item_id)
    assert show["requeue_count"] == 1

    # Take again
    take_result = core.take(consumer="worker")

    # Requeue again
    core.set_status(item_id, "pending")

    # Show after second requeue
    show = core.show(item_id)
    assert show["requeue_count"] == 2


def test_core_show_full_directive(temp_db):
    """Test show returns full directive details including timestamps."""
    # Push a directive
    push_result = core.push(
        json.dumps({"intent": "test"}),
        priority="P0",
    )
    item_id = push_result["id"]

    # Show it
    show = core.show(item_id)

    assert show["id"] == item_id
    assert show["status"] == "pending"
    assert show["priority"] == "P0"
    assert show["created_at"]  # Should have created_at


def test_core_listing_filters(temp_db):
    """Test listing with various filters."""
    # Push multiple directives
    push1 = core.push("work1", lane="lane1", priority="P0")
    push2 = core.push("work2", lane="lane2", priority="P1")

    # Take and mark one as done
    core.take(consumer="worker", lane="lane1")
    core.set_status(push1["id"], "done")

    # List pending only (default)
    listing = core.listing()
    assert len(listing) == 1
    assert listing[0]["id"] == push2["id"]

    # List all statuses
    listing = core.listing(all_statuses=True)
    assert len(listing) == 2


def test_core_touch_lease(temp_db):
    """Test renewing a lease."""
    # Push and take
    push_result = core.push("work")
    item_id = push_result["id"]

    take_result = core.take(consumer="worker", lease=10)
    initial_lease = take_result  # We don't have lease_until in take_result

    # Touch to renew
    touch_result = core.touch(item_id, lease=30)
    assert touch_result["id"] == item_id
    assert touch_result["lease_until"]


def test_core_stats(temp_db):
    """Test stats reporting."""
    # Push some directives
    core.push("work1")
    core.push("work2")

    # Take one
    core.take(consumer="worker")

    # Get stats
    stats = core.stats()

    # Check the stats structure
    assert "by_status" in stats
    assert "consumers" in stats


def test_core_thread_status(temp_db):
    """Test thread status tracking."""
    # Push a parent
    parent = core.push("parent_work")

    # Push children
    child1 = core.push("child1", parent=parent["id"])
    child2 = core.push("child2", parent=parent["id"])

    # Check thread status
    thread = core.thread_status(parent["id"])

    assert thread["root"] == parent["id"]
    assert thread["total"] == 3  # parent + 2 children
    assert thread["open"] == 3  # all pending
    assert thread["status"] == "open"


def test_core_reap(temp_db):
    """Test reaping expired leases."""
    import time

    # Push and take with very short lease
    push_result = core.push("work")
    item_id = push_result["id"]

    core.take(consumer="worker", lease=1)

    # Wait for lease to expire (need at least 1 second + some buffer)
    time.sleep(1.5)

    # Reap expired leases
    reap_result = core.reap(expired_leases=True)

    # Should have reclaimed the item
    assert reap_result["reclaimed"] >= 1


def test_core_priority_ordering(temp_db):
    """Test that take respects priority ordering."""
    # Push in reverse priority order
    p2 = core.push("low", priority="P2")
    p0 = core.push("high", priority="P0")
    p1 = core.push("normal", priority="P1")

    # Take should get P0 first
    take1 = core.take(consumer="worker1")
    assert take1["id"] == p0["id"]

    # Release and take should get P1
    core.set_status(p0["id"], "done")
    take2 = core.take(consumer="worker2")
    assert take2["id"] == p1["id"]

    # Release and take should get P2
    core.set_status(p1["id"], "done")
    take3 = core.take(consumer="worker3")
    assert take3["id"] == p2["id"]


def test_core_blocked_by_dependencies(temp_db):
    """Test deferring with blocked_by dependencies."""
    # Push parent and child
    parent = core.push("parent_work")
    child = core.push("child_work")

    # Defer child blocked by parent
    defer_result = core.defer(child["id"], blocked_by=[str(parent["id"])])
    assert defer_result["status"] == "deferred"
    assert "blocked_by" in defer_result

    # Peek should return parent (not child which is deferred)
    peek = core.peek()
    assert peek is not None
    assert peek["id"] == parent["id"]

    # Mark parent as done (terminal)
    core.set_status(parent["id"], "done")

    # Now peek should return the child (promoted from deferred)
    peek = core.peek()
    assert peek is not None
    assert peek["id"] == child["id"]


def test_core_lane_isolation(temp_db):
    """Test that lanes are isolated."""
    # Push to different lanes
    lane1 = core.push("work1", lane="lane1")
    lane2 = core.push("work2", lane="lane2")

    # Take from lane1
    take1 = core.take(consumer="worker", lane="lane1")
    assert take1["id"] == lane1["id"]
    assert take1["lane"] == "lane1"

    # Peek lane2 should return lane2's work
    peek2 = core.peek(lane="lane2")
    assert peek2["id"] == lane2["id"]
    assert peek2["lane"] == "lane2"


def test_core_parked_excluded_from_reap(temp_db):
    """Test that parked directives are not affected by reap."""
    # Push and take
    push_result = core.push("work")
    item_id = push_result["id"]

    core.take(consumer="worker", lease=1)

    # Park it
    core.park(item_id)

    # Wait for lease to expire
    import time
    time.sleep(0.2)

    # Reap expired leases
    reap_result = core.reap(expired_leases=True)

    # Should not have reaped the parked item
    show = core.show(item_id)
    assert show["status"] == "parked"


def test_core_set_status_to_pending_increments_requeue(temp_db):
    """Test that set_status to 'pending' increments requeue_count."""
    # Push, take, set to pending multiple times
    push_result = core.push("work")
    item_id = push_result["id"]

    for i in range(3):
        core.take(consumer=f"worker{i}")
        core.set_status(item_id, "pending")

        show = core.show(item_id)
        assert show["requeue_count"] == i + 1
