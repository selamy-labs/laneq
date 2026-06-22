"""gRPC status code mapping tests.

Verifies that QueueError outcomes map to the correct gRPC status codes
for the Go adapter to interpret correctly.
"""

import json
import os
import tempfile
from pathlib import Path

import grpc
import pytest

from laneq import core
from laneq.core import NotFoundError, PreconditionError, QueueError
from laneq.grpc_server import LaneqServicer


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        os.environ["LANEQ_DB"] = str(db_path)
        yield db_path


class TestGRPCStatusCodeMapping:
    """Test gRPC status code mapping for QueueError subclasses."""

    def test_show_missing_item_raises_not_found(self, temp_db):
        """Show on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.show(9999)

    def test_reprioritize_missing_item_raises_not_found(self, temp_db):
        """Reprioritize on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.reprioritize(9999, "P1")

    def test_touch_missing_item_raises_not_found(self, temp_db):
        """Touch on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.touch(9999, lease=1800)

    def test_touch_pending_item_raises_failed_precondition(self, temp_db):
        """Touch on pending (not taken) item raises PreconditionError."""
        # Create a pending item
        response = core.push(json.dumps({"intent": "test"}), priority="P1")
        item_id = response["id"]

        # Touch it (it's pending, not taken)
        with pytest.raises(PreconditionError, match="no taken item"):
            core.touch(item_id, lease=1800)

    def test_unpark_missing_item_raises_not_found(self, temp_db):
        """Unpark on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.unpark(9999)

    def test_unpark_pending_item_raises_failed_precondition(self, temp_db):
        """Unpark on pending (not parked) item raises PreconditionError."""
        # Create a pending item
        response = core.push(json.dumps({"intent": "test"}), priority="P1")
        item_id = response["id"]

        # Try to unpark it (it's pending, not parked)
        with pytest.raises(PreconditionError, match="no parked item"):
            core.unpark(item_id)

    def test_park_missing_item_raises_not_found(self, temp_db):
        """Park on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.park(9999)

    def test_park_pending_item_raises_failed_precondition(self, temp_db):
        """Park on pending (not taken) item raises PreconditionError."""
        # Create a pending item
        response = core.push(json.dumps({"intent": "test"}), priority="P1")
        item_id = response["id"]

        # Try to park it (it's pending, not taken)
        with pytest.raises(PreconditionError, match="no taken item"):
            core.park(item_id)

    def test_push_empty_body_raises_queue_error(self, temp_db):
        """Push with empty body raises QueueError (not a subclass)."""
        with pytest.raises(QueueError, match="empty body"):
            core.push("", priority="P1")

    def test_push_invalid_parent_raises_not_found(self, temp_db):
        """Push with non-existent parent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no parent"):
            core.push(json.dumps({"intent": "test"}), priority="P1", parent=9999)

    def test_set_status_missing_item_raises_not_found(self, temp_db):
        """SetStatus on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.set_status(9999, "done")

    def test_defer_missing_dependency_raises_not_found(self, temp_db):
        """Defer with non-existent dependency raises NotFoundError."""
        # Create an item to defer
        response = core.push(json.dumps({"intent": "test"}), priority="P1")
        item_id = response["id"]

        # Try to defer it with a non-existent dependency
        with pytest.raises(NotFoundError, match="no dependency"):
            core.defer(item_id, blocked_by=["9999"])

    def test_defer_missing_item_raises_not_found(self, temp_db):
        """Defer on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.defer(9999, until="2026-06-22T19:00:00Z")

    def test_defer_self_blocked_raises_queue_error(self, temp_db):
        """Defer with self-dependency raises QueueError (not subclass)."""
        # Create an item
        response = core.push(json.dumps({"intent": "test"}), priority="P1")
        item_id = response["id"]

        # Try to block it on itself
        with pytest.raises(QueueError, match="cannot be blocked by itself"):
            core.defer(item_id, blocked_by=[str(item_id)])

    def test_thread_status_missing_item_raises_not_found(self, temp_db):
        """ThreadStatus on non-existent ID raises NotFoundError."""
        with pytest.raises(NotFoundError, match="no item"):
            core.thread_status(9999)


class TestGRPCStatusCodeMapperFunction:
    """Test the _queue_error_code helper in LaneqServicer."""

    def test_mapper_maps_not_found_error(self):
        """_queue_error_code maps NotFoundError to NOT_FOUND."""
        servicer = LaneqServicer()
        exc = NotFoundError("test")
        code = servicer._queue_error_code(exc)
        assert code == grpc.StatusCode.NOT_FOUND

    def test_mapper_maps_precondition_error(self):
        """_queue_error_code maps PreconditionError to FAILED_PRECONDITION."""
        servicer = LaneqServicer()
        exc = PreconditionError("test")
        code = servicer._queue_error_code(exc)
        assert code == grpc.StatusCode.FAILED_PRECONDITION

    def test_mapper_maps_base_queue_error_to_invalid_argument(self):
        """_queue_error_code maps base QueueError to INVALID_ARGUMENT."""
        servicer = LaneqServicer()
        exc = QueueError("test")
        code = servicer._queue_error_code(exc)
        assert code == grpc.StatusCode.INVALID_ARGUMENT
