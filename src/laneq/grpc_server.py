"""gRPC server for laneq queue operations.

Maps each proto RPC to laneq's core.py functions, converting between proto messages
and core.py dicts. Honors priority levels (P0/P1/P2), optional timestamps as Unix SECONDS,
and empty take/peek returns (directive unset in response).
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Any

import grpc

from laneq import core
from laneq.core import NotFoundError, PreconditionError, QueueError
from laneq.grpc import laneq_pb2, laneq_pb2_grpc


class LaneqServicer(laneq_pb2_grpc.LaneqServicer):
    """gRPC service implementation mapping proto RPCs to laneq core functions."""

    def _queue_error_code(self, exc: QueueError) -> grpc.StatusCode:
        """Map QueueError subclass to gRPC status code."""
        if isinstance(exc, NotFoundError):
            return grpc.StatusCode.NOT_FOUND
        if isinstance(exc, PreconditionError):
            return grpc.StatusCode.FAILED_PRECONDITION
        return grpc.StatusCode.INVALID_ARGUMENT

    def _priority_to_proto(self, priority_str: str) -> int:
        """Convert priority string (P0/P1/P2) to proto enum value."""
        mapping = {"P0": 1, "P1": 2, "P2": 3}  # PRIORITY_P0=1, PRIORITY_P1=2, PRIORITY_P2=3
        return mapping.get(priority_str, 2)  # Default to P1

    def _priority_from_proto(self, priority_int: int) -> str:
        """Convert proto enum value to priority string (P0/P1/P2)."""
        mapping = {1: "P0", 2: "P1", 3: "P2"}  # PRIORITY_P0=1, PRIORITY_P1=2, PRIORITY_P2=3
        return mapping.get(priority_int, "P1")  # Default to P1

    def _status_to_proto(self, status_str: str) -> int:
        """Convert status string to proto enum value."""
        mapping = {
            "pending": 1,  # STATUS_PENDING
            "taken": 2,  # STATUS_TAKEN
            "deferred": 3,  # STATUS_DEFERRED
            "done": 4,  # STATUS_DONE
            "dropped": 5,  # STATUS_DROPPED
            "parked": 6,  # STATUS_PARKED
        }
        return mapping.get(status_str, 0)  # STATUS_UNSPECIFIED

    def _status_from_proto(self, status_int: int) -> str:
        """Convert proto enum value to status string."""
        mapping = {
            1: "pending",  # STATUS_PENDING
            2: "taken",  # STATUS_TAKEN
            3: "deferred",  # STATUS_DEFERRED
            4: "done",  # STATUS_DONE
            5: "dropped",  # STATUS_DROPPED
            6: "parked",  # STATUS_PARKED
        }
        return mapping.get(status_int, "pending")

    def _dict_to_directive(self, d: dict[str, Any], now_unix: int | None = None) -> laneq_pb2.Directive:
        """Convert a core.py dict to a proto Directive message."""
        directive = laneq_pb2.Directive()
        directive.id = str(d.get("id", ""))
        directive.priority = self._priority_to_proto(d.get("priority", "P1"))
        directive.body = d.get("body", "")
        directive.status = self._status_to_proto(d.get("status", "pending"))
        directive.lane = d.get("lane", "default") or "default"
        directive.taken_by = d.get("taken_by") or ""
        parent = d.get("parent")
        if parent is not None:
            directive.parent_id = str(parent)
        directive.requeue_count = d.get("requeue_count", 0)

        # Convert timestamps from ISO format to Unix SECONDS
        created_at = d.get("created_at")
        if created_at:
            directive.created_at_unix = int(self._parse_iso_timestamp(created_at))

        taken_at = d.get("taken_at")
        if taken_at:
            directive.taken_at_unix = int(self._parse_iso_timestamp(taken_at))

        done_at = d.get("done_at")
        if done_at:
            directive.done_at_unix = int(self._parse_iso_timestamp(done_at))

        lease_until = d.get("lease_until")
        if lease_until:
            directive.lease_until_unix = int(self._parse_iso_timestamp(lease_until))

        not_before = d.get("not_before")
        if not_before:
            directive.not_before_unix = int(self._parse_iso_timestamp(not_before))

        blocked_by_str = d.get("blocked_by")
        if blocked_by_str:
            # Parse comma-separated list of IDs
            directive.blocked_by.extend(blocked_by_str.split(","))

        return directive

    def _parse_iso_timestamp(self, iso_str: str) -> float:
        """Parse ISO timestamp (YYYY-MM-DDTHH:MM:SSZ) to Unix timestamp (seconds)."""
        if not iso_str:
            return 0.0
        try:
            # Parse ISO format with Z suffix; timezone.utc ensures UTC interpretation
            dt_str = iso_str.rstrip("Z")
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            return 0.0

    async def Push(self, request: laneq_pb2.PushRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.PushResponse:
        """Enqueue a new directive."""
        try:
            priority = self._priority_from_proto(request.priority)
            parent_id = int(request.parent_id) if request.parent_id else None
            lane = request.lane or "default"

            result = core.push(request.body, priority=priority, parent=parent_id, lane=lane)

            response = laneq_pb2.PushResponse()
            response.id = str(result["id"])
            response.priority = request.priority or 2  # Default to P1
            response.lane = result["lane"]
            response.parent_id = str(result["parent"]) if result["parent"] else ""
            response.status = self._status_to_proto(result["status"])
            response.summary = result.get("summary", "")
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))

    async def Take(self, request: laneq_pb2.TakeRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.TakeResponse:
        """Claim the next eligible directive."""
        try:
            consumer = request.consumer or "-"
            lane = request.lane or "default"
            lease_seconds = max(1, request.lease_duration_ms // 1000) if request.lease_duration_ms else 1800
            reap_stale = request.reap_stale_seconds if request.reap_stale_seconds > 0 else None

            result = core.take(consumer=consumer, lease=lease_seconds, lane=lane, reap_stale_seconds=reap_stale)

            response = laneq_pb2.TakeResponse()
            response.consumer = consumer
            response.lane = lane

            if result:
                # Fetch the full directive record (core.take() returns only {id, body})
                full_record = core.show(result["id"])
                # Use the full converter so all fields are present and correct
                response.directive.CopyFrom(self._dict_to_directive(full_record))
                # Update taken_by since the record may not have that field set yet
                response.directive.taken_by = consumer
            # If result is None, response.directive is unset (empty Directive)

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))

    async def Peek(self, request: laneq_pb2.PeekRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.PeekResponse:
        """Query the next eligible directive without claiming."""
        try:
            lane = request.lane or "default"
            result = core.peek(lane=lane)

            response = laneq_pb2.PeekResponse()
            if result:
                # Fetch the full directive record (core.peek() returns only {id, priority, lane, body})
                full_record = core.show(result["id"])
                # Use the full converter so all fields are present and correct
                response.directive.CopyFrom(self._dict_to_directive(full_record))
            # If result is None, response.directive is unset

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))

    async def Show(self, request: laneq_pb2.ShowRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.ShowResponse:
        """Retrieve full details of a directive by ID."""
        try:
            item_id = int(request.id)
            result = core.show(item_id)

            response = laneq_pb2.ShowResponse()
            response.directive.CopyFrom(self._dict_to_directive(result))

            # Populate thread items
            for thread_item in result.get("thread", []):
                ti = laneq_pb2.ThreadItem()
                ti.id = str(thread_item["id"])
                ti.status = self._status_to_proto(thread_item["status"])
                ti.created_at_unix = 0  # Not provided by core.show()
                response.thread.append(ti)

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Listing(
        self, request: laneq_pb2.ListingRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.ListingResponse:
        """Query directives with optional filters."""
        try:
            lane = request.lane if request.lane else None
            thread = int(request.thread) if request.thread else None

            results = core.listing(all_statuses=request.all_statuses, lane=lane, thread=thread)

            response = laneq_pb2.ListingResponse()
            for item in results:
                directive = self._dict_to_directive(item)
                response.directives.append(directive)

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Reprioritize(
        self, request: laneq_pb2.ReprioritizeRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.ReprioritizeResponse:
        """Change a directive's priority."""
        try:
            item_id = int(request.id)
            priority = self._priority_from_proto(request.priority)
            result = core.reprioritize(item_id, priority)

            response = laneq_pb2.ReprioritizeResponse()
            response.id = str(result["id"])
            response.priority = request.priority
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def SetStatus(
        self, request: laneq_pb2.SetStatusRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.SetStatusResponse:
        """Change a directive's status (pending/done/dropped)."""
        try:
            item_id = int(request.id)
            status = self._status_from_proto(request.status)
            result = core.set_status(item_id, status)

            response = laneq_pb2.SetStatusResponse()
            response.id = str(result["id"])
            response.status = request.status
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Defer(
        self, request: laneq_pb2.DeferRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.DeferResponse:
        """Defer a directive until a specified time or delay."""
        try:
            item_id = int(request.id)

            # Convert until_unix to ISO format if provided
            until = None
            if request.until_unix:
                until = self._unix_to_iso_timestamp(request.until_unix)

            # Convert delay_ms to delay string (seconds)
            delay = None
            if request.delay_ms > 0:
                delay = max(1, request.delay_ms // 1000)

            blocked_by = list(request.blocked_by) if request.blocked_by else None

            result = core.defer(item_id, until=until, delay=delay, blocked_by=blocked_by)

            response = laneq_pb2.DeferResponse()
            response.id = str(result["id"])
            response.status = self._status_to_proto(result["status"])

            if result.get("not_before"):
                response.not_before_unix = int(self._parse_iso_timestamp(result["not_before"]))

            response.blocked_by.extend(request.blocked_by or [])
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Touch(
        self, request: laneq_pb2.TouchRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.TouchResponse:
        """Renew the lease on a claimed directive."""
        try:
            item_id = int(request.id)
            lease_seconds = max(1, request.lease_duration_ms // 1000) if request.lease_duration_ms else 1800

            result = core.touch(item_id, lease=lease_seconds)

            response = laneq_pb2.TouchResponse()
            response.id = str(result["id"])

            if result.get("lease_until"):
                response.lease_until_unix = int(self._parse_iso_timestamp(result["lease_until"]))

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Reap(self, request: laneq_pb2.ReapRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.ReapResponse:
        """Reclaim expired leases and stale deferred directives."""
        try:
            result = core.reap(expired_leases=request.expired_leases, stale_seconds=request.stale_seconds or 21600)

            response = laneq_pb2.ReapResponse()
            response.mode = result["mode"]
            response.reclaimed = result["reclaimed"]
            response.detail = f"Reclaimed {result['reclaimed']} directives"
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))

    async def Stats(
        self, request: laneq_pb2.StatsRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.StatsResponse:
        """Return queue statistics."""
        try:
            result = core.stats()

            response = laneq_pb2.StatsResponse()

            # Build by_status map
            for item in result.get("by_status", []):
                key = f"{item['status']}"
                response.by_status[key] = item["count"]

            # Build consumer stats
            for consumer_item in result.get("consumers", []):
                cs = laneq_pb2.ConsumerStats()
                cs.consumer = consumer_item["consumer"]
                cs.active_leases = consumer_item["count"]
                cs.total_claimed = consumer_item["count"]  # Not tracked separately in core
                cs.total_completed = 0  # Not available
                response.consumers.append(cs)

            return response
        except core.QueueError as e:  # pragma: no cover - defensive; Stats aggregation shouldn't fail
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def ThreadStatus(
        self, request: laneq_pb2.ThreadStatusRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.ThreadStatusResponse:
        """Query the status of a directive thread."""
        try:
            item_id = int(request.id)
            result = core.thread_status(item_id)

            response = laneq_pb2.ThreadStatusResponse()
            response.root = str(result["root"])
            response.status = self._status_to_proto(result["status"])
            response.total = result["total"]
            response.open = result["open"]

            for item in result.get("open_items", []):
                ti = laneq_pb2.ThreadItem()
                ti.id = str(item["id"])
                ti.status = self._status_to_proto(item["status"])
                ti.created_at_unix = 0  # Not provided
                response.open_items.append(ti)

            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Park(self, request: laneq_pb2.ParkRequest, context: grpc.aio.ServicerContext) -> laneq_pb2.ParkResponse:
        """Move a claimed directive into parked status (durable hold)."""
        try:
            item_id = int(request.id)
            result = core.park(item_id)

            response = laneq_pb2.ParkResponse()
            response.id = str(result["id"])
            response.status = self._status_to_proto(result["status"])
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    async def Unpark(
        self, request: laneq_pb2.UnparkRequest, context: grpc.aio.ServicerContext
    ) -> laneq_pb2.UnparkResponse:
        """Remove a directive from parked status (returns to pending)."""
        try:
            item_id = int(request.id)
            result = core.unpark(item_id)

            response = laneq_pb2.UnparkResponse()
            response.id = str(result["id"])
            response.status = self._status_to_proto(result["status"])
            return response
        except core.QueueError as e:
            await context.abort(self._queue_error_code(e), str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

    def _unix_to_iso_timestamp(self, unix_seconds: int) -> str:
        """Convert Unix timestamp (seconds) to ISO format (YYYY-MM-DDTHH:MM:SSZ)."""
        if not unix_seconds:
            return ""
        try:
            dt_tuple = time.gmtime(unix_seconds)
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", dt_tuple)
        except (ValueError, OSError):  # pragma: no cover - defensive guard for out-of-range timestamps
            return ""


async def serve(host: str = "localhost", port: int = 50051):  # pragma: no cover
    """Start the async gRPC server (process entrypoint; exercised by the real-wire path)."""
    from laneq.grpc_auth import build_interceptor_from_env

    auth_interceptor = build_interceptor_from_env()
    interceptors = [auth_interceptor] if auth_interceptor is not None else []
    server = grpc.aio.server(interceptors=interceptors)
    laneq_pb2_grpc.add_LaneqServicer_to_server(LaneqServicer(), server)
    addr = f"{host}:{port}"
    server.add_insecure_port(addr)
    await server.start()
    print(f"laneq-grpc: listening on {addr}", file=sys.stderr)
    await server.wait_for_termination()


def main():  # pragma: no cover
    """Entry point for the laneq-grpc command."""
    parser = argparse.ArgumentParser(description="gRPC server for laneq queue operations")
    parser.add_argument("--addr", default="localhost:50051", help="Listen address (default: localhost:50051)")
    args = parser.parse_args()

    host, port = args.addr.rsplit(":", 1)
    port = int(port)

    import asyncio

    asyncio.run(serve(host, port))


if __name__ == "__main__":  # pragma: no cover
    main()
