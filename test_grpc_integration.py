#!/usr/bin/env python3
"""Integration test for gRPC server - can be run manually to verify the server works."""

import asyncio
import json
import os
import tempfile
from pathlib import Path

import pytest
import grpc

from laneq.grpc import laneq_pb2, laneq_pb2_grpc
from laneq.grpc_server import LaneqServicer


@pytest.mark.asyncio
async def test_integration():
    """Test the gRPC server with a real channel."""
    # Setup temp database
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["LANEQ_DB"] = str(Path(tmpdir) / "test.db")

        # Start server
        server = grpc.aio.server()
        laneq_pb2_grpc.add_LaneqServicer_to_server(LaneqServicer(), server)
        server.add_insecure_port("127.0.0.1:50053")
        await server.start()

        try:
            # Connect client
            async with grpc.aio.insecure_channel("127.0.0.1:50053") as channel:
                stub = laneq_pb2_grpc.LaneqStub(channel)

                # Test Push
                print("Testing Push...")
                push_response = await stub.Push(
                    laneq_pb2.PushRequest(
                        body=json.dumps({"intent": "test_task", "origin": "integration_test"}),
                        priority=laneq_pb2.PRIORITY_P0,
                    )
                )
                print(f"  Pushed directive #{push_response.id}")
                assert push_response.id
                assert push_response.status == laneq_pb2.STATUS_PENDING

                # Test Peek
                print("Testing Peek...")
                peek_response = await stub.Peek(laneq_pb2.PeekRequest())
                print(f"  Peeked directive #{peek_response.directive.id}")
                assert peek_response.directive.id == push_response.id

                # Test Take
                print("Testing Take...")
                take_response = await stub.Take(
                    laneq_pb2.TakeRequest(consumer="test-worker", lease_duration_ms=30000)
                )
                print(f"  Took directive #{take_response.directive.id}")
                assert take_response.directive.status == laneq_pb2.STATUS_TAKEN
                assert take_response.directive.taken_by == "test-worker"

                # Test Park
                print("Testing Park...")
                park_response = await stub.Park(
                    laneq_pb2.ParkRequest(id=push_response.id, consumer="test-worker")
                )
                print(f"  Parked directive #{park_response.id}")
                assert park_response.status == laneq_pb2.STATUS_PARKED

                # Test Unpark
                print("Testing Unpark...")
                unpark_response = await stub.Unpark(
                    laneq_pb2.UnparkRequest(id=push_response.id)
                )
                print(f"  Unparked directive #{unpark_response.id}")
                assert unpark_response.status == laneq_pb2.STATUS_PENDING

                # Test Show
                print("Testing Show...")
                show_response = await stub.Show(
                    laneq_pb2.ShowRequest(id=push_response.id)
                )
                print(f"  Show returned full directive: {show_response.directive.id}")
                assert show_response.directive.created_at_unix > 0

                # Test SetStatus
                print("Testing SetStatus...")
                set_status_response = await stub.SetStatus(
                    laneq_pb2.SetStatusRequest(id=push_response.id, status=laneq_pb2.STATUS_DONE)
                )
                print(f"  Set status to {set_status_response.status}")
                assert set_status_response.status == laneq_pb2.STATUS_DONE

                # Test Stats
                print("Testing Stats...")
                stats_response = await stub.Stats(laneq_pb2.StatsRequest())
                print(f"  Stats: {dict(stats_response.by_status)}")
                assert stats_response.by_status["done"] >= 1

                print("\nAll integration tests passed!")
        finally:
            await server.stop(grace=1)


if __name__ == "__main__":
    asyncio.run(test_integration())
