"""Real TCP gRPC transport uses the native protected-stage transaction."""

from __future__ import annotations

import hashlib
import json

import grpc
import pytest
import pytest_asyncio

from laneq import core
from laneq.grpc import laneq_pb2 as pb
from laneq.grpc import laneq_pb2_grpc as rpc
from laneq.grpc_auth import GrantAuthInterceptor
from laneq.grpc_server import LaneqServicer


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "stages.db"))
    server = grpc.aio.server()
    rpc.add_LaneqServicer_to_server(LaneqServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield rpc.LaneqStub(channel)
    finally:
        await server.stop(0)


def manifest(claim):
    return {
        "input_digest": claim.input_digest,
        "artifact_digest": "a" * 64,
        "receipt_ref": "artifact://dev/exact-receipt",
        "receipt_digest": "b" * 64,
        "successors": [
            {"admission_key": "dev:review:artifact-a", "body": "exact review", "lane": "dev:review", "priority": "P1"}
        ],
    }


async def claim_root(client):
    await client.AdmitStage(pb.AdmitStageRequest(admission_key="dev:implementation", body="exact", lane="dev:muse"))
    return await client.Take(pb.TakeRequest(consumer="dev-muse-1", lane="dev:muse", recovery_hold=True))


@pytest.mark.asyncio
async def test_stage_roundtrip_and_lost_response_reconciliation(client):
    root = await client.AdmitStage(
        pb.AdmitStageRequest(admission_key="dev:implementation", body="exact", lane="dev:muse")
    )
    repeat = await client.AdmitStage(
        pb.AdmitStageRequest(admission_key="dev:implementation", body="exact", lane="dev:muse")
    )
    assert root.directive.id == repeat.directive.id
    assert root.directive.recovery_hold
    assert not (await client.Take(pb.TakeRequest(lane="dev:muse"))).HasField("directive")
    assert not (await client.Peek(pb.PeekRequest(lane="dev:muse"))).HasField("directive")
    assert (await client.Peek(pb.PeekRequest(lane="dev:muse", recovery_hold=True))).directive.id == root.directive.id
    claim = await client.Take(pb.TakeRequest(consumer="dev-muse-1", lane="dev:muse", recovery_hold=True))
    assert claim.input_digest == hashlib.sha256(b"exact").hexdigest()
    assert claim.claim_token and claim.directive.HasField("lease_until_unix")
    request = pb.CompleteHandoffRequest(
        id=claim.directive.id, claim_token=claim.claim_token, manifest_json=json.dumps(manifest(claim))
    )
    result = await client.CompleteHandoff(request)
    replay = await client.CompleteHandoff(request)
    assert result.receipt_json == replay.receipt_json
    observed = await client.GetHandoffReceipt(pb.GetHandoffReceiptRequest(id=claim.directive.id))
    assert observed.found and observed.receipt_json == result.receipt_json
    receipt = json.loads(result.receipt_json)
    review = await client.Take(pb.TakeRequest(lane="dev:review", recovery_hold=True))
    assert review.directive.id == str(receipt["successor_ids"][0])
    assert review.directive.parent_id == claim.directive.id
    assert review.directive.recovery_hold
    assert (await client.Show(pb.ShowRequest(id=claim.directive.id))).directive.status == pb.STATUS_DONE
    listed = await client.Listing(pb.ListingRequest(all_statuses=True))
    assert len(listed.directives) == 2 and all(item.recovery_hold for item in listed.directives)
    partial = await client.Listing(pb.ListingRequest(thread=claim.directive.id))
    assert len(partial.directives) == 2
    assert all(not item.HasField("recovery_hold") for item in partial.directives)


@pytest.mark.asyncio
async def test_unknown_receipt_is_explicit_and_does_not_claim_work(client):
    response = await client.GetHandoffReceipt(pb.GetHandoffReceiptRequest(id="123"))
    assert not response.found and response.receipt_json == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("priority", 99), ("admission_key", ""), ("body", ""), ("lane", "")])
async def test_invalid_stage_admission_fails_without_enqueuing(client, field, value):
    data = {"admission_key": "work", "body": "exact", "lane": "dev:muse", field: value}
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await client.AdmitStage(pb.AdmitStageRequest(**data))
    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert core.listing(all_statuses=True) == []


@pytest.mark.asyncio
async def test_changed_admission_key_is_a_precondition_failure(client):
    await claim_root(client)
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await client.AdmitStage(
            pb.AdmitStageRequest(admission_key="dev:implementation", body="changed", lane="dev:muse")
        )
    assert caught.value.code() == grpc.StatusCode.FAILED_PRECONDITION


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "text",
    ["{", '{"input_digest":"a","input_digest":"b"}', " " * 131073, "[" * 1100 + "]" * 1100, "[" * 10000 + "]" * 10000],
)
async def test_invalid_manifest_cannot_complete_or_create_successors(client, text):
    claim = await claim_root(client)
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await client.CompleteHandoff(
            pb.CompleteHandoffRequest(id=claim.directive.id, claim_token=claim.claim_token, manifest_json=text)
        )
    assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert core.show(int(claim.directive.id))["status"] == "taken"
    assert len(core.listing(all_statuses=True)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("item_id", ["no-id", "0", "-1", "9223372036854775808"])
async def test_invalid_ids_are_invalid_argument_on_both_handoff_rpcs(client, item_id):
    for method, request in [
        (client.GetHandoffReceipt, pb.GetHandoffReceiptRequest(id=item_id)),
        (client.CompleteHandoff, pb.CompleteHandoffRequest(id=item_id, manifest_json="{}", claim_token="token")),
    ]:
        with pytest.raises(grpc.aio.AioRpcError) as caught:
            await method(request)
        assert caught.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,expected", [("token", grpc.StatusCode.FAILED_PRECONDITION), ("id", grpc.StatusCode.NOT_FOUND)]
)
async def test_stale_or_missing_owner_cannot_publish_review(client, change, expected):
    claim = await claim_root(client)
    request = pb.CompleteHandoffRequest(
        id=claim.directive.id, claim_token=claim.claim_token, manifest_json=json.dumps(manifest(claim))
    )
    setattr(request, change if change == "id" else "claim_token", "999" if change == "id" else "stale")
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await client.CompleteHandoff(request)
    assert caught.value.code() == expected
    assert len(core.listing(all_statuses=True)) == 1


@pytest.mark.asyncio
async def test_generic_force_completion_cannot_bypass_stage_handoff(client):
    claim = await claim_root(client)
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await client.SetStatus(pb.SetStatusRequest(id=claim.directive.id, status=pb.STATUS_DONE, force=True))
    assert caught.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert core.show(int(claim.directive.id))["status"] == "taken"


@pytest.mark.asyncio
async def test_new_rpcs_inherit_enforced_sender_auth(tmp_path, monkeypatch):
    path = tmp_path / "denied.db"
    monkeypatch.setenv("LANEQ_DB", str(path))
    auth = GrantAuthInterceptor(public_keys={}, audience="laneq://private", mode="enforce")
    server = grpc.aio.server(interceptors=[auth])
    rpc.add_LaneqServicer_to_server(LaneqServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            client = rpc.LaneqStub(channel)
            for method, request in [
                (client.AdmitStage, pb.AdmitStageRequest(admission_key="work", body="exact", lane="dev:muse")),
                (client.CompleteHandoff, pb.CompleteHandoffRequest(id="1")),
                (client.GetHandoffReceipt, pb.GetHandoffReceiptRequest(id="1")),
            ]:
                with pytest.raises(grpc.aio.AioRpcError) as caught:
                    await method(request)
                assert caught.value.code() == grpc.StatusCode.UNAUTHENTICATED
            assert not path.exists()
    finally:
        await server.stop(0)
