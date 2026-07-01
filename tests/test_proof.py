"""Tests for per-request proof verification + nonce replay cache (anti-replay)."""

import json
from datetime import datetime, timedelta, timezone

import pyseto
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from laneq.auth import GrantError, ReplayCache, verify_proof

AUDIENCE = "laneq://agent-host:9999"
METHOD = "/laneq.Laneq/Take"
NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)


def _keypair():
    sk = Ed25519PrivateKey.generate()
    priv_pem = sk.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    pub_pem = sk.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return (
        pyseto.Key.new(version=4, purpose="public", key=priv_pem),
        pyseto.Key.new(version=4, purpose="public", key=pub_pem),
    )


def _sign_proof(signing_key, *, aud=AUDIENCE, method=METHOD, now=NOW, iat_offset=0, nonce="n1"):
    claims = {"aud": aud, "method": method, "iat": int(now.timestamp()) + iat_offset, "nonce": nonce}
    return pyseto.encode(signing_key, json.dumps(claims).encode(), footer=b"")


def test_verify_proof_valid_returns_claims():
    client_sign, client_verify = _keypair()
    proof = _sign_proof(client_sign)
    claims = verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)
    assert claims["nonce"] == "n1"
    assert claims["method"] == METHOD


def test_verify_proof_rejects_wrong_client_key():
    client_sign, _ = _keypair()
    _, other_verify = _keypair()
    proof = _sign_proof(client_sign)  # not signed by other_verify's key
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=other_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_wrong_audience():
    client_sign, client_verify = _keypair()
    proof = _sign_proof(client_sign, aud="laneq://other:9999")
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_wrong_method():
    client_sign, client_verify = _keypair()
    proof = _sign_proof(client_sign, method="/laneq.Laneq/Done")  # proof for a different RPC
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_stale_iat():
    client_sign, client_verify = _keypair()
    proof = _sign_proof(client_sign, iat_offset=-120)  # 2 minutes old, outside ±30s skew
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW, skew_seconds=30)


def test_verify_proof_rejects_future_iat():
    client_sign, client_verify = _keypair()
    proof = _sign_proof(client_sign, iat_offset=120)  # 2 minutes in the future
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW, skew_seconds=30)


def test_verify_proof_rejects_missing_nonce():
    client_sign, client_verify = _keypair()
    claims = {"aud": AUDIENCE, "method": METHOD, "iat": int(NOW.timestamp())}  # no nonce
    proof = pyseto.encode(client_sign, json.dumps(claims).encode(), footer=b"")
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_malformed_payload():
    client_sign, client_verify = _keypair()
    proof = pyseto.encode(client_sign, b"not-json", footer=b"")
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_non_object_payload():
    client_sign, client_verify = _keypair()
    proof = pyseto.encode(client_sign, b"7", footer=b"")  # valid JSON, not an object
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_missing_iat():
    client_sign, client_verify = _keypair()
    claims = {"aud": AUDIENCE, "method": METHOD, "nonce": "n1"}  # no iat
    proof = pyseto.encode(client_sign, json.dumps(claims).encode(), footer=b"")
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW)


def test_verify_proof_rejects_replayed_nonce():
    client_sign, client_verify = _keypair()
    cache = ReplayCache(ttl_seconds=60)
    proof = _sign_proof(client_sign, nonce="once")
    # First use succeeds...
    verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW, replay_cache=cache)
    # ...replay of the SAME nonce is rejected.
    with pytest.raises(GrantError):
        verify_proof(proof, client_key=client_verify, audience=AUDIENCE, method=METHOD, now=NOW, replay_cache=cache)


def test_replay_cache_first_use_then_reject():
    cache = ReplayCache(ttl_seconds=60)
    assert cache.check_and_add("a", NOW) is True
    assert cache.check_and_add("a", NOW) is False  # immediate replay


def test_replay_cache_expires_after_ttl():
    cache = ReplayCache(ttl_seconds=60)
    assert cache.check_and_add("a", NOW) is True
    later = NOW + timedelta(seconds=61)  # past TTL → pruned, allowed again
    assert cache.check_and_add("a", later) is True
