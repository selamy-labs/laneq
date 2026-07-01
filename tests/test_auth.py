"""Tests for PASETO v4.public grant verification (laneq host-to-host auth)."""

import json
from datetime import datetime, timezone

import pyseto
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from laneq.auth import GrantError, verify_grant

AUDIENCE = "laneq://agent-host:9999"
NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)


def _keypair():
    """Return (signing_key, verify_key) as pyseto v4.public Keys for a fresh Ed25519 pair."""
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
    signing_key = pyseto.Key.new(version=4, purpose="public", key=priv_pem)
    verify_key = pyseto.Key.new(version=4, purpose="public", key=pub_pem)
    return signing_key, verify_key


def _sign(signing_key, *, aud=AUDIENCE, now=NOW, ttl=1800, nbf_offset=0, kid="k1", sub="agent-host"):
    claims = {
        "iss": "mac-issuer",
        "sub": sub,
        "aud": aud,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()) + nbf_offset,
        "exp": int(now.timestamp()) + ttl,
        "jti": "test-jti",
    }
    footer = json.dumps({"kid": kid}).encode()
    return pyseto.encode(signing_key, json.dumps(claims).encode(), footer=footer)


def test_verify_grant_valid_returns_claims():
    signing_key, verify_key = _keypair()
    token = _sign(signing_key)

    claims = verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)

    assert claims["sub"] == "agent-host"
    assert claims["aud"] == AUDIENCE
    assert claims["iss"] == "mac-issuer"


def test_verify_grant_rejects_expired():
    signing_key, verify_key = _keypair()
    token = _sign(signing_key, ttl=-1)  # exp one second in the past
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_not_yet_valid():
    signing_key, verify_key = _keypair()
    token = _sign(signing_key, nbf_offset=3600)  # nbf one hour in the future
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_wrong_audience():
    signing_key, verify_key = _keypair()
    token = _sign(signing_key, aud="laneq://other-host:9999")
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_forged_signature():
    _, verify_key = _keypair()
    other_signing_key, _ = _keypair()
    token = _sign(other_signing_key)  # signed by a key NOT in the trusted set
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_tampered_token():
    signing_key, verify_key = _keypair()
    token = _sign(signing_key)
    tampered = token[:30] + bytes([token[30] ^ 0x01]) + token[31:]  # flip one bit
    with pytest.raises(GrantError):
        verify_grant(tampered, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_malformed_payload():
    signing_key, verify_key = _keypair()
    token = pyseto.encode(signing_key, b"not-json-at-all", footer=b"")
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_missing_exp():
    signing_key, verify_key = _keypair()
    claims = {"iss": "mac-issuer", "sub": "agent-host", "aud": AUDIENCE}  # no exp
    token = pyseto.encode(signing_key, json.dumps(claims).encode(), footer=b"")
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_non_numeric_exp():
    signing_key, verify_key = _keypair()
    claims = {"iss": "mac-issuer", "sub": "agent-host", "aud": AUDIENCE, "exp": "not-a-number"}
    token = pyseto.encode(signing_key, json.dumps(claims).encode(), footer=b"")
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_non_numeric_nbf():
    signing_key, verify_key = _keypair()
    claims = {
        "iss": "mac-issuer",
        "sub": "agent-host",
        "aud": AUDIENCE,
        "exp": int(NOW.timestamp()) + 1800,
        "nbf": "not-a-number",
    }
    token = pyseto.encode(signing_key, json.dumps(claims).encode(), footer=b"")
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_rejects_non_object_payload():
    signing_key, verify_key = _keypair()
    token = pyseto.encode(signing_key, b"42", footer=b"")  # valid JSON, but not an object
    with pytest.raises(GrantError):
        verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)


def test_verify_grant_accepts_without_nbf():
    signing_key, verify_key = _keypair()
    claims = {
        "iss": "mac-issuer",
        "sub": "agent-host",
        "aud": AUDIENCE,
        "exp": int(NOW.timestamp()) + 1800,
    }
    token = pyseto.encode(signing_key, json.dumps(claims).encode(), footer=b"")
    result = verify_grant(token, public_keys=[verify_key], audience=AUDIENCE, now=NOW)
    assert result["sub"] == "agent-host"


def test_verify_grant_accepts_during_key_rotation_overlap():
    # A token signed by the *next* key verifies when both current+next are trusted.
    _, current_verify = _keypair()
    next_signing, next_verify = _keypair()
    token = _sign(next_signing, kid="k2")
    claims = verify_grant(token, public_keys=[current_verify, next_verify], audience=AUDIENCE, now=NOW)
    assert claims["sub"] == "agent-host"
