"""Tests for the gRPC grant+proof auth interceptor (off / log-only / enforce)."""

import json
from datetime import datetime, timezone

import grpc
import pyseto
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from laneq.grpc_auth import GrantAuthInterceptor, build_interceptor_from_env

AUD = "laneq://agent-host:9999"
METHOD = "/laneq.Laneq/Take"
NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)


def _pem_pair():
    sk = Ed25519PrivateKey.generate()
    priv = sk.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    pub = sk.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return priv, pub


def _sign_key(priv_pem):
    return pyseto.Key.new(version=4, purpose="public", key=priv_pem)


def _verify_key(pub_pem):
    return pyseto.Key.new(version=4, purpose="public", key=pub_pem)


def _grant(issuer_priv, client_pub, *, aud=AUD, now=NOW, ttl=1800, cnf="default"):
    claims = {
        "iss": "mac",
        "sub": "agent-host",
        "aud": aud,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int(now.timestamp()) + ttl,
        "jti": "j1",
    }
    if cnf == "default":
        claims["cnf"] = {"kid": "c1", "key": client_pub.decode()}
    elif cnf is not None:
        claims["cnf"] = cnf
    footer = json.dumps({"kid": "k1"}).encode()
    return pyseto.encode(_sign_key(issuer_priv), json.dumps(claims).encode(), footer=footer).decode()


def _proof(client_priv, *, aud=AUD, method=METHOD, now=NOW, nonce="n1"):
    claims = {"aud": aud, "method": method, "iat": int(now.timestamp()), "nonce": nonce}
    return pyseto.encode(_sign_key(client_priv), json.dumps(claims).encode(), footer=b"").decode()


class _Details:
    def __init__(self, method, metadata):
        self.method = method
        self.invocation_metadata = metadata


class _Aborted(Exception):
    pass


class _FakeContext:
    def __init__(self):
        self.code = None
        self.details = None

    async def abort(self, code, details):
        self.code = code
        self.details = details
        raise _Aborted(details)


async def _continuation(_details):
    return "PASSTHROUGH"


def _interceptor(issuer_pub, mode, **kw):
    return GrantAuthInterceptor(public_keys=[_verify_key(issuer_pub)], audience=AUD, mode=mode, clock=lambda: NOW, **kw)


async def _assert_denied(handler):
    """A deny result is an RpcMethodHandler whose unary_unary aborts UNAUTHENTICATED."""
    assert handler != "PASSTHROUGH"
    ctx = _FakeContext()
    with pytest.raises(_Aborted):
        await handler.unary_unary(b"request", ctx)
    assert ctx.code == grpc.StatusCode.UNAUTHENTICATED


@pytest.mark.asyncio
async def test_enforce_valid_grant_and_proof_passes_through():
    ipriv, ipub = _pem_pair()
    cpriv, cpub = _pem_pair()
    md = (("laneq-grant", _grant(ipriv, cpub)), ("laneq-proof", _proof(cpriv)))
    result = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, md))
    assert result == "PASSTHROUGH"


@pytest.mark.asyncio
async def test_enforce_missing_grant_and_proof_denies():
    _, ipub = _pem_pair()
    handler = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, ()))
    await _assert_denied(handler)


@pytest.mark.asyncio
async def test_enforce_forged_proof_denies():
    ipriv, ipub = _pem_pair()
    _, cpub = _pem_pair()
    wrong_priv, _ = _pem_pair()  # proof signed by a key that is NOT the grant's cnf
    md = (("laneq-grant", _grant(ipriv, cpub)), ("laneq-proof", _proof(wrong_priv)))
    handler = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, md))
    await _assert_denied(handler)


@pytest.mark.asyncio
async def test_enforce_grant_without_cnf_denies():
    ipriv, ipub = _pem_pair()
    cpriv, _ = _pem_pair()
    md = (("laneq-grant", _grant(ipriv, None, cnf=None)), ("laneq-proof", _proof(cpriv)))
    handler = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, md))
    await _assert_denied(handler)


@pytest.mark.asyncio
async def test_enforce_malformed_cnf_denies():
    ipriv, ipub = _pem_pair()
    cpriv, _ = _pem_pair()
    md = (("laneq-grant", _grant(ipriv, None, cnf={"kid": "c1"})), ("laneq-proof", _proof(cpriv)))  # no key
    handler = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, md))
    await _assert_denied(handler)


@pytest.mark.asyncio
async def test_enforce_replayed_proof_denies_second_use():
    ipriv, ipub = _pem_pair()
    cpriv, cpub = _pem_pair()
    interceptor = _interceptor(ipub, "enforce")
    md = (("laneq-grant", _grant(ipriv, cpub)), ("laneq-proof", _proof(cpriv, nonce="same")))
    first = await interceptor.intercept_service(_continuation, _Details(METHOD, md))
    assert first == "PASSTHROUGH"
    second = await interceptor.intercept_service(_continuation, _Details(METHOD, md))
    await _assert_denied(second)


@pytest.mark.asyncio
async def test_log_only_allows_invalid_grant():
    _, ipub = _pem_pair()
    result = await _interceptor(ipub, "log-only").intercept_service(_continuation, _Details(METHOD, ()))
    assert result == "PASSTHROUGH"  # verified + logged, but allowed


@pytest.mark.asyncio
async def test_off_bypasses_verification():
    _, ipub = _pem_pair()
    result = await _interceptor(ipub, "off").intercept_service(_continuation, _Details(METHOD, ()))
    assert result == "PASSTHROUGH"


@pytest.mark.asyncio
async def test_enforce_bad_cnf_key_material_denies():
    ipriv, ipub = _pem_pair()
    cpriv, _ = _pem_pair()
    md = (
        ("laneq-grant", _grant(ipriv, None, cnf={"kid": "c1", "key": "not-a-valid-pem"})),
        ("laneq-proof", _proof(cpriv)),
    )
    handler = await _interceptor(ipub, "enforce").intercept_service(_continuation, _Details(METHOD, md))
    await _assert_denied(handler)


def test_invalid_mode_raises():
    _, ipub = _pem_pair()
    with pytest.raises(ValueError):
        GrantAuthInterceptor(public_keys=[_verify_key(ipub)], audience=AUD, mode="bogus")


def test_build_interceptor_from_env_off_returns_none():
    assert build_interceptor_from_env({"LANEQ_AUTH_MODE": "off"}) is None
    assert build_interceptor_from_env({}) is None  # default is off (auth disabled until configured)


def test_build_interceptor_from_env_enforce(tmp_path):
    _, ipub = _pem_pair()
    pem = tmp_path / "issuer.pem"
    pem.write_bytes(ipub)
    interceptor = build_interceptor_from_env(
        {
            "LANEQ_AUTH_MODE": "enforce",
            "LANEQ_AUTH_AUDIENCE": AUD,
            "LANEQ_AUTH_PUBKEY_PATHS": str(pem),
        }
    )
    assert isinstance(interceptor, GrantAuthInterceptor)


def test_build_interceptor_requires_audience():
    with pytest.raises(ValueError):
        build_interceptor_from_env({"LANEQ_AUTH_MODE": "enforce"})


def test_build_interceptor_requires_pubkeys():
    with pytest.raises(ValueError):
        build_interceptor_from_env({"LANEQ_AUTH_MODE": "enforce", "LANEQ_AUTH_AUDIENCE": AUD})
