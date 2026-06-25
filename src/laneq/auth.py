"""PASETO v4.public grant verification for laneq host-to-host auth.

A grant is a PASETO v4.public (Ed25519) token minted by the trust root (issuer).
laneq verifies the signature with the issuer's public key(s) and checks the
audience/expiry claims. See the agent-sandbox design doc
2026-06-24-laneq-grant-paseto-design.md for the full token contract.

This module is transport-agnostic (no gRPC): the gRPC interceptor wraps it.
"""

import json

import pyseto


class GrantError(Exception):
    """Raised when a PASETO grant fails verification (fail-closed)."""


def verify_grant(token, *, public_keys, audience, now):
    """Verify a PASETO v4.public grant.

    Args:
        token: the PASETO token (str or bytes).
        public_keys: a non-empty sequence of trusted pyseto v4.public Keys. Each is
            tried in turn, so current+next keys can be trusted during rotation.
        audience: the expected ``aud`` claim (this laneq instance).
        now: a timezone-aware ``datetime`` used for ``exp``/``nbf`` checks (injected
            for determinism).

    Returns:
        The validated claims as a dict.

    Raises:
        GrantError: on any verification failure — bad/forged signature, untrusted
            key, malformed payload, expired, not-yet-valid, or audience mismatch.
            The verifier fails closed: any unexpected error becomes a GrantError.
    """
    try:
        decoded = pyseto.decode(public_keys, token)
    except Exception as exc:  # fail closed on any signature/format error
        raise GrantError(f"grant signature verification failed: {exc}") from exc

    try:
        claims = json.loads(decoded.payload)
    except (ValueError, TypeError) as exc:
        raise GrantError("grant payload is not valid JSON") from exc
    if not isinstance(claims, dict):
        raise GrantError("grant payload is not a claims object")

    now_ts = now.timestamp()

    exp = claims.get("exp")
    if exp is None:
        raise GrantError("grant is missing the exp claim")
    if now_ts >= exp:
        raise GrantError("grant has expired")

    nbf = claims.get("nbf")
    if nbf is not None and now_ts < nbf:
        raise GrantError("grant is not yet valid (nbf)")

    aud = claims.get("aud")
    if aud != audience:
        raise GrantError(f"grant audience mismatch: {aud!r} != {audience!r}")

    return claims


class ReplayCache:
    """Bounded TTL cache of seen proof nonces (anti-replay).

    Sized to the proof freshness window: a nonce is remembered for ``ttl_seconds``
    (which should be >= the verifier's skew window) and then pruned, so the cache
    cannot grow without bound.
    """

    def __init__(self, ttl_seconds=60):
        self._ttl = ttl_seconds
        self._seen = {}  # nonce -> expiry unix timestamp

    def check_and_add(self, nonce, now):
        """Return True if the nonce is fresh (and record it); False if already seen."""
        now_ts = now.timestamp()
        self._seen = {n: exp for n, exp in self._seen.items() if exp > now_ts}
        if nonce in self._seen:
            return False
        self._seen[nonce] = now_ts + self._ttl
        return True


def verify_proof(proof, *, client_key, audience, method, now, skew_seconds=30, replay_cache=None):
    """Verify a per-request proof signed by the client's ``cnf`` key (anti-replay).

    The proof binds a request to the client keypair the grant was issued for, and to
    a single method/target/time/nonce so a captured grant or proof cannot be replayed.

    Raises:
        GrantError: bad/forged proof signature, wrong audience or method, timestamp
            outside the skew window, missing nonce, or a replayed nonce. Fails closed.
    """
    try:
        decoded = pyseto.decode([client_key], proof)
    except Exception as exc:  # fail closed on any signature/format error
        raise GrantError(f"proof signature verification failed: {exc}") from exc

    try:
        claims = json.loads(decoded.payload)
    except (ValueError, TypeError) as exc:
        raise GrantError("proof payload is not valid JSON") from exc
    if not isinstance(claims, dict):
        raise GrantError("proof payload is not a claims object")

    if claims.get("aud") != audience:
        raise GrantError("proof audience mismatch")
    if claims.get("method") != method:
        raise GrantError("proof method mismatch")

    iat = claims.get("iat")
    if iat is None or abs(now.timestamp() - iat) > skew_seconds:
        raise GrantError("proof timestamp outside skew window")

    nonce = claims.get("nonce")
    if not nonce:
        raise GrantError("proof is missing a nonce")
    if replay_cache is not None and not replay_cache.check_and_add(nonce, now):
        raise GrantError("proof nonce replay detected")

    return claims
