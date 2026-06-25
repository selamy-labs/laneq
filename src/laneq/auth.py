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
