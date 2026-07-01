"""gRPC server interceptor enforcing laneq PASETO grant + per-request proof auth.

Wires the transport-agnostic verifiers in ``laneq.auth`` onto the gRPC request path.
Modes: ``off`` (bypass), ``log-only`` (verify + log failures, but allow — for safe
rollout), ``enforce`` (reject invalid calls with UNAUTHENTICATED).
"""

import hashlib
import logging
import os
from datetime import datetime, timezone

import grpc
import pyseto

from laneq.auth import (
    GrantError,
    ReplayCache,
    public_key_from_cnf,
    verify_grant,
    verify_proof,
)

GRANT_METADATA_KEY = "laneq-grant"
PROOF_METADATA_KEY = "laneq-proof"
MODES = ("off", "log-only", "enforce")


class GrantAuthInterceptor(grpc.aio.ServerInterceptor):
    """Verify a sender-constrained grant + per-request proof on every RPC."""

    def __init__(self, *, public_keys, audience, mode="enforce", skew_seconds=30, clock=None, logger=None):
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
        self._public_keys = public_keys
        self._audience = audience
        self._mode = mode
        self._skew = skew_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._log = logger or logging.getLogger("laneq.auth")
        self._replay = ReplayCache(ttl_seconds=max(60, skew_seconds * 2))

    async def intercept_service(self, continuation, handler_call_details):
        if self._mode == "off":
            return await continuation(handler_call_details)
        method = handler_call_details.method
        metadata = dict(handler_call_details.invocation_metadata or ())
        handler = await continuation(handler_call_details)
        if handler is None:
            return None
        if handler.unary_unary is None:
            message = "auth interceptor only supports unary-unary RPCs"
            self._log.warning("laneq auth rejected method=%s: %s", method, message)
            if self._mode == "enforce":
                return _deny(message)
            return handler

        async def authenticated_unary_unary(request, context):
            try:
                request_sha256 = _request_sha256(request)
                self._authenticate(metadata, method, request_sha256)
            except GrantError as exc:
                self._log.warning("laneq auth rejected method=%s: %s", method, exc)
                if self._mode == "enforce":
                    await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
            return await handler.unary_unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            authenticated_unary_unary,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )

    def _authenticate(self, metadata, method, request_sha256):
        grant_token = metadata.get(GRANT_METADATA_KEY)
        proof_token = metadata.get(PROOF_METADATA_KEY)
        if not grant_token or not proof_token:
            raise GrantError("missing grant or proof metadata")
        now = self._clock()
        claims = verify_grant(grant_token, public_keys=self._public_keys, audience=self._audience, now=now)
        client_key = public_key_from_cnf(claims.get("cnf"))
        verify_proof(
            proof_token,
            client_key=client_key,
            audience=self._audience,
            method=method,
            request_sha256=request_sha256,
            now=now,
            skew_seconds=self._skew,
            replay_cache=self._replay,
        )


def _deny(details):
    async def abort(_request, context):
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, details)

    return grpc.unary_unary_rpc_method_handler(abort)


def _request_sha256(request):
    try:
        payload = request.SerializeToString(deterministic=True)
    except Exception as exc:
        raise GrantError("request cannot be serialized for proof binding") from exc
    return hashlib.sha256(payload).hexdigest()


def build_interceptor_from_env(env=None):
    """Construct a GrantAuthInterceptor from environment config, or None when disabled.

    Env vars:
      LANEQ_AUTH_MODE         off (default) | log-only | enforce. ``off``/unset → None
                              (no interceptor installed; laneq serves as before).
      LANEQ_AUTH_AUDIENCE     the ``aud`` this server expects (required unless off).
      LANEQ_AUTH_PUBKEY_PATHS os.pathsep-separated PEM files of trusted issuer public
                              keys (>= 1 required unless off).
      LANEQ_AUTH_SKEW_SECONDS proof freshness window (default 30).
    """
    env = env if env is not None else os.environ
    mode = env.get("LANEQ_AUTH_MODE", "off")
    if mode == "off":
        return None
    audience = env.get("LANEQ_AUTH_AUDIENCE")
    if not audience:
        raise ValueError("LANEQ_AUTH_AUDIENCE is required when LANEQ_AUTH_MODE != off")
    public_keys = []
    for path in env.get("LANEQ_AUTH_PUBKEY_PATHS", "").split(os.pathsep):
        path = path.strip()
        if not path:
            continue
        with open(path, "rb") as handle:
            public_keys.append(pyseto.Key.new(version=4, purpose="public", key=handle.read()))
    if not public_keys:
        raise ValueError("LANEQ_AUTH_PUBKEY_PATHS must list at least one issuer public key")
    skew = int(env.get("LANEQ_AUTH_SKEW_SECONDS", "30"))
    return GrantAuthInterceptor(public_keys=public_keys, audience=audience, mode=mode, skew_seconds=skew)
