"""Registry-URL sanitization for failure logs/diagnostics (terminal finding F5).

Unlike a NATS URL (``bus.redact_url`` — password only), a registry URL is an
HTTP(S) URL that may carry credentials in userinfo AND secrets in the query
string (``?token=...``). Every failure-path log line and diagnostic echoes it,
so it is reduced to ``scheme://host[:port]/path`` — enough to identify WHICH
endpoint failed, never enough to leak a credential.
"""

from __future__ import annotations

import httpx
import pytest

from fleet_nats.registry import (
    _ENV_URL,
    _safe_exc,
    _safe_registry_url,
    load_catalog_detailed,
)

_TOKEN = "s3cr3t-token"  # noqa: S105 — test fixture, not a real credential


# --------------------------------------------------------------------------- #
# _safe_registry_url unit tests
# --------------------------------------------------------------------------- #


def test_safe_registry_url_strips_query() -> None:
    out = _safe_registry_url(f"https://k-board.example/api?token={_TOKEN}")
    assert _TOKEN not in out
    assert out == "https://k-board.example/api"


def test_safe_registry_url_strips_userinfo() -> None:
    out = _safe_registry_url(f"https://user:{_TOKEN}@k-board.example:8443/api")
    assert _TOKEN not in out
    assert "user" not in out
    assert out == "https://k-board.example:8443/api"


def test_safe_registry_url_strips_fragment() -> None:
    out = _safe_registry_url(f"https://k-board.example/api#{_TOKEN}")
    assert _TOKEN not in out
    assert out == "https://k-board.example/api"


def test_safe_registry_url_plain_url_passthrough() -> None:
    assert _safe_registry_url("https://k-board.example/api") == "https://k-board.example/api"
    assert _safe_registry_url("http://192.0.2.15:8080/x") == "http://192.0.2.15:8080/x"


def test_safe_registry_url_none_and_empty_render_unset() -> None:
    assert _safe_registry_url(None) == "unset"
    assert _safe_registry_url("") == "unset"


def test_safe_registry_url_unparseable_fails_closed() -> None:
    # No scheme delimiter → cannot safely decompose; must NOT echo raw (may hold
    # a bare token@ we can't parse). Fail closed.
    out = _safe_registry_url(f"garbage-{_TOKEN}")
    assert _TOKEN not in out


# --------------------------------------------------------------------------- #
# _safe_exc unit tests — the exception itself must never carry the URL/token
# --------------------------------------------------------------------------- #


def test_safe_exc_status_error_renders_class_and_status_not_url() -> None:
    # raise_for_status()'s auto-generated message embeds the full requested URL
    # (query token and all); _safe_exc must reduce it to class name + status code,
    # never str(exc). Build the exception the real way — via raise_for_status —
    # so the message under test is the one production actually raises.
    url = f"https://k-board.example/api?token={_TOKEN}"
    request = httpx.Request("GET", url)
    response = httpx.Response(404, request=request)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        assert _TOKEN in str(exc)  # the raw exception DOES leak the token...
        out = _safe_exc(exc)
        assert _TOKEN not in out  # ...but _safe_exc does not.
        assert out == "HTTPStatusError status=404"
    else:  # pragma: no cover — 404 always raises
        raise AssertionError("raise_for_status did not raise on a 404")


def test_safe_exc_transport_error_renders_class_only() -> None:
    # A transport error has no response; _safe_exc renders the class name only —
    # never str(exc), which attaches the request URL on many httpx errors.
    exc = httpx.ConnectError(f"failed connecting to {_TOKEN}")
    out = _safe_exc(exc)
    assert _TOKEN not in out
    assert out == "ConnectError"


# --------------------------------------------------------------------------- #
# Failure-path log leak test — the finding's explicit requirement
# --------------------------------------------------------------------------- #


async def test_fetch_failure_log_never_leaks_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token-bearing registry URL that fails to fetch must NOT appear in any
    log line — the sanitized ``scheme://host/path`` is logged instead."""
    from loguru import logger

    url = f"https://user:{_TOKEN}@k-board.invalid:9/api?token={_TOKEN}"
    monkeypatch.setenv(_ENV_URL, url)

    records: list[str] = []
    sink_id = logger.add(
        lambda m: records.append(m.record["message"]),
        level="DEBUG",
        filter="fleet_nats.registry",
    )

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = httpx.AsyncClient(transport=httpx.MockTransport(refuse))
    try:
        loaded = await load_catalog_detailed(client=client)
    finally:
        await client.aclose()
        logger.remove(sink_id)

    assert loaded.registry_unreachable is True
    joined = "\n".join(records)
    assert _TOKEN not in joined, joined
    # The sanitized host/path DID make it to the log (operator can identify it).
    assert "k-board.invalid:9/api" in joined, joined


async def test_fetch_status_error_log_never_leaks_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 (HTTPStatusError from raise_for_status) on a token-bearing URL must
    NOT leak the token — the exception str embeds the full requested URL, so the
    log renders _safe_exc (class + status), never str(exc) (re-check residual 5)."""
    from loguru import logger

    url = f"https://user:{_TOKEN}@k-board.invalid:9/api?token={_TOKEN}"
    monkeypatch.setenv(_ENV_URL, url)

    records: list[str] = []
    sink_id = logger.add(
        lambda m: records.append(m.record["message"]),
        level="DEBUG",
        filter="fleet_nats.registry",
    )

    def not_found(request: httpx.Request) -> httpx.Response:
        # A real registry 404 (endpoint not deployed): raise_for_status() turns
        # this into an HTTPStatusError whose str carries the full request URL.
        return httpx.Response(404, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(not_found))
    try:
        loaded = await load_catalog_detailed(client=client)
    finally:
        await client.aclose()
        logger.remove(sink_id)

    assert loaded.registry_unreachable is True
    joined = "\n".join(records)
    # Neither the userinfo token nor the query token appears anywhere in the log.
    assert _TOKEN not in joined, joined
    # The status code (safe, useful) and the sanitized host/path DID make it.
    assert "status=404" in joined, joined
    assert "k-board.invalid:9/api" in joined, joined
