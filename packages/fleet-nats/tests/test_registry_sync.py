"""Strict sync-confirmation tests (terminal finding F3).

``registry_sync.sync`` must treat ONLY a clean ``HTTP 200`` whose body echoes
the stored catalog (subjects+role_lanes round-trip; the version is CLIENT-owned,
so the drift check ignores it) as success. A 3xx redirect, a 204 No-Content, a
200 with an unparseable body, or a 200 whose stored catalog drifted from what
was sent are all failures with a distinct non-zero exit code — never a false
"seeded" success.

Also (#155): the SEED payload version must be ``>= 1``. The code catalog's
version is ``0`` (the fallback/bootstrap sentinel), which the endpoint validator
rejects (``version < 1`` → HTTP 400 → the table never seeds → F1 #103 falls back
forever). ``registry_sync`` clamps the seed version UP to ``1`` so the PUT is
acceptable; the ``0`` sentinel is deliberately NOT changed at the source.
"""

from __future__ import annotations

import json

import httpx
import pytest

from fleet_nats.registry import code_catalog
from fleet_nats.registry_sync import _seed_version, sync

_BASE = "https://k-board.example"


def _stored_body(*, version: int = 5) -> str:
    """The server's echo of the seed catalog — the round-trippable success body."""
    cat = code_catalog()
    return json.dumps(
        {
            "version": version,  # client-owned; drift check ignores it
            "subjects": dict(cat.subjects),
            "role_lanes": {role: list(lanes) for role, lanes in cat.role_lanes.items()},
        }
    )


def _captured_put(monkeypatch: pytest.MonkeyPatch, response: httpx.Response) -> dict[str, object]:
    """Patch ``httpx.put`` to capture the PUT json payload, returning it live."""
    captured: dict[str, object] = {}

    def fake_put(url: str, **kwargs: object) -> httpx.Response:
        captured["json"] = kwargs.get("json")
        captured["headers"] = kwargs.get("headers")
        return response

    monkeypatch.setenv("FLEET_REGISTRY_TOKEN", "test-token")
    monkeypatch.setattr("fleet_nats.registry_sync.httpx.put", fake_put)
    return captured


def _patch_put(monkeypatch: pytest.MonkeyPatch, response: httpx.Response) -> None:
    def fake_put(url: str, **_kwargs: object) -> httpx.Response:
        return response

    monkeypatch.setenv("FLEET_REGISTRY_TOKEN", "test-token")
    monkeypatch.setattr("fleet_nats.registry_sync.httpx.put", fake_put)


def test_sync_success_requires_200_and_matching_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_put(monkeypatch, httpx.Response(200, text=_stored_body(version=9)))
    assert sync(_BASE) == 0


def test_seed_version_clamps_up_to_one() -> None:
    """The seed-version helper never emits below 1 (#155).

    The code catalog's version is 0 (the fallback sentinel), which the endpoint
    validator rejects as < 1. ``_seed_version`` clamps up to 1; an already-valid
    version passes through unchanged.
    """
    assert _seed_version(0) == 1  # the code-catalog case — the bug this fixes
    assert _seed_version(1) == 1
    assert _seed_version(7) == 7  # a future versioned catalog is untouched
    assert _seed_version(-3) == 1  # defensive: never emit a negative


def test_seed_payload_version_is_at_least_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PUT payload the endpoint would receive carries version >= 1 (#155).

    This is the regression guard for the seed bug: ``code_catalog().version == 0``
    used to be sent verbatim, so the endpoint's ``version < 1`` validator returned
    HTTP 400, the table stayed empty, GET 404'd, and F1 (#103) fell back forever.
    The seed now sends version 1 — a payload the validator ACCEPTS.
    """
    captured = _captured_put(monkeypatch, httpx.Response(200, text=_stored_body(version=1)))
    assert sync(_BASE) == 0
    payload = captured["json"]
    assert isinstance(payload, dict)
    sent_version = payload["version"]
    assert isinstance(sent_version, int)
    # The endpoint validator's accept rule is exactly this: version >= 1.
    assert sent_version >= 1, f"seed must send version >= 1, sent {sent_version!r}"
    assert sent_version == 1, "first seed of the version-0 code catalog serves version 1"
    # Regression: the pre-#155 code sent the code catalog's raw version (0), which
    # the validator rejected. Prove we are no longer sending that.
    assert code_catalog().version == 0, "the code-catalog fallback sentinel must STAY 0"
    assert sent_version != code_catalog().version, (
        "the seed must NOT send the code catalog's version-0 sentinel verbatim (#155)"
    )
    # The catalog CONTENT still round-trips unchanged — only the version is clamped.
    assert payload["subjects"] == dict(code_catalog().subjects)


def test_sync_redirect_is_not_success(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 3xx is NOT a confirmed store — httpx.put does not auto-follow here.
    _patch_put(monkeypatch, httpx.Response(302, headers={"location": "/elsewhere"}))
    assert sync(_BASE) == 4


def test_sync_204_no_content_is_not_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_put(monkeypatch, httpx.Response(204))
    assert sync(_BASE) == 4


def test_sync_200_with_unparseable_body_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_put(monkeypatch, httpx.Response(200, text="not json at all"))
    assert sync(_BASE) == 5


def test_sync_200_with_non_catalog_body_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # Valid JSON, but not a catalog (missing subjects/role_lanes/version).
    _patch_put(monkeypatch, httpx.Response(200, text=json.dumps({"ok": True})))
    assert sync(_BASE) == 5


def test_sync_200_with_drifted_catalog_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # 200 + a valid catalog, but subjects do NOT match what was sent → drift.
    drifted = json.dumps(
        {
            "version": 3,
            "subjects": {"agent.inbox": "{env}.WRONG.inbox.{agent}"},
            "role_lanes": {"worker": ["inbox"], "manager": ["inbox"]},
        }
    )
    _patch_put(monkeypatch, httpx.Response(200, text=drifted))
    assert sync(_BASE) == 6


def test_sync_404_endpoint_not_deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_put(monkeypatch, httpx.Response(404))
    assert sync(_BASE) == 3


def test_sync_transport_error_is_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(url: str, **_kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setenv("FLEET_REGISTRY_TOKEN", "test-token")
    monkeypatch.setattr("fleet_nats.registry_sync.httpx.put", boom)
    assert sync(_BASE) == 2


def test_sync_failure_log_sanitizes_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """F3 log lines route the URL through the F5 sanitizer — a token in the base
    URL must not leak into a sync failure log."""
    from loguru import logger

    token = "s3cr3t-token"  # noqa: S105 — test fixture
    _patch_put(monkeypatch, httpx.Response(500))

    records: list[str] = []
    sink_id = logger.add(
        lambda m: records.append(m.record["message"]),
        level="DEBUG",
        filter="fleet_nats.registry_sync",
    )
    try:
        rc = sync(f"https://user:{token}@k-board.example/x?t={token}")
    finally:
        logger.remove(sink_id)
    assert rc == 4
    assert token not in "\n".join(records), records


class _FakeKBoard:
    """A stateful stand-in for the k-board registry endpoint (#155).

    Models the ACTUAL server contract instead of hardcoding a 200: the PUT
    validator rejects ``version < 1`` with HTTP 400 (this is exactly the check
    that made the seed fail), stores an accepted catalog, and GET echoes what was
    stored. A round-trip through this fake proves the seed the client now sends is
    ACCEPTED and readable — the same shape a real endpoint enforces — rather than
    a mock that unconditionally returns 200.
    """

    def __init__(self) -> None:
        self.stored: dict[str, object] | None = None

    def _validate(self, payload: object) -> str | None:
        """The endpoint's payload validator; returns an error string or ``None``."""
        if not isinstance(payload, dict):
            return "payload must be an object"
        version = payload.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            return "version must be an integer >= 1"  # the exact 400 the seed hit
        if not isinstance(payload.get("subjects"), dict) or not payload["subjects"]:
            return "subjects must be a non-empty object"
        if not isinstance(payload.get("role_lanes"), dict) or not payload["role_lanes"]:
            return "role_lanes must be a non-empty object"
        return None

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            payload = json.loads(request.content or b"null")
            err = self._validate(payload)
            if err is not None:
                return httpx.Response(400, json={"error": err})
            self.stored = payload
            return httpx.Response(200, json=payload)  # echo the stored catalog
        if request.method == "GET":
            if self.stored is None:
                return httpx.Response(404, json={"error": "not seeded"})
            return httpx.Response(200, json=self.stored)
        return httpx.Response(405)


def test_seed_round_trips_through_a_version_validating_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: seed PUT is accepted by the real ``version >= 1`` validator,
    then GET returns the stored catalog → F1 (#103) would exit fallback (#155).

    The fake enforces the endpoint's actual 400-on-``version<1`` rule, so this
    proves the fix against the true contract, not a rigged 200.
    """
    import asyncio
    from collections.abc import Mapping

    from fleet_nats.registry import SubjectCatalog, fetch_catalog

    board = _FakeKBoard()
    transport = httpx.MockTransport(board.handler)

    # Route registry_sync's ``httpx.put`` through the stateful fake endpoint.
    # registry_sync always passes ``params={"tenant": ...}`` (a str->str mapping)
    # and a JSON dict, so narrow the captured kwargs to the types httpx accepts.
    def routed_put(url: str, **kwargs: object) -> httpx.Response:
        params = kwargs.get("params")
        assert params is None or isinstance(params, Mapping)
        typed_params: Mapping[str, str] | None = params
        with httpx.Client(transport=transport) as client:
            return client.put(url, params=typed_params, json=kwargs.get("json"))

    monkeypatch.setenv("FLEET_REGISTRY_TOKEN", "test-token")
    monkeypatch.setattr("fleet_nats.registry_sync.httpx.put", routed_put)

    # 1. The seed PUT is ACCEPTED (200) — no more 400 (#155).
    assert sync(_BASE) == 0
    assert board.stored is not None, "the catalog must be stored after a successful seed"
    assert isinstance(board.stored["version"], int)
    assert board.stored["version"] >= 1

    # 2. A subsequent GET returns the stored catalog → F1 exits fallback.
    async def _fetch() -> SubjectCatalog:
        async with httpx.AsyncClient(transport=transport) as client:
            return await fetch_catalog(_BASE, client=client)

    fetched = asyncio.run(_fetch())
    assert fetched.version >= 1
    assert dict(fetched.subjects) == dict(code_catalog().subjects)
    assert dict(fetched.role_lanes) == dict(code_catalog().role_lanes)


def test_version_zero_payload_would_be_rejected_by_the_validator() -> None:
    """Regression proof (#155): the PRE-fix payload (code catalog's version 0)
    is exactly what the endpoint validator rejects with a 400.

    Exercises the SAME validator the round-trip test uses against the old payload
    shape, locking in WHY the seed used to fail — so a future change that reverts
    to sending version 0 fails loudly here.
    """
    board = _FakeKBoard()
    cat = code_catalog()
    old_payload = {  # what registry_sync sent before #155 — version 0 verbatim
        "version": cat.version,
        "subjects": dict(cat.subjects),
        "role_lanes": {role: list(lanes) for role, lanes in cat.role_lanes.items()},
    }
    assert cat.version == 0
    assert board._validate(old_payload) is not None, "version-0 seed MUST be rejected"
    # The fixed payload (version clamped to 1) passes the same validator.
    new_payload = {**old_payload, "version": _seed_version(cat.version)}
    assert board._validate(new_payload) is None, "the clamped version-1 seed MUST pass"


def test_sync_refuses_without_token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PUT is authenticated (k-board write auth): no token, no request, exit 7."""

    def fail_put(url: str, **_kwargs: object) -> httpx.Response:
        raise AssertionError("no request may be sent without a bearer token")

    monkeypatch.delenv("FLEET_REGISTRY_TOKEN", raising=False)
    monkeypatch.setattr("fleet_nats.registry_sync.httpx.put", fail_put)
    assert sync(_BASE) == 7


def test_sync_sends_bearer_header(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _captured_put(monkeypatch, httpx.Response(200, text=_stored_body(version=1)))
    assert sync(_BASE) == 0
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "Bearer test-token"


def test_sync_rejected_bearer_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    for status in (401, 403):
        _patch_put(monkeypatch, httpx.Response(status))
        assert sync(_BASE) == 8
