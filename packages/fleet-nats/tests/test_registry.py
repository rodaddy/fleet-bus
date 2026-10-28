"""Subject-registry tests (the k-board registry endpoint): code catalog, fetch, loud fallback."""

from __future__ import annotations

import json

import httpx
import pytest

from fleet_nats import (
    CatalogVersionTooLowError,
    RegistryUnreachableError,
    SubjectCatalog,
    code_catalog,
    connect_catalog,
    diff_catalogs,
    enforce_min_version,
    fetch_catalog,
    load_catalog,
    load_catalog_detailed,
)
from fleet_nats.registry import _parse_catalog
from fleet_nats.subjects import (
    agent_ack,
    agent_ack_wildcard,
    agent_arbitrate,
    agent_claim,
    agent_dispatch,
    agent_dispatch_wildcard,
    agent_heartbeat,
    agent_heartbeat_wildcard,
    agent_inbox,
    agent_relay,
    agent_relay_wildcard,
    agent_status,
    agent_status_wildcard,
    agent_telemetry,
    agent_telemetry_wildcard,
    agent_wake,
    agent_wake_wildcard,
    dead_letter,
    direct,
    factory_runner,
    factory_runner_wildcard,
    hub_control,
    hub_control_wildcard,
    hub_show_work,
    kboard_event,
    ob_context_pack,
    slug,
    token_cost,
    token_cost_wildcard,
    wildcard,
)

_ENV_URL = "FLEET_SUBJECT_REGISTRY_URL"
_MIN_ENV = "FLEET_SUBJECT_CATALOG_MIN_VERSION"


def _catalog(version: int, source: str = "kboard") -> SubjectCatalog:
    return SubjectCatalog(
        subjects={"agent.inbox": "{env}.agent.inbox.{agent}"},
        role_lanes={"worker": ("inbox",)},
        source=source,
        version=version,
    )


def test_registry_code_catalog_templates_match_the_builders() -> None:
    """The catalog is the documentation-of-record twin of subjects.py — every
    template, rendered with slugged tokens, must equal its builder's output."""
    cat = code_catalog().subjects
    agent, worker, task, topic, chan = "Wren Bot", "finch", "t 1", "deploy", "dash-dev"
    job = "Job 42"
    expected = {
        "agent.ack": (agent_ack("dev", task), {"task_id": slug(task)}),
        "agent.ack.channel": (
            agent_ack("dev", task, channel=chan),
            {"task_id": slug(task), "channel": slug(chan)},
        ),
        "agent.ack.wildcard": (agent_ack_wildcard("dev", task), {"task_id": slug(task)}),
        "agent.arbitrate": (agent_arbitrate("dev", topic), {"topic": slug(topic)}),
        "agent.claim": (agent_claim("dev", task), {"task_id": slug(task)}),
        "agent.dead_letter": (dead_letter("dev"), {}),
        "agent.dispatch": (agent_dispatch("dev", worker), {"worker": slug(worker)}),
        "agent.dispatch.channel": (
            agent_dispatch("dev", worker, channel=chan),
            {"worker": slug(worker), "channel": slug(chan)},
        ),
        "agent.dispatch.wildcard": (
            agent_dispatch_wildcard("dev", worker),
            {"worker": slug(worker)},
        ),
        "agent.heartbeat": (agent_heartbeat("dev", agent), {"agent": slug(agent)}),
        "agent.heartbeat.wildcard": (agent_heartbeat_wildcard("dev"), {}),
        "agent.inbox": (agent_inbox("dev", agent), {"agent": slug(agent)}),
        "agent.relay": (agent_relay("dev", agent), {"to": slug(agent)}),
        "agent.relay.channel": (
            agent_relay("dev", agent, channel=chan),
            {"to": slug(agent), "channel": slug(chan)},
        ),
        "agent.relay.wildcard": (agent_relay_wildcard("dev", agent), {"to": slug(agent)}),
        "agent.status": (agent_status("dev", task), {"task_id": slug(task)}),
        "agent.status.channel": (
            agent_status("dev", task, channel=chan),
            {"task_id": slug(task), "channel": slug(chan)},
        ),
        "agent.status.wildcard": (agent_status_wildcard("dev", task), {"task_id": slug(task)}),
        "agent.telemetry": (agent_telemetry("dev", task), {"task_id": slug(task)}),
        "agent.telemetry.channel": (
            agent_telemetry("dev", task, channel=chan),
            {"task_id": slug(task), "channel": slug(chan)},
        ),
        "agent.telemetry.wildcard": (
            agent_telemetry_wildcard("dev", task),
            {"task_id": slug(task)},
        ),
        "agent.token_cost": (token_cost("dev", agent), {"agent": slug(agent)}),
        "agent.token_cost.wildcard": (token_cost_wildcard("dev"), {}),
        "agent.wake": (agent_wake("dev", worker), {"worker": slug(worker)}),
        "agent.wake.channel": (
            agent_wake("dev", worker, channel=chan),
            {"worker": slug(worker), "channel": slug(chan)},
        ),
        "agent.wake.wildcard": (agent_wake_wildcard("dev", worker), {"worker": slug(worker)}),
        "direct": (direct("dev", agent), {"agent": slug(agent)}),
        "factory.runner": (factory_runner("dev", job), {"job_id": slug(job)}),
        "factory.runner.wildcard": (factory_runner_wildcard("dev"), {}),
        "hub.control": (hub_control("dev", task), {"task_id": slug(task)}),
        "hub.control.wildcard": (hub_control_wildcard("dev"), {}),
        "hub.show_work": (hub_show_work("dev"), {}),
        "kboard.event": (
            kboard_event("dev", "claimed", task),
            {"event": "claimed", "task_id": slug(task)},
        ),
        "layer.wildcard": (wildcard("dev", "agent"), {"layer": "agent"}),
        "ob.context_pack": (ob_context_pack("dev"), {}),
    }
    assert set(cat) == set(expected)  # every builder registered, nothing extra
    for key, (built, tokens) in expected.items():
        assert cat[key].format(env="dev", **tokens) == built, key


def test_registry_parse_is_strict() -> None:
    good = {
        "version": 1,
        "subjects": {"agent.inbox": "{env}.agent.inbox.{agent}"},
        "role_lanes": {"worker": ["inbox"]},
    }
    parsed = _parse_catalog(good)
    assert parsed.source == "kboard"
    assert parsed.version == 1
    assert parsed.role_lanes == {"worker": ("inbox",)}

    with pytest.raises(ValueError, match="must be an object"):
        _parse_catalog([good])
    with pytest.raises(ValueError, match="version must be an int"):
        _parse_catalog({**good, "version": True})  # bool-as-int rejected
    with pytest.raises(ValueError, match="version must be an int"):
        _parse_catalog({**good, "version": "1"})
    with pytest.raises(ValueError, match="subjects must be a non-empty"):
        _parse_catalog({**good, "subjects": {}})
    with pytest.raises(ValueError, match="subject entry invalid"):
        _parse_catalog({**good, "subjects": {"agent.inbox": ""}})
    with pytest.raises(ValueError, match="role_lanes entry invalid"):
        _parse_catalog({**good, "role_lanes": {"worker": "inbox"}})


def _mock_client(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler)


async def test_registry_fetch_parses_the_contract() -> None:
    payload = {
        "version": 7,
        "subjects": {"agent.inbox": "{env}.agent.inbox.{agent}"},
        "role_lanes": {"worker": ["inbox"], "manager": ["inbox"]},
    }

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/nats-subjects"
        assert request.url.params["tenant"] == "pg"
        return httpx.Response(200, text=json.dumps(payload))

    async with _mock_client(httpx.MockTransport(handle)) as client:
        cat = await fetch_catalog("https://k-board.example", client=client)
    assert isinstance(cat, SubjectCatalog)
    assert (cat.source, cat.version) == ("kboard", 7)


async def test_registry_load_without_env_uses_code_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_ENV_URL, raising=False)
    cat = await load_catalog()
    assert cat.source == "code"
    assert cat.subjects == dict(code_catalog().subjects)


async def test_registry_load_falls_back_loudly_when_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _mock_client(httpx.MockTransport(refuse)) as client:
        cat = await load_catalog(client=client)
    assert cat.source == "code"  # fail-open: NEVER fail closed on Postgres


async def test_registry_load_falls_back_on_404_and_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(404, text="{}"))
    ) as client:
        assert (await load_catalog(client=client)).source == "code"

    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text='{"version": 1}'))
    ) as client:
        assert (await load_catalog(client=client)).source == "code"


async def test_registry_load_prefers_the_kboard_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The table is authoritative FOR NOW: a fetched catalog wins over code,
    # and the drift is reported (verified via diff below).
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    payload = {
        "version": 3,
        "subjects": {**dict(code_catalog().subjects), "agent.inbox": "{env}.agent.mail.{agent}"},
        "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
    }
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
    ) as client:
        cat = await load_catalog(client=client)
    assert cat.source == "kboard"
    assert cat.subjects["agent.inbox"] == "{env}.agent.mail.{agent}"
    assert diff_catalogs(cat, code_catalog()) == ("agent.inbox",)


async def test_registry_fallback_and_drift_log_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The loud-log contract IS the reconcile signal — assert the emissions,
    # not just the return values (a silent downgrade to DEBUG must fail here).
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: records.append((message.record["level"].name, message.record["message"])),
        level="DEBUG",
        filter="fleet_nats.registry",
    )
    try:
        monkeypatch.setenv(_ENV_URL, "https://k-board.example")

        def refuse(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        async with _mock_client(httpx.MockTransport(refuse)) as client:
            await load_catalog(client=client)
        assert any(level == "ERROR" and "FALLING BACK" in message for level, message in records), (
            records
        )

        records.clear()
        payload = {
            "version": 3,
            "subjects": {
                **dict(code_catalog().subjects),
                "agent.inbox": "{env}.agent.mail.{agent}",
            },
            "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
        }
        async with _mock_client(
            httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
        ) as client:
            await load_catalog(client=client)
        assert any(
            level == "ERROR" and "drift" in message and "agent.inbox" in message
            for level, message in records
        ), records
    finally:
        logger.remove(sink_id)


# --- catalog-version enforcement (#103) -------------------------------------


def test_enforce_min_version_disabled_accepts_anything() -> None:
    """min=0 disables enforcement — the bootstrap posture (code catalog, v0)."""
    enforce_min_version(_catalog(0, source="code"), min_version=0)  # no raise


def test_enforce_min_version_accepts_at_or_above_floor() -> None:
    enforce_min_version(_catalog(3), min_version=3)  # exactly at floor
    enforce_min_version(_catalog(4), min_version=3)  # above floor


def test_enforce_min_version_refuses_below_floor() -> None:
    with pytest.raises(CatalogVersionTooLowError) as exc:
        enforce_min_version(_catalog(2), min_version=3)
    assert exc.value.have_version == 2
    assert exc.value.min_version == 3
    assert exc.value.source == "kboard"
    # The message names have-version vs min-version (fail-loud).
    assert "have=2" in str(exc.value) and "min=3" in str(exc.value)


def test_enforce_min_version_refuses_code_fallback_below_floor() -> None:
    """A version-0 code fallback under a positive floor is the refuse-to-start
    case a registry outage produces — it must raise, not silently degrade."""
    with pytest.raises(CatalogVersionTooLowError) as exc:
        enforce_min_version(_catalog(0, source="code"), min_version=1)
    assert exc.value.source == "code"


async def test_connect_catalog_no_floor_falls_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No floor configured → today's fail-open behavior is preserved."""
    monkeypatch.delenv(_MIN_ENV, raising=False)
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _mock_client(httpx.MockTransport(refuse)) as client:
        cat = await connect_catalog(client=client)
    assert cat.source == "code"  # fell open, stayed up


async def test_connect_catalog_floor_refuses_on_registry_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A floor is set AND the registry is down → code fallback (v0) fails the
    floor → refuse to start. This is the enforced default (#103 acceptance)."""
    monkeypatch.setenv(_MIN_ENV, "2")
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _mock_client(httpx.MockTransport(refuse)) as client:
        with pytest.raises(CatalogVersionTooLowError):
            await connect_catalog(client=client)


async def test_connect_catalog_floor_met_by_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry serves a version at/above the floor → client starts."""
    monkeypatch.setenv(_MIN_ENV, "3")
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    payload = {
        "version": 3,
        "subjects": dict(code_catalog().subjects),
        "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
    }
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
    ) as client:
        cat = await connect_catalog(client=client)
    assert (cat.source, cat.version) == ("kboard", 3)


async def test_connect_catalog_floor_refuses_below_registry_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry reachable but serving a stale version below the floor → refuse."""
    monkeypatch.setenv(_MIN_ENV, "5")
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    payload = {
        "version": 4,
        "subjects": dict(code_catalog().subjects),
        "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
    }
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
    ) as client:
        with pytest.raises(CatalogVersionTooLowError) as exc:
            await connect_catalog(client=client)
    assert exc.value.have_version == 4 and exc.value.min_version == 5


async def test_connect_catalog_floor_without_registry_url_is_a_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positive floor with NO registry URL is the bootstrap trap (#103): the
    client could only ever hold the version-0 code catalog, so it would refuse to
    start every time while masquerading as a stale client. connect_catalog() now
    rejects the combination UP FRONT (before any fetch) with a ValueError naming
    BOTH env vars — not the below-floor CatalogVersionTooLowError, which would
    mis-diagnose a misconfiguration as a genuinely-behind client."""
    monkeypatch.setenv(_MIN_ENV, "9")
    monkeypatch.delenv(_ENV_URL, raising=False)  # no registry → code catalog, v0
    with pytest.raises(ValueError) as exc:
        await connect_catalog()
    message = str(exc.value)
    assert _MIN_ENV in message and _ENV_URL in message
    # The config error is distinct from the below-floor served-version path.
    assert not isinstance(exc.value, CatalogVersionTooLowError)


# --- distinguish "registry unreachable" from "served below-floor" (#103 P1-1) ---


async def test_load_catalog_detailed_flags_unreachable_only_on_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The diagnostic seam: registry_unreachable is True ONLY when a configured
    URL fails — not for an unset URL (bootstrap) nor a successful fetch."""
    # URL unset → bootstrap code catalog, NOT an outage.
    monkeypatch.delenv(_ENV_URL, raising=False)
    unset = await load_catalog_detailed()
    assert unset.catalog.source == "code" and unset.registry_unreachable is False

    # URL set but the fetch fails → code fallback under an OUTAGE.
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _mock_client(httpx.MockTransport(refuse)) as client:
        outage = await load_catalog_detailed(client=client)
    assert outage.catalog.source == "code" and outage.registry_unreachable is True

    # A 404 (endpoint not deployed) is also an outage for floor purposes.
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(404, text="{}"))
    ) as client:
        not_deployed = await load_catalog_detailed(client=client)
    assert not_deployed.catalog.source == "code" and not_deployed.registry_unreachable is True

    # A successful fetch is reachable.
    payload = {
        "version": 4,
        "subjects": dict(code_catalog().subjects),
        "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
    }
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
    ) as client:
        served = await load_catalog_detailed(client=client)
    assert served.catalog.source == "kboard" and served.registry_unreachable is False


def test_enforce_min_version_unreachable_raises_distinct_error() -> None:
    """A below-floor CODE FALLBACK under a registry outage raises the DISTINCT
    RegistryUnreachableError (k-board down), not a plain stale-client error."""
    with pytest.raises(RegistryUnreachableError) as exc:
        enforce_min_version(_catalog(0, source="code"), min_version=3, registry_unreachable=True)
    msg = str(exc.value)
    assert "registry unreachable" in msg.lower()
    assert "cannot verify catalog floor" in msg.lower()
    assert exc.value.min_version == 3
    # Still a CatalogVersionTooLowError subtype (existing handlers still catch it).
    assert isinstance(exc.value, CatalogVersionTooLowError)


def test_enforce_min_version_served_below_floor_is_stale_client_not_outage() -> None:
    """A registry that ANSWERED with a below-floor version is a stale client:
    the plain CatalogVersionTooLowError, NOT the unreachable subtype."""
    with pytest.raises(CatalogVersionTooLowError) as exc:
        enforce_min_version(_catalog(2), min_version=3, registry_unreachable=False)
    assert not isinstance(exc.value, RegistryUnreachableError)
    assert "stale client" in str(exc.value).lower()


async def test_connect_catalog_outage_vs_stale_raise_distinct_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end at connect: an outage under a floor raises the unreachable
    subtype; a served-below-floor version raises the plain type. Distinct-and-loud
    so an operator can tell 'k-board is down' from 'my client is behind' (#103)."""
    monkeypatch.setenv(_MIN_ENV, "3")
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")

    # Outage → RegistryUnreachableError.
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _mock_client(httpx.MockTransport(refuse)) as client:
        with pytest.raises(RegistryUnreachableError):
            await connect_catalog(client=client)

    # Served below floor → plain CatalogVersionTooLowError (NOT the subtype).
    payload = {
        "version": 2,
        "subjects": dict(code_catalog().subjects),
        "role_lanes": {role: list(lanes) for role, lanes in code_catalog().role_lanes.items()},
    }
    async with _mock_client(
        httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(payload)))
    ) as client:
        with pytest.raises(CatalogVersionTooLowError) as exc:
            await connect_catalog(client=client)
    assert not isinstance(exc.value, RegistryUnreachableError)


async def test_connect_catalog_unset_url_below_floor_is_a_config_error_not_an_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URL unset + a floor = a deliberately-misconfigured client (the bootstrap
    trap, #103), NOT a registry outage. It is rejected up front as a config error
    (ValueError naming both env vars) — never a RegistryUnreachableError (k-board
    is not "down"; the operator simply never pointed the client at it) and never
    the below-floor CatalogVersionTooLowError (there is no served version to be
    below — the client could only ever hold the version-0 code catalog)."""
    monkeypatch.setenv(_MIN_ENV, "3")
    monkeypatch.delenv(_ENV_URL, raising=False)
    with pytest.raises(ValueError) as exc:
        await connect_catalog()
    assert _MIN_ENV in str(exc.value) and _ENV_URL in str(exc.value)
    assert not isinstance(exc.value, RegistryUnreachableError)
    assert not isinstance(exc.value, CatalogVersionTooLowError)
