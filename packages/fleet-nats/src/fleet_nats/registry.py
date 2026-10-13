"""The subject registry — k-board's ``nats_subjects`` table, authoritative for now.

The fleet decision (2026-07-08, the k-board registry endpoint): while subjects are being
locked down, the catalog of subject TEMPLATES and the role→lanes map are
registered in k-board Postgres, and that table is **authoritative**. Clients
learn the catalog ONCE at connect (never on the hot path) and cache it; any
failure to reach the registry falls back LOUDLY to the baked-in code defaults
below — a missing registry can never take an agent offline (fail-open by
design; the bus must beat the board here).

The authority seam is deliberately thin: :func:`load_catalog` returns whichever
side is authoritative. Flipping to code-canonical later (the table becomes a
mirror) changes only :data:`AUTHORITY` and the sync tool's direction
(:mod:`fleet_nats.registry_sync`) — no client changes.

The templates here are the documentation-of-record twin of the
:mod:`fleet_nats.subjects` builders; ``tests/test_registry.py`` locks the two
together so they can never drift within the repo.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx
from loguru import logger

from fleet_nats.config import check_enforcement_config, min_catalog_version
from fleet_nats.tracing import traced

# Which side wins when both are readable. "kboard" = the table is the source of
# truth (current fleet decision); "code" = this module is (the later flip).
AUTHORITY = "kboard"

_ENV_URL = "FLEET_SUBJECT_REGISTRY_URL"
_API_PATH = "/api/nats-subjects"
_TENANT = "pg"
_DEFAULT_TIMEOUT_S = 5.0


def _safe_registry_url(url: str | None) -> str:
    """Return the registry URL safe to log: no userinfo, no query, no fragment.

    A registry URL may carry credentials in userinfo (``https://user:pass@host``)
    and secrets in the query string (``?token=...``); every failure-path log line
    and diagnostic echoes it. Reduce it to ``scheme://host[:port]/path`` — enough
    to tell an operator WHICH endpoint failed without leaking a credential.

    ``None`` or empty (the URL-unset case) renders as ``"unset"``. A string with
    no scheme delimiter is returned unchanged (nothing safe to slice).

    Args:
        url: The registry base URL, possibly with userinfo/query/fragment.

    Returns:
        ``scheme://host[:port]/path`` with userinfo, query, and fragment stripped.
    """
    if not url:
        return "unset"
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        # Not a URL we can safely decompose — but never echo it raw (it may hold
        # a bare ``token@`` or ``?token=`` we can't parse); fail closed.
        return "***unparseable-registry-url***"
    # ``hostname``/``port`` drop any ``user:pass@`` userinfo from the authority.
    host = parts.hostname or ""
    netloc = f"{host}:{parts.port}" if parts.port is not None else host
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _safe_exc(exc: BaseException) -> str:
    """Render a fetch exception safe to log — class name (+ HTTP status), never str.

    ``str(exc)`` on an httpx error embeds the FULL requested URL: an
    ``HTTPStatusError`` from ``raise_for_status()`` renders the request URL
    (``?tenant=...`` and any userinfo/query token in the base URL), and transport
    errors (``ConnectError``/``ReadTimeout``) attach the request URL too. So the
    URL is sanitized separately in the log line, but the exception itself must
    NEVER be interpolated raw (re-check residual 5). Reduce it to the exception
    CLASS plus, for a status error, the HTTP status code — enough to diagnose
    WHAT failed without echoing WHERE (the URL) a second, unscrubbed time.

    Args:
        exc: The exception raised by :func:`fetch_catalog`.

    Returns:
        ``ClassName`` or ``ClassName status=NNN`` — no URL, no free-form message.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status, int):
        return f"{type(exc).__name__} status={status}"
    return type(exc).__name__


# The code-canonical subject templates — one entry per lane the fleet uses,
# keyed by a stable dotted name. Placeholders are the slugged tokens the
# matching subjects.py builder applies. This is the seed payload for the
# k-board table and the fallback catalog when the registry is unreachable.
CODE_SUBJECTS: Mapping[str, str] = {
    "agent.ack": "{env}.agent.ack.{task_id}",
    "agent.ack.channel": "{env}.agent.{channel}.ack.{task_id}",
    "agent.ack.wildcard": "{env}.agent.*.ack.{task_id}",
    "agent.arbitrate": "{env}.agent.arbitrate.{topic}",
    "agent.claim": "{env}.agent.claim.{task_id}",
    "agent.dead_letter": "{env}.agent.dead-letter",
    "agent.dispatch": "{env}.agent.dispatch.{worker}",
    "agent.dispatch.channel": "{env}.agent.{channel}.dispatch.{worker}",
    "agent.dispatch.wildcard": "{env}.agent.*.dispatch.{worker}",
    "agent.heartbeat": "{env}.agent.heartbeat.{agent}",
    "agent.heartbeat.wildcard": "{env}.agent.heartbeat.*",
    "agent.inbox": "{env}.agent.inbox.{agent}",
    "agent.relay": "{env}.agent.relay.{to}",
    "agent.relay.channel": "{env}.agent.{channel}.relay.{to}",
    "agent.relay.wildcard": "{env}.agent.*.relay.{to}",
    "agent.status": "{env}.agent.status.{task_id}",
    "agent.status.channel": "{env}.agent.{channel}.status.{task_id}",
    "agent.status.wildcard": "{env}.agent.*.status.{task_id}",
    "agent.telemetry": "{env}.agent.telemetry.{task_id}",
    "agent.telemetry.channel": "{env}.agent.{channel}.telemetry.{task_id}",
    "agent.telemetry.wildcard": "{env}.agent.*.telemetry.{task_id}",
    "agent.token_cost": "{env}.agent.token_cost.{agent}",
    "agent.token_cost.wildcard": "{env}.agent.token_cost.*",
    "agent.wake": "{env}.agent.wake.{worker}",
    "agent.wake.channel": "{env}.agent.{channel}.wake.{worker}",
    "agent.wake.wildcard": "{env}.agent.*.wake.{worker}",
    "direct": "{env}.direct.{agent}",
    "factory.runner": "{env}.factory.runner.{job_id}",
    "factory.runner.wildcard": "{env}.factory.runner.*",
    "hub.control": "{env}.hub.control.{task_id}",
    "hub.control.wildcard": "{env}.hub.control.*",
    "hub.show_work": "{env}.hub.show-work",
    "kboard.event": "{env}.kboard.task.{event}.{task_id}",
    "layer.wildcard": "{env}.{layer}.>",
    "ob.context_pack": "{env}.ob.memory.context_pack",
}

# The canonical role→lanes map (issue #8). fleet_subscriber.role derives its
# lane sets from here so the registry and the running subscriber agree.
#
# This map is the SUBSCRIPTION registry: one entry per role that subscribes, and
# the registered twin of the k-board ``nats_role_lanes`` table. A publish-only
# principal has NO entry here — it subscribes to nothing, so there is nothing to
# register (see CODE_PUBLISH_ONLY_ROLES). The registry's own contract for such a
# principal is its subject TEMPLATE in CODE_SUBJECTS, not a lane row.
#
# The noise lanes (``ack``/``telemetry``, #116) are deliberately ABSENT here.
# They are publish-only FROM the agent: a worker emits an ack and its progress
# telemetry, and nothing in the fleet subscribes them for coordination. Adding
# them to a role's lane set would make every agent attach to its own noise — the
# exact per-turn cost the lanes exist to eliminate. Their registered contract is
# the CODE_SUBJECTS template plus the traffic-matrix row, not a lane assignment.
CODE_ROLE_LANES: Mapping[str, tuple[str, ...]] = {
    "manager": ("inbox",),
    "worker": ("dispatch", "relay", "wake", "inbox"),
}

# Roles that MUST NOT subscribe to anything (issue #222).
#
# Deliberately NOT an empty tuple in CODE_ROLE_LANES. Two reasons, and the
# second is the load-bearing one:
#
# 1. An empty tuple is indistinguishable from "this role has no lanes wired
#    YET" — a reader cannot tell a deliberate zero from an unfinished one.
# 2. CODE_ROLE_LANES is the registered twin of k-board's ``nats_role_lanes``
#    table. An entry here with no row there IS drift (``diff_catalogs`` compares
#    role keys by presence), and the k-board API rejects an empty lane list
#    outright ("lanes for <role> must be a non-empty unique list") because for a
#    SUBSCRIBING role an empty list is a misconfiguration that silently deafens
#    an agent. Publish-only is a code-side fact about a principal, not a
#    subscription to register.
#
# HONEST SCOPE — this is a client-side invariant, NOT a security boundary. It
# stops OUR subscriber from attaching lanes for this role; it cannot stop a
# process that holds a bus connection from calling `nc.subscribe()` directly. A
# runner box runs a coding agent with `bypassPermissions` over untrusted repo
# content, so it is exactly the principal that could ignore this. Real
# confinement is the server-side deny on the scoped credential
# (``deploy/nats/fleet-prod-nats-server.conf`` §4h, ``subscribe { deny = [">"] }``);
# this set is its code-side twin, never a replacement.
#
# NO REGISTERED TWIN, therefore NO DRIFT GATE. Unlike CODE_ROLE_LANES this set
# is code-only and deliberately so: ``AUTHORITY = "kboard"`` means anything in
# the catalog is overridable by the table, and "this principal must never
# subscribe" is exactly the fact a registry row must not be able to flip. The
# cost is that nothing surfaces a disagreement ahead of time — if k-board later
# grows a lane row for a publish-only role, the adapter refuses at connect
# (fail-loud, ``adapter.py``) rather than a drift gate catching it first.
CODE_PUBLISH_ONLY_ROLES: frozenset[str] = frozenset({"runner"})


@dataclass(frozen=True)
class SubjectCatalog:
    """One loaded subject catalog.

    Attributes:
        subjects: Template map, keyed by dotted lane name.
        role_lanes: Role name → the lane names that role subscribes.
        source: ``"kboard"`` (fetched) or ``"code"`` (baked-in defaults).
        version: The registry's version counter (``0`` for the code catalog).
    """

    subjects: Mapping[str, str]
    role_lanes: Mapping[str, tuple[str, ...]]
    source: str
    version: int


def code_catalog() -> SubjectCatalog:
    """The baked-in defaults — always available, never a network call."""
    return SubjectCatalog(
        subjects=dict(CODE_SUBJECTS),
        role_lanes=dict(CODE_ROLE_LANES),
        source="code",
        version=0,
    )


@dataclass(frozen=True)
class LoadedCatalog:
    """A loaded catalog plus WHY we hold this one — the diagnostic seam (#103).

    ``load_catalog`` returns whichever catalog is authoritative, but a
    version-``0`` code catalog can arrive for two very different reasons, and an
    operator staring at a refuse-to-start MUST be able to tell them apart:

    - **A genuine below-floor served version** — the registry answered and its
      version is stale. The client is behind; update it (or lower the floor).
    - **The registry was UNREACHABLE** — a configured registry URL that failed
      (connection error, 404-not-deployed, malformed payload), so we fell back
      to the version-0 code catalog. The client may already satisfy the floor;
      k-board is down. Do NOT treat this as a stale client — a k-board blip
      during a floored rolling restart lands here.

    Attributes:
        catalog: The loaded catalog (authoritative fetch OR a code fallback).
        registry_unreachable: ``True`` iff a registry URL was configured but the
            fetch failed, so ``catalog`` is the code fallback under an outage —
            NOT a served version. ``False`` for a successful fetch AND for the
            no-registry-configured bootstrap posture (an unset URL is a
            deliberate code-catalog run, not an outage).
    """

    catalog: SubjectCatalog
    registry_unreachable: bool


def _parse_catalog(payload: object) -> SubjectCatalog:
    """Validate a registry response strictly — a malformed registry is an error,
    not a catalog (the caller decides whether to fall back)."""
    if not isinstance(payload, dict):
        raise ValueError(f"registry payload must be an object, got {type(payload).__name__}")
    version = payload.get("version")
    subjects = payload.get("subjects")
    role_lanes = payload.get("role_lanes")
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError(f"registry version must be an int, got {version!r}")
    if not isinstance(subjects, dict) or not subjects:
        raise ValueError("registry subjects must be a non-empty object")
    if not isinstance(role_lanes, dict) or not role_lanes:
        raise ValueError("registry role_lanes must be a non-empty object")
    parsed_subjects: dict[str, str] = {}
    for key, template in subjects.items():
        if not isinstance(key, str) or not isinstance(template, str) or not template:
            raise ValueError(f"registry subject entry invalid: {key!r} -> {template!r}")
        parsed_subjects[key] = template
    parsed_lanes: dict[str, tuple[str, ...]] = {}
    for role, lanes in role_lanes.items():
        if (
            not isinstance(role, str)
            or not isinstance(lanes, list)
            or not all(isinstance(lane, str) and lane for lane in lanes)
        ):
            raise ValueError(f"registry role_lanes entry invalid: {role!r} -> {lanes!r}")
        parsed_lanes[role] = tuple(lanes)
    return SubjectCatalog(
        subjects=parsed_subjects, role_lanes=parsed_lanes, source="kboard", version=version
    )


@traced
async def fetch_catalog(
    base_url: str,
    *,
    timeout_s: float = _DEFAULT_TIMEOUT_S,
    client: httpx.AsyncClient | None = None,
) -> SubjectCatalog:
    """Fetch the registry catalog from k-board (the k-board registry endpoint contract).

    Args:
        base_url: The k-board base URL (no trailing path).
        timeout_s: Request timeout — applied to the one-shot client only. An
            injected ``client`` owns its own timeout config; this parameter is
            ignored on that path.
        client: Injected client (tests); when ``None`` a one-shot client is used.

    Returns:
        The fetched, strictly-validated catalog (``source="kboard"``).

    Raises:
        httpx.HTTPError: On any transport/status failure.
        ValueError: On a malformed payload.
    """
    url = f"{base_url.rstrip('/')}{_API_PATH}"
    if client is not None:
        resp = await client.get(url, params={"tenant": _TENANT})
        resp.raise_for_status()
        return _parse_catalog(resp.json())
    async with httpx.AsyncClient(timeout=timeout_s) as one_shot:
        resp = await one_shot.get(url, params={"tenant": _TENANT})
        resp.raise_for_status()
        return _parse_catalog(resp.json())


def diff_catalogs(a: SubjectCatalog, b: SubjectCatalog) -> tuple[str, ...]:
    """The keys whose templates/lanes differ between two catalogs (drift)."""
    keys = set(a.subjects) | set(b.subjects)
    drifted = [key for key in sorted(keys) if a.subjects.get(key) != b.subjects.get(key)]
    roles = set(a.role_lanes) | set(b.role_lanes)
    drifted += [
        f"role:{role}" for role in sorted(roles) if a.role_lanes.get(role) != b.role_lanes.get(role)
    ]
    return tuple(drifted)


@traced
async def load_catalog_detailed(*, client: httpx.AsyncClient | None = None) -> LoadedCatalog:
    """Load the active catalog ONCE, reporting WHETHER the registry was reachable.

    Same load logic as :func:`load_catalog` (reads ``FLEET_SUBJECT_REGISTRY_URL``,
    fetches, falls open loudly), but wraps the result in :class:`LoadedCatalog`
    so the caller can distinguish a code-catalog fallback caused by a registry
    OUTAGE from a genuine served version (#103). Never raises.

    Cases:

    - **URL unset** → code catalog; ``registry_unreachable=False`` (a deliberate
      no-registry bootstrap run, not an outage).
    - **URL set, fetch failed** (connection error / 404 / malformed) → code
      catalog; ``registry_unreachable=True`` (we could not verify anything).
    - **URL set, fetch OK** → the fetched catalog; ``registry_unreachable=False``.
    """
    base_url = os.environ.get(_ENV_URL)
    if not base_url:
        logger.debug("fleet_nats.registry | {} unset — code catalog", _ENV_URL)
        return LoadedCatalog(catalog=code_catalog(), registry_unreachable=False)
    try:
        fetched = await fetch_catalog(base_url, client=client)
    except Exception as exc:  # noqa: BLE001 — the fallback IS the contract; log loud, stay up
        # Sanitize BOTH the URL and the exception: an HTTPStatusError's str embeds
        # the full requested URL (query token included), so render the exception
        # via _safe_exc (class + status), never raw (re-check residual 5).
        logger.error(
            "fleet_nats.registry | fetch failed url={} ({}) — FALLING BACK to the "
            "baked-in code catalog (registry UNREACHABLE)",
            _safe_registry_url(base_url),
            _safe_exc(exc),
        )
        return LoadedCatalog(catalog=code_catalog(), registry_unreachable=True)
    drift = diff_catalogs(fetched, code_catalog())
    if drift:
        logger.error(
            "fleet_nats.registry | drift vs code defaults (registry is authoritative): {}",
            ", ".join(drift),
        )
    logger.info(
        "fleet_nats.registry | loaded catalog source=kboard version={} subjects={} drift={}",
        fetched.version,
        len(fetched.subjects),
        len(drift),
    )
    return LoadedCatalog(catalog=fetched, registry_unreachable=False)


@traced
async def load_catalog(*, client: httpx.AsyncClient | None = None) -> SubjectCatalog:
    """Load the active catalog ONCE (call at connect; cache the result).

    Reads ``FLEET_SUBJECT_REGISTRY_URL``:

    - **Unset** → the code catalog (registry opt-in; logged at DEBUG).
    - **Set** → fetch from k-board (authoritative for now). ANY failure —
      unreachable, non-200, malformed — logs an ERROR and falls back to the
      code catalog: fail-open, never fail-closed on Postgres.
    - On a successful fetch, drift against the code catalog is logged loudly
      (the registry wins; the drift log is the signal to reconcile
      ``subjects.py`` or the table).

    Never raises. Callers that need to know whether a code-catalog result came
    from an OUTAGE (vs a deliberate no-registry run or a served version) call
    :func:`load_catalog_detailed` instead — this thin wrapper drops that flag.
    """
    loaded = await load_catalog_detailed(client=client)
    return loaded.catalog


class CatalogVersionTooLowError(RuntimeError):
    """A participant's effective catalog version is below the configured floor.

    Raised at connect (:func:`connect_catalog`) when
    ``FLEET_SUBJECT_CATALOG_MIN_VERSION`` is set to a positive floor and the
    catalog the client actually loaded — whether the authoritative k-board
    catalog OR a code-catalog fallback (version ``0``) — sits below it. This is
    the refuse-to-start signal: a below-floor client would speak an obsolete
    taxonomy and split the fleet, so it must NOT come up.

    Attributes:
        have_version: The version the client actually has.
        min_version: The configured minimum floor.
        source: The catalog source (``"kboard"`` or ``"code"``) — a ``"code"``
            source below the floor means the client fell back and the fallback
            itself failed the floor (registry unreachable during enforcement).
    """

    def __init__(self, *, have_version: int, min_version: int, source: str) -> None:
        self.have_version = have_version
        self.min_version = min_version
        self.source = source
        super().__init__(
            f"catalog version too low: have={have_version} (source={source}) < "
            f"min={min_version} — refusing to start (set FLEET_SUBJECT_CATALOG_MIN_VERSION "
            f"lower, or update this client). A below-floor client would speak an obsolete "
            f"subject taxonomy and split the fleet. This is a STALE CLIENT, not a registry "
            f"outage — the registry answered and its version is below the floor."
        )


class RegistryUnreachableError(CatalogVersionTooLowError):
    """A floor is set but the registry was UNREACHABLE — floor is unverifiable.

    The distinct-and-loud half of the bootstrap trap fix (#103 P1-1). When a
    positive floor is configured and the client fell back to the version-``0``
    code catalog *because a configured registry URL failed* (connection error,
    404-not-deployed, malformed payload), we cannot tell whether this client
    actually satisfies the floor — k-board is down, the served version is
    unknown. Refuse to start (the safe default: a truly-below-floor client must
    not come up), but with a DISTINCT diagnostic so an operator can tell "k-board
    is down, my client may be fine" from "my client is genuinely stale"
    (:class:`CatalogVersionTooLowError`).

    Subclasses :class:`CatalogVersionTooLowError` so existing refuse-to-start
    handling and ``except CatalogVersionTooLowError`` callers are unchanged; only
    the message and type differ so the two failure modes are distinguishable.

    Residual (documented, not silently fixed): there is NO last-known-good
    on-disk cache, so a k-board blip during a floored rolling restart still
    refuses to start — but now it says so plainly ("registry unreachable, cannot
    verify catalog floor") instead of masquerading as a stale-client error. An
    operator restores k-board (or drops the floor) rather than chasing a
    phantom client-version mismatch.
    """

    def __init__(self, *, min_version: int, registry_url: str | None) -> None:
        self.registry_url = registry_url
        # Bypass CatalogVersionTooLowError.__init__ (its message asserts "the
        # registry answered", which is the opposite of this case) — set the same
        # attributes directly, then a distinct message.
        self.have_version = 0
        self.min_version = min_version
        self.source = "code"
        RuntimeError.__init__(
            self,
            f"registry unreachable, cannot verify catalog floor: a floor of "
            f"min={min_version} is set but the registry ({_safe_registry_url(registry_url)}) "
            f"could not be reached, so the client fell back to the version-0 code catalog — "
            f"refusing to start. This is a REGISTRY OUTAGE (k-board down / endpoint not "
            f"deployed), NOT necessarily a stale client: restore the registry, or drop "
            f"FLEET_SUBJECT_CATALOG_MIN_VERSION until it is back. There is no on-disk "
            f"last-known-good cache, so a floored client cannot start while k-board is down.",
        )


@traced
def enforce_min_version(
    catalog: SubjectCatalog,
    *,
    min_version: int,
    registry_unreachable: bool = False,
) -> None:
    """Refuse to proceed when ``catalog.version`` is below ``min_version``.

    ``min_version == 0`` disables enforcement (the bootstrap posture before the
    registry is deployed — a code-catalog fleet at version 0 must still start).
    A positive floor is fail-loud: a below-floor catalog raises rather than
    letting the client silently run an obsolete taxonomy.

    The two below-floor failure modes are made DISTINCT (#103 P1-1): a registry
    OUTAGE (``registry_unreachable=True``, the code fallback under a k-board
    blip) raises :class:`RegistryUnreachableError`; a genuinely stale served
    version raises :class:`CatalogVersionTooLowError`. Both still refuse to
    start (safe default), but the operator can tell them apart from the log/type.

    Args:
        catalog: The loaded catalog (from :func:`load_catalog`).
        min_version: The configured floor (``0`` = off).
        registry_unreachable: Whether the ``catalog`` is a code fallback caused
            by a registry outage (from :func:`load_catalog_detailed`).

    Raises:
        RegistryUnreachableError: When ``0 < min_version`` and the below-floor
            catalog is a code fallback under a registry outage.
        CatalogVersionTooLowError: When ``0 < min_version`` and the registry
            served a below-floor version.
    """
    if min_version <= 0:
        logger.debug(
            "fleet_nats.registry | version floor disabled (min=0) — accepting "
            "catalog source={} version={}",
            catalog.source,
            catalog.version,
        )
        return
    if catalog.version < min_version:
        if registry_unreachable:
            registry_url = os.environ.get(_ENV_URL)
            logger.error(
                "fleet_nats.registry | REFUSING TO START: registry UNREACHABLE (url={}) so "
                "the catalog floor {} cannot be verified — fell back to code catalog "
                "(version 0). This is a k-board outage, NOT necessarily a stale client; "
                "restore the registry or drop the floor",
                _safe_registry_url(registry_url),
                min_version,
            )
            raise RegistryUnreachableError(min_version=min_version, registry_url=registry_url)
        logger.error(
            "fleet_nats.registry | REFUSING TO START: catalog version {} (source={}) "
            "is below the configured floor {} — a below-floor client speaks an obsolete "
            "taxonomy and splits the fleet (registry answered; this is a STALE CLIENT)",
            catalog.version,
            catalog.source,
            min_version,
        )
        raise CatalogVersionTooLowError(
            have_version=catalog.version, min_version=min_version, source=catalog.source
        )
    logger.info(
        "fleet_nats.registry | catalog version {} (source={}) meets floor {}",
        catalog.version,
        catalog.source,
        min_version,
    )


@traced
async def connect_catalog(*, client: httpx.AsyncClient | None = None) -> SubjectCatalog:
    """Load the catalog AND enforce the configured version floor — the connect path.

    This is what a participant calls at connect (once; cache the result). It is
    :func:`load_catalog` plus the :func:`enforce_min_version` guard driven by
    ``FLEET_SUBJECT_CATALOG_MIN_VERSION`` (:func:`~fleet_nats.config.min_catalog_version`):

    - **No floor configured** (``min == 0``) → returns the loaded catalog,
        preserving today's fail-open behavior (a registry outage falls back to
        the code catalog and the client stays up). This is the bootstrap default.
    - **A floor is configured** (``min > 0``) → the loaded catalog MUST meet it.
        A registry outage falls back to the code catalog (version ``0``), which
        is below any positive floor, so the client REFUSES TO START — fail-loud,
        the enforced default whenever a minimum is set. Falling back is thus a
        loud, explicit, logged decision, never a silent downgrade. The two
        below-floor failure modes are DISTINCT (#103 P1-1): a registry outage
        raises :class:`RegistryUnreachableError` ("registry unreachable, cannot
        verify catalog floor"), a genuinely stale served version raises
        :class:`CatalogVersionTooLowError` — so an operator can tell "k-board is
        down" from "my client is behind".

    Args:
        client: Injected client (tests); ``None`` uses a one-shot client.

    Returns:
        The loaded catalog, once it satisfies the configured floor.

    Raises:
        ValueError: When a positive floor is configured but the registry URL is
            unset (the bootstrap-trap misconfiguration — caught up front, naming
            both env vars, instead of a misleading refuse-to-start).
        RegistryUnreachableError: When a positive floor is configured and the
            below-floor catalog is a code fallback under a registry outage.
        CatalogVersionTooLowError: When a positive floor is configured and the
            registry served a below-floor version.
    """
    min_version = min_catalog_version()
    # Fail loud up front on the misconfigured combination (#103 bootstrap trap):
    # a positive floor with no registry URL can only ever refuse to start while
    # masquerading as a stale client. Also warns (WARNING) when BOTH are unset —
    # the baked-in code catalog is in use and enforcement is off.
    check_enforcement_config(
        min_version=min_version, registry_url_set=bool(os.environ.get(_ENV_URL))
    )
    loaded = await load_catalog_detailed(client=client)
    catalog = loaded.catalog
    logger.debug(
        "fleet_nats.registry | connect_catalog source={} version={} min={} unreachable={}",
        catalog.source,
        catalog.version,
        min_version,
        loaded.registry_unreachable,
    )
    enforce_min_version(
        catalog, min_version=min_version, registry_unreachable=loaded.registry_unreachable
    )
    return catalog
