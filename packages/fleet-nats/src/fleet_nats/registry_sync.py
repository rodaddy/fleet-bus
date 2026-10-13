"""Seed/sync the k-board ``nats_subjects`` registry from the code catalog.

The sync direction encodes the authority seam (the k-board registry endpoint): while the
table is authoritative, this tool is how the CODE registry is exported INTO it
(seed + reconcile after a ``subjects.py`` change lands). When authority flips
to code-canonical, this same export keeps the table current as a mirror — the
tool's direction never changes; only who is *believed* does.

Usage (from ``packages/fleet-nats``)::

    .venv/bin/python -m fleet_nats.registry_sync [--base-url https://kboard.example.internal]

Exits non-zero with a clear message while the endpoint is not deployed —
contract-first: the payload shape is the k-board registry endpoint API.
"""

from __future__ import annotations

import argparse
import os
import sys

import httpx
from loguru import logger

from fleet_nats.registry import (
    _API_PATH,
    _TENANT,
    _parse_catalog,
    _safe_registry_url,
    code_catalog,
    diff_catalogs,
)
from fleet_nats.tracing import traced

_DEFAULT_BASE_URL = "https://kboard.example.internal"
_ENV_TOKEN = "FLEET_REGISTRY_TOKEN"


def _seed_version(catalog_version: int) -> int:
    """The version to PUT — a valid stored version, never below ``1``.

    The endpoint contract (the k-board registry endpoint) is CLIENT-OWNED: the client SENDS the
    version verbatim, the server stores it and ``409``s only on a stale write —
    it does NOT auto-bump. So the seed must carry a version the endpoint accepts,
    and its validator rejects ``version < 1`` (a stored catalog is always ``>= 1``;
    ``0`` is the CODE catalog's "unset / fallback / bootstrap" sentinel, never a
    stored value — see :func:`fleet_nats.registry.code_catalog`).

    The code catalog's version is ``0``, so seeding it verbatim used to 400 and
    leave the table empty forever (#155). Clamp UP to ``1``: the first real stored
    catalog is version ``1``. The ``0`` sentinel is deliberately NOT changed at the
    source — flipping ``code_catalog().version`` to ``1`` would make an unreachable-
    registry FALLBACK indistinguishable from a genuine served ``v1`` and silently
    satisfy a ``min_version=1`` floor with the fallback, defeating the #103
    refuse-to-start-on-outage guarantee. The seed is a distinct event; only IT
    owns a stored version. A future catalog already carrying a version ``>= 1``
    passes through unchanged.
    """
    return max(catalog_version, 1)


@traced
def sync(base_url: str, *, timeout_s: float = 10.0) -> int:
    """PUT the code catalog to the registry endpoint. Returns an exit code.

    Success (exit ``0``) requires a clean ``HTTP 200`` AND a response body that
    parses as a catalog whose subjects+role_lanes round-trip the seed. The version
    is CLIENT-owned (the endpoint stores what it is sent and ``409``s on a stale
    write — it does NOT auto-bump), so the stored version echoes the seed's; the
    drift check compares subjects+role_lanes only, not the version. Anything else
    fails loud with a message stating expected vs received:

    - ``2`` transport unreachable · ``3`` endpoint not deployed (404)
    - ``4`` not a 200 (a 3xx redirect or 204 No-Content is NOT a confirmed store)
    - ``5`` 200 but body did not validate as a catalog
    - ``6`` 200 but the stored catalog drifted from what was sent
    - ``7`` ``FLEET_REGISTRY_TOKEN`` unset · ``8`` bearer rejected (401/403)
    """
    catalog = code_catalog()
    payload = {
        # Version is CLIENT-owned: the endpoint stores what we send and 409s only
        # on a stale write — it never auto-bumps. The code catalog's version is 0
        # (the "unset / fallback" sentinel), which the endpoint validator rejects
        # (< 1), so clamp UP to a valid stored version >= 1 for the seed (#155).
        "version": _seed_version(catalog.version),
        "subjects": dict(catalog.subjects),
        "role_lanes": {role: list(lanes) for role, lanes in catalog.role_lanes.items()},
    }
    url = f"{base_url.rstrip('/')}{_API_PATH}"
    safe_url = _safe_registry_url(url)
    # The endpoint's PUT requires actor authentication (k-board write auth): a
    # bearer token plus a tenant-scoped registry-writer grant. The token is a
    # secret — read from the environment, sent only as a header, never logged.
    token = os.environ.get(_ENV_TOKEN, "")
    if not token:
        logger.error(
            "fleet_nats.registry_sync | {} not set — the registry PUT is authenticated "
            "(k-board write auth); export the registry-writer bearer token before seeding",
            _ENV_TOKEN,
        )
        return 7
    try:
        resp = httpx.put(
            url,
            params={"tenant": _TENANT},
            json=payload,
            timeout=timeout_s,
            headers={"Authorization": f"Bearer {token}"},
        )
    except httpx.HTTPError as exc:
        logger.error("fleet_nats.registry_sync | {} unreachable: {}", safe_url, exc)
        return 2
    if resp.status_code in (401, 403):
        logger.error(
            "fleet_nats.registry_sync | PUT {} rejected with HTTP {}: the {} bearer is "
            "missing a valid actor token or the tenant registry-writer grant",
            safe_url,
            resp.status_code,
            _ENV_TOKEN,
        )
        return 8
    if resp.status_code == 404:
        logger.error("fleet_nats.registry_sync | registry endpoint not deployed yet: {}", safe_url)
        return 3
    # Fail loud on ANYTHING that is not a clean 200 with a body we can trust. A
    # 3xx redirect, a 204 No-Content, or a 2xx with a mismatched/empty body all
    # mean "we cannot confirm the registry stored the catalog" — treat as failure
    # rather than logging a false success.
    if resp.status_code != 200:
        logger.error(
            "fleet_nats.registry_sync | PUT {} not confirmed: expected HTTP 200, received HTTP {}",
            safe_url,
            resp.status_code,
        )
        return 4
    # Body must parse as a catalog and match what we PUT (the server echoes the
    # stored catalog). Version is CLIENT-owned — the endpoint stores what we send
    # and 409s only on a stale write, it never auto-bumps — so the stored version
    # echoes the seed's; the drift check compares subjects+role_lanes only, which
    # MUST round-trip.
    try:
        stored = _parse_catalog(resp.json())
    except (ValueError, TypeError) as exc:
        logger.error(
            "fleet_nats.registry_sync | PUT {} returned HTTP 200 but the body did not "
            "validate as a catalog: expected the stored catalog, received unparseable "
            "response ({})",
            safe_url,
            exc,
        )
        return 5
    drift = diff_catalogs(catalog, stored)
    if drift:
        logger.error(
            "fleet_nats.registry_sync | PUT {} returned HTTP 200 but the stored catalog "
            "does NOT match what was sent: expected the seed catalog, received drift on {}",
            safe_url,
            ", ".join(drift),
        )
        return 6
    logger.info(
        "fleet_nats.registry_sync | seeded {} subjects + {} roles to {} (stored version={})",
        len(catalog.subjects),
        len(catalog.role_lanes),
        safe_url,
        stored.version,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Seed the k-board nats_subjects registry")
    parser.add_argument("--base-url", default=_DEFAULT_BASE_URL)
    args = parser.parse_args(argv)
    return sync(args.base_url)


if __name__ == "__main__":
    sys.exit(main())
