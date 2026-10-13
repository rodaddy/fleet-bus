"""Live registry drift gate (fleet-board #15) — pass-or-skip off-network.

Skips while the k-board endpoint is unreachable OR not yet deployed (404);
once it serves the contract, any drift between the table (authoritative for
now) and the code catalog fails here — the reconcile signal.
"""

from __future__ import annotations

import httpx
import pytest

from fleet_nats import code_catalog, diff_catalogs, fetch_catalog

_BASE_URL = "https://kboard.example.internal"


async def test_registry_live_kboard_table_matches_the_code_catalog() -> None:
    try:
        catalog = await fetch_catalog(_BASE_URL, timeout_s=5.0)
    except (httpx.HTTPError, ValueError) as exc:
        pytest.skip(f"registry endpoint unavailable ({type(exc).__name__}) — drift gate skipped")
    drift = diff_catalogs(catalog, code_catalog())
    assert drift == (), f"registry drift (table is authoritative): {drift}"
