"""Functional contract tests for the shared durable kill authority (#107)."""

from __future__ import annotations

import json
from typing import cast

import pytest

from fleet_nats import FleetKV, KVEntry, StaleRevisionError
from fleet_nats.killflag import FleetKVKillAuthority, KillAuthorityConfig, KillAuthorityError


class _KV:
    def __init__(self) -> None:
        self.value: bytes | None = None
        self.revision = 0

    async def get(self, key: str) -> KVEntry | None:
        if self.value is None:
            return None
        return KVEntry(key=key, value=self.value, revision=self.revision)

    async def create(self, key: str, value: bytes) -> int:
        if self.value is not None:
            raise StaleRevisionError("bucket", key, 0)
        self.value = value
        self.revision = 1
        return self.revision

    async def update(self, key: str, value: bytes, expected_revision: int) -> int:
        if expected_revision != self.revision:
            raise StaleRevisionError("bucket", key, expected_revision)
        self.value = value
        self.revision += 1
        return self.revision


def _authority(kv: _KV) -> FleetKVKillAuthority:
    return FleetKVKillAuthority(
        cast(FleetKV, kv), KillAuthorityConfig(bucket="fleet-kill-authority", key="fleet")
    )


async def test_one_authority_key_is_shared_by_operator_and_enforcement() -> None:
    kv = _KV()
    operator = _authority(kv)
    enforcement = _authority(kv)
    assert await operator.initialize_released() is False
    assert await enforcement.armed() is False
    assert await operator.set_armed(True) is True
    assert await enforcement.armed() is True
    assert await operator.set_armed(False) is False
    assert await enforcement.armed() is False


async def test_malformed_store_value_is_not_a_false_release() -> None:
    kv = _KV()
    kv.value = json.dumps({"armed": "yes"}).encode()
    kv.revision = 1
    with pytest.raises(KillAuthorityError, match="exactly"):
        await _authority(kv).armed()


async def test_missing_key_is_uncertainty_not_a_false_release() -> None:
    with pytest.raises(KillAuthorityError, match="missing"):
        await _authority(_KV()).armed()


async def test_initialize_refuses_to_clear_an_existing_authority() -> None:
    kv = _KV()
    authority = _authority(kv)
    await authority.initialize_released()
    await authority.set_armed(True)
    with pytest.raises(KillAuthorityError, match="already exists"):
        await authority.initialize_released()


async def test_cas_conflict_is_reconciled_without_blind_overwrite() -> None:
    class _RacingKV(_KV):
        def __init__(self) -> None:
            super().__init__()
            self.raced = False

        async def update(self, key: str, value: bytes, expected_revision: int) -> int:
            if not self.raced:
                self.raced = True
                self.value = b'{"armed":false}'
                self.revision += 1
                raise StaleRevisionError("bucket", key, expected_revision)
            return await super().update(key, value, expected_revision)

    kv = _RacingKV()
    await _authority(kv).initialize_released()
    assert await _authority(kv).set_armed(True) is True
    assert await _authority(kv).armed() is True
