"""The one durable, fleet-wide kill authority (fleet-bus #107).

The kill flag is one JetStream KV value.  The operator, hub, and worker runtime
all use this module's bucket/key resolver and the same CAS-backed store shape;
there is deliberately no local-file, roster, or mirror fallback that could
narrow a fleet-wide stop to one host.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from fleet_nats.bus import FleetBus
from fleet_nats.jetstream import FleetKV, StaleRevisionError, open_kv

_ENV_BUCKET = "FLEET_KILL_SWITCH_BUCKET"
_ENV_KEY = "FLEET_KILL_SWITCH_KEY"
DEFAULT_BUCKET = "fleet-kill-authority"
DEFAULT_KEY = "fleet"
_MAX_CAS_ATTEMPTS = 3


class KillAuthorityError(RuntimeError):
    """The shared durable kill authority could not be read or written safely."""


@dataclass(frozen=True, slots=True)
class KillAuthorityConfig:
    """The sole durable location of the fleet-wide armed signal."""

    bucket: str
    key: str

    @classmethod
    def from_env(cls) -> KillAuthorityConfig:
        """Read the authority location, rejecting empty/malformed overrides."""
        bucket = _value_from_env(_ENV_BUCKET, DEFAULT_BUCKET)
        key = _value_from_env(_ENV_KEY, DEFAULT_KEY)
        if any(char.isspace() for char in key):
            raise ValueError(f"{_ENV_KEY} must not contain whitespace, got {key!r}")
        return cls(bucket=bucket, key=key)


class FleetKVKillAuthority:
    """Strict boolean kill authority over one existing :class:`FleetKV` key."""

    def __init__(self, kv: FleetKV, config: KillAuthorityConfig) -> None:
        self._kv = kv
        self._config = config

    async def armed(self) -> bool:
        """Return the flag state; store/key uncertainty is an exception, never GO."""
        entry = await self._kv.get(self._config.key)
        if entry is None:
            raise KillAuthorityError(
                f"kill authority key {self._config.key!r} is missing from bucket "
                f"{self._config.bucket!r}; refusing a false release. Provision it with "
                "`fleet-kill initialize` before starting fleet enforcement."
            )
        return _decode_armed(entry.value)

    async def set_armed(self, armed: bool) -> bool:
        """CAS-write ``armed`` and return the committed state.

        A bounded retry handles a competing operator.  Exhaustion remains loud so
        an operator never receives a false success while the flag is uncertain.
        """
        payload = json.dumps({"armed": armed}, separators=(",", ":"), sort_keys=True).encode()
        for attempt in range(_MAX_CAS_ATTEMPTS):
            entry = await self._kv.get(self._config.key)
            try:
                if entry is None:
                    raise KillAuthorityError(
                        f"kill authority key {self._config.key!r} is missing; refusing to "
                        "arm or release an unprovisioned authority"
                    )
                await self._kv.update(self._config.key, payload, expected_revision=entry.revision)
            except StaleRevisionError:
                if attempt + 1 == _MAX_CAS_ATTEMPTS:
                    raise KillAuthorityError(
                        "kill authority changed concurrently; refusing to report an "
                        "unverified operator action"
                    ) from None
                continue
            return armed
        raise AssertionError("unreachable bounded CAS loop")

    async def initialize_released(self) -> bool:
        """Create the authority once in RELEASED state; never overwrite a live key."""
        payload = b'{"armed":false}'
        try:
            await self._kv.create(self._config.key, payload)
        except StaleRevisionError as exc:
            raise KillAuthorityError(
                f"kill authority key {self._config.key!r} already exists; refusing an "
                "initialization that could clear an armed fleet"
            ) from exc
        return False


async def open_kill_authority(bus: FleetBus) -> FleetKVKillAuthority:
    """Open the pre-provisioned fleet-wide authority; missing store fails closed."""
    config = KillAuthorityConfig.from_env()
    kv = await open_kv(bus, config.bucket)
    return FleetKVKillAuthority(kv, config)


def _value_from_env(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        raise ValueError(f"{name} is set but blank")
    return value


def _decode_armed(value: bytes) -> bool:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise KillAuthorityError("kill authority record is not valid JSON") from exc
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"armed"}
        or not isinstance(decoded["armed"], bool)
    ):
        raise KillAuthorityError("kill authority record must be exactly {'armed': <bool>}")
    return decoded["armed"]


__all__ = [
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "FleetKVKillAuthority",
    "KillAuthorityConfig",
    "KillAuthorityError",
    "open_kill_authority",
]
