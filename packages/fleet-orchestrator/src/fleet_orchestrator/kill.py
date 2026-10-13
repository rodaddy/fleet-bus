"""Operator command for the one store-backed fleet kill authority (#107)."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys
from collections.abc import Awaitable, Callable

from fleet_nats import BusConfig, FleetBus, open_kill_authority
from loguru import logger

_ENV_OPERATOR_DEADLINE = "FLEET_KILL_SWITCH_DEADLINE_SECONDS"
_DEFAULT_OPERATOR_DEADLINE_S = 5.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the bounded operator action against the one store-backed authority."""
    parser = argparse.ArgumentParser(prog="fleet-kill")
    parser.add_argument(
        "action", choices=("arm", "release", "status", "initialize"), nargs="?", default="arm"
    )
    return parser.parse_args(argv)


async def _run(action: str) -> int:
    """Perform one verified action inside one absolute, process-wide deadline."""
    bus = FleetBus(BusConfig.from_env())
    deadline = asyncio.get_running_loop().time() + _operator_deadline_from_env()
    try:
        await _before_deadline(deadline, bus.connect)
        authority = await _before_deadline(deadline, lambda: open_kill_authority(bus))
        if action == "status":
            armed = await _before_deadline(deadline, authority.armed)
            logger.info("fleet.kill | store authority armed={}", armed)
            return 0 if armed else 1
        if action == "initialize":
            await _before_deadline(deadline, authority.initialize_released)
            logger.warning("fleet.kill | store authority initialized released")
            return 0
        armed = await _before_deadline(deadline, lambda: authority.set_armed(action == "arm"))
        logger.warning("fleet.kill | store authority committed armed={}", armed)
        return 0
    finally:
        await _before_deadline(deadline, bus.close)


async def _before_deadline[T](deadline: float, operation: Callable[[], Awaitable[T]]) -> T:
    """Run ``operation`` only within the one hard aggregate operator deadline."""
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError("fleet-kill aggregate deadline expired")
    return await asyncio.wait_for(operation(), timeout=remaining)


def _operator_deadline_from_env() -> float:
    """Read an operator deadline that cannot exceed the five-second acceptance bound."""
    raw = os.environ.get(_ENV_OPERATOR_DEADLINE)
    if raw is None:
        return _DEFAULT_OPERATOR_DEADLINE_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_OPERATOR_DEADLINE} must be a number, got {raw!r}") from exc
    if not math.isfinite(value) or value <= 0 or value > _DEFAULT_OPERATOR_DEADLINE_S:
        raise ValueError(f"{_ENV_OPERATOR_DEADLINE} must be finite and in (0, 5], got {value}")
    return value


def main(argv: list[str] | None = None) -> int:
    """Run the fleet-wide store action or return a fail-loud non-zero status."""
    try:
        return asyncio.run(_run(_parse_args(argv).action))
    except Exception as exc:  # noqa: BLE001 -- authority uncertainty must reach the operator
        logger.error("fleet.kill | authority unavailable; refusing to claim success: {}", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
