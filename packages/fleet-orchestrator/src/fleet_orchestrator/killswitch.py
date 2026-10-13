"""Hub-side enforcement of the shared durable fleet kill authority (#107)."""

from __future__ import annotations

import math
import os
import threading
from collections.abc import Callable

from fleet_nats import StoreKillProbe
from loguru import logger

from fleet_orchestrator.governor import Governor

_ENV_KILL_POLL = "FLEET_KILL_SWITCH_POLL_SECONDS"
_DEFAULT_KILL_POLL_S = 1.0
ArmedProbe = Callable[[], bool | None]


def kill_poll_from_env() -> float:
    """Resolve a positive finite poll interval that preserves the five-second SLO."""
    raw = os.environ.get(_ENV_KILL_POLL)
    if raw is None:
        return _DEFAULT_KILL_POLL_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_KILL_POLL} must be a number, got {raw!r}") from exc
    if not math.isfinite(value) or value <= 0 or value > 4:
        raise ValueError(f"{_ENV_KILL_POLL} must be finite and in (0, 4], got {value}")
    return value


class KillSwitch:
    """Fail-closed thread watcher that gates every hub action through ``Governor``."""

    def __init__(
        self,
        governor: Governor,
        *,
        poll_s: float,
        probe: ArmedProbe,
        store_probe: StoreKillProbe | None = None,
    ) -> None:
        self._governor = governor
        self._poll_s = poll_s
        self._probe = probe
        self._store_probe = store_probe
        self._armed: bool | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def fail_closed(self) -> bool:
        """Whether the hub is stopped because the authority is armed or uncertain."""
        return self._armed is not False

    def check_once(self) -> bool:
        """Read one authority state; any error/unknown pauses all fleet work."""
        try:
            reading = self._probe()
        except Exception as exc:  # noqa: BLE001 -- the kill authority is last-line safety
            logger.error("orchestrator.killswitch | authority read failed: {}", exc)
            reading = None
        if reading is False:
            if self._armed is not False:
                self._governor.resume_all()
                logger.warning("orchestrator.killswitch | authority released; resuming")
            self._armed = False
            return False
        if reading is None:
            logger.error("orchestrator.killswitch | authority uncertain; pausing (fail closed)")
        elif self._armed is not True:
            logger.warning("orchestrator.killswitch | authority armed; pausing all")
        self._governor.pause_all()
        self._armed = True
        return True

    def start(self) -> None:
        """Complete tick-zero before any coordination bus/catalog action, then poll."""
        if self._thread is not None and self._thread.is_alive():
            return
        if self._store_probe is not None:
            self._store_probe.start()
        self.check_once()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="fleet-hub-killwatch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop both watcher and isolated durable-store reader."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_s + 1.0)
        self._thread = None
        if self._store_probe is not None:
            self._store_probe.stop()

    def _run(self) -> None:
        while not self._stop.wait(self._poll_s):
            self.check_once()


__all__ = ["ArmedProbe", "KillSwitch", "kill_poll_from_env"]
