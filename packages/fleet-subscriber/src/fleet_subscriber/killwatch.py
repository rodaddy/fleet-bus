"""Worker-side refuse and cancellation watch for the shared FleetKV kill flag."""

from __future__ import annotations

import threading
from collections.abc import Callable

from fleet_nats import BusConfig, StoreKillProbe
from loguru import logger

ArmedProbe = Callable[[], bool | None]


class WorkerKillWatch:
    """Fail-closed worker ingress gate, polled outside the subscriber event loop."""

    def __init__(
        self,
        *,
        poll_s: float,
        probe: ArmedProbe,
        on_arm: Callable[[], None],
        store_probe: StoreKillProbe | None = None,
    ) -> None:
        self._poll_s = poll_s
        self._probe = probe
        self._on_arm = on_arm
        self._store_probe = store_probe
        self._armed: bool | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def armed(self) -> bool:
        """Return true unless an authoritative released read has completed."""
        return self._armed is not False

    def check_once(self) -> bool:
        """Apply one authority read; unknown is armed and cancels live turns."""
        try:
            reading = self._probe()
        except Exception as exc:  # noqa: BLE001 -- safety gate must not crash
            logger.error("fleet_subscriber.killwatch | authority read failed: {}", exc)
            reading = None
        if reading is False:
            self._armed = False
            return False
        edge = self._armed is not True
        self._armed = True
        if reading is None:
            logger.error("fleet_subscriber.killwatch | authority uncertain; refusing turns")
        if edge:
            self._on_arm()
        return True

    def start(self) -> None:
        """Run tick-zero before catalog/bus work, then maintain a dedicated watcher."""
        if self._thread is not None and self._thread.is_alive():
            return
        if self._store_probe is not None:
            self._store_probe.start()
        self.check_once()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="fleet-worker-killwatch", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Join watcher and its control-store connection."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_s + 1.0)
        self._thread = None
        if self._store_probe is not None:
            self._store_probe.stop()

    def _run(self) -> None:
        while not self._stop.wait(self._poll_s):
            self.check_once()


def build_worker_kill_watch(
    config: BusConfig, *, poll_s: float, on_arm: Callable[[], None]
) -> WorkerKillWatch:
    """Build a worker watch over the same configuration the hub and CLI resolve."""
    store_probe = StoreKillProbe(config)
    return WorkerKillWatch(
        poll_s=poll_s, probe=store_probe.read, on_arm=on_arm, store_probe=store_probe
    )


__all__ = ["WorkerKillWatch", "build_worker_kill_watch"]
