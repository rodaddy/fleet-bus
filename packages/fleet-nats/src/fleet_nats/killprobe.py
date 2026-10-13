"""Dedicated-thread reader for the durable fleet kill authority (#107)."""

from __future__ import annotations

import asyncio
import threading

from loguru import logger

from fleet_nats.bus import FleetBus
from fleet_nats.config import BusConfig
from fleet_nats.killflag import FleetKVKillAuthority, open_kill_authority


class StoreKillProbe:
    """Read one durable kill flag outside a hub/worker's coordination event loop.

    A store outage yields ``None`` rather than a false release.  The caller's
    watcher maps that unknown state to a stop, preserving the ≤5-second boundary
    even when its ordinary NATS callback loop is saturated.
    """

    def __init__(self, config: BusConfig, *, timeout_s: float = 1.0) -> None:
        self._config = config
        self._timeout_s = timeout_s
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._reconnect = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._authority: FleetKVKillAuthority | None = None

    def start(self) -> None:
        """Start the isolated store loop and wait only for the bounded tick-zero probe."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._ready.clear()
        self._stop.clear()
        self._reconnect.clear()
        self._thread = threading.Thread(
            target=self._thread_main, name="fleet-kill-store", daemon=True
        )
        self._thread.start()
        self._ready.wait(timeout=self._timeout_s)

    def read(self) -> bool | None:
        """Return armed/released/unknown without blocking the coordination loop."""
        loop = self._loop
        authority = self._authority
        if loop is None or authority is None:
            return None
        future = asyncio.run_coroutine_threadsafe(authority.armed(), loop)
        try:
            return future.result(timeout=self._timeout_s)
        except Exception as exc:  # noqa: BLE001 -- an uncertain store must fail closed
            logger.error("fleet.kill_probe | durable store read uncertain: {}", exc)
            future.cancel()
            # A timed-out post-connect read can leave the NATS/KV handle wedged.
            # Drop that whole control-plane connection before the next poll so a
            # recovered authority can be observed (including an authoritative release).
            self._authority = None
            self._reconnect.set()
            return None

    def stop(self) -> None:
        """Stop and join the store thread without leaking a control-plane connection."""
        self._stop.set()
        self._reconnect.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._timeout_s + 1.0)
        self._thread = None
        self._authority = None
        self._loop = None

    def _thread_main(self) -> None:
        asyncio.run(self._serve())

    async def _serve(self) -> None:
        self._loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            bus = FleetBus(self._config)
            try:
                await asyncio.wait_for(bus.connect(), timeout=self._timeout_s)
                self._authority = await asyncio.wait_for(
                    open_kill_authority(bus), timeout=self._timeout_s
                )
                self._ready.set()
                await asyncio.to_thread(self._reconnect.wait)
                self._reconnect.clear()
            except Exception as exc:  # noqa: BLE001 -- retry while callers remain fail-closed
                self._authority = None
                self._ready.set()
                logger.error("fleet.kill_probe | durable store unavailable: {}", exc)
                await asyncio.sleep(self._timeout_s)
            finally:
                try:
                    await bus.close()
                except Exception as exc:  # noqa: BLE001 -- shutdown must not mask uncertainty
                    logger.warning("fleet.kill_probe | store connection close failed: {}", exc)
        self._authority = None


__all__ = ["StoreKillProbe"]
