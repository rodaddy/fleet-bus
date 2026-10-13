"""Worker kill-watch tests: refusal and real cancellation callback (#107)."""

from __future__ import annotations

import asyncio

import pytest

from fleet_subscriber.killwatch import WorkerKillWatch


def test_unknown_refuses_and_fires_cancellation_once() -> None:
    cancellations: list[str] = []
    watch = WorkerKillWatch(
        poll_s=0.01, probe=lambda: None, on_arm=lambda: cancellations.append("cancel")
    )
    assert watch.check_once()
    assert watch.armed()
    assert cancellations == ["cancel"]
    assert watch.check_once()
    assert cancellations == ["cancel"]


async def test_arm_cancels_a_real_inflight_task() -> None:
    armed = False
    cancelled = asyncio.Event()

    async def _turn() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(_turn())
    await asyncio.sleep(0)

    def _cancel() -> None:
        task.cancel()

    watch = WorkerKillWatch(poll_s=0.01, probe=lambda: armed, on_arm=_cancel)
    assert watch.check_once() is False
    armed = True
    assert watch.check_once() is True
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await task
