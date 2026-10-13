"""Unit tests for the headless dummy subscriber (gateway-free echo instrument).

A fake bus proves the lane wiring and the echo/complete behavior without NATS:
the dummy must subscribe the SAME role lanes as the real adapter, reply on the
task's status subject, and complete a dispatch with the real done sentinel.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from fleet_nats import BusConfig, Envelope, FleetBus, subjects
from fleet_nats.sentinel import is_done_envelope

from fleet_subscriber.dummy import DummySubscriber
from fleet_subscriber.role import SubscriberRole


@dataclass
class FakeBus:
    subscriptions: dict[str, Any] = field(default_factory=dict)
    published: list[tuple[str, Envelope]] = field(default_factory=list)

    async def subscribe(self, subject: str, handler: Any) -> Any:
        self.subscriptions[subject] = handler
        return object()

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


def _dummy(
    agent: str = "dummy-hub", role: SubscriberRole = SubscriberRole.WORKER
) -> tuple[DummySubscriber, FakeBus]:
    fake = FakeBus()
    config = BusConfig(agent_id=agent, url="nats://192.0.2.74:4222", env="dev")
    return DummySubscriber(cast(FleetBus, fake), config, role), fake


def _envelope(kind: str, task_id: str | None = None, channel: str | None = None) -> Envelope:
    return Envelope.new(
        msg_id="m1",
        ts="t",
        sender="pytest",
        kind=kind,
        task_id=task_id,
        channel=channel,
        payload={"text": "hello"},
    )


async def test_worker_start_subscribes_same_lanes_as_adapter() -> None:
    dummy, fake = _dummy()
    await dummy.start()
    assert set(fake.subscriptions) == {
        subjects.agent_dispatch("dev", "dummy-hub"),
        subjects.agent_dispatch_wildcard("dev", "dummy-hub"),
        subjects.agent_relay("dev", "dummy-hub"),
        subjects.agent_relay_wildcard("dev", "dummy-hub"),
        subjects.agent_wake("dev", "dummy-hub"),
        subjects.agent_wake_wildcard("dev", "dummy-hub"),
        subjects.agent_inbox("dev", "dummy-hub"),
    }


async def test_manager_start_subscribes_inbox_only() -> None:
    dummy, fake = _dummy(agent="dummy-mgr", role=SubscriberRole.MANAGER)
    await dummy.start()
    assert set(fake.subscriptions) == {subjects.agent_inbox("dev", "dummy-mgr")}


async def test_dispatch_echoes_status_then_done_sentinel() -> None:
    dummy, fake = _dummy()
    await dummy.on_envelope(_envelope("dispatch", task_id="t-1", channel="ch"))

    # Channel-SCOPED (#117), exactly like the real adapter's reply. The dummy is
    # the install-proof instrument, so it must publish the subject shape
    # production publishes — the bare form would still pass (the hub subscribes
    # both) while silently proving nothing about scoped routing.
    expected_subject = subjects.agent_status("dev", "t-1", channel="ch")
    assert [s for s, _ in fake.published] == [expected_subject, expected_subject]

    echo, done = (e for _, e in fake.published)
    assert echo.kind == "status"
    assert echo.sender == "dummy-hub"
    assert echo.task_id == "t-1"
    assert "received dispatch" in echo.payload["text"]
    # The completion is the REAL shared sentinel, not a hand-rolled dict.
    assert is_done_envelope(done)
    assert done.payload == {
        "header": {"actor": "dummy-hub"},
        "data": {"task_id": "t-1", "outcome": "completed"},
        "extras": {},
        "footer": {},
    }


async def test_non_dispatch_echoes_without_completing() -> None:
    dummy, fake = _dummy()
    await dummy.on_envelope(_envelope("inbox", channel="ch"))

    # No task_id -> keyed on the channel AND scoped to it; inbox never completes.
    assert [s for s, _ in fake.published] == [subjects.agent_status("dev", "ch", channel="ch")]
    (echo,) = (e for _, e in fake.published)
    assert echo.kind == "status" and not is_done_envelope(echo)


async def test_unkeyed_envelope_falls_back_to_agent_id() -> None:
    dummy, fake = _dummy()
    await dummy.on_envelope(_envelope("wake"))

    # No task_id and no channel -> the agent's own id keys the reply subject,
    # so even a bare poke produces an observable reply.
    assert [s for s, _ in fake.published] == [subjects.agent_status("dev", "dummy-hub")]


async def test_dummy_reply_subject_matches_the_real_adapter_scoping_rule() -> None:
    """The dummy's reply subject must track the real adapter's, not drift from it.

    The dummy exists to PROVE an install round-trips, so it has to publish the
    subject shape production publishes. Both sides derive the channel token from
    the one shared helper (``mapping.reply_channel``), so this pins that they
    agree — including the wildcard fallback, which is the case that would
    otherwise build an undeliverable subject.
    """
    cases = (("dash-dev", "dash-dev"), ("*", None), (">", None), (None, None))
    for channel, expected_scope in cases:
        dummy, fake = _dummy()
        await dummy.on_envelope(_envelope("dispatch", task_id="t-1", channel=channel))

        expected = subjects.agent_status("dev", "t-1", channel=expected_scope)
        assert [s for s, _ in fake.published] == [expected, expected], channel
        # A wildcard channel must never reach the wire on the subject.
        for subject, _ in fake.published:
            assert "*" not in subject and ">" not in subject, subject


async def test_dispatch_without_task_id_does_not_complete() -> None:
    dummy, fake = _dummy()
    await dummy.on_envelope(_envelope("dispatch", channel="ch"))

    # A dispatch missing its task_id can't emit a sentinel (nothing to complete).
    assert len(fake.published) == 1
    assert not is_done_envelope(fake.published[0][1])
