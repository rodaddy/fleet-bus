"""Headless dummy subscriber — a gateway-free echo agent for live verification.

``python -m fleet_subscriber.dummy`` connects to the bus as ``FLEET_AGENT_ID``,
subscribes the same role lanes the real adapter would (via ``lanes_for_role`` +
the adapter's lane→subject map — never hand-formatted subjects), and echoes every
envelope back as a ``status`` reply. A ``dispatch`` additionally gets the real
``done_envelope`` sentinel, so the full dispatch→status→done loop is provable
against a host with NO Hermes gateway installed (e.g. the fleet-hub test dummy).

This is a **test instrument**, not an agent: it never runs turns, never touches
Discord, and its replies are canned echo text. It exists so the self-provisioning
install (``scripts/install-agent.sh``) can be verified end-to-end on any host —
connz shows ``fleet-nats-<agent>``, and a round-trip proves the lanes are live.
"""

from __future__ import annotations

import asyncio
import signal
from datetime import UTC, datetime
from uuid import uuid4

from fleet_nats import BusConfig, Envelope, FleetBus, done_envelope, subjects
from loguru import logger

from fleet_subscriber.adapter import _LANE_BUILDERS
from fleet_subscriber.heartbeat import (
    HeartbeatEmitter,
    installed_package_version,
    interval_from_env,
)
from fleet_subscriber.mapping import reply_channel
from fleet_subscriber.role import SubscriberRole, lanes_for_role


class DummySubscriber:
    """Echo-only subscriber over an injected bus (unit-testable without NATS)."""

    def __init__(self, bus: FleetBus, config: BusConfig, role: SubscriberRole) -> None:
        self._bus = bus
        self._config = config
        self._role = role
        # Optional heartbeat (#26): attached by run(); the headless install-proof
        # instance beats like the real adapter so liveness is derivable from it.
        self.heartbeat: HeartbeatEmitter | None = None

    async def start(self) -> None:
        """Subscribe this agent's role lanes (same lane set as the real adapter)."""
        agent, env = self._config.agent_id, self._config.env
        for lane in lanes_for_role(self._role):
            for builder in _LANE_BUILDERS[lane]:
                subject = builder(env, agent)
                await self._bus.subscribe(subject, self.on_envelope)
                logger.info(
                    "fleet_subscriber.dummy | subscribed subject={} lane={} agent={} role={}",
                    subject,
                    lane,
                    agent,
                    self._role.value,
                )
        logger.info(
            "fleet_subscriber.dummy | ready agent={} env={} role={}", agent, env, self._role.value
        )

    async def on_envelope(self, env: Envelope) -> None:
        """Echo the envelope as a status reply; a dispatch also completes.

        The reply subject mirrors the real adapter's keying: the task's status
        lane when the inbound carries a ``task_id``, else the channel, else the
        agent's own id (a bare un-keyed poke still gets an observable reply).
        Never logs or echoes the inbound payload body (the never-log list).

        It also mirrors the real adapter's channel SCOPING (#117): an inbound
        carrying a channel is answered on that channel's slice, exactly as
        ``NatsAdapter.send`` does. Without this the install-proof round-trip
        would only ever exercise the bare form while every real reply rides the
        scoped one — and the divergence would stay invisible, because the hub
        subscribes both forms and the round-trip would still pass green.
        """
        if self.heartbeat is not None:
            self.heartbeat.note_turn()
        status_key = env.task_id or env.channel or self._config.agent_id
        subject = subjects.agent_status(
            self._config.env, status_key, channel=reply_channel(env.channel)
        )
        logger.info(
            "fleet_subscriber.dummy | envelope kind={} sender={} task={} -> reply subject={}",
            env.kind,
            env.sender,
            env.task_id,
            subject,
        )
        reply = Envelope.new(
            msg_id=uuid4().hex,
            ts=datetime.now(UTC).isoformat(),
            sender=self._config.agent_id,
            kind="status",
            task_id=env.task_id,
            channel=env.channel,
            payload={"text": f"dummy {self._config.agent_id} received {env.kind} (echo)"},
        )
        await self._bus.publish(subject, reply)

        if env.kind == "dispatch" and env.task_id:
            done = done_envelope(
                msg_id=uuid4().hex,
                ts=datetime.now(UTC).isoformat(),
                sender=self._config.agent_id,
                task_id=env.task_id,
                outcome="completed",
                channel=env.channel,
            )
            logger.info(
                "fleet_subscriber.dummy | done sentinel task={} subject={}", env.task_id, subject
            )
            await self._bus.publish(subject, done)


async def run() -> None:
    """Connect, subscribe, and serve until SIGINT/SIGTERM."""
    config = BusConfig.from_env()
    role = SubscriberRole.from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with FleetBus(config) as bus:
        dummy = DummySubscriber(bus, config, role)
        dummy.heartbeat = HeartbeatEmitter(
            bus,
            agent_id=config.agent_id,
            env=config.env,
            role=role.value,
            lanes=lanes_for_role(role),
            new_id=lambda: uuid4().hex,
            now=lambda: datetime.now(UTC).isoformat(),
            package_version=installed_package_version(),
            interval_s=interval_from_env(),
        )
        await dummy.start()
        beat_task = asyncio.create_task(dummy.heartbeat.run())
        try:
            await stop.wait()
        finally:
            dummy.heartbeat.stop()
            beat_task.cancel()
            try:
                await beat_task
            except asyncio.CancelledError:
                logger.debug("fleet_subscriber.dummy | heartbeat task cancelled")
    logger.info("fleet_subscriber.dummy | stopped agent={}", config.agent_id)


def main() -> None:
    """Sync entry point (``python -m fleet_subscriber.dummy``)."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
