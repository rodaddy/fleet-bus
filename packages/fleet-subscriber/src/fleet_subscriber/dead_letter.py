"""Dead-letter reporting (#25, Phase 3.3) — nothing vanishes silently.

An envelope the subscriber cannot process (mapping crash, malformed fields,
role mismatch — anything that raises out of the ingress path) is published as a
SANITIZED record on ``subjects.dead_letter(env)`` and logged; the subscriber
stays up. The record carries reason, source subject, and the source envelope's
id/kind/ts — NEVER the raw payload (the ``docs/logging.md`` never-log list).

The reporter itself must degrade loudly-but-safely: if the dead-letter publish
fails (bus down mid-error), it logs and drops — an error path must never take
the subscriber down or recurse.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from fleet_nats import Envelope, subjects
from loguru import logger

DEAD_LETTER_KIND = "dead_letter"


class _PublishBus(Protocol):
    """The one bus capability the reporter needs."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        """Publish an envelope."""
        ...


def dead_letter_envelope(
    *,
    msg_id: str,
    ts: str,
    sender: str,
    reason: str,
    source_subject: str,
    source_id: str,
    source_kind: str,
    source_ts: str,
) -> Envelope:
    """Build the sanitized dead-letter record.

    Args:
        msg_id: Unique id for the record itself.
        ts: ISO timestamp of the record.
        sender: The reporting agent.
        reason: Why processing failed (exception class + message from OUR code;
            never payload contents).
        source_subject: The lane the unprocessable envelope arrived on.
        source_id: The unprocessable envelope's id.
        source_kind: Its kind.
        source_ts: Its timestamp.

    Returns:
        The ``kind="dead_letter"`` :class:`Envelope`.
    """
    return Envelope.new(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        kind=DEAD_LETTER_KIND,
        payload={
            "reason": reason,
            "source_subject": source_subject,
            "source_id": source_id,
            "source_kind": source_kind,
            "source_ts": source_ts,
        },
    )


class DeadLetterReporter:
    """Publishes dead-letter records on the env's dead-letter lane.

    Args:
        bus: The connected bus (publish capability only).
        env: Subject env prefix.
        agent_id: The reporting agent.
        new_id: Message-id factory (injected).
        now: ISO-8601 timestamp factory (injected).
    """

    def __init__(
        self,
        bus: _PublishBus,
        *,
        env: str,
        agent_id: str,
        new_id: Callable[[], str],
        now: Callable[[], str],
    ) -> None:
        self._bus = bus
        self._subject = subjects.dead_letter(env)
        self._agent_id = agent_id
        self._new_id = new_id
        self._now = now

    async def report(self, *, reason: str, source_subject: str, source: Envelope) -> None:
        """Publish one dead-letter record; never raises.

        Args:
            reason: Sanitized failure reason (no payload contents).
            source_subject: The lane the envelope arrived on.
            source: The unprocessable envelope (only id/kind/ts are recorded).
        """
        record = dead_letter_envelope(
            msg_id=self._new_id(),
            ts=self._now(),
            sender=self._agent_id,
            reason=reason,
            source_subject=source_subject,
            source_id=source.id,
            source_kind=source.kind,
            source_ts=source.ts,
        )
        logger.warning(
            "fleet_subscriber.dead_letter | agent={} source_subject={} source_id={} reason={}",
            self._agent_id,
            source_subject,
            source.id,
            reason,
        )
        try:
            await self._bus.publish(self._subject, record)
        except Exception as exc:  # noqa: BLE001 — the error path must not kill the subscriber
            logger.error(
                "fleet_subscriber.dead_letter | publish failed agent={} source_id={}: {}",
                self._agent_id,
                source.id,
                exc,
            )
