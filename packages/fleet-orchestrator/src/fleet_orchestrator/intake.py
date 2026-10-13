"""Intake parsing + the per-worker offline key — the pure seam of the run-loop.

The Phase-9 hub (:mod:`fleet_orchestrator.loop`) opens a coordination session
when a new k-board task appears. This module holds the *pure* parts of that seam
so they stay unit-testable without a bus and keep the loop module focused on the
service wiring:

- :func:`parse_intake` — a k-board ``created`` event → an :class:`IntakeTask`
  (identity/counts + the task prompt text the ``created`` payload carries).
- :func:`card_text` — assemble a k-board card's title+body into that prompt text
  (the pure prose-shaping seam; the IO of fetching the card lives in the caller).
- :func:`offline_key` / :func:`unkey` — the per-worker offline-watch key. The
  shipped :class:`~fleet_orchestrator.offline.OfflineDetector` keys a watch by its
  task_id argument alone, so a task fanned out to N workers would keep only ONE
  watch; a ``{task_id}::{worker}`` composite gives every target its own ladder.
"""

from __future__ import annotations

from dataclasses import dataclass

from fleet_nats import Envelope
from fleet_nats.tracing import traced

# The k-board lifecycle event that OPENS a coordination session. A new task card
# in a "created" state is the fleet's intake trigger — the k-board is the handoff
# source of truth (design invariant), so its own created-event is the natural
# (and only sanctioned) seam to start coordinating a task.
INTAKE_EVENT = "created"
# Payload keys carried on the intake event. ``text`` is the ONLY prose key: it is
# the task prompt the worker must actually run (the k-board card's title+body). The
# shipped path was broken precisely because the ``created`` event carried identity
# but NOT this text, so every dispatch reached the worker empty and was dropped by
# the subscriber's ``envelope_to_message`` (fleet-bus #52). channel/workers stay
# identity/counts.
_PAYLOAD_CHANNEL = "channel"
_PAYLOAD_WORKERS = "workers"
_PAYLOAD_TEXT = "text"
# Separator for the per-worker offline-watch composite key. Task ids are k-board
# ids and worker ids are agent slugs; neither contains "::", so the split back to
# (task_id, worker) is unambiguous. rpartition splits on the LAST separator, so
# even a (hypothetical) task id containing "::" would still recover the worker.
OFFLINE_KEY_SEP = "::"


def card_text(title: str | None, body: str | None) -> str:
    """Build the worker prompt from a k-board card's title + body.

    The card's ``title`` is the one-line summary and ``body`` the detail; a worker
    turn wants both. Joined title-first with a blank line between so the prompt
    reads naturally; either side may be blank (a card with only a title, or only a
    body) and the join drops the empty side rather than leaving a dangling
    separator. Whitespace-only fields count as absent.

    This is the pure prose-assembly seam: IO (fetching the card / listing the
    board) lives in the poller and the ``--once`` path; this only shapes the two
    fields into the ``payload["text"]`` the worker's ``envelope_to_message`` reads.
    """
    parts = [p.strip() for p in (title, body) if p is not None and p.strip()]
    return "\n\n".join(parts)


def offline_key(task_id: str, worker: str) -> str:
    """The per-worker offline-watch key for a (task, worker) pair."""
    return f"{task_id}{OFFLINE_KEY_SEP}{worker}"


def unkey(composite: str) -> tuple[str, str]:
    """Split a per-worker offline key back into ``(task_id, worker)``.

    Uses ``rpartition`` on the LAST separator, so the worker (never containing
    ``::``) is always recovered even if a task id somehow carried the separator.
    A key with no separator returns ``("", composite)`` — the caller treats a
    blank task id as an unmatched lookup (a defensive no-op), never a crash.
    """
    task_id, _, worker = composite.rpartition(OFFLINE_KEY_SEP)
    return task_id, worker


@dataclass(frozen=True)
class IntakeTask:
    """A parsed intake — the identity of a task to start coordinating.

    Attributes:
        task_id: The k-board task id.
        channel: The originating Discord channel slug (per-channel isolation).
        workers: The workers to fan out to (the intake payload's list, or the
            hub's configured default when the event omits it).
        text: The task prompt the worker must run (the k-board card's title+body).
            This is what the dispatch carries as ``payload["text"]`` — without it
            the subscriber drops the dispatch as an empty turn (fleet-bus #52). May
            be empty (an event that predates the enrichment, or a card with no
            title/body): the hub dispatches it but WARNs loudly, never silently.
    """

    task_id: str
    channel: str | None
    workers: tuple[str, ...]
    text: str = ""


@traced
def parse_intake(envelope: Envelope, *, default_workers: tuple[str, ...]) -> IntakeTask | None:
    """Parse a k-board ``created`` event into an :class:`IntakeTask`.

    Pure: it reads the task id, channel, (optional) worker list, and the task
    prompt ``text`` off the envelope — no IO. A ``created`` event without a
    ``task_id`` cannot open a session and is dropped (logged by the caller). The
    prompt text is the payload's ``text`` key (the k-board card's title+body the
    publisher enriched onto the event); an event that omits it yields ``text=""``
    and the hub WARNs at dispatch rather than shipping an empty prompt silently.

    Args:
        envelope: The received ``kboard_event`` (created) envelope.
        default_workers: The hub's configured workers, used when the event
            carries no explicit ``workers`` list.

    Returns:
        The parsed :class:`IntakeTask`, or ``None`` when the event has no
        task_id to key a session on.
    """
    task_id = envelope.task_id
    if not task_id:
        return None
    raw_workers = envelope.payload.get(_PAYLOAD_WORKERS)
    # A payload workers list is accepted only when every entry is a non-empty
    # string that does NOT contain the offline-key separator — a worker id with
    # "::" would corrupt the per-worker offline key (unkey would mis-split it), so
    # a malformed list falls back to the trusted configured default rather than
    # keying a watch that can never be matched (codex cross-review, #4).
    if (
        isinstance(raw_workers, list)
        and raw_workers
        and all(isinstance(w, str) and w and OFFLINE_KEY_SEP not in w for w in raw_workers)
    ):
        workers = tuple(raw_workers)
    else:
        workers = default_workers
    raw_channel = envelope.payload.get(_PAYLOAD_CHANNEL)
    channel = raw_channel if isinstance(raw_channel, str) and raw_channel else envelope.channel
    # The task prompt. Accepted only as a non-empty string; anything else (absent,
    # None, wrong type) becomes "" so the hub's empty-text WARN fires rather than a
    # dispatch carrying a bogus non-string body downstream.
    raw_text = envelope.payload.get(_PAYLOAD_TEXT)
    text = raw_text if isinstance(raw_text, str) else ""
    return IntakeTask(task_id=task_id, channel=channel, workers=workers, text=text)
