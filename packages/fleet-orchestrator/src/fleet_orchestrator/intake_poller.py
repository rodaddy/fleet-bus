"""The k-board intake poller — the missing publisher for the ``created`` event.

The Phase-9 hub (:mod:`fleet_orchestrator.loop`) subscribes the intake lane
(``dev.kboard.task.created.*``) to auto-open coordination sessions, but nothing
in the fleet publishes a ``created`` k-board event: ``persist_transition`` emits
only ``claimed``/``status``/``completed`` (:mod:`fleet_orchestrator.kboard`
``_STATE_TO_EVENT``), and :class:`~fleet_orchestrator.kboard.KboardClient` has no
create. So the durable service NEVER auto-opens a session from a new card — intake
only works via ``--once`` (which bypasses the intake lane). See fleet-bus #44.

This module closes that gap **inside the hub lifecycle** (one service, no second
deploy unit): a small poller that periodically lists k-board tasks, finds cards
sitting in ``todo`` that the hub has not already coordinated, and publishes the
``created`` event on ``subjects.kboard_event(env, "created", task_id)`` with the
payload :func:`~fleet_orchestrator.intake.parse_intake` reads — identity
(channel/workers) PLUS the card's prompt ``text`` (title+body), which the dispatch
must carry or the worker subscriber drops the empty turn (fleet-bus #52).

Idempotency (no double-open on restart) rests on two independent guards:

1. **Status is the durable ledger.** Only cards in ``todo`` are candidates. The
   hub's own ``persist_transition`` CAS-moves a coordinated card ``todo→running``
   before it is ever done, so a restarted poller never re-sees a card the hub
   already picked up — the k-board row itself is the cross-restart memory.
2. **A per-process grace window** suppresses re-publishing a ``created`` event
   for the next few ticks after it is emitted, for a card that is still ``todo``
   but already being opened by the hub in this process (the k-board status write
   is async and the next tick may race it). This is belt-and-suspenders: the
   poller may re-publish the same ``created`` event (an app-level retry — the
   transport is at-most-once, C5, not a redelivering bus), keyed by ``task_id``,
   and :meth:`~fleet_orchestrator.loop.OrchestratorHub.start_task` dedups a
   tracked task on that ``task_id``, so a duplicate can never double-open — but
   suppressing it keeps the bus quiet and the logs honest. Crucially the
   suppression is time-boxed, not a
   permanent latch: if a card is STILL ``todo`` after the grace window (its
   ``created`` never opened a session — e.g. a handler error the bus logs but does
   not feed back here) it is re-published, so a silent open failure self-heals
   instead of stranding the card until a restart.

Scope note (single-hub): these guards make intake safe for the shipped
single-hub deployment (one ``fleet-orchestrator-hub`` per env). They do NOT make
it safe to run TWO hubs against the same board: without a server-side atomic
``claim_if_todo`` (``update_status`` CASes on version, not on "expected status is
todo") two hubs could both open the same card. A durable atomic claim is the
companion GAP-2 concern (no k-board create/claim mechanism) and is tracked as a
follow-up, out of scope for #44's publisher gap.

Determinism/test seams mirror the offline poll: :meth:`IntakePoller.poll_once`
advances exactly one tick and is drivable without sleeping; the background loop
(:meth:`IntakePoller.run`) is a thin wrapper over it, stoppable via an injected
:class:`asyncio.Event`. All logging is ``orchestrator.intake.*``; task ids and
statuses are coordination metadata and safe to log (never task prose).
"""

from __future__ import annotations

import asyncio
import math
import os
from collections.abc import Callable
from typing import Protocol

from fleet_nats import (
    ChannelMap,
    Envelope,
    PayloadTooLargeError,
    is_subject_safe_slug,
    subjects,
)
from fleet_nats.tracing import traced
from loguru import logger

from fleet_orchestrator.intake import INTAKE_EVENT, card_text
from fleet_orchestrator.kboard import KboardTask

# The k-board status a card is flipped to when it is PERMANENTLY undispatchable
# (its created-event envelope is over the bus max_payload — no retry will ever
# fit it). "blocked" is the durable dead-letter: only ``todo`` cards are intake
# candidates, so a blocked card drops out of the poll set for good (P1-4).
_POISON_STATUS = "blocked"

# The k-board status a fresh, un-picked-up card sits in — the intake trigger.
_TODO_STATUS = "todo"

# How many ticks a just-published card is suppressed before it may be
# re-published while STILL ``todo``. This bounds the "open failed silently" window
# (a ``created`` whose handler errored, leaving the card ``todo``) to at most this
# many poll intervals of latency before intake self-heals, while giving the hub's
# async ``todo→running`` status write ample time to land in the happy path (so a
# successfully-opening card is not needlessly re-emitted). A duplicate re-publish
# is harmless — ``start_task`` dedups a tracked task (codex review, MAJOR).
_REPUBLISH_AFTER_TICKS = 4

# Env knobs the poller owns. The poll cadence trades intake latency against
# k-board list load; default well under a human's card-creation reaction time so
# a new card opens a session within one window, but not so tight it hammers the
# board.
_ENV_INTAKE_POLL = "FLEET_INTAKE_POLL_SECONDS"
_ENV_INTAKE_ENABLED = "FLEET_INTAKE_POLL_ENABLED"
# The card-scope marker (fleet-bus #54). The poller opens ONLY ``todo`` cards
# whose title (stripped) starts with this prefix, so a board carrying foreign
# backlog (the fleet-hub first-enable grabbed 8 unrelated cards) is safe to poll. It
# uses the card ``title`` the k-board row already carries — no server change. The
# marker is REQUIRED whenever the poller is enabled (see :func:`intake_label_from_env`
# / :meth:`HubConfig.from_env`): an unscoped poller fails closed rather than
# grabbing every card on the board.
_ENV_INTAKE_LABEL = "FLEET_INTAKE_LABEL"
_DEFAULT_INTAKE_POLL_S = 15.0
# Truthy spellings for the on/off switch (fail-loud on anything else — a typo'd
# toggle must not silently pick a side).
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def intake_poll_from_env() -> float:
    """Read ``FLEET_INTAKE_POLL_SECONDS`` (default 15). Fail loud on junk.

    Raises:
        ValueError: If set non-numeric, non-finite, or non-positive — a typo'd
            cadence must not silently become the default (coding-standard:
            fail-loud config, no silent fallback).
    """
    raw = os.environ.get(_ENV_INTAKE_POLL)
    if raw is None:
        return _DEFAULT_INTAKE_POLL_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_INTAKE_POLL} must be a number, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{_ENV_INTAKE_POLL} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{_ENV_INTAKE_POLL} must be positive, got {value}")
    return value


def intake_enabled_from_env() -> bool:
    """Read ``FLEET_INTAKE_POLL_ENABLED`` (default ``True``). Fail loud on junk.

    The poller is ON by default (it is the only shipped ``created`` publisher, so
    the durable service is useless without it), but an operator can disable it —
    e.g. when a real k-board webhook publishes ``created`` directly, or to run the
    hub in a ``--once``-only smoke posture. An unrecognised value fails loud rather
    than being coerced to a silent default.

    Raises:
        ValueError: If set to anything outside the recognised truthy/falsy set.
    """
    raw = os.environ.get(_ENV_INTAKE_ENABLED)
    if raw is None:
        return True
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(
        f"{_ENV_INTAKE_ENABLED} must be one of {sorted(_TRUE_VALUES | _FALSE_VALUES)}, got {raw!r}"
    )


def intake_label_from_env() -> str:
    """Read ``FLEET_INTAKE_LABEL`` — the required card-scope marker (fleet-bus #54).

    The poller opens only ``todo`` cards whose title starts with this marker. It is
    REQUIRED (returns ``""`` when unset here; the caller — :meth:`HubConfig.from_env`
    — is what refuses to start an ENABLED poller with no marker, so a disabled
    poller need not carry one). Surrounding whitespace is stripped so a trailing
    space in the env does not silently widen the prefix.

    Returns:
        The stripped marker (``""`` when the env var is unset or blank).
    """
    return os.environ.get(_ENV_INTAKE_LABEL, "").strip()


class _KboardLister(Protocol):
    """The k-board capability the poller needs — list, and dead-letter a poison card.

    Narrower than :class:`~fleet_orchestrator.kboard.KboardClient` so the poller's
    contract is explicit; the return type is the concrete :class:`KboardTask`
    (``list`` is invariant, so a structural task-view protocol would not match the
    client's ``list[KboardTask]``). The poller reads ``task_id``/``status`` and, on
    a permanently-undispatchable (oversized) card, flips it off ``todo`` via
    :meth:`update_status` so intake stops re-picking it (P1-4).
    """

    async def list_tasks(self) -> list[KboardTask]:  # pragma: no cover - protocol
        ...

    async def update_status(  # pragma: no cover - protocol
        self, task_id: str, status: str
    ) -> KboardTask: ...


class _Publisher(Protocol):
    """The one bus capability the poller needs — publish an envelope."""

    async def publish(self, subject: str, envelope: Envelope) -> None:  # pragma: no cover
        ...


class IntakePoller:
    """Polls k-board for new ``todo`` cards and publishes their ``created`` event.

    Owns no coordination logic: it only turns "a new card exists on the board"
    into the intake bus event the hub already knows how to consume. It reads the
    board through the injected lister and publishes through the injected bus; ids
    and timestamps come from the same injected clock the rest of the hub uses, so
    the poller stays deterministic under test.

    A restart is safe: the durable ``todo`` status is the cross-process memory (a
    coordinated card is no longer ``todo``), and the per-process ``seen`` set plus
    ``start_task``'s own dedup make a duplicate ``created`` a harmless no-op.
    """

    def __init__(
        self,
        kboard: _KboardLister,
        bus: _Publisher,
        *,
        env: str,
        sender: str,
        default_workers: tuple[str, ...],
        channels: ChannelMap,
        home_channel: str,
        new_id: Callable[[], str],
        now: Callable[[], str],
        poll_s: float,
        label: str,
    ) -> None:
        """Wire the poller from the hub's collaborators and identity.

        Args:
            kboard: The k-board client (read side — ``list_tasks``).
            bus: The connected fleet bus (publish side).
            env: The subject env prefix (e.g. ``dev``).
            sender: The agent id stamped on the published ``created`` envelope
                (the manager id — the hub's own bus identity).
            default_workers: The workers the ``created`` payload carries so the
                hub fans a new card out to the configured trio (intake honours an
                explicit list, but the poller only knows the board's cards, not a
                per-card worker override — the hub default is the right carrier).
            channels: The persisted slug ↔ Discord-id portal map (fleet-bus #118).
                A card that names its OWN channel is resolved through this map, so
                a new channel is a k-board row rather than a redeploy.
            home_channel: The board's default channel — used ONLY for a card that
                names no channel of its own. It is canonicalised through
                ``channels`` like any other, so the board's spelling and the
                bridge's spelling cannot diverge. Empty means unchannelled
                (matching the pre-poller ``--once`` path).
            new_id: Injected message-id factory (determinism seam).
            now: Injected UTC-ISO timestamp factory (determinism seam).
            poll_s: The poll cadence in seconds (validated positive by the caller).
            label: The card-scope marker (fleet-bus #54) — only ``todo`` cards
                whose title starts with it are opened. Must be non-empty (the
                caller refuses to start an enabled poller with no marker); an empty
                marker here would match every card and is rejected as a programming
                error rather than silently opening the whole board.
        """
        if not label:
            raise ValueError(
                "IntakePoller requires a non-empty label (card-scope marker, fleet-bus #54); "
                "an empty marker would open every card on the board"
            )
        self._kboard = kboard
        self._bus = bus
        self._env = env
        self._sender = sender
        self._default_workers = default_workers
        self._channels = channels
        self._home_channel = home_channel
        self._new_id = new_id
        self._now = now
        self._poll_s = poll_s
        self._label = label
        # Per-card ticks-since-last-published counter. A published card is
        # suppressed for the next ``_REPUBLISH_AFTER_TICKS`` ticks (it is being
        # opened; the hub's CAS moves it off ``todo`` and it drops out of the
        # candidate set) — but NOT forever: if a card is STILL ``todo`` after the
        # grace window (the ``created`` never opened a session — a handler error
        # the bus logs but does not feed back here), it is re-published so a
        # transient open failure self-heals instead of stranding the card until
        # restart (codex review, MAJOR). ``start_task`` dedups a genuine
        # duplicate, so a re-publish for an already-opening card is a harmless
        # no-op. NOT durable state: the ``todo`` status is the cross-restart guard.
        self._suppressed_ticks: dict[str, int] = {}
        # Poison set: task ids whose ``created`` envelope is PERMANENTLY too large
        # for the bus (PayloadTooLargeError). Unlike a transient publish failure
        # (which self-heals on the next tick), an oversized card can NEVER be
        # published, so retrying it forever spins intake and floods logs (P1-4).
        # A poisoned card is dead-lettered on the board (flipped off ``todo``) so
        # it drops out of the candidate set durably; this in-process set is the
        # belt-and-braces guard that stops re-picking it even if the board flip
        # fails (e.g. k-board down) — the card is never re-attempted this process.
        self._poisoned: set[str] = set()

    @traced
    async def poll_once(self) -> int:
        """List the board once and publish ``created`` for each new ``todo`` card.

        A single, sleep-free tick — drivable deterministically under test. A
        k-board list failure is logged loud and swallowed (the next tick retries):
        one transient board outage must not kill the poll loop, exactly as the
        offline poll degrades (coding-standard: log-before-degrade, never fatal on
        a single tick).

        Returns:
            The number of ``created`` events published this tick (0 when the board
            is unreachable or holds no new ``todo`` cards).
        """
        try:
            tasks = await self._kboard.list_tasks()
        except Exception as exc:  # noqa: BLE001 — one bad list must not stop the poll
            logger.error("orchestrator.intake | list failed (retrying next tick): {}", exc)
            return 0

        # Advance the suppression clock one tick for every card we are currently
        # holding back, so the grace window is measured in poll ticks.
        self._suppressed_ticks = {
            task_id: ticks + 1 for task_id, ticks in self._suppressed_ticks.items()
        }

        published = 0
        todo_ids: set[str] = set()
        for task in tasks:
            task_id = task.task_id
            if not task_id:
                continue  # a board row without an id can't key a session — skip
            if task_id in self._poisoned:
                # PERMANENTLY undispatchable (oversized created event) that was
                # ALSO durably dead-lettered off ``todo``. Never re-attempt it this
                # process. A card whose board flip FAILED is deliberately NOT
                # poisoned — it stays ``todo`` so this loop retries the flip (P1-4).
                continue
            if task.status != _TODO_STATUS:
                # Not a fresh card (running/done, or already picked up). Clear any
                # suppression so a card legitimately reset to ``todo`` later opens
                # immediately (the board is the source of truth, not the counter).
                self._suppressed_ticks.pop(task_id, None)
                continue
            if not self._matches_label(task.title):
                # A ``todo`` card outside the fleet's scope (foreign backlog). Skip
                # it and clear any suppression — the poller must NEVER open an
                # unmarked card (fleet-bus #54: the fleet-hub first-enable grabbed 8
                # unrelated cards). Logged at debug so the skip is observable
                # without spamming the board's steady-state backlog every tick.
                self._suppressed_ticks.pop(task_id, None)
                logger.debug(
                    "orchestrator.intake | skip unmarked card task={} (title lacks marker {!r})",
                    task_id,
                    self._label,
                )
                continue
            todo_ids.add(task_id)
            if self._is_suppressed(task_id):
                continue  # published recently — the hub is (still) opening it
            # Carry the card's prompt (title+body) onto the created event so the
            # dispatch reaches the worker with real text (fleet-bus #52). An empty
            # prompt is published anyway (a card with no title/body is still a task
            # to open) but logged so an empty intake is never silent.
            text = card_text(task.title, task.body)
            if await self._publish_created(task_id, text, self._channel_for(task)):
                # Reset the suppression clock only on a SUCCESSFUL publish; a failed
                # publish leaves the card un-suppressed so the next tick retries it.
                self._suppressed_ticks[task_id] = 0
                published += 1
        # Forget suppression for any card no longer on the board at all (deleted),
        # so the dict cannot grow without bound across a long-lived process.
        self._suppressed_ticks = {
            task_id: ticks
            for task_id, ticks in self._suppressed_ticks.items()
            if task_id in todo_ids
        }
        if published:
            logger.info("orchestrator.intake | opened new cards count={}", published)
        return published

    def _matches_label(self, title: str | None) -> bool:
        """Whether a card's title carries the fleet-intake marker (fleet-bus #54).

        A card is in scope only when its title (stripped, so a leading space on the
        card does not defeat the match) starts with the configured marker prefix.
        The marker is guaranteed non-empty by the constructor, so this never
        degenerates to "match everything". A titleless card (``None``/blank) can
        never carry the marker and is out of scope.
        """
        return (title or "").strip().startswith(self._label)

    def _is_suppressed(self, task_id: str) -> bool:
        """Whether a just-published ``todo`` card is still inside its grace window.

        A card published within the last :data:`_REPUBLISH_AFTER_TICKS` ticks is
        suppressed; once the window elapses (the card is STILL ``todo`` — its
        ``created`` never opened a session) it is eligible for re-publish so a
        silent open failure self-heals rather than stranding the card.
        """
        ticks = self._suppressed_ticks.get(task_id)
        return ticks is not None and ticks < _REPUBLISH_AFTER_TICKS

    def _channel_for(self, task: KboardTask) -> str:
        """The channel slug stamped on a card's ``created`` event (fleet-bus #118).

        The card's OWN channel wins over the board default. Before #118 every card
        was stamped with one ``FLEET_INTAKE_CHANNEL`` value, so a board serving two
        channels opened every session in whichever one that env named — and adding
        a channel meant an env change plus a hub redeploy. A card may now carry its
        channel (a ``channel`` field on the row, as a slug OR as a Discord id), and
        the persisted portal map canonicalises whichever spelling it used.

        Resolution order:

        1. The card's ``channel`` field, canonicalised through the map.
        2. ``home_channel`` (the board default), canonicalised the same way.
        3. ``""`` — unchannelled, matching the pre-poller ``--once`` path.

        An unmapped channel is stamped through UNCHANGED rather than dropped: the
        map translates, it does not gate intake, and an unreachable k-board
        channel-map table must not stop cards from opening (the live posture is
        exactly this — ``/api/channel-map`` is not deployed yet, so EVERY card
        today takes the unresolved path). The outbound leg is where an unmapped
        channel refuses to post.

        Pass-through is bounded by ONE rule: the value must be subject-safe. This
        slug does not stay data — it becomes a NATS subject token at
        ``subjects.agent_dispatch(env, worker, channel=session.channel)``. A card
        carrying ``*`` would publish every dispatch onto
        ``{env}.agent.*.dispatch.{worker}``, which the manager's own publish grant
        ACCEPTS and which nats-py never validates client-side, so the card is
        marked coordinated while every dispatch silently vanishes; ``>`` makes
        ``subjects.slug`` raise inside the publish path, which is swallowed into a
        log line and the card is re-polled and dropped forever. A subject-unsafe
        card channel therefore falls back to the board default rather than being
        stamped, and an unsafe default drops to unchannelled — degrading to the
        pre-#118 behavior, never into the subject namespace.

        Args:
            task: The k-board card being opened.

        Returns:
            The channel slug to stamp (``""`` when the card is unchannelled).
        """
        raw = task.raw.get("channel")
        card_channel = raw.strip() if isinstance(raw, str) else ""
        for candidate in (card_channel, self._home_channel):
            if not candidate or not candidate.strip():
                continue
            stripped = candidate.strip()
            resolved = self._channels.resolve_slug(stripped)
            if resolved is not None:
                return resolved
            if not is_subject_safe_slug(stripped):
                # Refuse to feed an unresolvable, subject-unsafe token to a subject
                # builder. Fall through to the next candidate (board default, then
                # unchannelled) so intake still opens the card.
                logger.error(
                    "orchestrator.intake | channel {!r} is NOT subject-safe and has no "
                    "portal (source={} portals={}) task={} — refusing to stamp it; "
                    "falling back",
                    stripped,
                    self._channels.source,
                    len(self._channels),
                    task.task_id,
                )
                continue
            logger.warning(
                "orchestrator.intake | channel {} has no portal in the channel map "
                "(source={} portals={}) task={} — stamping it unresolved",
                stripped,
                self._channels.source,
                len(self._channels),
                task.task_id,
            )
            return stripped
        return ""

    async def _publish_created(self, task_id: str, text: str, channel: str) -> bool:
        """Publish one ``created`` intake event; return whether it was published.

        ``text`` is the card's prompt (title+body) — the payload's ``text`` key the
        dispatch carries to the worker (fleet-bus #52). An empty prompt is still
        published (a bodyless card is a task to open) but WARNs so it is never
        silent; the length (never the content) is logged so an operator can see the
        prompt is present without leaking prose (coding-standard: sanitized logging).

        A publish failure is logged loud and swallowed (returns ``False``) so one
        transient bus error cannot escape :meth:`poll_once` and kill the poll loop
        (codex review, MAJOR) — the card stays un-suppressed and the next tick
        retries it (coding-standard: log-before-degrade, never fatal on a tick).

        Args:
            task_id: The card being opened.
            text: The card's prompt (title+body).
            channel: The already-resolved channel slug (:meth:`_channel_for`);
                ``""`` for an unchannelled card.
        """
        subject = subjects.kboard_event(self._env, INTAKE_EVENT, task_id)
        # Decide present/empty on the STRIPPED text: the worker subscriber drops on
        # `not text.strip()`, so a whitespace-only card body is effectively empty
        # and must WARN too (codex review, MAJOR — mirrors the loop's dispatch WARN).
        if text.strip():
            logger.debug(
                "orchestrator.intake | created carries text task={} len={} preview={!r}",
                task_id,
                len(text),
                text[:80],
            )
        else:
            logger.warning(
                "orchestrator.intake | created has EMPTY text task={} (card has no title/body)",
                task_id,
            )
        envelope = Envelope.new(
            msg_id=self._new_id(),
            ts=self._now(),
            sender=self._sender,
            kind="kboard_event",
            task_id=task_id,
            channel=channel or None,
            payload={
                "channel": channel,
                "workers": list(self._default_workers),
                "text": text,
            },
        )
        try:
            await self._bus.publish(subject, envelope)
        except PayloadTooLargeError as exc:
            # PERMANENT failure: the created envelope is over the bus max_payload,
            # so NO retry will ever fit it. Treat it distinctly from a transient
            # publish error (which self-heals next tick) — poison the card so the
            # poller stops re-picking it, and dead-letter it on the board by
            # flipping it off ``todo`` (P1-4). Without this, one oversized card
            # re-publishes every _REPUBLISH_AFTER_TICKS ticks forever.
            logger.error(
                "orchestrator.intake | card PERMANENTLY undispatchable (oversized) "
                "task={} subject={} size={} limit={}; dead-lettering (blocked): {}",
                task_id,
                subject,
                exc.size,
                exc.limit,
                exc,
            )
            # Poison ONLY after the board dead-letter flip durably succeeds. If the
            # flip fails (transient k-board outage) the card stays ``todo`` and
            # un-poisoned so the NEXT poll re-picks it and retries the flip —
            # otherwise a flip failure would strand the card as ``todo`` forever
            # while the poison guard silently suppressed every retry this process.
            if await self._dead_letter(task_id):
                self._poisoned.add(task_id)
            return False
        except Exception as exc:  # noqa: BLE001 — one bad publish must not stop the poll
            logger.error(
                "orchestrator.intake | publish failed task={} (retrying next tick): {}",
                task_id,
                exc,
            )
            return False
        logger.info("orchestrator.intake | published created task={} subject={}", task_id, subject)
        return True

    async def _dead_letter(self, task_id: str) -> bool:
        """Flip a poison (oversized) card off ``todo`` so intake stops re-picking it.

        Returns ``True`` iff the board flip durably succeeded; the caller poisons
        the card ONLY on success. On failure (k-board down) the card stays ``todo``
        and un-poisoned, so the next poll retries the flip rather than silently
        suppressing the card forever — a transient k-board outage self-corrects on
        a later tick. Never re-raises into the poll loop.
        """
        try:
            updated = await self._kboard.update_status(task_id, _POISON_STATUS)
        except Exception as exc:  # noqa: BLE001 — dead-letter degrades loudly, never fatal
            logger.error(
                "orchestrator.intake | dead-letter blocked write not proven task={} "
                "status={}; card remains eligible for retry: {}",
                task_id,
                _POISON_STATUS,
                exc,
            )
            return False
        if updated.status != _POISON_STATUS:
            logger.warning(
                "orchestrator.intake | dead-letter request suppressed task={} "
                "durable_status={} (not reporting blocked)",
                task_id,
                updated.status,
            )
            return False
        logger.warning(
            "orchestrator.intake | dead-lettered oversized card task={} -> status={}",
            task_id,
            _POISON_STATUS,
        )
        return True

    @traced
    async def run(self, stop: asyncio.Event) -> None:
        """Poll every ``poll_s`` until ``stop`` is set (the hub's stop event).

        Mirrors the offline poll: wait on the stop event with a timeout so a stop
        signalled mid-interval returns promptly (no fixed-sleep teardown lag), and
        run one tick per timeout. The loop owns no clock of its own beyond the
        cadence — the tick's ids/timestamps come from the injected factories.
        """
        logger.info(
            "orchestrator.intake | poller started env={} interval={}s", self._env, self._poll_s
        )
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll_s)
                break  # stop signalled during the wait
            except TimeoutError:
                pass  # a normal poll tick
            await self.poll_once()
        logger.info("orchestrator.intake | poller stopped")
