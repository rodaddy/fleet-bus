"""Idempotent delivery guard (#25, Phase 3.2, C5) — app-level duplicates need dedup.

The transport is at-most-once today: core NATS, JetStream enabled nowhere, so a
message published while a subscriber is down is LOST (no backlog) and the bus
never redelivers on its own. Dispatch/relay/wake duplicates come from retry
ABOVE the bus (the orchestrator's own wake/redispatch, and reconnect re-sends),
never from JetStream consumer redelivery — JetStream stays invisible to agents.
Because a consumer can still see the same envelope twice, the subscriber keys a
bounded seen-set on the ``Envelope.id`` idempotency key (caller-supplied,
unique): a duplicate is a logged no-op, never a second agent turn. See
``docs/contracts/C5-ordering-retry-semantics.md``.

The set is a bounded LRU (default 1024, ``FLEET_DEDUP_WINDOW``) so memory stays
flat under load. A duplicate that slips past the window is reconciled at the
record: k-board is the source of truth, keyed by ``task_id`` — the double-run
is visible and correctable there (the documented backstop, not a silent hope).
"""

from __future__ import annotations

import os
from collections import OrderedDict

from loguru import logger

_ENV_WINDOW = "FLEET_DEDUP_WINDOW"
_DEFAULT_WINDOW = 1024


def window_from_env() -> int:
    """Read ``FLEET_DEDUP_WINDOW`` (default 1024). Fail loud on junk.

    Raises:
        ValueError: If set to a non-integer or non-positive value — a typo'd
            window must not silently disable or unbound the guard.
    """
    raw = os.environ.get(_ENV_WINDOW)
    if raw is None:
        return _DEFAULT_WINDOW
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_WINDOW} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{_ENV_WINDOW} must be positive, got {value}")
    return value


class DedupGuard:
    """Bounded LRU seen-set over envelope ids.

    Args:
        window: Maximum ids remembered; the oldest is evicted first.
    """

    def __init__(self, *, window: int = _DEFAULT_WINDOW) -> None:
        if window <= 0:
            raise ValueError(f"DedupGuard window must be positive, got {window}")
        self._window = window
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen_before(self, envelope_id: str) -> bool:
        """Record ``envelope_id``; return True if it was already seen.

        A re-seen id is refreshed to most-recent (true LRU) so a hot duplicate
        cannot age out while it is still arriving.
        """
        if envelope_id in self._seen:
            self._seen.move_to_end(envelope_id)
            logger.info("fleet_subscriber.dedup | duplicate envelope id={} — no-op", envelope_id)
            return True
        self._seen[envelope_id] = None
        if len(self._seen) > self._window:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)
