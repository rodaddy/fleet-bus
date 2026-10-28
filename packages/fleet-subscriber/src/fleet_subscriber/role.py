"""Per-agent subscriber role — which coordination lanes an agent subscribes to.

Issue #8: the trio runs the same subscriber plugin under two roles. Issue #222
adds a third, publish-only role for non-agent principals.

- **worker** (Wren, Finch) — receives work: subscribes to its own
  ``dispatch`` / ``relay`` / ``wake`` / ``inbox`` lanes (all per-agent subjects).
- **manager** (Heron) — coordinates: at the *adapter* level it subscribes only
  to its own ``inbox``. The task-keyed lanes a manager observes
  (``agent_status.{task_id}``, ``agent_arbitrate.{topic}``) are **not** subscribed
  here: their ids are unknown at connect-time, and the orchestrator already owns
  them per-task via ``watch_worker_status`` / ``serve_arbitration``. Subscribing
  them again on the adapter would be a redundant, broader wildcard racing the
  orchestrator's own handlers — so the manager's *adapter* lanes are deliberately
  minimal, and coordination subscriptions live where they already work.
- **runner** (software-factory runner boxes, #222) — PUBLISH-ONLY: subscribes to
  nothing. A runner is a disposable privileged LXC running a coding agent over
  untrusted repository content, so it must never be able to RECEIVE instructions
  from the bus. It publishes progress on ``factory.runner.{job_id}`` and receives
  work over the durable Postgres path, never over NATS.

The role is pure config (an enum + a lane-set function), so the lane selection is
unit-testable without a bus. ``connect()`` in the adapter builds concrete subjects
from the returned lane names via ``fleet_nats.subjects``.
"""

from __future__ import annotations

import os
from enum import StrEnum

from fleet_nats.registry import CODE_PUBLISH_ONLY_ROLES, CODE_ROLE_LANES

# The per-agent coordination lanes, by name — derived from the canonical
# role→lanes map in fleet_nats.registry (the subject-registry seed, fleet-board
# #15) so the registry seed and the running subscriber can never disagree. Each
# lane maps to a ``fleet_nats.subjects`` builder keyed on the agent id.
_WORKER_LANES: tuple[str, ...] = CODE_ROLE_LANES["worker"]
_MANAGER_LANES: tuple[str, ...] = CODE_ROLE_LANES["manager"]
# A publish-only role has no CODE_ROLE_LANES entry — it subscribes to nothing,
# so it registers no lanes (see CODE_PUBLISH_ONLY_ROLES).
_NO_LANES: tuple[str, ...] = ()


class SubscriberRole(StrEnum):
    """The role an agent's subscriber runs as (drives its lane set)."""

    WORKER = "worker"
    MANAGER = "manager"
    RUNNER = "runner"

    @classmethod
    def from_env(cls, default: SubscriberRole | None = None) -> SubscriberRole:
        """Read ``FLEET_ROLE`` (``worker``/``manager``/``runner``); default when unset.

        Args:
            default: Role to use when ``FLEET_ROLE`` is unset. When ``None`` and
                the var is unset, defaults to ``WORKER`` (the safe, least-capable
                role — a misconfigured agent listens to its own work lanes, never
                silently assumes the coordinator identity).

        Raises:
            ValueError: If ``FLEET_ROLE`` is set to an unknown value (fail loud —
                a typo'd role must not silently fall back).
        """
        raw = os.environ.get("FLEET_ROLE")
        if raw is None:
            return default if default is not None else cls.WORKER
        try:
            return cls(raw.strip().lower())
        except ValueError as exc:
            valid = sorted(r.value for r in cls)
            raise ValueError(f"FLEET_ROLE must be one of {valid}, got {raw!r}") from exc


# Role → lanes, by table rather than by branch. The previous two-role form was a
# ternary (``_WORKER_LANES if role is WORKER else _MANAGER_LANES``); adding any
# third role to that shape silently hands it the MANAGER lane set — a new role
# would have subscribed to a coordination inbox nobody intended. A total mapping
# keyed on the enum cannot mis-assign, and a role missing from it fails loud.
_ROLE_LANES: dict[SubscriberRole, tuple[str, ...]] = {
    SubscriberRole.WORKER: _WORKER_LANES,
    SubscriberRole.MANAGER: _MANAGER_LANES,
    SubscriberRole.RUNNER: _NO_LANES,
}


def is_publish_only(role: SubscriberRole) -> bool:
    """Whether ``role`` must never subscribe to any lane (issue #222).

    Client-side invariant only. It governs what THIS subscriber attaches; it is
    not an authorization boundary and cannot stop a process holding a bus
    connection from calling ``nc.subscribe()`` itself. Server-side NATS
    permissions on a scoped credential are the only real confinement.
    """
    return role.value in CODE_PUBLISH_ONLY_ROLES


def lanes_for_role(role: SubscriberRole) -> tuple[str, ...]:
    """The coordination lane names an agent subscribes to for its ``role``.

    Returns lane *names* (``dispatch``/``relay``/``wake``/``inbox``); the adapter
    turns each into a concrete subject via ``fleet_nats.subjects`` for its agent
    id. A publish-only role (``runner``) returns an empty tuple — it subscribes
    to nothing at all.

    Raises:
        ValueError: If ``role`` has no lane set — a role added to the enum but
            not to the table must fail loud, never inherit another role's lanes.
    """
    try:
        lanes = _ROLE_LANES[role]
    except KeyError as exc:  # pragma: no cover - guards a future enum addition
        raise ValueError(f"no lane set defined for role {role.value!r}") from exc
    if lanes and is_publish_only(role):
        raise ValueError(
            f"role {role.value!r} is publish-only but declares lanes {lanes!r}; "
            "CODE_ROLE_LANES and CODE_PUBLISH_ONLY_ROLES disagree"
        )
    return lanes
