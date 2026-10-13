"""Unit tests for the pure subscriber-role config (issue #8).

No bus, no env leakage across tests (monkeypatch scopes FLEET_ROLE): the lane
selection and the env parsing are provable in isolation.
"""

from __future__ import annotations

import pytest

from fleet_subscriber import SubscriberRole, lanes_for_role
from fleet_subscriber.role import is_publish_only


def test_worker_lanes_are_the_four_work_lanes() -> None:
    assert lanes_for_role(SubscriberRole.WORKER) == ("dispatch", "relay", "wake", "inbox")


def test_manager_lanes_are_inbox_only() -> None:
    # The manager's adapter lanes are deliberately minimal — the task-keyed
    # status/arbitrate lanes belong to the orchestrator, not the adapter.
    assert lanes_for_role(SubscriberRole.MANAGER) == ("inbox",)


def test_from_env_reads_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_ROLE", "worker")
    assert SubscriberRole.from_env() is SubscriberRole.WORKER


def test_from_env_reads_manager_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_ROLE", "  Manager ")
    assert SubscriberRole.from_env() is SubscriberRole.MANAGER


def test_from_env_defaults_to_worker_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_ROLE", raising=False)
    # Least-capable default: a misconfigured agent listens to its own work lanes,
    # never silently assumes the coordinator identity.
    assert SubscriberRole.from_env() is SubscriberRole.WORKER


def test_from_env_honors_explicit_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_ROLE", raising=False)
    assert SubscriberRole.from_env(default=SubscriberRole.MANAGER) is SubscriberRole.MANAGER


def test_from_env_rejects_unknown_role(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typo'd role must fail loud, never silently fall back to a default.
    monkeypatch.setenv("FLEET_ROLE", "supervisor")
    with pytest.raises(ValueError, match="FLEET_ROLE must be one of"):
        SubscriberRole.from_env()


def test_role_lanes_match_the_registry_seed() -> None:
    # The registry's role→lanes map (fleet-board #15 seed) and the running
    # subscriber must never disagree — role.py derives from it by construction.
    from fleet_nats.registry import CODE_ROLE_LANES

    assert lanes_for_role(SubscriberRole.WORKER) == CODE_ROLE_LANES["worker"]
    assert lanes_for_role(SubscriberRole.MANAGER) == CODE_ROLE_LANES["manager"]


def test_publish_only_roles_are_absent_from_the_subscription_map() -> None:
    """CODE_ROLE_LANES registers SUBSCRIBERS; a publish-only role is not one.

    It is the twin of k-board's ``nats_role_lanes`` table, so an entry here with
    no row there is drift (``diff_catalogs`` compares role keys by presence) —
    and the k-board API rejects an empty lane list outright. The runner's
    registered contract is its subject TEMPLATE, not a lane row.
    """
    from fleet_nats.registry import CODE_PUBLISH_ONLY_ROLES, CODE_ROLE_LANES

    assert CODE_PUBLISH_ONLY_ROLES
    for role in CODE_PUBLISH_ONLY_ROLES:
        assert role not in CODE_ROLE_LANES, role


# --- #222: the publish-only runner role ------------------------------------


def test_runner_subscribes_to_nothing() -> None:
    # A runner box must never RECEIVE an instruction from the bus.
    assert lanes_for_role(SubscriberRole.RUNNER) == ()


def test_runner_is_flagged_publish_only_and_others_are_not() -> None:
    assert is_publish_only(SubscriberRole.RUNNER)
    assert not is_publish_only(SubscriberRole.WORKER)
    assert not is_publish_only(SubscriberRole.MANAGER)


def test_from_env_reads_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_ROLE", "runner")
    assert SubscriberRole.from_env() is SubscriberRole.RUNNER


def test_every_role_has_its_own_lane_set() -> None:
    """No role may inherit another's lanes.

    The pre-#222 implementation was ``_WORKER_LANES if role is WORKER else
    _MANAGER_LANES`` — a two-branch ternary. Adding RUNNER to that shape would
    have silently handed it the MANAGER lane set (``("inbox",)``), i.e. a
    publish-only principal subscribed to a coordination inbox. Assert every
    enum member resolves through the table, so the fall-through cannot return.
    """
    for role in SubscriberRole:
        lanes = lanes_for_role(role)
        if is_publish_only(role):
            assert lanes == (), role
        else:
            assert lanes, role


def test_publish_only_role_declaring_lanes_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registry/role disagreement must raise, never silently subscribe.

    Guards the case where someone adds a lane to ``CODE_ROLE_LANES["runner"]``
    ("just so the runner can see its own job") while it remains in
    ``CODE_PUBLISH_ONLY_ROLES`` — the two sources of truth would disagree and
    the weaker one must not win.
    """
    import fleet_subscriber.role as role_module

    monkeypatch.setitem(role_module._ROLE_LANES, SubscriberRole.RUNNER, ("dispatch",))
    with pytest.raises(ValueError, match="publish-only but declares lanes"):
        lanes_for_role(SubscriberRole.RUNNER)
