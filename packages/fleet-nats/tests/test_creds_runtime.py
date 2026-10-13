"""Runtime guard: the .creds auth path is actually installable and importable.

Regression guard for fleet-bus #104. The recommended default credential
mechanism is a NATS ``.creds`` file passed via ``user_credentials``. nats-py
only pulls in the ``nkeys`` signing dependency through its ``nkeys`` extra, so a
plain ``nats-py>=2.15`` dependency resolves an environment where a real
``user_credentials`` connect dies with ``ModuleNotFoundError: nkeys``. The
pyproject dependency therefore declares ``nats-py[nkeys]>=2.15``; these tests
assert the extra is present in the shipped environment.

Fully off-network — no live NATS server required.
"""

from __future__ import annotations

import importlib.util

from nats.aio.client import Client


def test_nkeys_module_is_importable() -> None:
    """``nkeys`` must resolve in the installed env, or the .creds path is dead.

    Guards the #104 regression: without the ``nkeys`` extra this import raises
    ModuleNotFoundError and any real ``user_credentials`` connect fails.
    """
    spec = importlib.util.find_spec("nkeys")
    assert spec is not None, (
        "nkeys is not importable — nats-py must be installed with its 'nkeys' "
        "extra (nats-py[nkeys]) for .creds / user_credentials auth to work"
    )
    # find_spec resolving proves the module is loadable without importing it
    # (which would pull an untyped module into mypy's strict analysis).
    assert spec.loader is not None


def test_user_credentials_entry_point_is_reachable() -> None:
    """nats-py's connect surface exposes the ``user_credentials`` parameter.

    Cheap reachability check for the recommended .creds entry point — no
    network, just that the kwarg the bus splats into ``connect`` still exists.
    """
    assert "user_credentials" in Client.connect.__code__.co_varnames
