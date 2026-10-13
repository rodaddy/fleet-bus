"""Functional tests for BusConfig validation and subject building."""

from __future__ import annotations

from typing import Any

import pytest
from fleet_core.config import AuthCredentials as CoreAuthCredentials
from fleet_core.config import BusConfig as CoreBusConfig
from fleet_core.config import CredentialSource as CoreCredentialSource
from fleet_core.config import check_enforcement_config as core_check_enforcement_config
from fleet_core.config import min_catalog_version as core_min_catalog_version
from fleet_core.config import redact_url as core_redact_url

from fleet_nats import subjects
from fleet_nats.config import (
    AuthCredentials,
    BusConfig,
    check_enforcement_config,
    min_catalog_version,
    redact_url,
)


def test_config_import_path_reexports_fleet_core_contract() -> None:
    assert AuthCredentials is CoreAuthCredentials
    assert BusConfig is CoreBusConfig
    assert check_enforcement_config is core_check_enforcement_config
    assert min_catalog_version is core_min_catalog_version
    assert redact_url is core_redact_url


def test_config_credential_source_reexports_fleet_core_contract() -> None:
    from fleet_nats import CredentialSource

    assert CredentialSource is CoreCredentialSource


def test_config_defaults_valid() -> None:
    cfg = BusConfig(agent_id="nagatha")
    assert cfg.url.startswith("nats://")
    assert cfg.env == "dev"


def test_config_rejects_bad_url() -> None:
    with pytest.raises(ValueError):
        BusConfig(agent_id="x", url="http://nope")


def test_config_rejects_bad_env() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        BusConfig(agent_id="x", env="qa")  # not in {dev, prod, staging}


def test_config_rejects_empty_agent() -> None:
    with pytest.raises(ValueError):
        BusConfig(agent_id="")


# --- staging env / separate-server isolation (fleet-bus #105) ---------------

_STAGING_URL = "nats://192.0.2.99:4222"  # a distinct box, not the prod hub
_PROD_URL = "nats://192.0.2.74:4222"  # the live fleet hub (fleet-hub)


def test_config_staging_env_is_valid_with_separate_server() -> None:
    # staging is a first-class env AS LONG AS it points at a separate server.
    cfg = BusConfig(agent_id="drill", env="staging", url=_STAGING_URL)
    assert cfg.env == "staging"
    assert cfg.url == _STAGING_URL


def test_config_staging_on_prod_url_fails_loud() -> None:
    # The rejected anti-pattern: FLEET_ENV=staging while pointed at prod is
    # label-without-isolation — it must fail loud, never silently ship.
    with pytest.raises(ValueError, match="KNOWN PROD server"):
        BusConfig(agent_id="drill", env="staging", url=_PROD_URL)


@pytest.mark.parametrize(
    "prod_variant",
    [
        "nats://192.0.2.74:4222",
        "nats://192.0.2.74:4222/",  # trailing slash
        "nats://127.0.0.1:4222",  # loopback tunnel shorthand
        "nats://localhost:4222",  # localhost tunnel shorthand
        "nats://LocalHost:4222",  # host casing
        # --- canonicalizing guard (#105 P1): every numeric/portless spelling of a
        # prod endpoint must be caught, not just the literal typo ---
        "nats://192.0.2.74",  # PORTLESS — nats-py defaults to :4222 → prod
        "nats://127.0.0.1",  # portless loopback
        "nats://localhost",  # portless localhost
        "nats://3221226058:4222",  # decimal dword spelling of 192.0.2.74
        "nats://3221226058",  # portless decimal dword
        "nats://0xc000024a:4222",  # hex spelling of 192.0.2.74
        "nats://0xc0.0x00.0x02.0x4a:4222",  # dotted-hex spelling
        "nats://[::ffff:192.0.2.74]:4222",  # IPv4-mapped IPv6
        "nats://[::ffff:192.0.2.74]",  # portless IPv4-mapped IPv6
        "nats://[::1]:4222",  # IPv6 loopback
        "nats://[::1]",  # portless IPv6 loopback
        "nats://192.0.2.74.:4222",  # trailing FQDN dot WITH a port present (P2-3)
        "nats://localhost.:4222",  # FQDN dot on a name, port present
    ],
)
def test_config_staging_guard_is_not_bypassable_by_url_rewrite(prod_variant: str) -> None:
    # The guard canonicalizes: no rewrite of a prod URL — portless, numeric
    # (dword/hex/dotted-hex), IPv4-mapped IPv6, IPv6 loopback, or a trailing FQDN
    # dot beside a port — slips staging onto the prod box via the staging guard.
    with pytest.raises(ValueError, match="KNOWN PROD server"):
        BusConfig(agent_id="drill", env="staging", url=prod_variant)


@pytest.mark.parametrize(
    "creds_prod",
    [
        "nats://user:pass@192.0.2.74:4222",  # userinfo cannot mask the prod host
        "nats://token@127.0.0.1:4222",  # user-only userinfo, loopback
        "nats://a:b@LocalHost:4222/",  # userinfo + host casing + trailing slash
    ],
)
def test_config_staging_guard_strips_userinfo(creds_prod: str) -> None:
    # A credentialled prod URL must NOT slip a "staging" label onto prod — the
    # guard compares host:port after dropping user[:pass]@ (critical self-check:
    # NATS URLs can carry creds; redact_url proves the repo knows this).
    with pytest.raises(ValueError, match="KNOWN PROD server"):
        BusConfig(agent_id="drill", env="staging", url=creds_prod)


def test_config_staging_guard_is_not_dns_aware_documented_limit() -> None:
    # DOCUMENTED LIMITATION: a hostname that resolves to the prod IP but is not a
    # known literal is NOT caught (no DNS in config construction). This test pins
    # that behaviour so a future reader knows it's intentional, not an oversight —
    # add the DNS name to _PROD_ENDPOINTS_RAW (and the drill's `case` list) if one
    # is adopted, or the guard goes blind to it.
    cfg = BusConfig(agent_id="drill", env="staging", url="nats://fleet-hub.example:4222")
    assert cfg.env == "staging"  # passes: a name is not a known literal


def test_config_staging_guard_is_a_tripwire_not_isolation_dns_alias() -> None:
    # HIGH-1 (fleet-bus #105): a DNS alias that resolves to the prod IP constructs
    # fine — the guard canonicalizes IP LITERALS only, never resolving DNS. This
    # pins that the literal-IP guard is DEFENSE-IN-DEPTH against the common typo,
    # NOT a fail-closed isolation boundary. True isolation = distinct staging
    # credentials (HIGH-2 / docs). If this ever starts RAISING, the guard grew a
    # DNS resolution it must not have.
    cfg = BusConfig(agent_id="drill", env="staging", url="nats://fleet-hub.example.internal:4222")
    assert cfg.env == "staging"


@pytest.mark.parametrize(
    "leading_zero_prod",
    [
        "nats://0192.0.2.74:4222",  # leading-zero first octet
        "nats://192.00.2.74:4222",  # leading-zero middle octet
    ],
)
def test_config_staging_leading_zero_octet_is_rejected_as_malformed(
    leading_zero_prod: str,
) -> None:
    # HIGH-1 P2 (fleet-bus #105): a dotted-octet form with a LEADING ZERO reads as
    # octal on glibc/BSD but may read as decimal elsewhere — the guard/dialer
    # equivalence does NOT hold cross-platform. Worse, the OS resolver still maps
    # ``0192.0.2.74`` / ``192.00.2.74`` to prod 192.0.2.74, so silently
    # accepting the form would slip a "staging" label onto the prod box through the
    # guard's blind spot. We FAIL LOUD, naming the offending malformed host, rather
    # than construct.
    with pytest.raises(ValueError, match="malformed host"):
        BusConfig(agent_id="drill", env="staging", url=leading_zero_prod)


@pytest.mark.parametrize(
    "malformed_prod",
    [
        "  nats://192.0.2.74:4222  ",  # surrounding whitespace
        "NATS://192.0.2.74:4222",  # scheme casing (nats-py wants lowercase nats://)
    ],
)
def test_config_staging_malformed_prod_url_still_refused(malformed_prod: str) -> None:
    # These prod-pointing variants are rejected EARLIER by the nats:// URL
    # validator, not the staging guard — but the outcome that matters holds:
    # a mislabeled staging-on-prod connection is REFUSED, never constructed.
    with pytest.raises(ValueError):
        BusConfig(agent_id="drill", env="staging", url=malformed_prod)


def test_config_prod_env_on_prod_url_is_allowed() -> None:
    # The guard is staging-only: prod ON prod is exactly correct, never blocked.
    cfg = BusConfig(agent_id="nagatha", env="prod", url=_PROD_URL)
    assert cfg.env == "prod"


def test_config_staging_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.setenv("FLEET_ENV", "staging")
    cfg = BusConfig.from_env()
    assert (cfg.env, cfg.url) == ("staging", _STAGING_URL)


def test_config_staging_from_env_on_prod_url_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # from_env must enforce the same guard — the env-driven path is the real one.
    monkeypatch.setenv("FLEET_NATS_URL", _PROD_URL)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.setenv("FLEET_ENV", "staging")
    with pytest.raises(ValueError, match="KNOWN PROD server"):
        BusConfig.from_env()


def test_config_from_env_overridden_url_missing_env_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MED-4 (fleet-bus #105): a staging EnvironmentFile that overrides the URL but
    # omits FLEET_ENV must NOT silently default to dev.* subjects — fail loud,
    # naming FLEET_ENV, so the ambiguous deployment state is caught at construction.
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.delenv("FLEET_ENV", raising=False)
    with pytest.raises(ValueError, match="FLEET_ENV must be set explicitly"):
        BusConfig.from_env()


def test_config_from_env_overridden_url_blank_env_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MED-4: a BLANK FLEET_ENV (present-but-empty line in the EnvironmentFile) is
    # just as ambiguous as a missing one — also fail loud.
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.setenv("FLEET_ENV", "   ")  # whitespace-only counts as blank
    with pytest.raises(ValueError, match="FLEET_ENV must be set explicitly"):
        BusConfig.from_env()


def test_config_from_env_overridden_url_case_mismatched_env_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MED-4: a case-mismatched FLEET_ENV (e.g. "Staging") is non-blank, so it
    # passes the explicit-env check, but it is not a valid env token — it must
    # still fail loud via the _VALID_ENVS guard (envs are lowercase, not folded),
    # never silently starting on a bogus subject prefix.
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.setenv("FLEET_ENV", "Staging")
    with pytest.raises(ValueError, match="must be one of"):
        BusConfig.from_env()


def test_config_from_env_default_url_missing_env_still_defaults_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MED-4 is scoped to OVERRIDDEN urls: on the DEFAULT url a missing FLEET_ENV
    # keeps the historical dev default (no deployment ambiguity — it's the shared
    # dev box), so we do not break the common local/dev path.
    monkeypatch.delenv("FLEET_NATS_URL", raising=False)
    monkeypatch.setenv("FLEET_AGENT_ID", "drill")
    monkeypatch.delenv("FLEET_ENV", raising=False)
    cfg = BusConfig.from_env()
    assert cfg.env == "dev"


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_NATS_URL", "nats://10.0.0.1:4222")
    monkeypatch.setenv("FLEET_AGENT_ID", "bilby")
    monkeypatch.setenv("FLEET_ENV", "prod")
    cfg = BusConfig.from_env()
    assert cfg.url == "nats://10.0.0.1:4222"
    assert cfg.agent_id == "bilby"
    assert cfg.env == "prod"


def test_config_from_env_explicit_id_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "env-id")
    cfg = BusConfig.from_env(agent_id="explicit")
    assert cfg.agent_id == "explicit"


def test_min_catalog_version_defaults_to_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset means enforcement OFF — the bootstrap posture (#103)."""
    monkeypatch.delenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", raising=False)
    assert min_catalog_version() == 0


def test_min_catalog_version_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", "4")
    assert min_catalog_version() == 4


def test_min_catalog_version_rejects_junk(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo'd floor must fail loud, never silently disable the safety floor."""
    monkeypatch.setenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", "notanint")
    with pytest.raises(ValueError, match="must be an integer"):
        min_catalog_version()


def test_min_catalog_version_rejects_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", "-1")
    with pytest.raises(ValueError, match="non-negative"):
        min_catalog_version()


def test_check_enforcement_config_floor_without_registry_url_raises() -> None:
    """F2: a positive floor with no registry URL is the bootstrap trap — a floor
    that can only ever refuse to start. Must raise at startup naming BOTH vars."""
    with pytest.raises(ValueError) as exc:
        check_enforcement_config(min_version=3, registry_url_set=False)
    msg = str(exc.value)
    assert "FLEET_SUBJECT_CATALOG_MIN_VERSION" in msg
    assert "FLEET_SUBJECT_REGISTRY_URL" in msg


def test_check_enforcement_config_floor_with_registry_url_ok() -> None:
    """A floor WITH a registry URL is the valid enforcement posture — no raise."""
    check_enforcement_config(min_version=3, registry_url_set=True)


def test_check_enforcement_config_both_unset_warns_not_raises() -> None:
    """F2: both unset is the valid bootstrap posture (enforcement off) — it must
    NOT raise, but it logs a WARNING that the baked-in catalog is in use."""
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
        filter="fleet_core.config",
    )
    try:
        check_enforcement_config(min_version=0, registry_url_set=False)
    finally:
        logger.remove(sink_id)
    warnings = [msg for level, msg in records if level == "WARNING"]
    assert any("both unset" in msg and "enforcement OFF" in msg for msg in warnings), records


def test_check_enforcement_config_no_floor_with_url_is_quiet() -> None:
    """A registry URL with no floor (registry opt-in, enforcement off) is valid
    and NOT the both-unset case — no WARNING about the baked-in catalog."""
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
        filter="fleet_core.config",
    )
    try:
        check_enforcement_config(min_version=0, registry_url_set=True)
    finally:
        logger.remove(sink_id)
    assert not [msg for level, msg in records if level == "WARNING"], records


def test_subjects_mirror_dash_convention() -> None:
    assert subjects.agent_dispatch("dev", "skippy") == "dev.agent.dispatch.skippy"
    assert subjects.agent_status("prod", "t_123") == "prod.agent.status.t_123"
    assert subjects.agent_inbox("dev", "nagatha") == "dev.agent.inbox.nagatha"


def test_subjects_carry_staging_env_prefix() -> None:
    # #105: the {env} token flows cleanly — a staging client builds staging
    # subjects with no code change to subjects.py (registry templates too).
    assert subjects.agent_dispatch("staging", "skippy") == "staging.agent.dispatch.skippy"
    assert subjects.agent_inbox("staging", "nagatha") == "staging.agent.inbox.nagatha"
    assert subjects.wildcard("staging", "agent") == "staging.agent.>"


def test_subject_per_channel_isolation() -> None:
    s = subjects.agent_dispatch("dev", "bilby", channel="all-agents")
    assert s == "dev.agent.all-agents.dispatch.bilby"


def test_subject_slugging_normalises_tokens() -> None:
    # dots and spaces in tokens must not create extra subject levels
    assert subjects.agent_dispatch("dev", "Sk ippy") == "dev.agent.dispatch.sk-ippy"
    assert subjects.direct("dev", "Rico.Local") == "dev.direct.rico-local"


def test_layer_wildcards() -> None:
    assert subjects.wildcard("dev", "agent") == "dev.agent.>"
    assert subjects.wildcard("prod", "kboard") == "prod.kboard.>"


def _nats_match(pattern: str, subject: str) -> bool:
    """Minimal NATS matcher: per-token, '*' = exactly one token (no '>' needed here)."""
    pt, st = pattern.split("."), subject.split(".")
    return len(pt) == len(st) and all(p in ("*", s) for p, s in zip(pt, st, strict=True))


def test_dispatch_wildcard_matches_per_channel_publish_form() -> None:
    # REGRESSION (fleet-hub dummy find): the orchestrator PUBLISHES dispatches on
    # the per-channel form, which has one more token than the bare form — so a
    # bare-form subscription alone never receives a real coordinated dispatch.
    # The wildcard must match the publish form; the bare form covers channel-less.
    wildcard = subjects.agent_dispatch_wildcard("dev", "skippy")
    assert wildcard == "dev.agent.*.dispatch.skippy"
    per_channel = subjects.agent_dispatch("dev", "skippy", channel="all-agents")
    bare = subjects.agent_dispatch("dev", "skippy")
    assert _nats_match(wildcard, per_channel)
    assert not _nats_match(wildcard, bare)  # token counts differ — both forms needed
    assert not _nats_match(wildcard, subjects.agent_dispatch("dev", "bilby", channel="c"))


def test_hub_control_subject_and_wildcard() -> None:
    # The control plane (#59): a per-task control lane + a hub-wide wildcard.
    assert subjects.hub_control("dev", "task-1") == "dev.hub.control.task-1"
    assert subjects.hub_control("prod", "T 5.a") == "prod.hub.control.t-5-a"  # slugged
    assert subjects.hub_control_wildcard("dev") == "dev.hub.control.*"


def test_hub_control_wildcard_matches_the_publish_form() -> None:
    wildcard = subjects.hub_control_wildcard("dev")
    assert _nats_match(wildcard, subjects.hub_control("dev", "task-1"))
    assert _nats_match(wildcard, subjects.hub_control("dev", "task-2"))
    assert not _nats_match(wildcard, subjects.agent_status("dev", "task-1"))  # layer-isolated


def test_slug_rejects_empty_normalisation() -> None:
    # A whitespace-only token must fail loud, not publish an invalid subject
    # like "dev.agent..dispatch.w" the server would reject (message lost).
    with pytest.raises(ValueError, match="normalises to empty"):
        subjects.agent_dispatch("dev", "skippy", channel="   ")
    with pytest.raises(ValueError, match="normalises to empty"):
        subjects.agent_status("dev", "")


def test_slug_rejects_the_multi_token_wildcard() -> None:
    """A token carrying ``>`` is subject-token injection — reject it (#222).

    ``slug`` only ever produces ONE subject token, and ``>`` is never a
    legitimate one: wildcards in this module are structural (the ``STAR``
    constant, or a literal trailing ``.>`` in the builder). This bites hardest on
    ``factory_runner``, whose ``job_id`` can derive from repository content a
    runner box is processing — and whose ``{env}.factory.runner.*`` publish grant
    still ACCEPTS a literal ``>`` token, so the server does not catch it.
    """
    for bad in (">", "a>b", "job->1"):
        with pytest.raises(ValueError, match="multi-token wildcard"):
            subjects.factory_runner("prod", bad)
    # Every builder is covered, not just the new one.
    with pytest.raises(ValueError, match="multi-token wildcard"):
        subjects.agent_status("dev", ">")
    with pytest.raises(ValueError, match="multi-token wildcard"):
        subjects.kboard_event("dev", "claimed", ">")


def test_slug_still_allows_star_for_grant_patterns() -> None:
    """``*`` must keep working: callers build authz grant patterns with STAR.

    Unlike ``>`` it cannot widen a subject past a single token, and
    ``tests/test_authz_conf.py`` builds every expected allow-list entry by
    passing ``subjects.STAR`` as a token.
    """
    assert subjects.factory_runner("prod", subjects.STAR) == "prod.factory.runner.*"
    assert subjects.agent_dispatch("dev", subjects.STAR) == "dev.agent.dispatch.*"


def test_a_star_channel_collides_with_the_subscription_wildcard() -> None:
    """Pin the collision that makes ``*`` unsafe as a PUBLISHED channel token.

    ``slug`` must keep permitting ``*`` (grant patterns need it), so the shape
    below is buildable by design — which is exactly why the publish boundary,
    not the builder, has to refuse it.
    """
    assert subjects.agent_status("dev", "task-1", channel=subjects.STAR) == (
        subjects.agent_status_wildcard("dev", "task-1")
    )


def test_assert_publishable_refuses_wildcards_but_passes_concrete_subjects() -> None:
    """The publish-boundary guard: wildcards raise, concrete subjects pass through."""
    for bad in (
        subjects.agent_status("dev", "task-1", channel=subjects.STAR),
        subjects.agent_status_wildcard("dev", "task-1"),
        subjects.agent_relay("dev", "skippy", channel=subjects.STAR),
        subjects.factory_runner("prod", subjects.STAR),
        "dev.agent.>",
    ):
        with pytest.raises(ValueError, match="wildcard-bearing subject"):
            subjects.assert_publishable(bad)

    for good in (
        subjects.agent_status("dev", "task-1"),
        subjects.agent_status("dev", "task-1", channel="dash-dev"),
        subjects.ob_context_pack("dev"),
    ):
        assert subjects.assert_publishable(good) == good


# --- AuthCredentials must be structurally secret-safe (fleet-bus #104) ---

# Canary built at runtime so secret scanners never see a password-shaped literal.
_SECRET_PASSWORD = "-".join(("zz", "repr", "canary", "zz"))  # noqa: S105 — synthetic marker
_SECRET_CREDS_PATH = "/etc/fleet/agents/nagatha-secret-layout.creds"


def test_auth_credentials_repr_redacts_password() -> None:
    auth = AuthCredentials(user="nagatha", password=_SECRET_PASSWORD)
    for rendered in (repr(auth), str(auth), f"{auth}"):
        assert _SECRET_PASSWORD not in rendered
        assert "set" in rendered  # field state surfaced, value hidden


def test_auth_credentials_repr_redacts_creds_path() -> None:
    auth = AuthCredentials(user_credentials=_SECRET_CREDS_PATH)
    for rendered in (repr(auth), str(auth), f"{auth}"):
        assert _SECRET_CREDS_PATH not in rendered
        assert "/etc/fleet" not in rendered  # no substring of the path leaks


def test_auth_credentials_repr_names_fields_and_states() -> None:
    auth = AuthCredentials(user="nagatha", password=_SECRET_PASSWORD)
    rendered = repr(auth)
    # Field NAMES are safe to surface; only VALUES are redacted.
    assert "user=set" in rendered
    assert "password=set" in rendered
    assert "user_credentials=unset" in rendered


def test_auth_credentials_attribute_access_and_equality() -> None:
    auth = AuthCredentials(user="nagatha", password=_SECRET_PASSWORD)
    # bus.py reads these attributes by name — access must stay identical.
    assert auth.user == "nagatha"
    assert auth.password == _SECRET_PASSWORD
    assert auth.user_credentials is None
    # Value-based equality still holds (frozen dataclass), and it is immutable.
    assert auth == AuthCredentials(user="nagatha", password=_SECRET_PASSWORD)
    assert auth != AuthCredentials()
    with pytest.raises((AttributeError, TypeError)):
        auth.password = "mutated"  # type: ignore[misc]


def test_require_auth_typo_raises_naming_var(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typo must fail loud, not silently disable the operator's auth request.
    monkeypatch.setenv("FLEET_AGENT_ID", "nagatha")
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", "treu")
    with pytest.raises(ValueError, match="FLEET_NATS_REQUIRE_AUTH"):
        BusConfig.from_env()


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", "on", " True "])
def test_require_auth_accepted_true_spellings(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "nagatha")
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", raw)
    assert BusConfig.from_env().require_auth is True


@pytest.mark.parametrize("raw", ["0", "false", "FALSE", "No", "off", " False "])
def test_require_auth_accepted_false_spellings(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "nagatha")
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", raw)
    assert BusConfig.from_env().require_auth is False


def test_require_auth_unset_preserves_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "nagatha")
    monkeypatch.delenv("FLEET_NATS_REQUIRE_AUTH", raising=False)
    assert BusConfig.from_env().require_auth is False


def test_require_auth_empty_preserves_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "nagatha")
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", "   ")
    assert BusConfig.from_env().require_auth is False


# --- OB context-pack builder (fleet-bus #221) --------------------------------


def test_ob_context_pack_matches_the_live_typescript_mirror() -> None:
    """The builder must be byte-identical to OB's live TS ``obContextPackSubject``.

    OB is ALREADY on this lane in production, so this is a wire-compat assertion,
    not a design choice: the TS mirror builds
    ``{slug(env)}.ob.memory.context_pack``. If this ever changes, the Python and
    TS sides stop meeting on the wire.

    Note the UNDERSCORE in ``context_pack`` — it is a fixed literal tail, NOT a
    slugged token (the point is the tail is not caller-derived).
    """
    assert subjects.ob_context_pack("dev") == "dev.ob.memory.context_pack"
    assert subjects.ob_context_pack("prod") == "prod.ob.memory.context_pack"
    assert subjects.ob_context_pack("staging") == "staging.ob.memory.context_pack"


def test_ob_context_pack_slugs_the_env_token() -> None:
    """The env token IS slugged here — parity with the TS mirror's slug helper.

    The TS mirror yields ``staging-lab.ob.…`` for ``"Staging Lab"``, so the
    Python builder must agree. This differs from the ``{env}.agent.…`` builders,
    which interpolate env raw — the divergence is deliberate and documented.
    """
    assert subjects.ob_context_pack("Staging Lab") == "staging-lab.ob.memory.context_pack"
    assert subjects.ob_context_pack("Prod.US") == "prod-us.ob.memory.context_pack"


def test_ob_context_pack_rejects_an_empty_env_token() -> None:
    """An env that normalises to empty must fail loud, exactly like the TS mirror.

    ``.ob.memory.context_pack`` (leading empty token) is an invalid NATS subject
    the server rejects — the message would be lost far from its cause.
    """
    for empty in ("", "   ", "\t"):
        with pytest.raises(ValueError, match="normalises to empty"):
            subjects.ob_context_pack(empty)


def test_ob_context_pack_rejects_the_multi_token_wildcard() -> None:
    """``>`` in the env token is subject-token injection — rejected like everywhere."""
    with pytest.raises(ValueError, match="multi-token wildcard"):
        subjects.ob_context_pack(">")


def test_ob_context_pack_lives_under_the_granted_ob_layer() -> None:
    """The subject must fall inside the ``{env}.ob.>`` grant the OB user holds.

    ``deploy/nats/fleet-prod-nats-server.conf`` grants that user publish+subscribe
    on ``dev.ob.>`` / ``prod.ob.>`` and nothing else, so a builder emitting
    anything outside that prefix would be refused by the server at runtime.
    """
    for env in ("dev", "prod"):
        assert subjects.ob_context_pack(env).startswith(f"{env}.ob.")
        # The layer wildcard that backs the grant covers it (drop the trailing '>').
        assert subjects.ob_context_pack(env).startswith(subjects.wildcard(env, "ob")[:-1])


# --- noise lanes (fleet-bus #116) --------------------------------------------


def test_noise_lanes_are_distinct_from_the_status_lane() -> None:
    """Acks/telemetry must NOT collide with the coordination status lane.

    This is the whole point of #116: a Discord bridge decides what to forward
    from the SUBJECT. If a noise subject were equal to the status subject, no
    bridge could tell them apart without parsing prose.
    """
    ack = subjects.agent_ack("dev", "t-1")
    telemetry = subjects.agent_telemetry("dev", "t-1")
    status = subjects.agent_status("dev", "t-1")
    assert len({ack, telemetry, status}) == 3
    assert ack == "dev.agent.ack.t-1"
    assert telemetry == "dev.agent.telemetry.t-1"


def test_noise_lanes_are_not_matched_by_a_status_subscription() -> None:
    """A status subscriber (bare OR channel wildcard) receives ZERO noise.

    The acceptance criterion for #116 stated as a routing fact: whatever a
    status-lane consumer subscribes, no ack/telemetry subject can match it.
    """
    for task in ("t-1", "t-2"):
        for channel in (None, "dash-dev"):
            for noise in (
                subjects.agent_ack("dev", task, channel=channel),
                subjects.agent_telemetry("dev", task, channel=channel),
            ):
                assert not _nats_match(subjects.agent_status("dev", task), noise)
                assert not _nats_match(subjects.agent_status_wildcard("dev", task), noise)


def test_noise_lanes_are_channel_scopable() -> None:
    """Both noise lanes take the same channel axis as the coordination lanes."""
    assert subjects.agent_ack("dev", "t-1", channel="dash-dev") == "dev.agent.dash-dev.ack.t-1"
    assert (
        subjects.agent_telemetry("dev", "t-1", channel="dash-dev")
        == "dev.agent.dash-dev.telemetry.t-1"
    )


def test_noise_wildcards_match_only_their_own_scoped_lane() -> None:
    """Each noise wildcard matches its scoped form and nothing from the other lane."""
    ack_wild = subjects.agent_ack_wildcard("dev", "t-1")
    tel_wild = subjects.agent_telemetry_wildcard("dev", "t-1")
    assert ack_wild == "dev.agent.*.ack.t-1"
    assert tel_wild == "dev.agent.*.telemetry.t-1"
    assert _nats_match(ack_wild, subjects.agent_ack("dev", "t-1", channel="dash-dev"))
    assert _nats_match(tel_wild, subjects.agent_telemetry("dev", "t-1", channel="dash-dev"))
    # Cross-lane isolation.
    assert not _nats_match(ack_wild, subjects.agent_telemetry("dev", "t-1", channel="dash-dev"))
    assert not _nats_match(tel_wild, subjects.agent_ack("dev", "t-1", channel="dash-dev"))
    # Task isolation.
    assert not _nats_match(ack_wild, subjects.agent_ack("dev", "t-2", channel="dash-dev"))


# --- channel scoping beyond dispatch (fleet-bus #117) ------------------------


@pytest.mark.parametrize(
    ("builder", "wildcard_builder", "lane"),
    [
        (subjects.agent_dispatch, subjects.agent_dispatch_wildcard, "dispatch"),
        (subjects.agent_status, subjects.agent_status_wildcard, "status"),
        (subjects.agent_relay, subjects.agent_relay_wildcard, "relay"),
        (subjects.agent_wake, subjects.agent_wake_wildcard, "wake"),
        (subjects.agent_ack, subjects.agent_ack_wildcard, "ack"),
        (subjects.agent_telemetry, subjects.agent_telemetry_wildcard, "telemetry"),
    ],
)
def test_every_scopable_lane_has_the_same_two_forms(
    builder: Any, wildcard_builder: Any, lane: str
) -> None:
    """Channel scoping is uniform across lanes — one shape, six lanes (#117).

    Before #117 only ``dispatch`` could be scoped, so a channel-scoped subscriber
    had to fall back to unscoped subjects for every other flow. This pins that
    each lane exposes the SAME bare/scoped/wildcard triple with the channel token
    in the SAME position, which is what makes a scoped subscribe uniform.
    """
    bare = builder("dev", "x")
    scoped = builder("dev", "x", channel="dash-dev")
    wild = wildcard_builder("dev", "x")
    assert bare == f"dev.agent.{lane}.x"
    assert scoped == f"dev.agent.dash-dev.{lane}.x"
    assert wild == f"dev.agent.*.{lane}.x"
    # Per-token matching: the wildcard covers the scoped form, never the bare one.
    assert _nats_match(wild, scoped)
    assert not _nats_match(wild, bare)


@pytest.mark.parametrize(
    ("builder", "lane"),
    [
        (subjects.agent_dispatch, "dispatch"),
        (subjects.agent_status, "status"),
        (subjects.agent_relay, "relay"),
        (subjects.agent_wake, "wake"),
        (subjects.agent_ack, "ack"),
        (subjects.agent_telemetry, "telemetry"),
    ],
)
def test_a_scoped_subscribe_receives_only_its_own_channel(builder: Any, lane: str) -> None:
    """THE #117 acceptance criterion: a scoped subscribe sees only its channel.

    A subscriber scoped to ``dash-dev`` builds the concrete scoped subject. Every
    other channel's publish on the same lane, for the same target, must NOT match
    it — otherwise "channel scoping" would be a label rather than isolation.
    """
    mine = builder("dev", "x", channel="dash-dev")
    assert _nats_match(mine, mine)
    for other in ("dash-prod", "rico-lab", "all-agents"):
        theirs = builder("dev", "x", channel=other)
        assert not _nats_match(mine, theirs), f"{lane}: {other} leaked into dash-dev"


def test_channel_scoping_is_not_access_control() -> None:
    """Lane (scoping) and namespace (access control) are separate axes (#117).

    Scoping narrows WHICH traffic a correctly-configured subscriber attaches to.
    It does NOT restrict WHO may listen: the per-channel wildcard — the shape the
    live server grants use (``dev.agent.*.dispatch.*``) — still matches every
    channel's slice. Access control is the credential grant, not the subject.
    """
    every_channel = subjects.agent_dispatch_wildcard("dev", "skippy")
    for channel in ("dash-dev", "dash-prod", "someone-elses-channel"):
        assert _nats_match(every_channel, subjects.agent_dispatch("dev", "skippy", channel=channel))


def test_scoped_builders_reject_an_empty_channel_token() -> None:
    """An explicitly-empty channel must fail loud on EVERY scopable lane.

    ``dev.agent..status.t-1`` is an invalid subject the server rejects (message
    lost far from the cause) — the same guard ``agent_dispatch`` already had, now
    proven for every lane that gained the axis.
    """
    for builder in (
        subjects.agent_status,
        subjects.agent_relay,
        subjects.agent_wake,
        subjects.agent_ack,
        subjects.agent_telemetry,
    ):
        with pytest.raises(ValueError, match="normalises to empty"):
            builder("dev", "x", channel="   ")


def test_scoped_builders_reject_the_multi_token_wildcard_in_a_channel() -> None:
    """A ``>`` channel token would widen the subject past one token — rejected."""
    for builder in (
        subjects.agent_status,
        subjects.agent_relay,
        subjects.agent_wake,
        subjects.agent_ack,
        subjects.agent_telemetry,
    ):
        with pytest.raises(ValueError, match="multi-token wildcard"):
            builder("dev", "x", channel=">")


def test_channel_is_slugged_in_every_scoped_lane() -> None:
    """A channel with spaces/dots must not create extra subject levels."""
    for builder in (
        subjects.agent_status,
        subjects.agent_relay,
        subjects.agent_wake,
        subjects.agent_ack,
        subjects.agent_telemetry,
        subjects.agent_dispatch,
    ):
        built = builder("dev", "x", channel="Dash.Dev Lab")
        assert built.split(".")[2] == "dash-dev-lab"
        assert len(built.split(".")) == 5  # env.agent.channel.lane.tail — no extra levels
