"""Contract tests for fleet_core.config (promoted from fleet-nats, C3 / #168).

fleet-core owns the one connection contract. These tests pin the invariants the
promotion MUST preserve — env names/defaults, the prod-URL staging tripwire,
credential precedence/validation, secret redaction, and fail-loud behaviour —
exercised through the fleet_core import path (C6 later adds fleet-nats re-exports).
The exhaustive canonicalization matrix lives in fleet-nats' own suite; here we
prove the contract behaves identically once owned by fleet-core.
"""

from __future__ import annotations

import pytest

from fleet_core import AuthCredentials, BusConfig, CredentialSource
from fleet_core.config import _DEFAULT_ENV, _DEFAULT_URL, redact_url

_PROD_URL = "nats://192.0.2.74:4222"  # the live fleet hub (fleet-hub)
_STAGING_URL = "nats://192.0.2.99:4222"  # a distinct box, not the prod hub
_SECRET = "s3cr3t-pw"  # noqa: S105 — test fixture password, not a real credential


# --- defaults / basic validation -------------------------------------------


def test_defaults_valid() -> None:
    cfg = BusConfig(agent_id="nagatha")
    assert cfg.url == _DEFAULT_URL
    assert cfg.env == _DEFAULT_ENV == "dev"
    assert cfg.credential_source is CredentialSource.NONE


def test_rejects_bad_url_scheme() -> None:
    with pytest.raises(ValueError, match="nats://"):
        BusConfig(agent_id="x", url="http://nope")


def test_bad_url_scheme_error_redacts_userinfo_password() -> None:
    with pytest.raises(ValueError) as excinfo:
        BusConfig(agent_id="x", url=f"http://user:{_SECRET}@host.example:4222")
    assert _SECRET not in str(excinfo.value)
    assert "user:***@" in str(excinfo.value)


def test_schemeless_bad_url_scheme_error_redacts_userinfo_password() -> None:
    with pytest.raises(ValueError) as excinfo:
        BusConfig(agent_id="x", url=f"user:{_SECRET}@host.example:4222")
    assert _SECRET not in str(excinfo.value)
    assert "user:***@" in str(excinfo.value)


def test_rejects_bad_env() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        BusConfig(agent_id="x", env="qa")


def test_rejects_empty_agent() -> None:
    with pytest.raises(ValueError):
        BusConfig(agent_id="")


# --- staging isolation tripwire (fleet-bus #105) ---------------------------


def test_staging_on_separate_server_ok() -> None:
    cfg = BusConfig(agent_id="drill", env="staging", url=_STAGING_URL)
    assert cfg.env == "staging"


@pytest.mark.parametrize(
    "prod_variant",
    [
        _PROD_URL,
        "nats://192.0.2.74",  # portless → :4222
        "nats://3221226058:4222",  # decimal dword spelling
        "nats://user:pass@192.0.2.74:4222",  # userinfo cannot mask the host
        "nats://[::1]:4222",  # IPv6 loopback failure domain
    ],
)
def test_staging_on_prod_url_fails_loud(prod_variant: str) -> None:
    # The tripwire canonicalizes: no rewrite of a prod endpoint slips a "staging"
    # label onto prod through the guard.
    with pytest.raises(ValueError, match="KNOWN PROD server"):
        BusConfig(agent_id="drill", env="staging", url=prod_variant)


def test_leading_zero_octet_fails_loud_every_env() -> None:
    # Ambiguous octal-vs-decimal host is rejected for every env, not just staging.
    with pytest.raises(ValueError, match="leading zero"):
        BusConfig(agent_id="x", env="dev", url="nats://0192.0.2.74:4222")


# --- credential precedence / validation ------------------------------------


def test_credential_source_none_by_default() -> None:
    assert BusConfig(agent_id="x").credential_source is CredentialSource.NONE


def test_user_password_source_and_resolution() -> None:
    cfg = BusConfig(agent_id="x", nats_user="u", nats_password="p")
    assert cfg.credential_source is CredentialSource.USER_PASSWORD
    creds = cfg.auth_credentials()
    assert creds == AuthCredentials(user="u", password="p")


def test_creds_file_wins_over_user_password(tmp_path) -> None:  # type: ignore[no-untyped-def]
    creds_file = tmp_path / "agent.creds"
    creds_file.write_text("-----BEGIN NATS USER JWT-----\n")
    cfg = BusConfig(
        agent_id="x",
        nats_user="u",
        nats_password="p",
        nats_creds_file=str(creds_file),
    )
    assert cfg.credential_source is CredentialSource.CREDS_FILE
    assert cfg.auth_credentials() == AuthCredentials(user_credentials=str(creds_file))


def test_half_configured_user_password_fails_loud() -> None:
    with pytest.raises(ValueError, match="both nats_user and nats_password"):
        BusConfig(agent_id="x", nats_user="u")  # password missing


def test_missing_creds_file_fails_loud_without_leaking_path() -> None:
    secret_path = "/deploy/secret/layout/agent.creds"
    with pytest.raises(ValueError) as excinfo:
        BusConfig(agent_id="x", nats_creds_file=secret_path)
    # The resolved path (which encodes deployment layout) must NOT appear.
    assert secret_path not in str(excinfo.value)


# --- secret redaction (structural, never a value) --------------------------


def test_authcredentials_repr_never_leaks_values() -> None:
    creds = AuthCredentials(user="u", password="hunter2", user_credentials="/x.creds")
    text = repr(creds)
    assert "hunter2" not in text
    assert "/x.creds" not in text
    assert "set" in text and "unset" not in text  # all three fields set


def test_busconfig_repr_never_leaks_secret_or_creds_path() -> None:
    cfg = BusConfig(agent_id="x", nats_user="u", nats_password="hunter2")
    text = repr(cfg)
    assert "hunter2" not in text  # password is repr=False


# --- URL userinfo redaction (repr + both URL-bearing exceptions) -----------


def test_redact_url_strips_password_keeps_host_port() -> None:
    out = redact_url(f"nats://user:{_SECRET}@host.example:4222")
    assert _SECRET not in out
    assert out == "nats://user:***@host.example:4222"  # host/port preserved


def test_redact_url_passthrough_and_bare_user() -> None:
    # Nothing secret to hide: unchanged.
    assert redact_url("nats://host:4222") == "nats://host:4222"
    assert redact_url("nats://user@host:4222") == "nats://user@host:4222"
    assert redact_url("host:4222") == "host:4222"
    assert redact_url("user@host:4222") == "user@host:4222"


def test_redact_url_schemeless_userinfo_password_fails_closed() -> None:
    out = redact_url(f"user:{_SECRET}@host.example:4222")
    assert _SECRET not in out
    assert out == "user:***@host.example:4222"


def test_redact_url_raw_slash_password_fails_closed() -> None:
    # A raw '/' in the password makes urlsplit().password read None (fail-open);
    # the string-authority helper must still redact it. Preserves IPv6 brackets too.
    assert _SECRET not in redact_url(f"nats://user:p/a{_SECRET}@[::1]:4222")


def test_busconfig_repr_redacts_url_userinfo_password() -> None:
    # A URL-embedded password must not leak through repr(cfg)/str(cfg)/f-string.
    cfg = BusConfig(agent_id="x", env="dev", url=f"nats://user:{_SECRET}@192.0.2.99:4222")
    text = repr(cfg)
    assert _SECRET not in text
    assert "192.0.2.99:4222" in text  # host/port context preserved
    assert "user:***@" in text


def test_staging_on_prod_url_error_redacts_userinfo_password() -> None:
    # The staging-on-prod fail-loud error echoes the URL — it must not leak a
    # userinfo password embedded in a prod URL.
    with pytest.raises(ValueError) as excinfo:
        BusConfig(agent_id="x", env="staging", url=f"nats://user:{_SECRET}@192.0.2.74:4222")
    assert _SECRET not in str(excinfo.value)
    assert "KNOWN PROD server" in str(excinfo.value)


def test_from_env_overridden_url_error_redacts_userinfo_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The "FLEET_ENV must be set" fail-loud error echoes FLEET_NATS_URL — a
    # userinfo password in that env value must not leak into the message.
    monkeypatch.setenv("FLEET_NATS_URL", f"nats://user:{_SECRET}@192.0.2.99:4222")
    monkeypatch.delenv("FLEET_ENV", raising=False)
    with pytest.raises(ValueError) as excinfo:
        BusConfig.from_env(agent_id="x")
    assert _SECRET not in str(excinfo.value)
    assert "FLEET_ENV must be set" in str(excinfo.value)


def test_from_env_schemeless_overridden_url_error_redacts_userinfo_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_NATS_URL", f"user:{_SECRET}@192.0.2.99:4222")
    monkeypatch.delenv("FLEET_ENV", raising=False)
    with pytest.raises(ValueError) as excinfo:
        BusConfig.from_env(agent_id="x")
    assert _SECRET not in str(excinfo.value)
    assert "user:***@" in str(excinfo.value)
    assert "FLEET_ENV must be set" in str(excinfo.value)


# --- from_env: env names / defaults / fail-loud ----------------------------


def test_from_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "FLEET_NATS_URL",
        "FLEET_AGENT_ID",
        "FLEET_ENV",
        "FLEET_NATS_USER",
        "FLEET_NATS_PASSWORD",
        "FLEET_NATS_CREDS",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = BusConfig.from_env()
    assert cfg.url == _DEFAULT_URL
    assert cfg.agent_id == "unknown"
    assert cfg.env == "dev"


def test_from_env_reads_named_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.setenv("FLEET_ENV", "staging")
    monkeypatch.setenv("FLEET_AGENT_ID", "bilby")
    cfg = BusConfig.from_env()
    assert cfg.url == _STAGING_URL
    assert cfg.env == "staging"
    assert cfg.agent_id == "bilby"


def test_from_env_overridden_url_without_env_fails_loud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # MED-4 (#105): a non-default URL with no FLEET_ENV is ambiguous — fail loud.
    monkeypatch.setenv("FLEET_NATS_URL", _STAGING_URL)
    monkeypatch.delenv("FLEET_ENV", raising=False)
    with pytest.raises(ValueError, match="FLEET_ENV must be set"):
        BusConfig.from_env()


def test_from_env_blank_creds_normalise_to_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A blank export must mean "unset", not half-configure user/password.
    monkeypatch.setenv("FLEET_NATS_USER", "")
    monkeypatch.setenv("FLEET_NATS_PASSWORD", "")
    monkeypatch.delenv("FLEET_NATS_URL", raising=False)
    monkeypatch.delenv("FLEET_ENV", raising=False)
    cfg = BusConfig.from_env(agent_id="x")
    assert cfg.credential_source is CredentialSource.NONE
