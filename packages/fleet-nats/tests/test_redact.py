"""Functional tests for the display-time credential scrub (#64).

The contract is redact-on-suspicion: every credential SHAPE goes; a benign false
positive is acceptable, a leak is not. These lock the shapes the fleet worries
about most (webhooks, provider tokens, key=value secrets), prove benign prose
survives, and prove the vendored gitleaks tier actually compiled and catches a
provider token the hand-rolled tier alone would miss.
"""

from __future__ import annotations

import importlib
import tomllib
from importlib import resources

import pytest
from loguru import logger

import fleet_nats.redact as redact_module
from fleet_nats.redact import (
    redact_text,
    vendored_pattern_count,
    vendored_skipped_rule_ids,
    vendored_source_digest,
)

_EXPECTED_SOURCE_DIGEST = "c61d4cf45c27a1eb4a345d191f776f7f264981338ac40ba51a4cbd4aa398e4eb"

_LEAKY = [
    "https://discord.com/api/webhooks/123456789/abcDEF-ghiJKL",
    "https://hooks.slack.com/services/T00/B00/XXXXXXXX",
    "nats://user:supersecretpass@192.0.2.74:4222",
    "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
    "my key is sk-ant-api03-abcdefghijklmnopqrstuv0123456789",
    "gh token ghp_0123456789abcdefghijklmnopqrstuvwx",
    "slack xox" + "b-1234567890-abcdefghijklmnop",  # fake; split so push scanners skip it
    "aws AKIAIOSFODNN7EXAMPLE key",
    "api_key=deadbeefcafebabe1234",
    "password: hunter2hunter2hunter2",
]


@pytest.mark.parametrize("text", _LEAKY)
def test_leaky_shapes_are_redacted(text: str) -> None:
    scrubbed = redact_text(text)
    assert "[REDACTED]" in scrubbed
    # None of the raw secret material survives verbatim.
    for token in (
        "webhooks/123456789",
        "supersecretpass",
        "eyJhbGci",
        "sk-ant-api03",
        "ghp_0123456789",
        "xoxb-1234567890",
        "AKIAIOSFODNN7EXAMPLE",
        "deadbeefcafebabe",
        "hunter2hunter2",
    ):
        assert token not in scrubbed


def test_benign_prose_survives() -> None:
    text = "I finished the audit and dissent on the metric; results attached."
    assert redact_text(text) == text


def test_empty_is_noop() -> None:
    assert redact_text("") == ""


# ---- MAJOR: bare-long-token no longer eats readable ids ---------------------

_SURVIVES = [
    "graduation-2026-07-09-run-5-bus-frames",  # hyphenated run id
    "t_grad51783562393",  # task id (underscore + digits, no long base64 run)
    "dev.agent.status.t_grad51783562393",  # a subject
    "/opt/fleet-bus/trace/grad-5-bus.jsonl",  # a path
    "graduationrunfiveauditreportmilestone",  # long, letters-only
    "17835623931783562393178356239312",  # long, digits-only
]


@pytest.mark.parametrize("text", _SURVIVES)
def test_readable_ids_survive(text: str) -> None:
    # The receipt's value is its identifiers — they must NOT be clobbered.
    assert "[REDACTED]" not in redact_text(text)


_STILL_CAUGHT = [
    "dGhpc0lzQVNlY3JldFRva2VuMTIzNDU2Nzg5MDEyMzQ1",  # 44-char base64, mixed
    "AKIAIOSFODNN7EXAMPLEplusdeadbeef1234567890xyz",  # long mixed no-hyphen
]


@pytest.mark.parametrize("text", _STILL_CAUGHT)
def test_real_bare_secrets_still_caught(text: str) -> None:
    assert "[REDACTED]" in redact_text(text)


# ---- vendored gitleaks tier -------------------------------------------------


def test_vendored_patterns_compiled() -> None:
    # Source identity makes a re-vendor an intentional review event. Compilation
    # outcomes may vary across supported Python regex engines, but every named
    # regex rule must remain observable as either compiled or explicitly skipped.
    assert vendored_source_digest() == _EXPECTED_SOURCE_DIGEST, "vendored source drifted"
    raw = resources.files("fleet_nats").joinpath("redaction_patterns.toml").read_bytes()
    rules = tomllib.loads(raw.decode("utf-8"))["rules"]
    source_rule_ids = tuple(
        rule["id"]
        for rule in rules
        if isinstance(rule, dict) and isinstance(rule.get("regex"), str) and rule["regex"]
    )
    skipped_rule_ids = vendored_skipped_rule_ids()

    assert source_rule_ids
    assert len(source_rule_ids) == len(set(source_rule_ids)), "vendored rule IDs must be unique"
    assert len(skipped_rule_ids) == len(set(skipped_rule_ids)), "skipped rule IDs must be unique"
    assert set(skipped_rule_ids) <= set(source_rule_ids), "unknown skipped rule ID"
    assert vendored_pattern_count() + len(skipped_rule_ids) == len(source_rule_ids), (
        "every vendored regex rule must be compiled or explicitly skipped"
    )


class _BrokenResource:
    def __init__(self, error: OSError | UnicodeDecodeError) -> None:
        self._error = error

    def joinpath(self, _name: str) -> _BrokenResource:
        return self

    def read_bytes(self) -> bytes:
        raise self._error


class _Resource:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def joinpath(self, _name: str) -> _Resource:
        return self

    def read_bytes(self) -> bytes:
        return self._content


@pytest.mark.parametrize(
    "error",
    [
        OSError("missing vendored config"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
    ],
    ids=["missing", "corrupt"],
)
def test_vendored_load_failure_degrades_to_handrolled_tier(
    monkeypatch: pytest.MonkeyPatch, error: OSError | UnicodeDecodeError
) -> None:
    captured: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: captured.append((message.record["level"].name, str(message))),
        level="WARNING",
    )
    try:
        # Reloading exercises the actual import-time initialization path rather
        # than manually replacing _PATTERNS after a direct loader call.
        with monkeypatch.context() as patch:
            patch.setattr(resources, "files", lambda _package: _BrokenResource(error))
            reloaded = importlib.reload(redact_module)
            assert reloaded._VENDORED_PATTERNS == ()
            assert reloaded._PATTERNS == reloaded._HANDROLLED_PATTERNS
            secret = "Authorization: " + "Bearer " + "abcdefghijklmnop" + "qrstuvwxyz012345"
            assert reloaded.redact_text(secret) == "[REDACTED]"
    finally:
        # Restore module-level initialization for the remainder of this process.
        importlib.reload(redact_module)
        logger.remove(sink_id)

    degradation_message = (
        f"redact.vendored | could not load redaction_patterns.toml ({error}); "
        "hand-rolled patterns only"
    )
    degradation_warnings = [
        (level, message) for level, message in captured if degradation_message in message
    ]
    assert len(degradation_warnings) == 1
    assert degradation_warnings[0][0] == "WARNING"


def test_futurewarning_vendor_rule_is_skipped_and_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    # Python currently compiles this RE2-style nested set with FutureWarning;
    # treating that warning as an error prevents a semantic mismatch from being
    # counted as a usable redaction rule.
    resource = _Resource(b'[[rules]]\nid = "nested-set"\nregex = "[[a-z]"\n')
    captured: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: captured.append((message.record["level"].name, str(message))),
        level="WARNING",
    )
    try:
        with monkeypatch.context() as patch:
            patch.setattr(resources, "files", lambda _package: resource)
            vendored, skipped_ids = redact_module._load_vendored_patterns()
    finally:
        importlib.reload(redact_module)
        logger.remove(sink_id)

    assert vendored == ()
    assert skipped_ids == ("nested-set",)
    warnings = [message for level, message in captured if level == "WARNING"]
    assert any(
        "skipped 1 Python-incompatible rule(s): nested-set" in message for message in warnings
    )


def test_vendored_tier_catches_a_provider_token() -> None:
    # A GitLab PAT (glpat-...) — a shape the hand-rolled tier does NOT enumerate,
    # so a hit here proves the vendored gitleaks patterns are actually applied.
    # Built from parts so the fake fixture is never a contiguous PAT-shaped literal
    # in source (keeps org-wide secret scanners off a test fixture).
    fake_pat = "glpat-" + "ABCDEF1234567890abcd"
    text = f"token {fake_pat} here"
    scrubbed = redact_text(text)
    assert fake_pat not in scrubbed
    assert "[REDACTED]" in scrubbed


def test_sendgrid_shape_is_caught_by_provider_fallback() -> None:
    # Whether the upstream rule compiles depends on the Python regex engine.
    # The provider fallback is the stable floor for its URL-safe base64 shape.
    fake_token = "SG." + "A" * 20 + "-_" + "." + "z" * 40 + "-_Z"
    assert any(pattern.search(fake_token) for pattern in redact_module._HANDROLLED_PATTERNS)
    scrubbed = redact_text(f"sendgrid token {fake_token}")
    assert fake_token not in scrubbed
    assert "A" * 20 + "-_" not in scrubbed
    assert "z" * 40 + "-_Z" not in scrubbed
    assert "[REDACTED]" in scrubbed


def test_sendgrid_fallback_does_not_redact_readable_ids() -> None:
    text = "receipt SG.release-2026-build-id is ready"
    assert redact_text(text) == text
