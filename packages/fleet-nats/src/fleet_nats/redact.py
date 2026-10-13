"""Display-time credential scrub for rendered show-your-work output (#64).

The show-your-work trail DISPLAYS a task's coordination record to a human or a
model (a Discord thread, a tool result). The rendered text is trusted-agent
prose, but "trusted" is not "guaranteed secret-free": an agent could paste a
token, a webhook URL, or a bearer header into a reply. Before any of it renders,
this module redacts anything that PATTERN-matches a credential SHAPE so operators
can share the trail safely.

This is a DISPLAY-TIME REDACTOR, not a secret scanner and not a scanning tool.
The contract is redact-on-suspicion, not parse-and-be-sure: a false positive
(over-redacting a token-shaped-but-benign string) is acceptable; a leak is not.

Pattern sources
---------------
1. A small hand-rolled tier (the fleet's top leak worries — webhook URLs,
   userinfo URLs, bearer headers, provider-token prefixes, ``key=value`` secret
   pairs) lifted forward from the closed PR #67 engine.
2. The vendored gitleaks default ruleset (``redaction_patterns.toml``, MIT,
   pinned tag v8.30.1) — 222 credential-shape regexes compiled once at import.

The caller (``showwork._redact``) wraps every call fail-closed: if this function
ever raises, the caller drops the whole frame rather than render it unscrubbed.
This module therefore stays total for normal string input — plain regex
substitution, no I/O at call time (the vendored file is read once at import).
"""

from __future__ import annotations

import hashlib
import re
import tomllib
import warnings
from importlib import resources

from loguru import logger

_PLACEHOLDER = "[REDACTED]"

# The vendored gitleaks ruleset lives beside this module (see the file's header
# for provenance + MIT license). Compiled once at import into _VENDORED_PATTERNS.
_VENDORED_RESOURCE = "redaction_patterns.toml"
_VENDORED_SOURCE_DIGEST: str | None = None

# Hand-rolled tier (PR #67): each pattern targets a credential SHAPE. Ordered
# widest-net-first; every match is replaced with the placeholder. Deliberately
# broad — a benign false positive is cheaper than a leaked secret. These stay
# even though the vendored ruleset overlaps: they encode the fleet's specific
# worries (Discord webhooks, nats:// userinfo) as a guaranteed floor that does
# not depend on the vendored file loading.
_HANDROLLED_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Discord/Slack/generic webhook URLs (the fleet's #1 leak worry).
    re.compile(r"https?://(?:\S*?)(?:webhook|hooks)\S*", re.IGNORECASE),
    # Any URL carrying an inline userinfo credential (user:pass@host).
    re.compile(r"[a-z][a-z0-9+.\-]*://[^\s/@]+:[^\s/@]+@\S+", re.IGNORECASE),
    # Bearer / Authorization header values.
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]+"),
    re.compile(r"(?i)\bauthorization\s*[:=]\s*\S+"),
    # Common provider token prefixes (OpenAI sk-, GitHub ghp_/gho_/ghs_, xoxb-,
    # AWS AKIA, Anthropic sk-ant-). Length-bounded to a token, not a sentence.
    re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9._\-]{16,}"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9\-]{10,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    # SendGrid API keys are URL-safe base64 segments: ``SG.<22>.<43>``. Keep
    # this provider-specific so hyphenated human-readable receipt IDs survive.
    re.compile(r"(?<![A-Za-z0-9_-])SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}(?![A-Za-z0-9_-])"),
    # key=value / token: value pairs where the key names a secret.
    re.compile(
        r"(?i)\b(?:api[_-]?key|secret|token|password|passwd|pwd|access[_-]?key)"
        r"\s*[:=]\s*[^\s,;]+"
    ),
)


def _load_vendored_patterns() -> tuple[tuple[re.Pattern[str], ...], tuple[str, ...]]:
    """Compile the vendored gitleaks ``[[rules]].regex`` set, fail-open per rule.

    The vendored file is upstream gitleaks config (Go/RE2 regex dialect). Most of
    its 221 regex-bearing rules compile cleanly under Python ``re``; the few that
    use an RE2-only construct or warning-producing incompatible syntax are
    SKIPPED (logged once at warning with rule IDs)
    rather than crashing import. The hand-rolled tier plus the compiling rules
    still redact, and this is a display redactor where over-redaction is fine but
    a broken import (no redaction at all) is a leak. A missing/corrupt vendored
    file degrades to the hand-rolled tier alone, logged once at warning — never a
    crash into the render path.
    """
    global _VENDORED_SOURCE_DIGEST

    compiled: list[re.Pattern[str]] = []
    _VENDORED_SOURCE_DIGEST = None
    try:
        raw = resources.files("fleet_nats").joinpath(_VENDORED_RESOURCE).read_bytes()
        data = tomllib.loads(raw.decode("utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning(
            "redact.vendored | could not load {} ({}); hand-rolled patterns only",
            _VENDORED_RESOURCE,
            exc,
        )
        return (), ()
    _VENDORED_SOURCE_DIGEST = hashlib.sha256(raw).hexdigest()
    rules = data.get("rules", [])
    if not isinstance(rules, list):
        logger.warning(
            "redact.vendored | {} has no [[rules]] list; hand-rolled only", _VENDORED_RESOURCE
        )
        return (), ()
    skipped_ids: list[str] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        regex = rule.get("regex")
        if not isinstance(regex, str) or not regex:
            continue
        try:
            # A warning means Python accepted a different or future-unstable
            # interpretation of an RE2-authored pattern. Do not count it as a
            # compatible redaction rule.
            with warnings.catch_warnings():
                warnings.simplefilter("error", FutureWarning)
                compiled.append(re.compile(regex))
        except (re.error, FutureWarning):
            # RE2-only or warning-producing syntax is Python-incompatible. The
            # rule id (if any) is engine-safe to log.
            rule_id = rule.get("id")
            skipped_ids.append(rule_id if isinstance(rule_id, str) else "<missing-id>")
    if skipped_ids:
        logger.warning(
            "redact.vendored | skipped {} Python-incompatible rule(s): {}",
            len(skipped_ids),
            ", ".join(skipped_ids),
        )
    logger.info(
        "redact.vendored | compiled {} vendored gitleaks pattern(s) (skipped {})",
        len(compiled),
        len(skipped_ids),
    )
    return tuple(compiled), tuple(skipped_ids)


_VENDORED_PATTERNS, _VENDORED_SKIPPED_IDS = _load_vendored_patterns()

# The full ordered scrub set: hand-rolled floor first (fleet-specific worries),
# then the vendored gitleaks tier. Every match → the placeholder.
_PATTERNS: tuple[re.Pattern[str], ...] = _HANDROLLED_PATTERNS + _VENDORED_PATTERNS

# A bare long high-entropy token: >=32 chars of a NO-HYPHEN base64/hex alphabet.
# Deliberately excludes ``-`` so it never eats a readable id (``graduation-2026-
# 07-09-run-5-bus-frames``, ``t_grad51783562393``): hyphen-separated identifiers
# are the whole point of a receipt and must survive. Applied via a guarded
# callback (:func:`_redact_bare_token`) that ALSO requires mixed letters+digits,
# so a natural-language 40-letter word or an all-digit epoch is not clobbered
# while a real 40-char base64 secret still is.
_BARE_TOKEN = re.compile(r"\b[A-Za-z0-9+/]{32,}={0,2}\b")


def _redact_bare_token(match: re.Match[str]) -> str:
    """Redact a bare long token ONLY when it looks high-entropy (letters+digits).

    The receipt's value is its identifiers; over-redacting a run id or a long
    English token would gut it. A credential-shaped token mixes letters and
    digits — require BOTH before redacting, so ``graduationrunfiveauditreport``
    (letters only) and ``17835623931783562393`` (digits only) survive while
    ``dGhpc0lzQVNlY3JldFRva2VuMTIzNDU2Nzg5`` goes.
    """
    token = match.group(0)
    has_letter = any(c.isalpha() for c in token)
    has_digit = any(c.isdigit() for c in token)
    return _PLACEHOLDER if has_letter and has_digit else token


def redact_text(text: str) -> str:
    """Return ``text`` with every credential-shaped substring replaced.

    Args:
        text: The raw text to scrub (may be empty).

    Returns:
        The scrubbed text. Empty input returns empty. Never raises for normal
        string input — the caller still wraps it fail-closed as belt-and-braces.
    """
    if not text:
        return text
    scrubbed = text
    for pattern in _PATTERNS:
        scrubbed = pattern.sub(_PLACEHOLDER, scrubbed)
    # The bare-long-token net is applied last (named patterns win first) via a
    # guarded callback so it catches real secrets without eating readable ids.
    scrubbed = _BARE_TOKEN.sub(_redact_bare_token, scrubbed)
    return scrubbed


def vendored_pattern_count() -> int:
    """Number of vendored gitleaks patterns that compiled (test/observability)."""
    return len(_VENDORED_PATTERNS)


def vendored_skipped_rule_ids() -> tuple[str, ...]:
    """IDs of vendored rules skipped as Python-incompatible."""
    return _VENDORED_SKIPPED_IDS


def vendored_source_digest() -> str | None:
    """SHA-256 of the loaded vendored source, or ``None`` when it could not load."""
    return _VENDORED_SOURCE_DIGEST
