"""Connection configuration for the fleet NATS bus.

Connection config is deployment-specific and comes from the environment, never
baked into the library (matches the Dash decision to keep connection strings out
of the shared contract). The contract/envelope/subjects live in code; where to
connect and who you are come from env.
"""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.parse import urlsplit

from loguru import logger

# The live fleet NATS server (honcho / fleet-hub). Overridable via FLEET_NATS_URL so
# a consumer on a different network segment or a local dev node can repoint.
_DEFAULT_URL = "nats://192.0.2.74:4222"
_DEFAULT_ENV = "dev"
_IDENTITY_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")

# nats-py rewrites a portless ``nats://host`` to ``nats://host:4222`` before
# dialing (client.py). The guard must default the same way or a portless prod
# URL slips through. All known prod endpoints listen on 4222.
_NATS_DEFAULT_PORT = 4222

# The set of known PROD/live server *endpoints* as (host_or_ip, port). This guard
# is DEFENSE-IN-DEPTH against the common literal-IP typo (labeling a connection
# ``staging`` while its URL still spells a prod IP) — it is NOT, and cannot be, the
# isolation boundary. A DNS hostname that resolves to the prod IP is added OUTSIDE
# this repo and is invisible here (the guard does not resolve DNS — see
# ``_is_known_prod_url``), so a string/IP check can never be fail-closed isolation.
# TRUE staging isolation is enforced by DISTINCT staging CREDENTIALS/identity
# (fleet-bus #105 HIGH-2 / docs/f3-staging-shadow-hub.md): prod creds INVALID on
# staging, staging creds INVALID on prod. This guard is only a secondary tripwire
# for the literal-IP mistake.
#
# fleet-hub is the live fleet hub; the loopback/localhost forms (incl. IPv6 ::1) are
# the SSH-tunnel shorthands an operator on the hub would type, and would still land
# ON prod.
#
# Hosts are stored in canonical form: IP literals normalized through
# ``ipaddress.ip_address()`` (so every numeric spelling — dword, hex, dotted-hex,
# IPv4-mapped IPv6 — folds to one value), hostnames lowercased. The guard
# canonicalizes the candidate URL the same way and compares the (host, port)
# tuple, so no numeric/portless/userinfo rewrite of a prod IP literal can bypass
# the tripwire.
_PROD_ENDPOINTS_RAW = (
    ("192.0.2.74", _NATS_DEFAULT_PORT),
    ("127.0.0.1", _NATS_DEFAULT_PORT),
    ("::1", _NATS_DEFAULT_PORT),  # IPv6 loopback — same failure domain as 127.0.0.1
    ("localhost", _NATS_DEFAULT_PORT),
)


def _canonical_host(host: str) -> str:
    """Canonicalize a URL host: numeric IPs folded the way the dialer folds them.

    A numeric host has MANY spellings that all reach the same box, and nats-py's
    dialer resolves them through the platform ``getaddrinfo``/libc parser — so the
    guard must fold them the same way or it disagrees with the dialer about "the
    host" (the exact P1 bypass class):

    - ``socket.inet_aton`` is the libc IPv4 parser: it collapses decimal dword
      (``3221226058``), hex (``0xc000024a``), dotted-hex (``0xc0.0x00.0x02.0x4a``),
      and plain dotted-quad all to canonical ``192.0.2.74``. This equivalence
      with the dialer holds only for glibc-style numeric-node handling; a
      DOTTED-OCTET form with a LEADING ZERO (e.g. ``0192.0.2.74``) is a known
      cross-platform DIVERGENCE — glibc reads it as octal, macOS/BSD ``inet_aton``
      does too but other resolvers/stacks may read it as decimal — so we do NOT
      trust the canonicalization for those. A leading-zero octet FAILS LOUD as a
      malformed host (:class:`ValueError` naming the offending form) rather than
      being folded to a value the dialer might disagree with — rejecting it (vs
      silently passing it through) stops a leading-zero prod spelling (e.g.
      ``0192.0.2.74``, which the OS resolver still maps to prod) from slipping a
      ``staging`` label onto the prod box through the guard's blind spot.
    - IPv6 (incl. ``[...]`` already unbracketed by ``urlsplit``): parsed via
      ``ipaddress``; an IPv4-mapped address (``::ffff:192.0.2.74``) is folded to
      its embedded IPv4 so it matches the v4 prod literal, and other v6 addresses
      (e.g. ``::1``) canonicalize to their compressed form.
    - A non-numeric hostname is not an IP, so it stays as its lowercased self.
      DNS is intentionally NOT resolved here (would hang config construction and
      couple the library to name resolution — see the guard docstring). This is
      why the guard is a literal-IP tripwire, NOT the isolation boundary.
    """
    stripped = host.strip().rstrip(".")  # fold a trailing FQDN dot on the host only
    # Reject dotted forms with a LEADING-ZERO octet before canonicalizing: their
    # octal-vs-decimal reading diverges across resolvers (glibc/BSD octal vs
    # others' decimal), so inet_aton's fold may NOT match the dialer. Fail loud —
    # a leading-zero prod spelling (e.g. ``0192.0.2.74``) still resolves to prod
    # at the OS, so silently accepting it would let it slip the staging guard.
    # Only guards the dotted case (contains '.'); a bare dword/hex has no octet
    # ambiguity.
    if "." in stripped and any(
        len(octet) > 1 and octet[0] == "0" and octet.isdigit() for octet in stripped.split(".")
    ):
        raise ValueError(
            f"BusConfig.url has a malformed host {host!r}: a dotted-quad octet with a "
            "leading zero reads as octal on some resolvers and decimal on others, so "
            "its target is ambiguous — spell every octet without a leading zero."
        )
    # IPv4: inet_aton matches the dialer's numeric-host folding (glibc-style),
    # minus the leading-zero octet forms rejected above.
    try:
        return socket.inet_ntoa(socket.inet_aton(stripped))
    except OSError:
        pass
    # IPv6: fold an IPv4-mapped address down to its v4 form so it matches the v4
    # prod literal; otherwise use the canonical compressed v6 string.
    try:
        ip = ipaddress.ip_address(stripped)
    except ValueError:
        return stripped.lower()
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return str(ip.ipv4_mapped)
    return str(ip)


_KNOWN_PROD_ENDPOINTS = frozenset(
    (_canonical_host(host), port) for host, port in _PROD_ENDPOINTS_RAW
)


def redact_url(url: str) -> str:
    """Return ``url`` safe to log: any userinfo password (``user:pass@``) is hidden.

    A NATS URL may carry credentials (``nats://user:pass@host:4222``). This config
    boundary echoes the URL in :meth:`BusConfig.__repr__` and in two fail-loud
    construction errors, so the password must never reach a log line. When the
    authority-like portion contains userinfo with a password, the password is
    rewritten to ``***``; a bare username (no ``:``) is left intact, and a URL with
    no userinfo is returned unchanged. Host and port are always preserved.

    This deliberately operates on the raw string rather than ``urllib.parse``
    accessors: ``urlsplit().password`` reads ``None`` when a password contains an
    un-percent-encoded ``/`` (it is mis-parsed as the path), which would fail
    *open* and leak the secret. For a NATS URL — ``nats://[user[:pass]@]host:port``
    with no path/query in practice — the authoritative signal is the userinfo
    ``@``. We locate the LAST ``@`` (a password may itself contain ``@``), treat
    everything before it as userinfo, and if that userinfo has a ``:`` we redact
    the password. The same raw userinfo check is applied to schemeless operator
    typos such as ``user:pass@host:4222`` before validation rejects the URL. This
    fails *closed* (over-redacts an odd URL rather than leaking) and preserves
    IPv6 bracketed hosts, which sit after the ``@``.

    Owned here in fleet-core (not imported from fleet-nats) so the config boundary
    carries its own log-safety guarantee with no cross-package dependency; the
    behaviour mirrors ``fleet_nats.bus.redact_url``.

    Args:
        url: The NATS server URL, possibly containing userinfo credentials.

    Returns:
        The URL with any password redacted, safe for logs and error messages.
    """
    scheme_sep = "://"
    sep_idx = url.find(scheme_sep)
    if sep_idx == -1:
        return _redact_userinfo_password(prefix="", rest=url, original=url)
    prefix = url[: sep_idx + len(scheme_sep)]
    rest = url[sep_idx + len(scheme_sep) :]
    return _redact_userinfo_password(prefix=prefix, rest=rest, original=url)


def _redact_userinfo_password(*, prefix: str, rest: str, original: str) -> str:
    """Redact ``user:password@`` in a raw URL authority-like string."""
    at_idx = rest.rfind("@")
    if at_idx == -1:
        # No userinfo → no secret to hide.
        return original
    userinfo = rest[:at_idx]
    host_and_tail = rest[at_idx + 1 :]
    if ":" not in userinfo:
        # Bare username, no password component.
        return original
    user = userinfo.split(":", 1)[0]
    return f"{prefix}{user}:***@{host_and_tail}"


# ``dev``   — the shared live server, dev subject prefix (default).
# ``prod``  — the shared live server, prod subject prefix.
# ``staging`` — a SEPARATE staging server (fleet-bus #105). The token is a
#   labeling/guard convenience; the isolation comes from FLEET_NATS_URL pointing
#   at a distinct host, which the staging guard below enforces.
_VALID_ENVS = frozenset({"dev", "prod", "staging"})
_STAGING_ENV = "staging"

# The minimum acceptable subject-catalog version. ``0`` (the default) DISABLES
# enforcement — the bootstrap posture before the registry is deployed, where a
# fleet running the baked-in code catalog (version 0) must still start. Set to a
# positive integer via FLEET_SUBJECT_CATALOG_MIN_VERSION to enforce a floor: a
# participant whose effective catalog version is below it REFUSES TO START
# (fail-loud). It is never baked into the library; the operator turns it on at
# rollout time, after k-board serves a versioned catalog (issue #103).
_MIN_CATALOG_VERSION_ENV = "FLEET_SUBJECT_CATALOG_MIN_VERSION"

# The registry URL env var. Named here (not imported from registry.py) so the
# enforcement-config invariant can be validated without a config→registry import
# cycle; registry.py owns the fetch and re-exports this same name.
_REGISTRY_URL_ENV = "FLEET_SUBJECT_REGISTRY_URL"


def check_enforcement_config(*, min_version: int, registry_url_set: bool) -> None:
    """Validate the catalog-enforcement env combination at startup — fail loud.

    Enforcement (``FLEET_SUBJECT_CATALOG_MIN_VERSION > 0``) is meaningless
    without a registry to fetch a served version from: with the URL unset the
    client can only ever hold the baked-in version-0 code catalog, so a positive
    floor would refuse to start every time while *looking* like a stale-client
    problem. That mis-diagnosis is the bootstrap trap (#103) — so this rejects
    the combination up front, naming BOTH env vars.

    Args:
        min_version: The configured floor (from :func:`min_catalog_version`).
        registry_url_set: Whether ``FLEET_SUBJECT_REGISTRY_URL`` is set to a
            non-empty value.

    Raises:
        ValueError: When ``min_version > 0`` but the registry URL is unset.
    """
    if min_version > 0 and not registry_url_set:
        raise ValueError(
            f"{_MIN_CATALOG_VERSION_ENV}={min_version} enables catalog-version "
            f"enforcement, but {_REGISTRY_URL_ENV} is unset — with no registry the "
            f"client can only load the baked-in version-0 code catalog, which is "
            f"below any positive floor, so it would ALWAYS refuse to start. Set "
            f"{_REGISTRY_URL_ENV} to the k-board base URL, or unset "
            f"{_MIN_CATALOG_VERSION_ENV} to run the bootstrap (enforcement-off) posture."
        )
    if min_version <= 0 and not registry_url_set:
        logger.warning(
            "fleet_core.config | {} and {} are both unset — running the baked-in "
            "version-0 code catalog with enforcement OFF (bootstrap posture). This is "
            "expected before the registry is deployed; set both to enforce a floor.",
            _MIN_CATALOG_VERSION_ENV,
            _REGISTRY_URL_ENV,
        )


def min_catalog_version() -> int:
    """The configured minimum catalog version (``0`` = enforcement disabled).

    Read from ``FLEET_SUBJECT_CATALOG_MIN_VERSION``. Fails loud on junk (a typo
    must not silently disable a safety floor) and rejects negatives (a negative
    floor is meaningless and almost certainly a mistake).

    Returns:
        The configured floor, or ``0`` when unset (enforcement off).

    Raises:
        ValueError: If the env var is set but not a non-negative integer.
    """
    value = _int_env(_MIN_CATALOG_VERSION_ENV, 0)
    if value < 0:
        raise ValueError(f"{_MIN_CATALOG_VERSION_ENV} must be a non-negative integer, got {value}")
    return value


class CredentialSource(StrEnum):
    """Which authn mechanism a :class:`BusConfig` will present at connect.

    This names the *source*, never the *value* — it is safe to log. ``NONE`` is
    the unauthenticated state that exists until the F2 server half is enforced:
    the client stays permissive (does not fail closed) but MUST announce it
    loudly so an unauthenticated connection is never silent.
    """

    NONE = "none"
    CREDS_FILE = "creds-file"  # nats-py user_credentials (.creds), the NATS idiom
    USER_PASSWORD = "user-password"  # nats-py user + password


@dataclass(frozen=True)
class AuthCredentials:
    """The resolved nats-py connect auth arguments (may hold secret values).

    Each field maps to a nats-py ``connect`` keyword: ``None`` means "don't
    pass it". Kept as named fields (not a ``dict``) so each is passed to
    ``connect`` explicitly and stays type-checked — a ``**dict`` splat cannot be
    verified against nats-py's typed connect signature. Holds secret *values*.

    Structurally secret-safe: :meth:`__repr__` (and therefore ``str`` and any
    f-string / assertion diff) renders only field *names* with ``set``/``unset``
    — never a value. No caller can stringify an instance into a leak, whether
    through a stray ``logger.debug("auth={}", auth)``, an exception traceback
    with the instance in frame locals, or a pytest assert diff. The log-safe
    :attr:`BusConfig.credential_source` still names the mechanism.
    """

    user: str | None = None
    password: str | None = None
    user_credentials: str | None = None

    def __repr__(self) -> str:
        """Redacting repr: field names + ``set``/``unset``, never a value.

        ``set`` means the field holds a (secret) value; ``unset`` means it is
        ``None``. The password value and the identity-encoding creds-file path
        never appear in repr/str/f-string rendering.
        """

        def state(value: str | None) -> str:
            return "unset" if value is None else "set"

        return (
            f"AuthCredentials(user={state(self.user)}, "
            f"password={state(self.password)}, "
            f"user_credentials={state(self.user_credentials)})"
        )


@dataclass(frozen=True)
class BusConfig:
    """Immutable connection config for a bus participant.

    Attributes:
        url: NATS server URL (``FLEET_NATS_URL``).
        agent_id: This participant's identity, used to build its inbox subject
            and to stamp the ``from`` field of every envelope (``FLEET_AGENT_ID``).
        env: Environment prefix for all subjects — ``dev``, ``prod`` or
            ``staging`` (``FLEET_ENV``). ``staging`` REQUIRES a separate server
            URL: combining it with a known prod URL fails loud (fleet-bus #105).
        name: Human-readable connection name shown in NATS monitoring.
        connect_timeout_s: Seconds to wait for the initial connection.
        max_reconnect_attempts: ``-1`` means retry forever
            (``FLEET_RECONNECT_MAX``).
        reconnect_wait_s: Whole seconds between reconnect attempts
            (``FLEET_RECONNECT_WAIT_SECONDS``) — nats-py types this as int.
        nats_user: The NATS username for user/pass authn (``FLEET_NATS_USER``).
            Paired with ``nats_password``. A secret value — never logged.
        nats_password: The paired secret for user/pass authn, read from
            ``FLEET_NATS_PASSWORD``. A secret value — never logged.
        nats_creds_file: Filesystem path to a nats-py ``.creds`` file
            (``FLEET_NATS_CREDS``), passed to nats-py as ``user_credentials``.
            The path itself is not logged (it can encode agent identity /
            deployment layout); only its presence is surfaced. Takes precedence
            over user/password when both are set.
        require_auth: When True (``FLEET_NATS_REQUIRE_AUTH``), an unauthenticated
            connect (``credential_source is NONE``) is a hard error at connect
            instead of a permissive-but-loud WARNING. Default False keeps the
            rollout-window behaviour (permissive); a post-rollout deployment
            opts in to fail *closed* so it cannot silently ship an anonymous
            agent to prod.

    The secret-bearing fields (``nats_password``) and the identity-encoding
    creds path (``nats_creds_file``) are marked ``repr=False`` so the
    dataclass auto-``repr``/``str`` never spills a value or a path — the
    log-safe :attr:`credential_source` names the mechanism instead. This is
    structural, not by convention: no caller can accidentally stringify the
    config (a stray ``logger.debug("cfg={}", cfg)``, an exception traceback
    with the config in frame locals, or a pytest assert diff) into a leak.
    """

    url: str = _DEFAULT_URL
    agent_id: str = "unknown"
    env: str = _DEFAULT_ENV
    name: str = "fleet-nats"
    connect_timeout_s: int = 5  # whole seconds — nats-py connect_timeout expects int
    max_reconnect_attempts: int = -1
    reconnect_wait_s: int = 2
    nats_user: str | None = None
    # Secret value / identity-encoding path: repr=False so neither ever lands in
    # repr(cfg)/str(cfg)/an f-string. credential_source is the log-safe surface.
    nats_password: str | None = field(default=None, repr=False)
    nats_creds_file: str | None = field(default=None, repr=False)
    require_auth: bool = False

    def __post_init__(self) -> None:
        if not self.url.startswith("nats://"):
            raise ValueError(f"BusConfig.url must be a nats:// URL, got {redact_url(self.url)!r}")
        # Reject a malformed host form (leading-zero dotted-quad octet) for EVERY
        # env, not just staging: its octal-vs-decimal target is ambiguous, so it
        # must never be dialed. ``_canonical_host`` fails loud on such forms.
        host = urlsplit(self.url.strip()).hostname
        if host is not None:
            _canonical_host(host)
        if self.env not in _VALID_ENVS:
            raise ValueError(
                f"BusConfig.env must be one of {sorted(_VALID_ENVS)}, got {self.env!r}"
            )
        if self.env == _STAGING_ENV and _is_known_prod_url(self.url):
            # The rejected anti-pattern (fleet-bus #105, codex review): labeling a
            # connection ``staging`` while it still points at the prod server is
            # NOT isolation — it shares the prod failure domain, config, and load.
            # Staging isolation is a SEPARATE SERVER; make the mislabel loud so it
            # can never silently ship "staging" traffic onto prod.
            safe_url = redact_url(self.url)
            raise ValueError(
                f"BusConfig: FLEET_ENV={_STAGING_ENV!r} points at a KNOWN PROD server "
                f"({safe_url!r}). Staging isolation is a SEPARATE server (fleet-bus #105), "
                f"not a subject prefix on prod — set FLEET_NATS_URL to the staging box. "
                f"Known prod endpoints (host, port; rejected for staging): "
                f"{sorted(_KNOWN_PROD_ENDPOINTS)}"
            )
        if not self.agent_id:
            raise ValueError("BusConfig.agent_id must be non-empty")
        if _IDENTITY_TOKEN_RE.fullmatch(self.agent_id) is None:
            raise ValueError(
                "BusConfig.agent_id must be a canonical NATS identity token ([a-z0-9][a-z0-9_-]*)"
            )
        if self.max_reconnect_attempts < -1:
            raise ValueError(
                f"BusConfig.max_reconnect_attempts must be >= -1 (-1 = forever), "
                f"got {self.max_reconnect_attempts}"
            )
        if not self.reconnect_wait_s > 0:
            raise ValueError(
                f"BusConfig.reconnect_wait_s must be positive, got {self.reconnect_wait_s}"
            )
        # Half-configured user/password is a deploy error that would fail *open*
        # (connect anonymously with a username that means nothing) — fail loud.
        if bool(self.nats_user) != bool(self.nats_password):
            raise ValueError(
                "BusConfig requires both nats_user and nats_password together "
                "(one without the other is a misconfiguration)"
            )
        if (
            self.credential_source is CredentialSource.USER_PASSWORD
            and self.nats_user is not None
            and _IDENTITY_TOKEN_RE.fullmatch(self.nats_user) is None
        ):
            raise ValueError(
                "BusConfig.nats_user must be a canonical NATS identity token ([a-z0-9][a-z0-9_-]*)"
            )
        # Pre-validate the creds file at construction, NOT lazily at connect.
        # nats-py opens the .creds file during the handshake; a missing/unreadable
        # path raises FileNotFoundError/PermissionError whose str() carries the
        # FULL resolved path — which would flow into the generic connect-failure
        # ERROR log and leak the deployment layout the path encodes. Validating
        # here fails loud with an actionable message that names the ENV VAR, never
        # the resolved path, and turns a raw OSError-at-connect into a clean
        # construction-time error. (Only when a creds file is the active source —
        # see credential_source precedence.)
        if self.credential_source is CredentialSource.CREDS_FILE:
            path = self.nats_creds_file
            assert path is not None  # CREDS_FILE ⇒ nats_creds_file is set
            if not os.path.isfile(path) or not os.access(path, os.R_OK):
                raise ValueError(
                    "FLEET_NATS_CREDS points to a missing or unreadable file "
                    "(check the path exists and is readable by this process); "
                    "the resolved path is intentionally omitted from this error."
                )

    def __repr__(self) -> str:
        """Redacting repr: the ``url`` renders with any userinfo password hidden.

        The dataclass auto-``repr`` would echo the raw ``url``, so a
        ``nats://user:secret@host`` config could leak the password through a stray
        ``logger.debug("cfg={}", cfg)``, an exception traceback with the config in
        frame locals, or a pytest assert diff. This override renders every
        non-``repr=False`` field exactly as the auto-``repr`` would, but passes
        ``url`` through :func:`redact_url` first — host and port are preserved. The
        secret-bearing ``nats_password`` and identity-encoding ``nats_creds_file``
        remain absent (they are ``repr=False``).
        """
        return (
            f"BusConfig(url={redact_url(self.url)!r}, "
            f"agent_id={self.agent_id!r}, "
            f"env={self.env!r}, "
            f"name={self.name!r}, "
            f"connect_timeout_s={self.connect_timeout_s!r}, "
            f"max_reconnect_attempts={self.max_reconnect_attempts!r}, "
            f"reconnect_wait_s={self.reconnect_wait_s!r}, "
            f"nats_user={self.nats_user!r}, "
            f"require_auth={self.require_auth!r})"
        )

    @property
    def credential_source(self) -> CredentialSource:
        """Which authn mechanism this config will present at connect (log-safe).

        Creds-file wins when both a creds file and user/password are set: the
        ``.creds`` file is the richer NATS-native credential (JWT + nkey seed),
        so a deployment that has one should not silently downgrade to a shared
        user/password. Reports the *source*, never any secret value.
        """
        if self.nats_creds_file:
            return CredentialSource.CREDS_FILE
        if self.nats_user and self.nats_password:
            return CredentialSource.USER_PASSWORD
        return CredentialSource.NONE

    @property
    def inbox_prefix(self) -> str:
        """Return this principal's deterministic, NATS-safe reply inbox prefix.

        Request/reply subscriptions are authorization boundaries: a client must
        never receive replies addressed to another authenticated principal. For
        user/password auth, the authenticated NATS user is the principal. For
        creds-file auth, the configured canonical agent identity is sent to the
        server unchanged, so a mismatched server grant is denied rather than
        collapsed onto another principal's inbox.
        """
        identity = (
            self.nats_user
            if self.credential_source is CredentialSource.USER_PASSWORD
            else self.agent_id
        )
        assert identity is not None
        return f"_INBOX.{identity}"

    def auth_credentials(self) -> AuthCredentials:
        """Resolve the nats-py connect auth arguments from configured creds.

        Returns an :class:`AuthCredentials` carrying exactly the fields the
        active :attr:`credential_source` needs (all ``None`` for the
        unauthenticated path — current behaviour preserved). May contain secret
        *values*; the caller passes each field to ``connect`` explicitly and
        MUST NOT log the result. Log :attr:`credential_source` instead.
        """
        source = self.credential_source
        if source is CredentialSource.CREDS_FILE:
            return AuthCredentials(user_credentials=self.nats_creds_file)
        if source is CredentialSource.USER_PASSWORD:
            return AuthCredentials(user=self.nats_user, password=self.nats_password)
        return AuthCredentials()

    @classmethod
    def from_env(cls, agent_id: str | None = None) -> BusConfig:
        """Build config from environment variables.

        Args:
            agent_id: Explicit identity; falls back to ``FLEET_AGENT_ID`` then
                ``unknown``.

        Returns:
            A validated :class:`BusConfig`.
        """
        resolved_id = agent_id or os.getenv("FLEET_AGENT_ID") or "unknown"
        raw_url = os.getenv("FLEET_NATS_URL")
        raw_env = os.getenv("FLEET_ENV")
        # MED-4 (fleet-bus #105): a staging EnvironmentFile that overrides
        # FLEET_NATS_URL but omits (or blanks) FLEET_ENV would SILENTLY default to
        # `dev` and start a healthy-looking service on dev.* subjects — ambiguous
        # deployment state masquerading as success. When the URL is explicitly
        # overridden to a non-default value, FLEET_ENV MUST be set explicitly:
        # fail loud (naming FLEET_ENV) rather than silently choosing `dev`.
        if raw_url is not None and raw_url != _DEFAULT_URL and not (raw_env or "").strip():
            raise ValueError(
                "FLEET_ENV must be set explicitly when FLEET_NATS_URL is overridden "
                f"(FLEET_NATS_URL={redact_url(raw_url)!r}, FLEET_ENV={raw_env!r}). A non-default "
                "server with no FLEET_ENV is ambiguous deployment state — refusing to "
                f"default to {_DEFAULT_ENV!r}. Set FLEET_ENV to one of "
                f"{sorted(_VALID_ENVS)} (e.g. FLEET_ENV=staging for the shadow hub)."
            )
        return cls(
            url=raw_url if raw_url is not None else _DEFAULT_URL,
            agent_id=resolved_id,
            env=raw_env if raw_env is not None else _DEFAULT_ENV,
            name=os.getenv("FLEET_NATS_NAME", f"fleet-nats-{resolved_id}"),
            max_reconnect_attempts=_int_env("FLEET_RECONNECT_MAX", -1),
            reconnect_wait_s=_int_env("FLEET_RECONNECT_WAIT_SECONDS", 2),
            # Per-agent NATS credentials (F2, fleet-bus #104). Absent by default:
            # the client stays permissive until the fleet-hub server half enforces.
            # Empty-string env values normalise to None (unset) so a blank export
            # never half-configures user/password into a fail-loud state.
            nats_user=_str_env("FLEET_NATS_USER"),
            nats_password=_str_env("FLEET_NATS_PASSWORD"),
            nats_creds_file=_str_env("FLEET_NATS_CREDS"),
            # Opt-in fail-closed for post-rollout deployments (default permissive).
            require_auth=_bool_env("FLEET_NATS_REQUIRE_AUTH"),
        )


def _str_env(var: str) -> str | None:
    """Read an optional string env var, treating empty/whitespace as unset.

    A blank export (``FLEET_NATS_USER=``) is a common deploy artifact; it must
    mean "not configured", not "configured with an empty username" — the latter
    would trip the both-or-neither user/password guard for no reason.
    """
    raw = os.getenv(var)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _bool_env(var: str) -> bool:
    """Read an optional boolean env var; fail loud on junk (default False).

    Accepted (case-insensitive) true values: ``1``, ``true``, ``yes``, ``on``.
    Accepted false values: ``0``, ``false``, ``no``, ``off``. Unset or empty
    keeps the default (False). Any other non-empty value raises — a typo like
    ``treu`` must not silently disable an operator's request to require auth.
    """
    raw = os.getenv(var)
    if raw is None:
        return False
    normalized = raw.strip().lower()
    if normalized == "":
        return False
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(
        f"{var} must be one of "
        f"{sorted(_TRUE_VALUES)} (true) or {sorted(_FALSE_VALUES)} (false), "
        f"got {raw!r}"
    )


def _is_known_prod_url(url: str) -> bool:
    """True if ``url`` points at a known PROD/live server IP literal (fleet-bus #105).

    This is DEFENSE-IN-DEPTH against the common literal-IP typo — NOT an isolation
    boundary. TRUE staging isolation is enforced by distinct staging CREDENTIALS/
    identity (HIGH-2; see the ``_PROD_ENDPOINTS_RAW`` note and the runbook), which
    make prod creds invalid on staging and vice-versa. This function only catches a
    prod *IP literal* spelled into the URL; it CANNOT catch a DNS alias that
    resolves to the prod IP (that record lives outside this repo and is never
    resolved here), so it must not be relied on as fail-closed isolation.

    Within the literal-IP class it is a *canonicalizing* check, not a string
    blocklist. It parses the URL and compares a canonical ``(host, port)`` tuple
    against :data:`_KNOWN_PROD_ENDPOINTS`, so every equivalent spelling of a prod
    IP endpoint is caught — not just the literal typo:

    - **portless** ``nats://192.0.2.74`` — the missing port defaults to 4222,
      exactly as nats-py rewrites it before dialing, so portless prod is caught;
    - **numeric IP spellings** — decimal dword ``nats://3221226058``, hex
      ``nats://0xc000024a``, dotted-hex, and IPv4-mapped IPv6
      ``nats://[::ffff:192.0.2.74]`` all canonicalize (via
      :func:`_canonical_host` / ``ipaddress.ip_address``) to ``192.0.2.74``;
    - **IPv6 loopback** ``nats://[::1]`` folds into the loopback failure domain;
    - **userinfo stripped** — ``urlsplit`` exposes ``hostname``/``port`` with any
      ``user[:pass]@`` already removed, so credentials cannot mask the prod host;
    - trailing slash, whitespace, ``NATS://`` scheme casing, host casing, and a
      trailing FQDN dot are all folded out (the dot is stripped from the *host*,
      so ``nats://host.:4222`` normalizes even with a port present).

    NOT DNS-aware (the core reason this is a tripwire, not isolation): a hostname
    that *resolves* to the prod IP but is not a known literal is NOT caught —
    resolving DNS during config construction would hang and couple the library to
    name resolution, and the resolving record can be added outside this repo.
    Adding a known prod DNS name to :data:`_PROD_ENDPOINTS_RAW` (and the drill's
    ``case`` list) narrows this one blind spot but does not make the guard
    fail-closed; only distinct staging credentials do that.
    """
    parts = urlsplit(url.strip())
    host = parts.hostname  # urlsplit already strips userinfo and IPv6 brackets
    if not host:
        return False
    try:
        port = parts.port if parts.port is not None else _NATS_DEFAULT_PORT
    except ValueError:
        # A non-numeric port is malformed; the nats:// validator / dialer will
        # reject it. It is not a known prod endpoint.
        return False
    return (_canonical_host(host), port) in _KNOWN_PROD_ENDPOINTS


def _int_env(var: str, default: int) -> int:
    """Read an int env var; fail loud on junk (a typo must not become policy)."""
    raw = os.getenv(var)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{var} must be an integer, got {raw!r}") from exc
