"""Default fleet-core transport boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import TYPE_CHECKING, NoReturn

from loguru import logger
from nats.aio.client import Client as NATS
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import Error as JetStreamError

from fleet_core.config import BusConfig, CredentialSource, redact_url
from fleet_core.spec import Envelope, signal_prefers_core

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription
    from nats.js import JetStreamContext
    from nats.js.api import ConsumerInfo

EnvelopeHandler = Callable[[Envelope], Awaitable[None]]
RawMessageHandler = Callable[["Msg"], Awaitable[None]]

DURABLE_RETRY_DELAY_S = 1.0


class DeliveryMode(StrEnum):
    """Transport delivery mode selected by the policy seam."""

    CORE = "core"
    DURABLE = "durable"


class DurableNotProvisionedError(RuntimeError):
    """Durable delivery was selected before JetStream streams are provisioned."""

    def __init__(self, *, subject: str, operation: str, reason: str | None = None) -> None:
        self.subject = subject
        self.operation = operation
        self.reason = reason
        detail = f" Reason: {reason}." if reason else ""
        super().__init__(
            f"durable {operation} selected for {subject!r}, but fleet-core durable "
            "JetStream delivery is not provisioned for that subject. Missing JetStream "
            "capability or matching streams must fail loud; no core-NATS fallback was "
            f"attempted.{detail}"
        )


class PayloadTooLargeError(ValueError):
    """A publish was refused because the encoded envelope exceeds the bus limit."""

    def __init__(self, subject: str, size: int, limit: int) -> None:
        self.subject = subject
        self.size = size
        self.limit = limit
        super().__init__(
            f"publish refused on {subject!r}: encoded envelope is {size} bytes, "
            f"over the {limit}-byte NATS max_payload limit "
            f"(over by {size - limit} bytes). Truncation policy: NONE - the bus "
            "never truncates; the caller must split the work, offload the body to "
            "a side channel and publish a reference, or raise the server max_payload."
        )


_AUTH_REJECT_MARKERS = (
    "authorization violation",
    "permissions violation",
    "authentication",
    "authorization",
)


def _is_auth_rejection(exc: BaseException | None) -> bool:
    """True when nats-py surfaced a server auth rejection."""
    if exc is None:
        return False
    msg = str(exc).lower()
    return any(marker in msg for marker in _AUTH_REJECT_MARKERS)


class FleetBus:
    """Fleet-core default NATS transport with a durable/core policy seam."""

    def __init__(self, config: BusConfig) -> None:
        self._config = config
        self._nc = NATS()
        self._closing = False

    @property
    def config(self) -> BusConfig:
        """The immutable config this bus was built with."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Whether the underlying NATS client is currently connected."""
        return self._nc.is_connected

    async def __aenter__(self) -> FleetBus:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def delivery_mode_for(self, subject: str, envelope: Envelope) -> DeliveryMode:
        """Select delivery mode for an already-built subject and envelope.

        This is the policy seam. It chooses core vs durable transport for a
        validated envelope from the envelope's OWN typed built-in signal — the
        ``Heartbeat`` / ``Presence`` model owns ``jetstream=False`` and is the
        sole ephemeral exception. Every other built-in signal is durable by
        default. The decision is read from the typed signal model, never from the
        wire ``kind`` string, the subject, or an opaque payload dict, so a signal
        cannot be mis-routed by spoofing ``kind`` or a payload key. It does not
        construct subjects, parse payloads, or bypass payload guards.
        """
        _ = subject
        if signal_prefers_core(envelope.typed_signal):
            return DeliveryMode.CORE
        return DeliveryMode.DURABLE

    def jetstream(self, **opts: object) -> JetStreamContext:
        """Return this connection's JetStream context.

        The method exposes the connected nats-py JetStream context for later
        durable wiring. It does not provision streams or consumers.
        """
        if not self._nc.is_connected:
            logger.error(
                "fleetbus.jetstream | requested before connect agent={}",
                self._config.agent_id,
            )
            raise RuntimeError(
                "FleetBus.jetstream() requires a connected bus - call connect() "
                f"first (agent={self._config.agent_id})."
            )
        logger.debug("fleetbus.jetstream | context requested agent={}", self._config.agent_id)
        return self._nc.jetstream(**opts)

    async def connect(self) -> None:
        """Open the NATS connection, bounding only the initial connect attempt."""
        cred_source = self._config.credential_source
        logger.debug(
            "fleetbus.connect | url={} agent={} env={} cred_source={}",
            redact_url(self._config.url),
            self._config.agent_id,
            self._config.env,
            cred_source.value,
        )
        if cred_source is CredentialSource.NONE:
            if self._config.require_auth:
                raise ValueError(
                    "fleetbus.connect: FLEET_NATS_REQUIRE_AUTH is set but this "
                    f"agent ({self._config.agent_id}) has no credentials configured - "
                    "set FLEET_NATS_CREDS or FLEET_NATS_USER+FLEET_NATS_PASSWORD, "
                    "or unset FLEET_NATS_REQUIRE_AUTH to allow anonymous connect."
                )
            logger.warning(
                "fleetbus.connect | agent={} connecting WITHOUT credentials - "
                "unauthenticated bus access (spoofable). Configure FLEET_NATS_CREDS "
                "or FLEET_NATS_USER/FLEET_NATS_PASSWORD once server-side auth is enforced.",
                self._config.agent_id,
            )
        else:
            if (
                cred_source is CredentialSource.CREDS_FILE
                and self._config.nats_user
                and self._config.nats_password
            ):
                logger.warning(
                    "fleetbus.connect | agent={} both .creds and user/password "
                    "configured - using creds-file, ignoring user/password.",
                    self._config.agent_id,
                )
            logger.info(
                "fleetbus.connect | agent={} using credentials source={}",
                self._config.agent_id,
                cred_source.value,
            )

        budget = self._config.connect_timeout_s + 2
        agent = self._config.agent_id
        self._closing = False

        async def _on_disconnected() -> None:
            if self._closing:
                logger.debug("fleetbus.reconnect | disconnected during close agent={}", agent)
                return
            logger.warning("fleetbus.reconnect | disconnected agent={} - retrying", agent)

        async def _on_reconnected() -> None:
            logger.info("fleetbus.reconnect | reconnected agent={}", agent)

        async def _on_closed() -> None:
            if self._closing:
                logger.debug("fleetbus.reconnect | closed cleanly agent={}", agent)
                return
            logger.error(
                "fleetbus.reconnect | connection CLOSED agent={} - reconnect budget "
                "exhausted; no further delivery",
                agent,
            )

        async def _on_error(exc: Exception) -> None:
            # A permissions violation arrives HERE, not as a raise from publish():
            # NATS core publish is fire-and-forget, so the server's refusal comes
            # back asynchronously. Logging it as a generic "client error" would
            # mean a principal whose grant is wrong watches its messages vanish
            # with no stated reason — worst for a publish-only principal (#222),
            # whose whole job is pushing to one narrow subject. Say what was
            # refused and that the message was DROPPED.
            if _is_auth_rejection(exc):
                logger.error(
                    "fleetbus.authz | REFUSED agent={} source={}: {} — the server "
                    "denied this subject for this credential; the message was "
                    "DROPPED (not queued, not retried). Check this principal's "
                    "publish/subscribe grants in the server authorization config.",
                    agent,
                    self._config.credential_source.value,
                    exc,
                )
                return
            logger.warning("fleetbus.reconnect | client error agent={}: {}", agent, exc)

        auth = self._config.auth_credentials()
        try:
            await asyncio.wait_for(
                self._nc.connect(
                    servers=[self._config.url],
                    name=self._config.name,
                    connect_timeout=self._config.connect_timeout_s,
                    max_reconnect_attempts=self._config.max_reconnect_attempts,
                    reconnect_time_wait=self._config.reconnect_wait_s,
                    disconnected_cb=_on_disconnected,
                    reconnected_cb=_on_reconnected,
                    closed_cb=_on_closed,
                    error_cb=_on_error,
                    user=auth.user,
                    password=auth.password,
                    user_credentials=auth.user_credentials,
                    inbox_prefix=self._config.inbox_prefix,
                ),
                timeout=budget,
            )
        except TimeoutError as exc:
            safe_url = redact_url(self._config.url)
            auth_raised = self._raise_if_auth_rejection(cred_source, self._nc.last_error, exc)
            if auth_raised is not None:
                raise auth_raised from exc
            logger.error("fleetbus.connect | timed out after {}s url={}", budget, safe_url)
            raise TimeoutError(
                f"fleetbus.connect: server {safe_url} unreachable within {budget}s"
            ) from exc
        except OSError as exc:
            if cred_source is not CredentialSource.CREDS_FILE:
                logger.error(
                    "fleetbus.connect | failed url={} reason={}",
                    redact_url(self._config.url),
                    exc,
                )
                raise
            reason = exc.strerror or exc.__class__.__name__
            logger.error(
                "fleetbus.connect | credentials file could not be opened agent={} "
                "url={} FLEET_NATS_CREDS reason={} - file was validated at construction; "
                "it was deleted or its permissions changed before connect",
                self._config.agent_id,
                redact_url(self._config.url),
                reason,
            )
            raise ConnectionError(
                "fleetbus.connect: could not open the credentials file named by "
                f"FLEET_NATS_CREDS for this agent ({self._config.agent_id}): "
                f"{reason} - the file passed validation at construction but was "
                "deleted or made unreadable before connect (path withheld)."
            ) from exc
        except Exception as exc:
            auth_raised = self._raise_if_auth_rejection(cred_source, exc, exc)
            if auth_raised is None and _is_auth_rejection(self._nc.last_error):
                auth_raised = self._raise_if_auth_rejection(
                    self._config.credential_source, self._nc.last_error, exc
                )
            if auth_raised is not None:
                raise auth_raised from exc
            logger.error(
                "fleetbus.connect | failed url={} reason={}", redact_url(self._config.url), exc
            )
            raise
        logger.info("fleetbus.connect | connected agent={}", self._config.agent_id)

    def _raise_if_auth_rejection(
        self,
        cred_source: CredentialSource,
        candidate: BaseException | None,
        cause: BaseException,
    ) -> Exception | None:
        """Return an actionable, secret-free error if candidate is an auth reject."""
        _ = cause
        if not _is_auth_rejection(candidate):
            return None
        safe_url = redact_url(self._config.url)
        if cred_source is CredentialSource.NONE:
            logger.error(
                "fleetbus.connect | server refused anonymous connect agent={} url={} "
                "- server requires credentials but none are configured",
                self._config.agent_id,
                safe_url,
            )
            return ConnectionError(
                "fleetbus.connect: server requires credentials but this agent "
                f"({self._config.agent_id}) has none configured "
                "(FLEET_NATS_CREDS / FLEET_NATS_USER+PASSWORD unset)."
            )
        logger.error(
            "fleetbus.connect | authorization refused agent={} url={} source={}",
            self._config.agent_id,
            safe_url,
            cred_source.value,
        )
        return ConnectionError(
            "fleetbus.connect: server refused this agent's credentials "
            f"({self._config.agent_id}, source={cred_source.value}) - the presented "
            "credential is not accepted (check the FLEET_NATS_* value/rotation)."
        )

    async def close(self) -> None:
        """Drain and close the connection. Safe to call when not connected."""
        self._closing = True
        drained = False
        try:
            if self._nc.is_connected:
                await self._nc.drain()
                drained = True
        except Exception as exc:
            logger.warning("fleetbus.close | drain failed, will close directly: {}", exc)
        if not drained:
            try:
                await self._nc.close()
            except Exception as exc:
                logger.warning("fleetbus.close | close failed: {}", exc)
        logger.debug("fleetbus.close | closed agent={}", self._config.agent_id)

    def _guard_payload_size(self, subject: str, data: bytes) -> None:
        """Fail loud before wire calls when encoded data exceeds max_payload."""
        limit = self._nc.max_payload
        size = len(data)
        if size > limit:
            logger.error(
                "fleetbus.publish | payload guard TRIPPED subject={} size={} limit={} "
                "(truncation policy: NONE - raising)",
                subject,
                size,
                limit,
            )
            raise PayloadTooLargeError(subject, size, limit)

    @staticmethod
    def _guard_publish_subject(subject: str) -> None:
        """Fail loud before the wire call when a publish subject holds a wildcard.

        A publish subject must be fully concrete. ``*`` and ``>`` are legal only
        in a subscription or a server-side grant pattern, and NATS rejects a
        wildcard-bearing publish **asynchronously** via ``error_cb`` — the
        publish call itself returns cleanly, so without this guard the message is
        silently never delivered and the failure surfaces far away as a task that
        hangs to timeout with no error at the call site.

        This is reachable from real traffic, not just misuse: a channel token is
        publisher-controlled (``mapping.chat_id = envelope.channel``) and
        fleet-bus #117 routes it into the ``status``/``relay``/``wake``/``ack``/
        ``telemetry`` subject forms, so a hostile inbound ``channel="*"`` would
        otherwise produce a reply subject byte-identical to the hub's
        subscription wildcard. Guarding centrally here (rather than at each
        builder) means no publish site can forget it, and it cannot break the
        authorization-grant builders, which format patterns without publishing.
        """
        if "*" in subject or ">" in subject:
            logger.error(
                "fleetbus.publish | wildcard subject guard TRIPPED subject={} "
                "(NATS would drop this publish asynchronously - raising)",
                subject,
            )
            raise ValueError(
                f"refusing to publish a wildcard-bearing subject (NATS would drop it): {subject!r}"
            )

    def check_payload_size(self, subject: str, envelope: Envelope) -> None:
        """Size-check an envelope without publishing."""
        self._guard_payload_size(subject, envelope.to_bytes())

    async def publish(
        self,
        subject: str,
        envelope: Envelope,
        *,
        mode: DeliveryMode | None = None,
    ) -> None:
        """Publish an envelope.

        Durable is the default policy. The durable path uses JetStream only
        after the server proves a pre-provisioned stream matches the subject; it
        never creates streams and never falls back to core NATS. Explicit
        ``mode=CORE`` uses core NATS.
        """
        logger.debug("fleetbus.publish | subject={} kind={}", subject, envelope.kind)
        self._guard_publish_subject(subject)
        data = envelope.to_bytes()
        self._guard_payload_size(subject, data)
        selected_mode = self._selected_mode(
            subject=subject, envelope=envelope, mode=mode, typed_signal=None
        )
        if selected_mode is DeliveryMode.DURABLE:
            await self._publish_durable(subject, data)
            return
        try:
            await self._nc.publish(subject, data)
        except Exception as exc:
            logger.error("fleetbus.publish | subject={} failed: {}", subject, exc)
            raise

    async def subscribe(
        self,
        subject: str,
        handler: EnvelopeHandler,
        *,
        mode: DeliveryMode | None = None,
        consumer: str | None = None,
        typed_signal: object | None = None,
    ) -> Subscription:
        """Subscribe to decoded envelopes; bad messages are logged and dropped."""
        selected_mode = self._selected_mode(
            subject=subject, envelope=None, mode=mode, typed_signal=typed_signal
        )
        if selected_mode is DeliveryMode.DURABLE:
            return await self._subscribe_durable(subject, handler, consumer=consumer)

        async def _on_message(msg: Msg) -> None:
            try:
                envelope = Envelope.from_bytes(msg.data)
            except ValueError as exc:
                logger.warning("fleetbus.subscribe | bad message on {}: {}", subject, exc)
                return
            try:
                await handler(envelope)
            except Exception as exc:
                logger.error(
                    "fleetbus.subscribe | handler error subject={} kind={}: {}",
                    subject,
                    envelope.kind,
                    exc,
                )

        logger.info("fleetbus.subscribe | subject={}", subject)
        return await self._nc.subscribe(subject, cb=_on_message)

    async def raw_subscribe(
        self,
        subject: str,
        handler: RawMessageHandler,
        *,
        mode: DeliveryMode | None = None,
        consumer: str | None = None,
        typed_signal: object | None = None,
    ) -> Subscription:
        """Subscribe with the raw NATS message for request/reply responders.

        Durable raw handlers own decode and acknowledgement. The transport still
        validates a finite server-side ``MaxDeliver`` before binding, so a raw
        handler that forgets to ack cannot redeliver forever.
        """
        selected_mode = self._selected_mode(
            subject=subject, envelope=None, mode=mode, typed_signal=typed_signal
        )
        if selected_mode is DeliveryMode.DURABLE:
            return await self._raw_subscribe_durable(subject, handler, consumer=consumer)
        logger.info("fleetbus.raw_subscribe | subject={}", subject)
        return await self._nc.subscribe(subject, cb=handler)

    async def request(
        self, subject: str, envelope: Envelope, *, timeout_s: float = 5.0
    ) -> Envelope | None:
        """Send a core-NATS request and return one reply envelope or None."""
        logger.debug("fleetbus.request | subject={} kind={}", subject, envelope.kind)
        data = envelope.to_bytes()
        self._guard_payload_size(subject, data)
        try:
            reply = await self._nc.request(subject, data, timeout=timeout_s)
        except NoRespondersError:
            logger.warning("fleetbus.request | no responders on {}", subject)
            return None
        except NatsTimeoutError:
            logger.warning("fleetbus.request | timeout ({}s) on {}", timeout_s, subject)
            return None
        except Exception as exc:
            logger.error("fleetbus.request | subject={} failed: {}", subject, exc)
            raise
        try:
            return Envelope.from_bytes(reply.data)
        except ValueError as exc:
            logger.warning("fleetbus.request | undecodable reply on {}: {}", subject, exc)
            return None

    async def reply(self, msg: Msg, envelope: Envelope) -> None:
        """Reply to a request message over core NATS."""
        if not msg.reply:
            logger.warning("fleetbus.reply | message has no reply subject; dropping")
            return
        data = envelope.to_bytes()
        self._guard_payload_size(msg.reply, data)
        try:
            await self._nc.publish(msg.reply, data)
        except Exception as exc:
            logger.error("fleetbus.reply | failed: {}", exc)
            raise

    def _selected_mode(
        self,
        *,
        subject: str,
        envelope: Envelope | None,
        mode: DeliveryMode | None,
        typed_signal: object | None = None,
    ) -> DeliveryMode:
        """Resolve explicit mode or apply the delivery policy seam."""
        if mode is not None:
            return mode
        if envelope is None:
            if signal_prefers_core(typed_signal):
                return DeliveryMode.CORE
            return DeliveryMode.DURABLE
        return self.delivery_mode_for(subject, envelope)

    def _raise_durable_not_provisioned(
        self, *, subject: str, operation: str, reason: str | None = None
    ) -> NoReturn:
        """Raise the typed durable parking error with a guard log."""
        logger.error(
            "fleetbus.{} | durable selected subject={} but JetStream delivery is "
            "not provisioned; refusing core fallback reason={}",
            operation,
            subject,
            reason,
        )
        raise DurableNotProvisionedError(subject=subject, operation=operation, reason=reason)

    async def _publish_durable(self, subject: str, data: bytes) -> None:
        """Publish through JetStream after validating a matching existing stream."""
        try:
            js = self.jetstream()
            stream = await js.find_stream_name_by_subject(subject)
            ack = await js.publish(subject, data, stream=stream)
        except (JetStreamError, NoRespondersError, NatsTimeoutError) as exc:
            self._raise_durable_not_provisioned(
                subject=subject, operation="publish", reason=str(exc)
            )
        except TimeoutError as exc:
            self._raise_durable_not_provisioned(
                subject=subject, operation="publish", reason=str(exc)
            )
        except Exception as exc:
            logger.error("fleetbus.publish | durable subject={} failed: {}", subject, exc)
            raise
        if ack.stream != stream:
            logger.error(
                "fleetbus.publish | durable ack stream mismatch subject={} expected={} got={}",
                subject,
                stream,
                ack.stream,
            )
            raise DurableNotProvisionedError(
                subject=subject,
                operation="publish",
                reason=f"stream validation mismatch: expected {stream!r}, got {ack.stream!r}",
            )
        logger.info(
            "fleetbus.publish | durable subject={} stream={} seq={}",
            subject,
            ack.stream,
            ack.seq,
        )

    async def _durable_stream_and_consumer(
        self, *, subject: str, operation: str, consumer: str | None
    ) -> tuple[JetStreamContext, str, str]:
        """Validate the pre-provisioned stream and named durable consumer."""
        if not consumer:
            self._raise_durable_not_provisioned(
                subject=subject,
                operation=operation,
                reason="durable subscribe requires caller-supplied consumer identity",
            )
        assert consumer is not None
        try:
            js = self.jetstream()
            stream = await js.find_stream_name_by_subject(subject)
            info = await js.consumer_info(stream, consumer)
            self._validate_finite_max_deliver(info, subject=subject, operation=operation)
        except (JetStreamError, NoRespondersError, NatsTimeoutError) as exc:
            self._raise_durable_not_provisioned(
                subject=subject, operation=operation, reason=str(exc)
            )
        except TimeoutError as exc:
            self._raise_durable_not_provisioned(
                subject=subject, operation=operation, reason=str(exc)
            )
        except Exception as exc:
            logger.error("fleetbus.{} | durable subject={} failed: {}", operation, subject, exc)
            raise
        return js, stream, consumer

    def _validate_finite_max_deliver(
        self, info: ConsumerInfo, *, subject: str, operation: str
    ) -> None:
        """Reject durable consumers that can redeliver forever."""
        max_deliver = info.config.max_deliver
        if max_deliver is None or max_deliver < 1:
            self._raise_durable_not_provisioned(
                subject=subject,
                operation=operation,
                reason=(
                    "durable consumer must be pre-provisioned with finite "
                    f"MaxDeliver >= 1, got {max_deliver!r}"
                ),
            )

    async def _subscribe_durable(
        self, subject: str, handler: EnvelopeHandler, *, consumer: str | None
    ) -> Subscription:
        """Bind to an existing JetStream consumer and ack only successful work."""
        js, stream, consumer_name = await self._durable_stream_and_consumer(
            subject=subject, operation="subscribe", consumer=consumer
        )

        async def _on_message(msg: Msg) -> None:
            try:
                envelope = Envelope.from_bytes(msg.data)
            except ValueError as exc:
                logger.warning("fleetbus.subscribe | bad durable message on {}: {}", subject, exc)
                await msg.term()
                return
            try:
                await handler(envelope)
            except Exception as exc:
                logger.error(
                    "fleetbus.subscribe | durable handler error subject={} kind={}: {}",
                    subject,
                    envelope.kind,
                    exc,
                )
                await msg.nak(delay=DURABLE_RETRY_DELAY_S)
                return
            await msg.ack()

        logger.info(
            "fleetbus.subscribe | durable subject={} stream={} consumer={}",
            subject,
            stream,
            consumer_name,
        )
        return await js.subscribe(
            subject,
            cb=_on_message,
            durable=consumer_name,
            stream=stream,
            manual_ack=True,
        )

    async def _raw_subscribe_durable(
        self, subject: str, handler: RawMessageHandler, *, consumer: str | None
    ) -> Subscription:
        """Bind raw durable messages to an existing JetStream consumer.

        Raw handlers receive the full ``Msg`` and therefore own decode and ack.
        """
        js, stream, consumer_name = await self._durable_stream_and_consumer(
            subject=subject, operation="raw_subscribe", consumer=consumer
        )
        logger.info(
            "fleetbus.raw_subscribe | durable subject={} stream={} consumer={}",
            subject,
            stream,
            consumer_name,
        )
        return await js.subscribe(
            subject,
            cb=handler,
            durable=consumer_name,
            stream=stream,
            manual_ack=True,
        )


FleetTransport = FleetBus
