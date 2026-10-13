"""Subject-naming convention for the fleet bus.

Mirrors the live Dash convention ``{env}.{domain}.{...}`` (e.g.
``prod.signal.momentum.CL``): dot-delimited, env-prefixed, hierarchical. The
subject tree is MULTI-LAYER by interaction type so a flood/failure in one layer
cannot contaminate another and each is independently observable:

    {env}.agent.<...>      crew coordination (Discord-originated trio DAP)
    {env}.direct.<...>     local/human agents -> trio, no Discord overhead
    {env}.human.<...>      human/local-agent topics
    {env}.kboard.<...>     k-board task lifecycle events
    {env}.hub.<...>        out-of-band control plane for the manager run-loop
    {env}.factory.<...>    publish-only telemetry from disposable runner boxes
    {env}.ob.<...>         Open Brain memory transport (OB-owned domain)

Agent coordination subjects are further segmentable per Discord channel so
questions in different channels never cross-contaminate.

Two orthogonal axes (do not conflate them — fleet-bus #117)
-----------------------------------------------------------

- **lane = channel scoping.** The optional ``channel`` token narrows WHICH
  slice of traffic a subscriber sees. It is a routing/isolation axis: a
  subscriber scoped to ``dash-dev`` receives only ``dash-dev`` traffic because
  NATS wildcards match per-token, not because it lacks permission to hear more.
- **namespace = access control.** WHO may publish/subscribe a subject is the
  server-side credential grant (``deploy/nats/fleet-prod-nats-server.conf``).

Channel scoping is NOT an authorization boundary. A principal whose grant covers
``{env}.agent.*.status.{task}`` can subscribe every channel's slice of that lane;
the scoping only decides what a *correctly configured* subscriber attaches to.

Noise lanes (fleet-bus #116)
----------------------------

Acks, tool-echo, and progress markers are needed-but-noisy: they must stay fully
logged and mineable, but must never cost an LLM turn. They get their OWN
subjects (``{env}.agent.ack.…`` / ``{env}.agent.telemetry.…``) rather than riding
the coordination ``status`` lane, so a Discord bridge simply does not forward
them — suppression is a SUBJECT fact a bridge can act on without parsing prose,
not a heuristic applied to a mixed lane.

Build subjects only through these helpers — nothing downstream should hand-format
a subject string.
"""

from __future__ import annotations

# NATS wildcard for a single token; used when subscribing across a layer.
STAR = "*"


def slug(value: str) -> str:
    """Normalise a token for use in a subject (no dots or spaces).

    Args:
        value: Raw token (agent id, channel name, task id, ...).

    Returns:
        A subject-safe token.

    Raises:
        ValueError: If the token normalises to nothing (e.g. whitespace-only) —
            an empty token would silently produce an invalid NATS subject like
            ``dev.agent..dispatch.w`` that the server rejects (message lost) —
            or if it contains ``>``, the NATS multi-token wildcard.

    The ``>`` rejection is a hard rule, not normalisation: a token is ONE subject
    token, and ``>`` is never a legitimate one. Wildcards in this module are
    always structural — the :data:`STAR` constant or a literal trailing ``.>`` in
    the builder — so nothing legitimate ever routes ``>`` through here. Left
    un-rejected it becomes subject-token injection the moment a token derives
    from untrusted input: ``factory_runner(env, ">")`` (issue #222, whose
    ``job_id`` can come from repository content a runner box is processing)
    yields ``{env}.factory.runner.>``, which the single-token publish grant
    ``{env}.factory.runner.*`` still ACCEPTS — so a consumer parsing the job id
    back out of ``msg.subject`` reads a wildcard-shaped key and may write it into
    a database row, a path, or a log-routing key.

    ``*`` is deliberately NOT rejected: callers build authorization grant
    patterns by passing :data:`STAR` as a token (see ``tests/test_authz_conf.py``),
    and unlike ``>`` it cannot widen a subject beyond a single token.
    """
    token = value.strip().lower().replace(" ", "-").replace(".", "-")
    if not token:
        raise ValueError(f"subject token normalises to empty: {value!r}")
    if ">" in token:
        raise ValueError(
            f"subject token may not contain the NATS multi-token wildcard '>': {value!r}"
        )
    return token


def _agent_lane(env: str, lane: str, tail: str, channel: str | None) -> str:
    """Build one ``{env}.agent.[{channel}.]{lane}.{tail}`` subject.

    The ONE place the channel-scoping shape is expressed (fleet-bus #117). The
    scoped form inserts ``{channel}`` between ``agent`` and the lane name, which
    is the shape ``agent_dispatch`` established and the live server grants
    already match (``dev.agent.*.dispatch.*``). Every channel-scopable lane
    routes through here so the two forms can never drift lane-by-lane.

    Args:
        env: Environment prefix.
        lane: The lane name token (``dispatch``/``status``/``relay``/…).
        tail: The already-raw addressing token (slugged here, not by the caller).
        channel: The channel to scope to, or ``None``/empty for the bare form.

    Returns:
        The scoped subject when ``channel`` is truthy, else the bare form.
    """
    if channel:
        return f"{env}.agent.{slug(channel)}.{lane}.{slug(tail)}"
    return f"{env}.agent.{lane}.{slug(tail)}"


def _agent_lane_wildcard(env: str, lane: str, tail: str) -> str:
    """The ``{env}.agent.*.{lane}.{tail}`` subscription wildcard for a lane.

    A scoped publish form has one MORE token than its bare form and NATS
    wildcards match per-token, so a bare-form subscription never receives scoped
    traffic. A subscriber that wants every channel's slice must attach BOTH this
    wildcard and the bare form — that is why every scopable lane ships a wildcard
    builder beside it, not instead of it.
    """
    return f"{env}.agent.{STAR}.{lane}.{slug(tail)}"


def assert_publishable(subject: str) -> str:
    """Return ``subject`` unchanged, or raise if it can never be published.

    A NATS *publish* subject must be fully concrete. ``*`` and ``>`` are legal
    only in a SUBSCRIPTION or in a server-side grant pattern, and the server
    rejects a wildcard-bearing publish **asynchronously** — :meth:`FleetBus
    .publish` is fire-and-forget, so nothing raises at the call site and the
    message is simply never delivered. That failure surfaces far away as a task
    that hangs to timeout with no error, which is why this must be caught here.

    The concrete case this closes (fleet-bus #117): ``channel`` is a
    PUBLISHER-CONTROLLED token — ``mapping.chat_id = envelope.channel`` — and
    #117 routed it into ``status``/``relay``/``wake``/``ack``/``telemetry``.
    :func:`slug` deliberately permits ``*`` so callers can build authorization
    grant patterns (``agent_status(env, STAR, channel=STAR)`` in
    ``tests/test_authz_conf.py``), so the guard cannot live in ``slug`` without
    breaking grant construction. It belongs at the publish boundary instead:
    grant builders never call this, publish sites always do. A hostile inbound
    ``channel="*"`` would otherwise make the published reply subject
    byte-identical to the hub's subscription wildcard
    (``dev.agent.*.status.task-1``) and silently vanish.

    Args:
        subject: A fully built subject string headed for a publish call.

    Returns:
        ``subject``, unchanged, when it is concrete.

    Raises:
        ValueError: If ``subject`` contains ``*`` or ``>`` in any token.
    """
    if STAR in subject or ">" in subject:
        raise ValueError(
            f"refusing to publish a wildcard-bearing subject (NATS would drop it): {subject!r}"
        )
    return subject


def agent_dispatch(env: str, worker: str, *, channel: str | None = None) -> str:
    """Subject for dispatching a task to a worker (manager -> worker)."""
    return _agent_lane(env, "dispatch", worker, channel)


def agent_dispatch_wildcard(env: str, worker: str) -> str:
    """Subscription wildcard matching PER-CHANNEL dispatches to ``worker``.

    The per-channel publish form (``{env}.agent.{channel}.dispatch.{worker}``)
    has one more token than the bare form, and NATS wildcards match per-token —
    so a subscriber of the bare subject NEVER receives a per-channel dispatch.
    A worker must subscribe BOTH this wildcard and the bare form to receive
    every dispatch addressed to it.
    """
    return _agent_lane_wildcard(env, "dispatch", worker)


def agent_status(env: str, task_id: str, *, channel: str | None = None) -> str:
    """Subject for a worker's progress/heartbeat/complete on a task.

    Channel-scopable (fleet-bus #117). The dispatch that started the task is
    already per-channel, so its reply lane must be scopable too — otherwise a
    channel-scoped observer that receives only ``dash-dev`` dispatches would
    still have to watch every channel's status traffic to hear the answers.
    """
    return _agent_lane(env, "status", task_id, channel)


def agent_status_wildcard(env: str, task_id: str) -> str:
    """Subscription wildcard matching PER-CHANNEL status for ``task_id``.

    The hub watches a task whose channel it knows, so it normally subscribes the
    concrete scoped subject. This wildcard is for a cross-channel observer (a
    dashboard) that tracks a task id without knowing its channel.
    """
    return _agent_lane_wildcard(env, "status", task_id)


def agent_relay(env: str, to: str, *, channel: str | None = None) -> str:
    """Subject for the manager relaying needed context to an agent.

    Channel-scopable (#117): a relay carries the context missed in ONE channel's
    deliberation, so it belongs to that channel's slice, not to the agent globally.
    """
    return _agent_lane(env, "relay", to, channel)


def agent_relay_wildcard(env: str, to: str) -> str:
    """Subscription wildcard matching PER-CHANNEL relays addressed to ``to``."""
    return _agent_lane_wildcard(env, "relay", to)


def agent_wake(env: str, worker: str, *, channel: str | None = None) -> str:
    """Subject for a lightweight wake ping to a worker.

    Channel-scopable (#117): a wake nudges a worker that went quiet on ONE
    channel's task, so it rides that channel's slice.
    """
    return _agent_lane(env, "wake", worker, channel)


def agent_wake_wildcard(env: str, worker: str) -> str:
    """Subscription wildcard matching PER-CHANNEL wakes addressed to ``worker``."""
    return _agent_lane_wildcard(env, "wake", worker)


def agent_ack(env: str, task_id: str, *, channel: str | None = None) -> str:
    """Subject for a worker's bare acknowledgement of a task (#116).

    An ack says "received, starting" and nothing else. It is needed — an operator
    and the hub both want to know a dispatch landed — but it is NOT actionable
    prose, so it must never reach a human Discord channel and never cost an LLM
    turn. Its OWN lane, deliberately NOT ``agent.status``: the Discord bridge
    decides what to forward from the SUBJECT, so an ack is excluded structurally
    rather than by a prose heuristic on a mixed lane that would eventually
    misclassify a real reply.

    App-plane. Channel-scopable (#117) so a scoped miner sees only its channel.
    """
    return _agent_lane(env, "ack", task_id, channel)


def agent_ack_wildcard(env: str, task_id: str) -> str:
    """Subscription wildcard matching PER-CHANNEL acks for ``task_id``."""
    return _agent_lane_wildcard(env, "ack", task_id)


def agent_telemetry(env: str, task_id: str, *, channel: str | None = None) -> str:
    """Subject for a worker's tool-echo / progress-marker telemetry (#116).

    The hourglass and floppy-disk progress markers, tool echoes, and other
    mid-turn chatter. Same contract as :func:`agent_ack`: fully logged and
    mineable, never forwarded to Discord, never an LLM turn. Kept separate from
    ``ack`` because the two have different volumes and different consumers — an
    ack is one message per dispatch, telemetry is unbounded per turn, so a miner
    that wants only delivery confirmation must not have to filter a firehose.

    App-plane. Channel-scopable (#117).
    """
    return _agent_lane(env, "telemetry", task_id, channel)


def agent_telemetry_wildcard(env: str, task_id: str) -> str:
    """Subscription wildcard matching PER-CHANNEL telemetry for ``task_id``."""
    return _agent_lane_wildcard(env, "telemetry", task_id)


def agent_claim(env: str, task_id: str) -> str:
    """Subject for a worker claiming a task (request/reply)."""
    return f"{env}.agent.claim.{slug(task_id)}"


def agent_arbitrate(env: str, topic: str) -> str:
    """Subject for asking the manager to decide (request/reply)."""
    return f"{env}.agent.arbitrate.{slug(topic)}"


def agent_inbox(env: str, agent: str) -> str:
    """Per-agent direct inbox subject."""
    return f"{env}.agent.inbox.{slug(agent)}"


def agent_heartbeat(env: str, agent: str) -> str:
    """Per-agent liveness subject."""
    return f"{env}.agent.heartbeat.{slug(agent)}"


def agent_heartbeat_wildcard(env: str) -> str:
    """Subscription wildcard matching every agent's heartbeat lane.

    The consumer (orchestrator liveness / future dashboard) derives topology
    from ALL agents' beats, whose ids are unknown at subscribe time.
    """
    return f"{env}.agent.heartbeat.{STAR}"


def dead_letter(env: str) -> str:
    """The dead-letter lane: sanitized records of unprocessable envelopes (#25).

    One lane per env (not per agent): the record itself carries the source
    subject + reason, and a single lane is what an operator watches.
    """
    return f"{env}.agent.dead-letter"


def token_cost(env: str, agent: str) -> str:
    """Per-agent token-cost feed subject (#27).

    One turn's measured token accounting is published here after each Hermes
    agent turn (the agent runtime's post-turn hook). Its own lane, NOT folded into the heartbeat
    (#26): the cadences differ — a beat is bounded-interval, a cost event is
    per-turn — and a governor summing spend must not miss a turn between beats.
    """
    return f"{env}.agent.token_cost.{slug(agent)}"


def token_cost_wildcard(env: str) -> str:
    """Subscription wildcard matching every agent's token-cost lane.

    The consumer (governor #13 / dashboard #14) sums spend across ALL agents,
    whose ids are unknown at subscribe time — mirrors
    :func:`agent_heartbeat_wildcard`.
    """
    return f"{env}.agent.token_cost.{STAR}"


def direct(env: str, agent: str) -> str:
    """Direct lane: a local/human agent messaging a fleet agent, off-Discord."""
    return f"{env}.direct.{slug(agent)}"


def kboard_event(env: str, event: str, task_id: str) -> str:
    """k-board lifecycle event subject (created|claimed|status|comment|completed)."""
    return f"{env}.kboard.task.{slug(event)}.{slug(task_id)}"


def hub_control(env: str, task_id: str) -> str:
    """Control-plane subject for an out-of-band op on a task's coordination.

    The manager run-loop (the hub) owns a task's live governor state in-process;
    a paused (over-budget) task is only resumable by that live instance. This is
    the lane a harness or a Nagatha-side steer publishes a ``control`` frame on
    to reach it — ``resume`` / ``pause`` (fleet-bus #59). Its OWN layer, not
    folded into ``agent``: control ops are hub-directed operator actions, not
    crew coordination, and must stay independently observable and isolated from a
    coordination flood.
    """
    return f"{env}.hub.control.{slug(task_id)}"


def hub_control_wildcard(env: str) -> str:
    """Subscription wildcard matching EVERY task's control lane.

    The hub subscribes this once at start (task ids are unknown at subscribe
    time and a control frame may target a task created later) — mirrors
    :func:`agent_heartbeat_wildcard` / :func:`token_cost_wildcard`.
    """
    return f"{env}.hub.control.{STAR}"


def hub_show_work(env: str) -> str:
    """Subject for the hub-owned pull-only ``show_work`` request/reply service."""
    return f"{env}.hub.show-work"


def factory_runner(env: str, job_id: str) -> str:
    """Publish-only telemetry subject for a software-factory runner box (#222).

    Its OWN layer, deliberately NOT ``agent.status.{task_id}``: a runner box is a
    disposable privileged LXC running a coding agent with ``bypassPermissions``
    over untrusted repository content. Folding its telemetry into the agent
    coordination tree would (a) collide a runner ``job_id`` with the agent task
    keyspace ``watch_worker_status`` subscribes per-task, and (b) put untrusted
    output on the same layer as crew coordination — exactly the contamination the
    multi-layer subject tree exists to prevent.

    A runner PUBLISHES here and subscribes to NOTHING. Dispatch stays on the
    durable path (Postgres job row + atomic claim + ``mcp2cli runner_run``): per
    C3 NATS core is at-most-once with no retention, so a dispatch published
    before a subscriber attached is silently lost. The bus is the wrong transport
    for handing out work and this lane does not carry any.
    """
    return f"{env}.factory.runner.{slug(job_id)}"


def factory_runner_wildcard(env: str) -> str:
    """Subscription wildcard matching EVERY runner job's telemetry lane.

    The consumer (a software-factory dashboard) renders live run progress
    for jobs whose ids are unknown at subscribe time — mirrors
    :func:`agent_heartbeat_wildcard` / :func:`token_cost_wildcard`.
    """
    return f"{env}.factory.runner.{STAR}"


def ob_context_pack(env: str) -> str:
    """Open Brain's agent context-pack request/reply subject (#221).

    ``{env}.ob.memory.context_pack`` — the ``ob`` domain is Open-Brain-owned and
    already granted server-side (``deploy/nats/fleet-prod-nats-server.conf``
    §4d grants ``open-brain`` publish+subscribe on ``{env}.ob.>``).

    Why this builder exists: OB is LIVE on this lane and, lacking a Python
    builder to mirror, hand-maintained its own TypeScript reimplementation
    (``open-brain/src/nats-subjects.ts``) — including a second copy of the slug
    rule. ``docs/consumers.md`` requires consumers to build subjects only through
    the shared helpers, so the fix is the builder here, not a better fork there.

    The env token IS slugged, unlike the ``{env}.agent.…`` builders which
    interpolate ``env`` raw. That is deliberate parity with the live TS mirror
    (``obContextPackSubject`` calls ``slugSubjectToken(env)``): changing it here
    would put the two languages on different wires for any non-trivial env
    string. The mismatch is safe in practice because ``BusConfig`` already
    constrains ``env`` to ``dev``/``prod``/``staging``, all slug-identical.

    The ``ob.memory.context_pack`` tail is a fixed, already-normalised literal —
    note the UNDERSCORE in ``context_pack``, which is byte-identical to the
    pre-fleet flat subject minus the env prefix. It is not passed through
    :func:`slug` (which would leave it unchanged anyway) so nothing implies the
    tail is caller-controlled.

    App-plane: the consumer is OB's deterministic context-pack worker, which
    answers a request/reply from Postgres. Receipt creates no LLM turn.
    """
    return f"{slug(env)}.ob.memory.context_pack"


def wildcard(env: str, layer: str) -> str:
    """Subscribe across an entire layer, e.g. ``dev.agent.>``.

    Args:
        env: Environment prefix.
        layer: One of ``agent``, ``direct``, ``human``, ``kboard``, ``hub``,
            ``factory``, ``ob``.

    Returns:
        A ``>``-terminated wildcard subject for the layer.
    """
    return f"{env}.{slug(layer)}.>"
