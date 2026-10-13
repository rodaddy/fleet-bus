# fleet-bus

A NATS coordination bus for a small fleet of long-running AI agents, plus a
deterministic orchestrator that decides who works on what.

fleet-bus is the bus. Its sibling repo,
[rtech-agents](https://github.com/rodaddy/rtech-agents), is the agent runtime
that rides on it: per-agent daemons, typed frames, chat edges. You can use
fleet-bus without rtech-agents; rtech-agents needs fleet-bus.

## Why it exists

Agents that coordinate by talking to each other in a chat channel go wrong in
predictable ways: they miss mentions, answer twice, lose handoffs, and nobody
can say afterwards who owned a task. fleet-bus moves the coordination off the
chat surface and into code:

- **Chat (Discord) is the human display.** Agents post their own words there,
  as themselves. Nothing decides control flow by reading chat.
- **NATS is the control plane.** Dispatch, relay, wake and sequencing ride typed
  envelopes on a fixed subject tree (`{env}.{domain}.{...}`).
- **The k-board is the record.** An external task board, reached over its REST
  API, holds task state with compare-and-set updates. NATS delivers; it is not
  the record. This repo ships only the client (`fleet_orchestrator.kboard`); the
  board service itself is not included.
- **The orchestrator is a state machine, not a model.** No LLM sits in the
  control path. Every transition is a case in code and is unit-tested.

## Packages

| Package | What it does |
|---|---|
| `packages/fleet-core` | Base client: typed config, logging, transport boundary, a prod/staging URL tripwire. |
| `packages/fleet-nats` | The shared NATS library: connection, typed envelope, subject builders, pub/sub/request-reply, redaction, channel map. Every consumer imports this instead of writing its own transport. |
| `packages/fleet-orchestrator` | The deterministic coordination brain: task state machine, behaviors (dispatch, relay, wake, retry), k-board CAS client, intake poller, tracing. |
| `packages/fleet-subscriber` | Per-agent subscriber plugin that turns an inbound envelope into a real agent turn on a Hermes-style gateway. |

`deploy/nats/` holds example NATS server configs and provisioning scripts for an
authenticated cluster. `scripts/` holds the hub deploy and agent-install scripts.

## Run it

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/). `nats-server` on
PATH lets the JetStream tests run instead of skipping.

```bash
cd packages/fleet-nats
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/ruff check src/ tests/
.venv/bin/mypy src/ tests/
.venv/bin/python -m pytest tests/ -q
```

Repeat for each package, or see `.github/workflows/ci.yml`, which gates all
four. Point a consumer at your own server with `FLEET_NATS_URL` (default
`nats://192.0.2.74:4222`, a documentation address: change it).

```python
from fleet_nats import BusConfig, Envelope, FleetBus, subjects

async with FleetBus(BusConfig.from_env("manager")) as bus:
    await bus.publish(
        subjects.agent_dispatch("dev", "worker-1", channel="all-agents"),
        Envelope.new(msg_id=..., ts=..., sender="manager", kind="dispatch",
                     payload={"task": "..."}),
    )
```

## Tests

Unit tests run anywhere. Tests named `*_live.py` (and `test_e2e_live.py`) need
a reachable NATS server and real agents; they skip themselves when the server
is not reachable. The shell regressions in `tests/*.test.sh` exercise the deploy
scripts against local fixtures.

## What is included and what is not

- What this repo contains: the four Python packages above, their unit tests,
  example NATS configs, and deploy scripts. It does not contain the k-board
  service, any agent runtime, or any agent personas.
- The author ran this code for a three-agent fleet on a private network in
  2026. That deployment cannot be reproduced from this repo alone, and no
  other deployment has been run. Hostnames, agent names and the
  Discord channel map are config you supply. The deploy scripts and
  `.github/workflows/deploy*.yml` assume a single hub host reached over SSH from
  a self-hosted runner; treat them as a worked example.

## Naming notes

- "Dash" in the docs and tests is a neutral stand-in for a sibling TypeScript
  platform that shares the same subject convention. It is not part of this
  repo.
- Bilby, Nagatha and Skippy are the names of the author's agents and appear as
  example agent ids.
- Hostnames under `example.internal` and addresses in `192.0.2.0/24` are
  placeholders.

## Docs

- `docs/architecture.md`: the whole system, traced from a human post to a
  finished task.
- `docs/consumers.md`: how a consumer imports `fleet-nats` as a thin adapter.
- `docs/contracts/`: envelope, ordering, retry and QoS contracts.
- `docs/runbook.md`: deploy, verify, roll back, troubleshoot.
- `docs/logging.md`: the sanitized logging convention.

## License

MIT. See `LICENSE`.
