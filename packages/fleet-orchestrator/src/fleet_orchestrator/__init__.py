"""fleet-orchestrator — the deterministic DAP coordination brain.

A pure state machine (:class:`CoordinationSession` + :func:`next_action`) with,
in later phases, bus-wired behaviors over ``fleet-nats``. No LLM in the control
path: coordination is explicit case-statements, provable in isolation.

Public surface:
    State                — the coordination state enum.
    EventKind / Event    — the inbound events that drive transitions.
    CoordinationSession  — one task worked by the trio (frozen data).
    ActionKind / Action  — the single decision a transition returns.
    next_action          — the pure ``(session, event) -> Action`` heart.
    LivenessTracker      — heartbeat-derived effective topology.
"""

from __future__ import annotations

from fleet_orchestrator.behaviors import IdClock, Orchestrator
from fleet_orchestrator.canary import (
    CanaryOutcome,
    CanaryResult,
    run_canary,
)
from fleet_orchestrator.config import HubConfig
from fleet_orchestrator.costfeed import CostLedger, sum_task_cost
from fleet_orchestrator.detect import decide_arbitration, detect_missing_handoff
from fleet_orchestrator.governor import (
    NARRATION_KIND,
    Breach,
    BreachKind,
    Budget,
    Governor,
    Narrator,
    render_narration,
)
from fleet_orchestrator.governor_store import (
    FleetKVGovernorStore,
    GovernorStore,
    durable_enabled_from_env,
    open_governor_store,
)
from fleet_orchestrator.intake import IntakeTask, parse_intake
from fleet_orchestrator.intake_poller import (
    IntakePoller,
    intake_enabled_from_env,
    intake_poll_from_env,
)
from fleet_orchestrator.kboard import (
    KboardClient,
    KboardConflictError,
    KboardError,
    KboardTask,
    event_for_state,
    status_for_state,
)
from fleet_orchestrator.killswitch import (
    KillSwitch,
    kill_poll_from_env,
)
from fleet_orchestrator.liveness import AgentView, LivenessTracker, watch_heartbeats
from fleet_orchestrator.loop import (
    OrchestratorHub,
    build_hub,
)
from fleet_orchestrator.offline import (
    Escalation,
    OfflineDecision,
    OfflineDetector,
    timeout_from_env,
)
from fleet_orchestrator.score import (
    Decision,
    DecisionKind,
    Score,
    ScoreEvent,
    ScoreEventKind,
    Scorekeeper,
    Stance,
    TurnSignal,
    assign_synthesis,
    circlejerk_n_from_env,
    decide,
    narrate_decision,
    narrate_score_event,
    parse_turn_signal,
)
from fleet_orchestrator.sentinel import (
    DEFAULT_OUTCOME,
    DONE_KIND,
    done_envelope,
    is_done_envelope,
    parse_done,
)
from fleet_orchestrator.state import (
    CoordinationSession,
    Event,
    EventKind,
    State,
)
from fleet_orchestrator.transitions import Action, ActionKind, next_action

__all__ = [
    "DEFAULT_OUTCOME",
    "DONE_KIND",
    "NARRATION_KIND",
    "Action",
    "ActionKind",
    "AgentView",
    "Breach",
    "BreachKind",
    "Budget",
    "CanaryOutcome",
    "CanaryResult",
    "CoordinationSession",
    "CostLedger",
    "Decision",
    "DecisionKind",
    "Escalation",
    "Event",
    "EventKind",
    "FleetKVGovernorStore",
    "Governor",
    "GovernorStore",
    "HubConfig",
    "IdClock",
    "IntakePoller",
    "IntakeTask",
    "KboardClient",
    "KboardConflictError",
    "KboardError",
    "KboardTask",
    "KillSwitch",
    "LivenessTracker",
    "Narrator",
    "OfflineDecision",
    "OfflineDetector",
    "Orchestrator",
    "OrchestratorHub",
    "Score",
    "ScoreEvent",
    "ScoreEventKind",
    "Scorekeeper",
    "Stance",
    "State",
    "TurnSignal",
    "assign_synthesis",
    "build_hub",
    "circlejerk_n_from_env",
    "decide",
    "decide_arbitration",
    "detect_missing_handoff",
    "done_envelope",
    "durable_enabled_from_env",
    "event_for_state",
    "intake_enabled_from_env",
    "intake_poll_from_env",
    "is_done_envelope",
    "kill_poll_from_env",
    "narrate_decision",
    "narrate_score_event",
    "next_action",
    "open_governor_store",
    "parse_done",
    "parse_intake",
    "parse_turn_signal",
    "render_narration",
    "run_canary",
    "status_for_state",
    "sum_task_cost",
    "timeout_from_env",
    "watch_heartbeats",
]

__version__ = "0.1.0"
