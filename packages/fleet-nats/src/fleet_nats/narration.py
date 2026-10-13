"""The narration wire kind — shared by the emitter and the consumer (#13).

The orchestrator's ``Narrator`` (fleet-orchestrator) publishes templated beat
envelopes with this kind on the MANAGER's inbox lane; the subscriber's mapping
(fleet-subscriber) recognizes it and forges an *announce* turn — never a bare
prompt. The constant lives here, in the shared transport library, so the two
sides can never drift on the wire value (the ``done_envelope`` precedent).
"""

from __future__ import annotations

NARRATION_KIND = "narration"
