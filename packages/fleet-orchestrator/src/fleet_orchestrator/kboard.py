"""The k-board client — the durable source of truth for task lifecycle.

k-board (Postgres, tenant ``pg``) is the handoff record; NATS delivers, it is not
the record (design invariant). This is a small CAS-safe HTTP client over the
k-board REST API:

    list_tasks()                 GET  /api/tasks?tenant=pg
    get_task(task_id)            GET  /api/tasks/{id}?tenant=pg
    update_status(id, status)    PATCH with expected_sync_version (CAS)
    post_event(id, type, actor)  POST /api/tasks/{id}/events

The update path is **compare-and-set**: the server rejects a PATCH whose
``expected_sync_version`` is stale (another writer moved the task first). We
handle that deterministically — refetch the current version and retry, bounded —
so a concurrent writer causes a retry, never a lost update or an infinite loop.

CAS correctness depends on a **server contract**: GET→PATCH is not atomic on the
client, and there is no ``If-Match`` header — ``expected_sync_version`` is a body
field the *server* must check and reject when stale (409/412). If the server ever
ignored that field, a lost update would be silent and undetectable here. That the
server honors it is verified live in Phase 7, not assertable from this code.

No auth (local, per the infra owner). No secrets are ever logged; ``task_id`` /
status / event-type are coordination metadata and safe to log.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx
from fleet_nats.tracing import traced
from loguru import logger

from fleet_orchestrator.state import State

# Map a coordination State onto the k-board status it persists as. The five
# coordination states collapse to k-board's three lifecycle values. DISPATCHED
# (work assigned/fanned out) maps to "running", NOT a distinct "todo→claimed"
# status: k-board has only todo/running/done, so the claim moment is observable
# on the *event* stream (State→"claimed" below) but not as an intermediate status
# value — a deliberate collapse, not a skipped transition. The working/relay/
# arbitrate states are all "running" (in flight), and DONE is terminal.
_STATE_TO_STATUS: dict[State, str] = {
    State.DISPATCHED: "running",
    State.WORKING: "running",
    State.RELAY_NEEDED: "running",
    State.ARBITRATING: "running",
    State.DONE: "done",
}

# Map a coordination State onto the kboard_event name it emits on the bus
# (created|claimed|status|comment|completed — the sanctioned event vocabulary).
_STATE_TO_EVENT: dict[State, str] = {
    State.DISPATCHED: "claimed",
    State.WORKING: "status",
    State.RELAY_NEEDED: "status",
    State.ARBITRATING: "status",
    State.DONE: "completed",
}


def status_for_state(state: State) -> str:
    """The k-board status a coordination :class:`State` persists as."""
    return _STATE_TO_STATUS[state]


def event_for_state(state: State) -> str:
    """The ``kboard_event`` name a coordination :class:`State` emits."""
    return _STATE_TO_EVENT[state]


# The live k-board base + tenant (per issue #7). Overridable for tests/other envs.
_DEFAULT_BASE_URL = "https://kboard.example.internal"
_TENANT = "pg"

# Write auth (fleet-board #26, fleet-bus #206): every write endpoint requires an
# actor bearer. The token is a secret — read from the environment, sent only as
# a header, never logged. The actor is the identity the bearer authenticates
# (the hub principal); it is needed because the API's ownership rule requires an
# unassigned card to be CLAIMED by the authenticated actor on its first write.
_ENV_TOKEN = "FLEET_KBOARD_TOKEN"
_ENV_ACTOR = "FLEET_KBOARD_ACTOR"

# Valid task status values the orchestrator writes. The live k-board server
# accepts a wider set (its GET /api/tasks status filter documents blocked, done,
# ready, review, running, scheduled, todo, triage); this frozenset is the subset
# the coordination brain actually persists. "blocked" is written by the offline
# escalation ladder when wake→redispatch exhaust (fleet-bus #73): the card must
# reflect that its workers are blocked, not sit at "running" forever.
VALID_STATUSES = frozenset({"todo", "running", "blocked", "done"})

# How many times to refetch-and-retry a CAS update before giving up. A small
# fixed bound: a genuine conflict resolves in one refetch; more than this means
# sustained contention worth surfacing, not silently spinning.
_MAX_CAS_RETRIES = 3


class KboardError(RuntimeError):
    """A k-board operation failed (non-2xx after retries, or a transport error)."""


class KboardConflictError(KboardError):
    """A CAS update could not be applied within the retry budget (sustained conflict)."""


@dataclass(frozen=True)
class KboardTask:
    """A minimal view of a k-board task — id, status, prompt fields, CAS version.

    Only the fields the orchestrator needs; the full row has more. ``sync_version``
    is the CAS token echoed back on a PATCH. ``title``/``body`` are the card's
    prompt fields — the task text the intake path must carry to the worker
    (fleet-bus #52); a card may legitimately have one, both, or (rarely) neither.
    """

    task_id: str
    status: str
    sync_version: int
    raw: dict[str, Any]
    title: str = ""
    body: str = ""

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> KboardTask:
        """Build from a k-board API row, tolerating missing optional fields.

        ``title``/``body`` are coerced to ``""`` when absent or ``null`` (the live
        k-board row carries both, but the client must never assume a field is
        present) so the prompt-assembly seam always sees plain strings.
        """
        return cls(
            task_id=str(row.get("task_id", "")),
            status=str(row.get("status", "")),
            sync_version=int(row.get("sync_version", 0)),
            raw=row,
            title=str(row.get("title") or ""),
            body=str(row.get("body") or ""),
        )


class KboardClient:
    """CAS-safe async client for the k-board task API.

    Use as an async context manager so the underlying HTTP connection is closed::

        async with KboardClient() as kb:
            await kb.update_status("t_123", "running")

    Ownership: an *injected* ``client`` is the caller's to close — this class only
    closes a client it created itself (see :meth:`close`), so a caller reusing one
    ``httpx.AsyncClient`` across many KboardClients is never closed out from under.
    """

    def __init__(
        self,
        *,
        base_url: str = _DEFAULT_BASE_URL,
        tenant: str = _TENANT,
        timeout_s: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """Store config; a caller may inject an ``httpx.AsyncClient`` (for tests)."""
        self._base_url = base_url.rstrip("/")
        self._tenant = tenant
        self._params = {"tenant": tenant}
        # Write auth (fleet-bus #206): bearer + actor from the environment. When
        # the token is unset the header is simply omitted and the server 401s
        # writes fail-loud — reads stay public either way.
        token = os.environ.get(_ENV_TOKEN, "")
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._actor = os.environ.get(_ENV_ACTOR, "")
        # Injected client (tests pass a mock transport); otherwise we own one.
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._owns_client = client is None

    async def __aenter__(self) -> KboardClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client if we own it."""
        if self._owns_client:
            await self._client.aclose()

    @traced
    async def list_tasks(self) -> list[KboardTask]:
        """List all tasks for the tenant."""
        resp = await self._get("/api/tasks")
        body = resp.json()
        rows = body if isinstance(body, list) else body.get("tasks", [])
        return [KboardTask.from_row(r) for r in rows]

    @traced
    async def get_task(self, task_id: str) -> KboardTask:
        """Fetch a single task (raises :class:`KboardError` on non-2xx)."""
        resp = await self._get(f"/api/tasks/{task_id}")
        return KboardTask.from_row(resp.json())

    @traced
    async def create_task(
        self, *, task_id: str, title: str, body: str = "", status: str = "todo"
    ) -> KboardTask:
        """Create a new ``todo`` card via ``POST /api/tasks`` and return its row.

        The proven create recipe (graduation harness ``run-graduation.sh`` /
        fleet-bus #9 rung ladder): a ``t_``-prefixed ``task_id``, ``status:"todo"``,
        and the tenant carried BOTH as the query param (via ``_post``) and in the
        body — the live server reads it from the body on create. The intake poller
        auto-opens a session from a ``todo`` card whose title starts with the
        configured ``FLEET_INTAKE_LABEL`` (``[fleet]``), so a canary card titled
        ``[fleet] …`` rides the exact production path.

        This is the create verb of the k-board lifecycle; the orchestrator's own
        coordination never creates cards (only reads/updates them), but the nightly
        canary (fleet-bus #89) needs to inject one real card per night, so the verb
        lives here — the one place that owns the tenant param + error mapping — not
        in a second hand-rolled HTTP client.

        Args:
            task_id: The card id (caller supplies a unique ``t_``-prefixed id).
            title: The card title — MUST carry the ``FLEET_INTAKE_LABEL`` prefix
                for the intake poller to open it.
            body: The task text the worker sees (fleet-bus #52).
            status: The initial status (default ``"todo"`` — the only status the
                intake poller opens from). Validated against :data:`VALID_STATUSES`.

        Returns:
            The created :class:`KboardTask` (from the server's echoed row).

        Raises:
            ValueError: If ``status`` is not a valid k-board status.
            KboardError: On any non-2xx / transport failure.
        """
        if status not in VALID_STATUSES:
            raise ValueError(
                f"create_task: invalid status {status!r}, want {sorted(VALID_STATUSES)}"
            )
        payload = {
            "task_id": task_id,
            "status": status,
            "tenant": self._tenant,
            "title": title,
            "body": body,
        }
        resp = await self._post("/api/tasks", payload)
        created = KboardTask.from_row(resp.json())
        logger.info(
            "kboard.create_task | task={} status={} title_len={} body_len={}",
            task_id,
            status,
            len(title),
            len(body),
        )
        return created

    @traced
    async def update_status(self, task_id: str, status: str) -> KboardTask:
        """CAS-update a task's status: GET current version → PATCH → retry on conflict.

        Args:
            task_id: The task to update.
            status: New status — must be one of :data:`VALID_STATUSES`.

        Returns:
            The updated :class:`KboardTask`.

        Raises:
            ValueError: If ``status`` is not a valid k-board status.
            KboardConflictError: If a stale-version conflict persists past the
                retry budget (sustained concurrent contention).
            KboardError: On any other non-2xx / transport failure.
        """
        if status not in VALID_STATUSES:
            raise ValueError(
                f"update_status: invalid status {status!r}, want {sorted(VALID_STATUSES)}"
            )

        # CAS loop: fetch the current version, PATCH with it, and on a 409/412
        # (stale version — someone else moved the task) refetch and retry. Bounded.
        # Repeating an already-committed status is a GET-only no-op. A freshly
        # observed terminal done card is also preserved against this issue's
        # blocked escalation, including after a conflict refetch.
        last_conflict: Exception | None = None
        for attempt in range(1, _MAX_CAS_RETRIES + 1):
            current = await self.get_task(task_id)
            if current.status == status:
                logger.info(
                    "kboard.update_status | task={} status={} already current attempt={}",
                    task_id,
                    status,
                    attempt,
                )
                return current
            if current.status == "done" and status == "blocked":
                logger.warning(
                    "kboard.update_status | preserving terminal done task={} "
                    "requested={} attempt={}",
                    task_id,
                    status,
                    attempt,
                )
                return current
            payload = {"status": status, "expected_sync_version": current.sync_version}
            # Ownership rule (fleet-board #26): an unassigned card must be CLAIMED
            # by the authenticated actor on its first write; once claimed by the
            # hub, only the hub may move it. Claim exactly once, never reassign.
            if self._actor and current.raw.get("assignee") is None:
                payload["assignee"] = self._actor
            try:
                resp = await self._patch(f"/api/tasks/{task_id}", payload)
            except KboardConflictError as exc:
                last_conflict = exc
                logger.warning(
                    "kboard.update_status | CAS conflict task={} attempt={}/{} — refetching",
                    task_id,
                    attempt,
                    _MAX_CAS_RETRIES,
                )
                continue
            updated = KboardTask.from_row(resp.json())
            logger.info(
                "kboard.update_status | task={} status={} version={}->{} attempt={}",
                task_id,
                status,
                current.sync_version,
                updated.sync_version,
                attempt,
            )
            return updated

        raise KboardConflictError(
            f"update_status: CAS conflict on {task_id} unresolved after {_MAX_CAS_RETRIES} retries"
        ) from last_conflict

    @traced
    async def post_event(
        self, task_id: str, event_type: str, actor: str, body: str | None = None
    ) -> None:
        """Post a lifecycle event (``event_type`` + ``actor``; ``body`` optional).

        Events are append-only and not CAS-guarded (they don't mutate the row's
        version), so no retry loop is needed.
        """
        payload: dict[str, Any] = {"event_type": event_type, "actor": actor}
        if body is not None:
            payload["body"] = body
        await self._post(f"/api/tasks/{task_id}/events", payload)
        logger.info("kboard.post_event | task={} event={} actor={}", task_id, event_type, actor)

    # ---- HTTP primitives (one place for the tenant param + error mapping) -----

    async def _get(self, path: str) -> httpx.Response:
        return self._checked(await self._client.get(self._url(path), params=self._params))

    async def _patch(self, path: str, json: dict[str, Any]) -> httpx.Response:
        return self._checked(
            await self._client.patch(
                self._url(path), params=self._params, json=json, headers=self._headers
            )
        )

    async def _post(self, path: str, json: dict[str, Any]) -> httpx.Response:
        return self._checked(
            await self._client.post(
                self._url(path), params=self._params, json=json, headers=self._headers
            )
        )

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    @staticmethod
    def _checked(resp: httpx.Response) -> httpx.Response:
        """Map a response to an error type. 409/412 → conflict; other 4xx/5xx → error.

        409/412 → :class:`KboardConflictError` applies to every verb, not just the
        CAS PATCH — only ``update_status`` treats a conflict specially (refetch +
        retry); a conflict surfacing on a GET/POST is not expected from the API and
        propagates as a hard error, which is the correct fail-loud behavior.

        No response body is logged (it may echo the task row); only the status
        code and request path, which carry no secrets.
        """
        if resp.status_code in (409, 412):
            raise KboardConflictError(
                f"kboard: CAS conflict ({resp.status_code}) on {resp.request.url.path}"
            )
        if resp.status_code >= 400:
            logger.error(
                "kboard | request failed status={} path={}", resp.status_code, resp.request.url.path
            )
            raise KboardError(f"kboard: {resp.status_code} on {resp.request.url.path}")
        return resp
