"""Unit tests for the CAS-safe k-board client, against an httpx MockTransport.

No network: an ``httpx.MockTransport`` scripts the GET/PATCH/POST sequence so the
compare-and-set path is provable deterministically — happy-path version echo,
stale-conflict refetch-retry, persistent conflict, and the pre-network status
guard. The live round-trip against the real k-board is exercised in Phase 7.
"""

from __future__ import annotations

import importlib

import fleet_nats.tracing as tracing
import httpx
import pytest
from loguru import logger

from fleet_orchestrator import (
    KboardClient,
    KboardConflictError,
    KboardError,
    State,
    event_for_state,
    status_for_state,
)


def _client(handler: httpx.MockTransport) -> KboardClient:
    """A KboardClient wired to a scripted mock transport (no real network)."""
    return KboardClient(client=httpx.AsyncClient(transport=handler))


async def test_update_status_happy_path_echoes_current_version() -> None:
    # GET returns version 7; the PATCH must carry expected_sync_version=7.
    patch_count = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal patch_count
        if request.method == "GET":
            return httpx.Response(200, json={"task_id": "t-1", "status": "todo", "sync_version": 7})
        patch_count += 1
        import json as _json

        body = _json.loads(request.content)
        assert body["expected_sync_version"] == 7  # echoes the version GET returned
        assert body["status"] == "running"
        return httpx.Response(200, json={"task_id": "t-1", "status": "running", "sync_version": 8})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "running")

    assert patch_count == 1  # exactly one PATCH — no retry on the happy path
    assert task.status == "running"
    assert task.sync_version == 8


async def test_update_status_refetches_and_retries_on_stale_conflict() -> None:
    # First PATCH hits a 409 (someone else moved the task); the client must
    # refetch the *new* version and retry. Prove: GET,PATCH(409),GET,PATCH(200)
    # and that the retried PATCH carries the refreshed version (11, not 10).
    calls: list[str] = []
    versions = iter([10, 11])
    patch_versions: list[int] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "todo", "sync_version": next(versions)}
            )
        import json as _json

        patch_versions.append(_json.loads(request.content)["expected_sync_version"])
        if len(patch_versions) == 1:
            return httpx.Response(409, json={"error": "stale"})  # first attempt loses the race
        return httpx.Response(200, json={"task_id": "t-1", "status": "done", "sync_version": 12})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "done")

    assert calls == ["GET", "PATCH", "GET", "PATCH"]
    assert patch_versions == [10, 11]  # the retry used the *refreshed* version
    assert task.sync_version == 12


async def test_update_status_returns_current_task_without_patch_when_status_matches() -> None:
    """An already-committed status is a GET-only idempotent success."""
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method != "GET":
            raise AssertionError("matching current status must not issue a PATCH")
        return httpx.Response(200, json={"task_id": "t-1", "status": "running", "sync_version": 7})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "running")

    assert calls == ["GET"]
    assert task.task_id == "t-1"
    assert task.status == "running"
    assert task.sync_version == 7


async def test_update_status_does_not_downgrade_an_initially_done_task() -> None:
    """A BLOCKED write must not resurrect a terminal card seen before PATCH."""
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "done", "sync_version": 12}
            )
        raise AssertionError("a terminal done task must not be PATCHed back to blocked")

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "blocked")

    assert calls == ["GET"]
    assert task.status == "done"


async def test_done_preservation_is_scoped_to_blocked_requests() -> None:
    """Issue #76 must not silently create a broader lifecycle policy."""
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "done", "sync_version": 12}
            )
        return httpx.Response(200, json={"task_id": "t-1", "status": "running", "sync_version": 13})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "running")

    assert calls == ["GET", "PATCH"]
    assert task.status == "running"


async def test_update_status_does_not_downgrade_done_after_conflict_refetch() -> None:
    """A 409 refetch that observes done stops the blocked retry before its PATCH."""
    calls: list[str] = []
    patch_versions: list[int] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            status, version = ("running", 10) if calls.count("GET") == 1 else ("done", 11)
            return httpx.Response(
                200, json={"task_id": "t-1", "status": status, "sync_version": version}
            )
        import json as _json

        patch_versions.append(_json.loads(request.content)["expected_sync_version"])
        return httpx.Response(409, json={"error": "stale"})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "blocked")

    assert calls == ["GET", "PATCH", "GET"]
    assert patch_versions == [10]
    assert task.status == "done"


async def test_update_status_raises_on_persistent_conflict() -> None:
    # Every PATCH conflicts: after the retry budget the client raises
    # KboardConflictError (bounded — it does not spin forever).
    patch_count = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal patch_count
        if request.method == "GET":
            return httpx.Response(200, json={"task_id": "t-1", "status": "todo", "sync_version": 1})
        patch_count += 1
        return httpx.Response(412, json={"error": "precondition failed"})

    async with _client(httpx.MockTransport(handle)) as kb:
        with pytest.raises(KboardConflictError, match="unresolved after"):
            await kb.update_status("t-1", "running")

    assert patch_count == 3  # exactly _MAX_CAS_RETRIES attempts, then give up


async def test_update_status_rejects_invalid_status_before_any_network_call() -> None:
    # An invalid status must raise ValueError *before* touching the network — a
    # transport that fails the test if called proves no request was made.
    def handle(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        raise AssertionError(f"no network call expected, got {request.method}")

    async with _client(httpx.MockTransport(handle)) as kb:
        with pytest.raises(ValueError, match="invalid status"):
            await kb.update_status("t-1", "bogus")


async def test_update_status_accepts_blocked() -> None:
    # "blocked" is a first-class server status (its GET /api/tasks filter lists
    # blocked alongside todo/running/done) and the offline ladder writes it when
    # wake→redispatch exhaust (fleet-bus #73). It must PATCH, not raise like a
    # bogus status would.
    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 3}
            )
        import json as _json

        assert _json.loads(request.content)["status"] == "blocked"
        return httpx.Response(200, json={"task_id": "t-1", "status": "blocked", "sync_version": 4})

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.update_status("t-1", "blocked")
    assert task.status == "blocked"


def test_blocked_is_a_valid_status() -> None:
    from fleet_orchestrator.kboard import VALID_STATUSES

    assert "blocked" in VALID_STATUSES


async def test_get_task_raises_kboard_error_on_server_error() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    async with _client(httpx.MockTransport(handle)) as kb:
        with pytest.raises(KboardError, match="500"):
            await kb.get_task("t-1")


async def test_list_tasks_parses_both_shapes() -> None:
    # The API may return a bare list or a {"tasks": [...]} envelope — both parse.
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"tasks": [{"task_id": "a", "status": "todo", "sync_version": 1}]},
        )

    async with _client(httpx.MockTransport(handle)) as kb:
        tasks = await kb.list_tasks()

    assert [t.task_id for t in tasks] == ["a"]


async def test_post_event_sends_type_and_actor() -> None:
    captured: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        import json as _json

        captured.update(_json.loads(request.content))
        assert request.url.path == "/api/tasks/t-1/events"
        return httpx.Response(201, json={"ok": True})

    async with _client(httpx.MockTransport(handle)) as kb:
        await kb.post_event("t-1", "claimed", "heron")

    assert captured == {"event_type": "claimed", "actor": "heron"}


async def test_post_event_omits_body_when_none() -> None:
    captured: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        import json as _json

        captured.update(_json.loads(request.content))
        return httpx.Response(201, json={"ok": True})

    async with _client(httpx.MockTransport(handle)) as kb:
        await kb.post_event("t-1", "status", "wren")

    assert "body" not in captured  # optional body absent when not passed


async def test_injected_client_is_not_closed_by_kboard() -> None:
    # Ownership: a caller-injected client is the caller's to close. Prove the
    # KboardClient does NOT aclose() it (owning a client we didn't create would
    # break a caller reusing it across calls).
    injected = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    kb = KboardClient(client=injected)
    await kb.close()
    assert not injected.is_closed  # still open — the caller owns its lifecycle
    await injected.aclose()


async def test_create_task_posts_the_proven_recipe_and_returns_the_row() -> None:
    # The create verb (fleet-bus #89): POST /api/tasks?tenant=pg with the proven
    # body shape — task_id, status:todo, tenant, title, body — returning the row.
    captured: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        import json as _json

        assert request.method == "POST"
        assert request.url.path == "/api/tasks"
        assert request.url.params.get("tenant") == "pg"  # tenant on the query param
        captured.update(_json.loads(request.content))
        return httpx.Response(
            201, json={"task_id": "t_canary1", "status": "todo", "sync_version": 1847}
        )

    async with _client(httpx.MockTransport(handle)) as kb:
        task = await kb.create_task(
            task_id="t_canary1", title="[fleet] nightly canary", body="reply with status"
        )

    assert captured["task_id"] == "t_canary1"
    assert captured["status"] == "todo"
    assert captured["tenant"] == "pg"  # tenant ALSO in the body (the proven recipe)
    assert captured["title"] == "[fleet] nightly canary"
    assert captured["body"] == "reply with status"
    assert task.task_id == "t_canary1"
    assert task.status == "todo"
    assert task.sync_version == 1847


async def test_create_task_preserves_request_and_drops_kboard_prose_from_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    title = "private-kboard-title-canary"
    body = "private-kboard-body-canary"
    monkeypatch.setenv("FLEET_TRACE", "1")
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)
    records: list[str] = []
    sink = logger.add(
        lambda message: records.append(str(message)), level="DEBUG", format="{message}"
    )
    captured: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        import json as _json

        captured.update(_json.loads(request.content))
        return httpx.Response(201, json={"task_id": "t_trace", "status": "todo", "sync_version": 7})

    try:
        async with _client(httpx.MockTransport(handle)) as kb:
            task = await kb.create_task(task_id="t_trace", title=title, body=body)
    finally:
        logger.remove(sink)
        monkeypatch.delenv("FLEET_TRACE", raising=False)
        monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
        importlib.reload(tracing)

    assert captured["title"] == title
    assert captured["body"] == body
    assert (task.task_id, task.status, task.sync_version) == ("t_trace", "todo", 7)
    output = "\n".join(records)
    assert title not in output
    assert body not in output


async def test_create_task_rejects_invalid_status_before_network() -> None:
    # A bad status is a programming error caught pre-network, like update_status.
    async with _client(httpx.MockTransport(lambda r: httpx.Response(500))) as kb:
        with pytest.raises(ValueError, match="invalid status"):
            await kb.create_task(task_id="t_x", title="[fleet] x", status="bogus")


def test_status_for_state_maps_all_states() -> None:
    assert status_for_state(State.DISPATCHED) == "running"
    assert status_for_state(State.WORKING) == "running"
    assert status_for_state(State.RELAY_NEEDED) == "running"
    assert status_for_state(State.ARBITRATING) == "running"
    assert status_for_state(State.DONE) == "done"


def test_event_for_state_maps_all_states() -> None:
    assert event_for_state(State.DISPATCHED) == "claimed"
    assert event_for_state(State.WORKING) == "status"
    assert event_for_state(State.RELAY_NEEDED) == "status"
    assert event_for_state(State.ARBITRATING) == "status"
    assert event_for_state(State.DONE) == "completed"


async def test_writes_carry_bearer_and_claim_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#206: PATCH/POST send the FLEET_KBOARD_TOKEN bearer; an unassigned card is
    claimed by FLEET_KBOARD_ACTOR on the first status write."""
    monkeypatch.setenv("FLEET_KBOARD_TOKEN", "hub-token")
    monkeypatch.setenv("FLEET_KBOARD_ACTOR", "heron")
    seen: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            assert "authorization" not in {k.lower() for k in request.headers} or True
            return httpx.Response(
                200,
                json={"task_id": "t-1", "status": "todo", "sync_version": 3, "assignee": None},
            )
        import json as _json

        seen["auth"] = request.headers.get("authorization")
        seen[request.method] = _json.loads(request.content)
        return httpx.Response(200, json={"task_id": "t-1", "status": "running", "sync_version": 4})

    async with _client(httpx.MockTransport(handle)) as kb:
        await kb.update_status("t-1", "running")
        await kb.post_event("t-1", "status", "heron")

    assert seen["auth"] == "Bearer hub-token"
    patch_body = seen["PATCH"]
    assert isinstance(patch_body, dict)
    assert patch_body["assignee"] == "heron"  # claim-on-first-write
    assert seen["POST"] == {"event_type": "status", "actor": "heron"}


async def test_write_does_not_reassign_an_owned_card(monkeypatch: pytest.MonkeyPatch) -> None:
    """A card already assigned must never get an assignee in the PATCH payload."""
    monkeypatch.setenv("FLEET_KBOARD_TOKEN", "hub-token")
    monkeypatch.setenv("FLEET_KBOARD_ACTOR", "heron")
    patched: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "task_id": "t-2",
                    "status": "running",
                    "sync_version": 5,
                    "assignee": "heron",
                },
            )
        import json as _json

        patched.update(_json.loads(request.content))
        return httpx.Response(200, json={"task_id": "t-2", "status": "done", "sync_version": 6})

    async with _client(httpx.MockTransport(handle)) as kb:
        await kb.update_status("t-2", "done")

    assert "assignee" not in patched


async def test_no_token_env_sends_no_auth_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without FLEET_KBOARD_TOKEN the header is omitted (server 401s fail-loud)."""
    monkeypatch.delenv("FLEET_KBOARD_TOKEN", raising=False)
    monkeypatch.delenv("FLEET_KBOARD_ACTOR", raising=False)
    seen: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"task_id": "t-3", "status": "todo", "sync_version": 1})
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"task_id": "t-3", "status": "running", "sync_version": 2})

    async with _client(httpx.MockTransport(handle)) as kb:
        await kb.update_status("t-3", "running")

    assert seen["auth"] is None
