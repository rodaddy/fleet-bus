"""Committed prod authz config ↔ subjects-builder drift guard (F2, #104).

``deploy/nats/fleet-prod-nats-server.conf`` encodes the posture §4 per-agent
allow-list matrix (docs/f2-nats-auth-posture.md) as literal subject strings.
Subjects must only ever be built via :mod:`fleet_nats.subjects` — so this test
rebuilds every §4 grant through those builders and compares it against the
committed config *parsed per brace-scoped account and user block*: exactly the
``FLEET`` and ``SYS`` accounts exist, each principal lives inside the right
account block, and each principal's subscribe/publish ALLOW **and** DENY lists
must match the expected sets EXACTLY — with no extra users and no extra
grants. String-presence checks are not enough (PR #183 findings 6 and r2-2): a
grant on the wrong user, a swapped direction, an added overbroad grant, a
renamed account, or a stray ``deny`` stranding an expected allow must fail,
not pass.

Auto-skips when the deploy tree is absent (package built/tested standalone).
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path

import nats
import pytest
from nats.aio.client import Client
from nats.aio.msg import Msg

from fleet_nats import subjects

_CONF = Path(__file__).parents[3] / "deploy" / "nats" / "fleet-prod-nats-server.conf"

pytestmark = pytest.mark.skipif(
    not _CONF.is_file(),
    reason="deploy/nats tree not present (standalone package checkout)",
)

_ENVS = ("dev", "prod")
_WORKERS = ("wren", "finch")
_MANAGER = "heron"

_FLEET_USERS = frozenset(
    {
        _MANAGER,
        *_WORKERS,
        "fleet-hub-control",
        "open-brain",
        "fleet-kboard",
        "fleet-runner",
        "local-dev",
    }
)
_SYS_USERS = frozenset({"fleet-sys-operator"})


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError("functional authz nats-server did not start")


# --- conf parsing -------------------------------------------------------------


def _strip_comments(text: str) -> str:
    """Drop #-comments so grant extraction never matches commented-out text."""
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def _balanced(conf: str, start: int) -> str:
    """The balanced ``{...}`` block opening at position ``start`` (a ``{``)."""
    assert conf[start] == "{", "block must start at an opening brace"
    depth = 0
    for j in range(start, len(conf)):
        if conf[j] == "{":
            depth += 1
        elif conf[j] == "}":
            depth -= 1
            if depth == 0:
                return conf[start : j + 1]
    raise AssertionError("unbalanced braces in conf")


def _enclosing_block(conf: str, idx: int) -> str:
    """The innermost ``{...}`` block containing position ``idx``."""
    depth = 0
    start = -1
    for i in range(idx, -1, -1):
        char = conf[i]
        if char == "}":
            depth += 1
        elif char == "{":
            if depth == 0:
                start = i
                break
            depth -= 1
    assert start >= 0, "no opening brace found for user entry"
    return _balanced(conf, start)


def _account_blocks(conf: str) -> dict[str, str]:
    """Map account name → its brace-scoped block inside ``accounts {...}``.

    Brace-matched, not string-searched: a renamed account, a user moved across
    the account boundary, or a second accounts entry must all surface here
    (PR #183 round-2 finding 2).
    """
    matches = list(re.finditer(r"\baccounts\s*(\{)", conf))
    assert len(matches) == 1, "expected exactly one accounts block"
    outer = _balanced(conf, matches[0].start(1))
    inner = outer[1:-1]
    blocks: dict[str, str] = {}
    pos = 0
    while True:
        match = re.search(r"([A-Za-z_][A-Za-z0-9_-]*)\s*(\{)", inner[pos:])
        if match is None:
            break
        name = match.group(1)
        block = _balanced(inner, pos + match.start(2))
        assert name not in blocks, f"duplicate account entry: {name}"
        blocks[name] = block
        pos = pos + match.start(2) + len(block)
    return blocks


def _user_blocks(conf: str) -> dict[str, str]:
    """Map user name → that user's full config block (asserts unique names)."""
    blocks: dict[str, str] = {}
    for match in re.finditer(r'(?<!s)\buser:\s*"([^"]+)"', conf):
        name = match.group(1)
        assert name not in blocks, f"duplicate user entry: {name}"
        blocks[name] = _enclosing_block(conf, match.start())
    return blocks


def _grant_set(block: str, direction: str, kind: str) -> set[str] | None:
    """The ``kind`` (allow/deny) subject set of a direction, None if absent."""
    dir_match = re.search(rf"{direction}\s*\{{([^{{}}]*)\}}", block, re.DOTALL)
    if dir_match is None:
        return None
    kind_match = re.search(rf"{kind}\s*=\s*\[([^\]]*)\]", dir_match.group(1), re.DOTALL)
    if kind_match is None:
        return None
    return set(re.findall(r'"([^"]+)"', kind_match.group(1)))


@pytest.fixture(scope="module")
def conf() -> str:
    return _strip_comments(_CONF.read_text())


@pytest.fixture(scope="module")
def accounts(conf: str) -> dict[str, str]:
    return _account_blocks(conf)


@pytest.fixture(scope="module")
def users(accounts: dict[str, str]) -> dict[str, str]:
    """User name → block, built ONLY from the two expected account scopes."""
    fleet = _user_blocks(accounts["FLEET"])
    sys_users = _user_blocks(accounts["SYS"])
    assert not set(fleet) & set(sys_users), "user present in both accounts"
    return {**fleet, **sys_users}


def _assert_grants_exact(
    block: str,
    *,
    sub_allow: set[str] | None,
    pub_allow: set[str] | None,
    sub_deny: set[str] | None = None,
    pub_deny: set[str] | None = None,
) -> None:
    """All four grant sets of a user block match EXACTLY (absent = None).

    Deny sets are compared too: an unexpected ``deny`` beside a correct allow
    list strands the granted access while every allow-only assertion stays
    green (PR #183 round-2 finding 2's counterexample).
    """
    assert _grant_set(block, "subscribe", "allow") == sub_allow
    assert _grant_set(block, "publish", "allow") == pub_allow
    assert _grant_set(block, "subscribe", "deny") == sub_deny
    assert _grant_set(block, "publish", "deny") == pub_deny


# --- expected grants, built via the builders ----------------------------------


def _worker_grants(worker: str) -> tuple[set[str], set[str]]:
    """(subscribe, publish) grants for a §4a worker, built via the builders."""
    sub: set[str] = set()
    pub: set[str] = set()
    for env in _ENVS:
        sub |= {
            subjects.agent_dispatch_wildcard(env, worker),
            subjects.agent_dispatch(env, worker),
            # Channel-scoped relay/wake (#117): the hub publishes the scoped form
            # for a channelled session, so a bare-only grant would have the server
            # REFUSE delivery of every real relay/wake — silent starvation.
            subjects.agent_relay_wildcard(env, worker),
            subjects.agent_relay(env, worker),
            subjects.agent_wake_wildcard(env, worker),
            subjects.agent_wake(env, worker),
            subjects.agent_inbox(env, worker),
            subjects.direct(env, worker),
        }
        pub |= {
            subjects.agent_status(env, subjects.STAR),
            # The worker replies on the channel-scoped status form (#117).
            subjects.agent_status_wildcard(env, subjects.STAR),
            # Noise lanes (#116) — the worker PUBLISHES its acks/telemetry here.
            # Both forms, since the reply channel decides which one it builds.
            subjects.agent_ack(env, subjects.STAR),
            subjects.agent_ack_wildcard(env, subjects.STAR),
            subjects.agent_telemetry(env, subjects.STAR),
            subjects.agent_telemetry_wildcard(env, subjects.STAR),
            subjects.agent_claim(env, subjects.STAR),
            subjects.agent_arbitrate(env, subjects.STAR),
            subjects.hub_show_work(env),
            subjects.agent_heartbeat(env, worker),
            subjects.token_cost(env, worker),
        }
    sub.add(f"_INBOX.{worker}.>")
    return sub, pub


def _manager_grants() -> tuple[set[str], set[str]]:
    """(subscribe, publish) grants for the §4b manager."""
    sub: set[str] = set()
    pub: set[str] = set()
    for env in _ENVS:
        sub |= {
            subjects.agent_inbox(env, _MANAGER),
            # No builder for responder-side full-depth wildcards; §4b specifies
            # `.>` (task ids/topics can be multi-token only via slug, but the
            # posture table is explicit).
            f"{env}.agent.status.>",
            # The channel-scoped status form (#117). watch_worker_status
            # subscribes BOTH, so the grant must cover both or a scoped reply is
            # refused by the server and the task hangs to timeout.
            f"{env}.agent.*.status.>",
            f"{env}.agent.claim.>",
            f"{env}.agent.arbitrate.>",
            subjects.agent_heartbeat_wildcard(env),
            subjects.token_cost_wildcard(env),
            subjects.hub_control_wildcard(env),
            subjects.hub_show_work(env),
            # Runner telemetry is READ-only for the manager (#222): it appears
            # in the subscribe set and deliberately NOT in the publish set, so a
            # manager can observe factory progress but cannot forge it.
            subjects.factory_runner_wildcard(env),
            "_INBOX.heron.>",
        }
        pub |= {
            subjects.agent_dispatch(env, subjects.STAR, channel=subjects.STAR),
            subjects.agent_dispatch(env, subjects.STAR),
            subjects.agent_relay(env, subjects.STAR),
            # The channel-scoped relay/wake forms the hub now publishes (#117).
            subjects.agent_relay(env, subjects.STAR, channel=subjects.STAR),
            subjects.agent_wake(env, subjects.STAR),
            subjects.agent_wake(env, subjects.STAR, channel=subjects.STAR),
            subjects.dead_letter(env),
            subjects.kboard_event(env, subjects.STAR, subjects.STAR),
            subjects.agent_heartbeat(env, _MANAGER),
            subjects.token_cost(env, _MANAGER),
            subjects.hub_show_work(env),
        }
    return sub, pub


# --- structural tests ---------------------------------------------------------


def test_exact_account_set(accounts: dict[str, str]) -> None:
    """Exactly the FLEET and SYS accounts exist — a rename must fail."""
    assert set(accounts) == {"FLEET", "SYS"}


def test_exact_user_set(conf: str, users: dict[str, str]) -> None:
    """Exactly the §4 principals exist — no anon seat, no stray users.

    Compared against the whole conf too, so a user defined OUTSIDE the two
    account scopes (where the ``users`` fixture would never see it) fails.
    """
    assert set(users) == _FLEET_USERS | _SYS_USERS
    assert set(_user_blocks(conf)) == _FLEET_USERS | _SYS_USERS


def test_account_membership(accounts: dict[str, str]) -> None:
    """Each principal lives inside its own account's brace scope, exactly."""
    assert set(_user_blocks(accounts["FLEET"])) == _FLEET_USERS
    assert set(_user_blocks(accounts["SYS"])) == _SYS_USERS


def test_anonymous_denied(conf: str) -> None:
    """Fail-closed floor: no anonymous mapping, no default-permit seat."""
    assert "no_auth_user" not in conf
    assert "fleet-anon" not in conf
    for name, block in _user_blocks(conf).items():
        for direction in ("subscribe", "publish"):
            grants = _grant_set(block, direction, "allow") or set()
            assert ">" not in grants, f"bare wildcard {direction} grant on {name}"


# --- exact per-user grant tests -----------------------------------------------


@pytest.mark.parametrize("worker", _WORKERS)
def test_worker_grants_exact(users: dict[str, str], worker: str) -> None:
    sub, pub = _worker_grants(worker)
    block = users[worker]
    _assert_grants_exact(block, sub_allow=sub, pub_allow=pub)
    # Workers are requesters, never responders.
    assert "allow_responses" not in block


def test_manager_grants_exact(users: dict[str, str]) -> None:
    sub, pub = _manager_grants()
    block = users[_MANAGER]
    _assert_grants_exact(block, sub_allow=sub, pub_allow=pub)
    # Responder side of request/reply (posture §4 note).
    assert "allow_responses: true" in block


def test_hub_control_grants_exact(users: dict[str, str]) -> None:
    """Pub-only seat: an omitted direction is ALLOW-ALL in NATS — must deny."""
    expected = {subjects.hub_control(env, subjects.STAR) for env in _ENVS}
    _assert_grants_exact(
        users["fleet-hub-control"],
        sub_allow=None,
        pub_allow=expected,
        sub_deny={">"},
    )


def test_kboard_grants_exact(users: dict[str, str]) -> None:
    """Pub-only seat: an omitted direction is ALLOW-ALL in NATS — must deny."""
    expected = {subjects.kboard_event(env, subjects.STAR, subjects.STAR) for env in _ENVS}
    _assert_grants_exact(
        users["fleet-kboard"],
        sub_allow=None,
        pub_allow=expected,
        sub_deny={">"},
    )


def test_runner_grants_exact(users: dict[str, str]) -> None:
    """Publish-only runner seat (#222): the subscribe deny IS the boundary.

    A runner box runs a coding agent with bypassPermissions over untrusted repo
    content, so ``sub_deny={">"}`` is the only thing that actually stops it from
    receiving instructions — the client-side publish-only role set cannot. An
    omitted subscribe direction would be ALLOW-ALL in NATS, i.e. silently the
    exact opposite of the intent, which is why the deny is asserted explicitly.
    """
    expected = {subjects.factory_runner(env, subjects.STAR) for env in _ENVS}
    _assert_grants_exact(
        users["fleet-runner"],
        sub_allow=None,
        pub_allow=expected,
        sub_deny={">"},
    )


def test_open_brain_grants_exact(users: dict[str, str]) -> None:
    expected = {f"{env}.ob.>" for env in _ENVS}
    _assert_grants_exact(users["open-brain"], sub_allow=expected, pub_allow=expected)


def test_local_dev_grants_exact(users: dict[str, str]) -> None:
    _assert_grants_exact(
        users["local-dev"],
        sub_allow={subjects.direct(env, "local-dev") for env in _ENVS},
        pub_allow={subjects.direct(env, subjects.STAR) for env in _ENVS},
    )


def test_sys_operator_scoped(users: dict[str, str]) -> None:
    """§4g least-privilege monitoring: $SYS read + request/reply lane only."""
    _assert_grants_exact(
        users["fleet-sys-operator"],
        sub_allow={"$SYS.>", "_INBOX.fleet-sys-operator.>"},
        pub_allow={"$SYS.REQ.>"},
    )


def test_allow_responses_manager_only(conf: str, users: dict[str, str]) -> None:
    assert conf.count("allow_responses") == 1
    assert "allow_responses" in users[_MANAGER]


def test_sys_account_wired(conf: str) -> None:
    assert "system_account: SYS" in conf


def test_no_literal_secrets(conf: str) -> None:
    """Every password is a $FLEET_NATS_PW_* env expansion — never a value."""
    passwords = re.findall(r"password:\s*(\S+)", conf)
    assert len(passwords) == len(_FLEET_USERS | _SYS_USERS)
    for value in passwords:
        assert re.fullmatch(r"\$FLEET_NATS_PW_[A-Z_]+", value), (
            f"non-indirected password entry: {value}"
        )


@pytest.mark.skipif(shutil.which("nats-server") is None, reason="nats-server not installed")
@pytest.mark.asyncio
async def test_real_server_enforces_representative_account_and_grant_shapes(
    tmp_path: Path,
) -> None:
    """Exercise the committed config against real nats-server authorization."""
    client_port = _free_port()
    monitor_port = _free_port()
    runtime_conf = tmp_path / "nats-server.conf"
    runtime_conf.write_text(
        _CONF.read_text()
        .replace("listen: 0.0.0.0:4222", f"listen: 127.0.0.1:{client_port}")
        .replace("http_port: 8223", f"http_port: {monitor_port}")
    )
    passwords = {
        name: f"TestOnly{name.replace('-', '')}CredentialAAAAAAAA"
        for name in _FLEET_USERS | _SYS_USERS
    }
    env = {
        **os.environ,
        "FLEET_NATS_PW_HERON": passwords["heron"],
        "FLEET_NATS_PW_WREN": passwords["wren"],
        "FLEET_NATS_PW_FINCH": passwords["finch"],
        "FLEET_NATS_PW_HUB_CONTROL": passwords["fleet-hub-control"],
        "FLEET_NATS_PW_OPEN_BRAIN": passwords["open-brain"],
        "FLEET_NATS_PW_KBOARD": passwords["fleet-kboard"],
        "FLEET_NATS_PW_RUNNER": passwords["fleet-runner"],
        "FLEET_NATS_PW_LOCAL_DEV": passwords["local-dev"],
        "FLEET_NATS_PW_SYS_OPERATOR": passwords["fleet-sys-operator"],
    }
    proc = subprocess.Popen(  # noqa: S603 - fixed test binary and argv
        [shutil.which("nats-server") or "nats-server", "-c", str(runtime_conf)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    clients: list[Client] = []
    try:
        _wait_for_port(client_port)
        url = f"nats://127.0.0.1:{client_port}"

        async def connect(user: str) -> Client:
            client = await nats.connect(
                url,
                user=user,
                password=passwords[user],
                inbox_prefix=f"_INBOX.{user}",
            )
            clients.append(client)
            return client

        manager = await connect("heron")
        worker = await connect("wren")
        control = await connect("fleet-hub-control")
        local_dev = await connect("local-dev")
        sys_operator = await connect("fleet-sys-operator")

        own_dispatch = await worker.subscribe("dev.agent.dispatch.wren")
        cross_dispatch = await worker.subscribe("dev.agent.dispatch.finch")
        await worker.flush()
        await manager.publish("dev.agent.dispatch.wren", b"allowed")
        await manager.publish("dev.agent.dispatch.finch", b"cross-list")
        await manager.flush()
        assert (await own_dispatch.next_msg(timeout=1)).data == b"allowed"
        with pytest.raises(TimeoutError):
            await cross_dispatch.next_msg(timeout=0.2)

        control_received = await manager.subscribe("dev.hub.control.test")
        await manager.flush()
        await control.publish("dev.hub.control.test", b"allowed")
        await control.flush()
        assert (await control_received.next_msg(timeout=1)).data == b"allowed"

        direct_received = await worker.subscribe("dev.direct.wren")
        await worker.flush()
        await local_dev.publish("dev.direct.wren", b"allowed")
        await local_dev.flush()
        assert (await direct_received.next_msg(timeout=1)).data == b"allowed"

        async def respond(message: Msg) -> None:
            await manager.publish(message.reply, b"manager-response")
            await manager.flush()

        await manager.subscribe("dev.agent.claim.test", cb=respond)
        await manager.flush()
        reply = await worker.request("dev.agent.claim.test", b"request", timeout=1)
        assert reply.data == b"manager-response"

        cross_reply_inbox = await worker.subscribe("_INBOX.wren.>")
        forbidden_cross_inbox = await connect("finch")
        forbidden_reply_inbox = await forbidden_cross_inbox.subscribe("_INBOX.wren.>")
        await worker.flush()
        await forbidden_cross_inbox.flush()

        await manager.subscribe("dev.hub.show-work", cb=respond)
        await manager.flush()
        show_work_reply = await worker.request("dev.hub.show-work", b"show-work", timeout=1)
        assert show_work_reply.data == b"manager-response"
        assert (await cross_reply_inbox.next_msg(timeout=1)).data == b"manager-response"
        with pytest.raises(TimeoutError):
            await forbidden_reply_inbox.next_msg(timeout=0.2)

        authz_failures: list[Exception] = []

        async def capture_authz_failure(error: Exception) -> None:
            authz_failures.append(error)

        # --- #222: the runner seat is publish-only, proven against the server --
        # AC2 wants a real refusal, not a client-side convention. The runner
        # connects with its own credential and the server itself must (a) let the
        # telemetry publish through and (b) refuse EVERY control-plane subscribe.
        runner_failures: list[Exception] = []

        async def capture_runner_failure(error: Exception) -> None:
            runner_failures.append(error)

        runner = await nats.connect(
            url,
            user="fleet-runner",
            password=passwords["fleet-runner"],
            inbox_prefix="_INBOX.fleet-runner",
            error_cb=capture_runner_failure,
        )
        clients.append(runner)

        # (a) telemetry publish is allowed and really lands.
        runner_telemetry = await manager.subscribe("dev.factory.runner.job-42")
        await manager.flush()
        await runner.publish("dev.factory.runner.job-42", b"progress")
        await runner.flush()
        assert (await runner_telemetry.next_msg(timeout=1)).data == b"progress"

        # (b) every control lane #222 names is refused at the SERVER. Each
        # subscribe must raise a permissions violation — a runner that could
        # attach to any of these could receive instructions, which is the one
        # thing its threat model forbids.
        for forbidden in (
            "dev.agent.dispatch.wren",
            "dev.agent.relay.wren",
            "dev.agent.wake.wren",
            "dev.agent.inbox.heron",
            "dev.agent.token_cost.wren",
            "dev.hub.control.test",
            ">",
        ):
            runner_failures.clear()
            await runner.subscribe(forbidden)
            await runner.flush()
            deadline = time.monotonic() + 1
            while not runner_failures and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert runner_failures, f"runner subscribe to {forbidden!r} was NOT refused"
            assert "permissions violation" in str(runner_failures[-1]).lower(), forbidden

        # (c) the PUBLISH boundary is real too, not just the subscribe deny. A
        # runner that can publish onto a coordination lane could inject work or
        # forge another principal's traffic even while deaf.
        for forbidden_pub in (
            "dev.agent.dispatch.wren",
            "dev.agent.status.task-1",
            "dev.agent.heartbeat.fleet-runner",
            "dev.kboard.task.completed.task-1",
            "dev.hub.control.test",
        ):
            runner_failures.clear()
            await runner.publish(forbidden_pub, b"nope")
            await runner.flush()
            deadline = time.monotonic() + 1
            while not runner_failures and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert runner_failures, f"runner publish to {forbidden_pub!r} was NOT refused"
            assert "permissions violation" in str(runner_failures[-1]).lower(), forbidden_pub

        mismatched_creds_identity = await nats.connect(
            url,
            user="wren",
            password=passwords["wren"],
            inbox_prefix="_INBOX.finch",
            error_cb=capture_authz_failure,
        )
        clients.append(mismatched_creds_identity)
        await mismatched_creds_identity.subscribe("_INBOX.finch.>")
        await mismatched_creds_identity.flush()
        deadline = time.monotonic() + 1
        while not authz_failures and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert authz_failures
        assert "permissions violation" in str(authz_failures[-1]).lower()

        sys_reply = await sys_operator.request("$SYS.REQ.SERVER.PING", b"", timeout=1)
        assert sys_reply.data
        isolated = await sys_operator.subscribe("dev.hub.control.test")
        await sys_operator.flush()
        await control.publish("dev.hub.control.test", b"fleet-account")
        await control.flush()
        with pytest.raises(TimeoutError):
            await isolated.next_msg(timeout=0.2)
    finally:
        for client in clients:
            await client.close()
        proc.terminate()
        proc.wait(timeout=5)
