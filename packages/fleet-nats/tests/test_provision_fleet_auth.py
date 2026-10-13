"""Fail-closed tests for the final NATS auth install boundary."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[3]
_SCRIPT = _ROOT / "deploy/nats/provision-fleet-auth.sh"
_CONF = _ROOT / "deploy/nats/fleet-prod-nats-server.conf"

pytestmark = pytest.mark.skipif(not _SCRIPT.is_file(), reason="deploy tree absent")

_PRINCIPALS = {
    "NAGATHA": "nagatha",
    "SKIPPY": "skippy",
    "BILBY": "bilby",
    "HUB_CONTROL": "fleet-hub-control",
    "OPEN_BRAIN": "open-brain",
    "KBOARD": "fleet-kboard",
    "RUNNER": "fleet-runner",
    "LOCAL_DEV": "local-dev",
    "SYS_OPERATOR": "fleet-sys-operator",
}
_PASSWORD = "A" * 32


def _write_env(
    path: Path,
    *,
    drift: tuple[str, str] | None = None,
    password_drift: str | None = None,
    users: bool = True,
) -> None:
    lines = []
    for suffix, username in _PRINCIPALS.items():
        if drift and drift[0] == suffix:
            username = drift[1]
        if users:
            lines.append(f"FLEET_NATS_PW_{suffix}_USER={username}")
        password = "B" * 32 if password_drift == suffix else _PASSWORD
        lines.append(f"FLEET_NATS_PW_{suffix}={password}")
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)


def _run(
    tmp_path: Path,
    command: str,
    env_file: Path,
    *,
    exec_start: str | None = None,
    curl_ok: bool = True,
    curl_present: bool = True,
) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    tools = {
        "nats-server": "#!/bin/sh\nexit 0\n",
        "systemctl": """#!/bin/sh
if [ "$1" = show ]; then
  printf '%s\\n' "$TEST_EXEC_START"
fi
exit 0
""",
        "curl": f"#!/bin/sh\nexit {0 if curl_ok else 1}\n",
        "bw": """#!/bin/sh
if [ "$1" = status ]; then
  printf '{"status":"unlocked"}\\n'
elif [ "$1" = get ] && [ "$2" = password ]; then
  printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\\n'
elif [ "$1" = get ] && [ "$2" = username ]; then
  case "$3" in
    fleet-bus-nats-hub-control) printf 'fleet-hub-control\\n' ;;
    fleet-bus-nats-open-brain) printf 'open-brain\\n' ;;
    fleet-bus-nats-kboard) printf 'fleet-kboard\\n' ;;
    fleet-bus-nats-runner) printf 'fleet-runner\\n' ;;
    fleet-bus-nats-local-dev) printf 'local-dev\\n' ;;
    fleet-bus-nats-sys-operator) printf 'fleet-sys-operator\\n' ;;
    fleet-bus-nats-*) printf '%s\\n' "${3#fleet-bus-nats-}" ;;
    *) exit 1 ;;
  esac
else
  exit 1
fi
""",
    }
    if not curl_present:
        del tools["curl"]
        for name in ("bash", "dirname", "env", "grep", "tr"):
            target = shutil.which(name)
            assert target is not None
            (bin_dir / name).symlink_to(target)
    for name, body in tools.items():
        tool = bin_dir / name
        tool.write_text(body)
        tool.chmod(0o755)
    env = {
        **os.environ,
        "PATH": str(bin_dir) if not curl_present else f"{bin_dir}:{os.environ['PATH']}",
        "FLEET_NATS_AUTH_ENV_FILE": str(env_file),
        "FLEET_NATS_CONF_DEST": str(tmp_path / "installed.conf"),
        "FLEET_NATS_DROPIN_DIR": str(tmp_path / "dropin"),
        "BW_SESSION": "test-session-not-a-secret",
        "TEST_EXEC_START": exec_start
        or f"/opt/homebrew/bin/nats-server -c {tmp_path / 'installed.conf'}",
    }
    return subprocess.run([str(_SCRIPT), command], env=env, text=True, capture_output=True)


def test_direct_check_rejects_stale_password_only_material(tmp_path: Path) -> None:
    env_file = tmp_path / "manual.env"
    _write_env(env_file, users=False)
    result = _run(tmp_path, "check", env_file)
    assert result.returncode != 0
    assert "username/password unit" in result.stderr


def test_direct_install_rejects_username_drift_before_install(tmp_path: Path) -> None:
    env_file = tmp_path / "drift.env"
    _write_env(env_file, drift=("SKIPPY", "bilby"))
    result = _run(tmp_path, "install", env_file)
    assert result.returncode != 0
    assert "identity mismatch for FLEET_NATS_PW_SKIPPY" in result.stderr
    assert not (tmp_path / "installed.conf").exists()


def test_direct_install_rejects_drifted_password_before_install(tmp_path: Path) -> None:
    env_file = tmp_path / "drifted-password.env"
    _write_env(env_file, password_drift="SKIPPY")
    result = _run(tmp_path, "install", env_file)
    assert result.returncode != 0
    assert "vault credential mismatch for fleet-bus-nats-skippy" in result.stderr
    assert "B" * 32 not in result.stderr
    assert not (tmp_path / "installed.conf").exists()


def test_install_uses_validated_material_and_committed_config(tmp_path: Path) -> None:
    env_file = tmp_path / "auth.env"
    _write_env(env_file)
    result = _run(tmp_path, "install", env_file)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "installed.conf").read_bytes() == _CONF.read_bytes()
    dropin = (tmp_path / "dropin/10-fleet-auth.conf").read_text()
    assert f"EnvironmentFile={env_file}" in dropin


def test_install_rejects_effective_execstart_config_drift_before_mutation(tmp_path: Path) -> None:
    env_file = tmp_path / "auth.env"
    _write_env(env_file)
    result = _run(tmp_path, "install", env_file, exec_start="/usr/bin/nats-server -c /wrong.conf")
    assert result.returncode != 0
    assert "effective ExecStart does not reference" in result.stderr
    assert not (tmp_path / "installed.conf").exists()
    assert not (tmp_path / "dropin").exists()


def test_monitor_probe_failure_rolls_back_installed_artifacts(tmp_path: Path) -> None:
    env_file = tmp_path / "auth.env"
    _write_env(env_file)
    destination = tmp_path / "installed.conf"
    destination.write_text("prior config\n")
    result = _run(tmp_path, "install", env_file, curl_ok=False)
    assert result.returncode != 0
    assert "monitor probe failed" in result.stderr
    assert destination.read_text() == "prior config\n"
    assert not (tmp_path / "dropin/10-fleet-auth.conf").exists()


def test_install_requires_curl_before_mutation(tmp_path: Path) -> None:
    env_file = tmp_path / "auth.env"
    _write_env(env_file)
    result = _run(tmp_path, "install", env_file, curl_present=False)
    assert result.returncode != 0
    assert "curl not found" in result.stderr
    assert not (tmp_path / "installed.conf").exists()
    assert not (tmp_path / "dropin").exists()
