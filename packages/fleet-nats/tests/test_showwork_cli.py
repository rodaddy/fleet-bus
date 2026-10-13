"""Functional tests for the fleet-show-work CLI (#64, test-cov HIGH-3).

Drives ``fleet_nats.showwork_cli.main`` against temp journals — exit codes, the
md/json output shapes, journal-path precedence, and dir-glob vs single-file. The
journal fixtures use the REAL thin ``TraceRecord`` shape (action-derived kinds,
no body).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fleet_nats.showwork_cli import _resolve_journal, main

TASK = "t_grad51783562393"

_FRAMES: list[dict[str, object]] = [
    {
        "ts": "2026-07-09T02:00:09.148173+00:00",
        "subject": "dev.agent.dispatch.skippy",
        "action": "dispatch",
        "task_id": TASK,
        "target": "skippy",
    },
    {
        "ts": "2026-07-09T02:01:14.039736+00:00",
        "action": "complete",
        "state": "done",
        "task_id": TASK,
    },
]


def _journal(tmp_path: Path, name: str = "run-bus.jsonl") -> Path:
    jf = tmp_path / name
    jf.write_text("\n".join(json.dumps(f) for f in _FRAMES), encoding="utf-8")
    return jf


# ---- exit codes -------------------------------------------------------------


def test_found_exit_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    jf = _journal(tmp_path)
    assert main([TASK, "--journal", str(jf)]) == 0
    out = capsys.readouterr().out
    assert "## Summary" in out and "## Transcript" in out


def test_missing_journal_exit_two(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([TASK, "--journal", str(tmp_path / "nope")]) == 2
    assert "error:" in capsys.readouterr().err


def test_zero_frames_exit_two(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    jf = _journal(tmp_path)
    assert main(["t_nonexistent", "--journal", str(jf)]) == 2
    assert "no frames found" in capsys.readouterr().err


def test_missing_task_id_argparse_exits(tmp_path: Path) -> None:
    # argparse raises SystemExit(2) when the positional task_id is absent.
    with pytest.raises(SystemExit):
        main(["--journal", str(_journal(tmp_path))])


# ---- output shapes ----------------------------------------------------------


def test_format_json_shape(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    jf = _journal(tmp_path)
    assert main([TASK, "--journal", str(jf), "--format", "json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["task_id"] == TASK
    assert "summary" in doc and "frames" in doc
    assert doc["summary"]["outcome"] == "completed"


def test_format_md_default(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    jf = _journal(tmp_path)
    assert main([TASK, "--journal", str(jf)]) == 0
    out = capsys.readouterr().out
    assert out.lstrip().startswith("# Deliberation trail")
    assert "EDT" in out


# ---- journal-path precedence + dir glob -------------------------------------


def test_resolve_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    # explicit --journal beats env beats default.
    monkeypatch.setenv("FLEET_JOURNAL_PATH", "/env/path")
    assert _resolve_journal("/explicit") == Path("/explicit")
    assert _resolve_journal(None) == Path("/env/path")
    monkeypatch.delenv("FLEET_JOURNAL_PATH", raising=False)
    assert _resolve_journal(None) == Path("/opt/fleet-bus/trace")


def test_dir_glob_reads_all_jsonl(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _journal(tmp_path, "a-bus.jsonl")
    _journal(tmp_path, "b-bus.jsonl")
    # Point at the DIR, not a file — both *.jsonl are globbed.
    assert main([TASK, "--journal", str(tmp_path)]) == 0
    assert "## Summary" in capsys.readouterr().out


def test_env_journal_used_when_no_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    jf = _journal(tmp_path)
    monkeypatch.setenv("FLEET_JOURNAL_PATH", str(jf))
    assert main([TASK]) == 0
    assert "## Summary" in capsys.readouterr().out
