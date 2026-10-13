"""Hub-owned, authenticated request/reply responder for ``show_work`` (#64).

Only the orchestrator process reads the durable journal.  Subscribers receive a
bounded redacted markdown response and never receive a journal path or raw
exception text.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import stat
from collections.abc import Callable, Iterator
from pathlib import Path

from fleet_nats import Envelope, FleetBus, subjects
from fleet_nats.showwork import parse_frame, reconstruct, render_markdown
from loguru import logger
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from fleet_orchestrator.trace import PRODUCTION_TRACE_PATH

_MAX_REQUEST_BYTES = 512
_MAX_TASK_ID_CHARS = 256
_MAX_RESPONSE_BYTES = 12_000
_MAX_LINE_BYTES = 16_384
_MAX_MATCHING_FRAMES = 128
_ARTIFACT_DIR = Path("/var/lib/fleet-orchestrator/show-work")
_MAX_ARTIFACT_COUNT = 32
_MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
_MAX_ARTIFACT_TOTAL_BYTES = 64 * 1024 * 1024
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_COPY_CHUNK_BYTES = 64 * 1024


class ShowWorkLimitError(ValueError):
    """Stable internal signal for bounded retrieval refusal."""


def _artifact_name(task_id: str, markdown: str) -> str:
    digest = hashlib.sha256(f"{task_id}\0{markdown}".encode()).hexdigest()
    return f"show-work-{digest}.md"


def _open_regular(path: str | Path, *, dir_fd: int | None = None) -> int:
    """Open one existing regular file without following a symlink."""
    if not _NOFOLLOW:
        raise ShowWorkLimitError("safe filesystem open unavailable")
    before = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ShowWorkLimitError("journal or artifact is not a regular file")
    fd = os.open(path, os.O_RDONLY | _NOFOLLOW, dir_fd=dir_fd)
    try:
        after = os.fstat(fd)
        if not stat.S_ISREG(after.st_mode) or (after.st_dev, after.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ShowWorkLimitError("journal or artifact is not a regular file")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_directory(path: str | Path, *, dir_fd: int | None = None) -> int:
    """Open one directory without accepting a symlink or replacement race."""
    if not _NOFOLLOW:
        raise ShowWorkLimitError("safe filesystem open unavailable")
    before = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
    if not stat.S_ISDIR(before.st_mode):
        raise ShowWorkLimitError("artifact or journal directory is not a directory")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | _NOFOLLOW, dir_fd=dir_fd)
    try:
        after = os.fstat(fd)
        if not stat.S_ISDIR(after.st_mode) or (after.st_dev, after.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ShowWorkLimitError("artifact or journal directory is not a directory")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _artifact_entries(directory_fd: int) -> list[tuple[str, os.stat_result]]:
    """Return only verified regular private artifacts, refusing hostile entries."""
    entries: list[tuple[str, os.stat_result]] = []
    for name in os.listdir(directory_fd):
        if not name.startswith("show-work-") or not name.endswith(".md"):
            continue
        fd = _open_regular(name, dir_fd=directory_fd)
        try:
            entries.append((name, os.fstat(fd)))
        finally:
            os.close(fd)
    return entries


def _enforce_artifact_retention(
    directory_fd: int, incoming_bytes: int, *, replacing: str | None = None
) -> None:
    """Evict oldest verified artifacts until count and byte caps permit one more."""
    entries = sorted(
        (entry for entry in _artifact_entries(directory_fd) if entry[0] != replacing),
        key=lambda item: (item[1].st_mtime_ns, item[0]),
    )
    total = sum(entry.st_size for _, entry in entries)
    while entries and (
        len(entries) >= _MAX_ARTIFACT_COUNT or total + incoming_bytes > _MAX_ARTIFACT_TOTAL_BYTES
    ):
        name, entry = entries.pop(0)
        os.unlink(name, dir_fd=directory_fd)
        total -= entry.st_size
    if len(entries) >= _MAX_ARTIFACT_COUNT or total + incoming_bytes > _MAX_ARTIFACT_TOTAL_BYTES:
        raise ShowWorkLimitError("artifact retention limit")


def _artifact_matches(fd: int, expected: bytes) -> bool:
    """Return whether an open artifact has exactly the expected UTF-8 bytes."""
    current = os.fstat(fd)
    if current.st_size != len(expected) or current.st_size > _MAX_ARTIFACT_BYTES:
        return False
    digest = hashlib.sha256()
    while chunk := os.read(fd, _COPY_CHUNK_BYTES):
        digest.update(chunk)
    return digest.digest() == hashlib.sha256(expected).digest()


def _write_bytes(fd: int, data: bytes) -> None:
    """Write all bytes to an already-private artifact descriptor."""
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written == 0:
            raise OSError("artifact write made no progress")
        offset += written


def _write_full(task_id: str, markdown: str) -> str | None:
    """Persist an already-redacted full render as a private hub artifact."""
    if not _NOFOLLOW:
        return None
    try:
        encoded = markdown.encode("utf-8", "strict")
    except UnicodeError:
        raise ShowWorkLimitError("artifact encoding") from None
    if len(encoded) > _MAX_ARTIFACT_BYTES:
        raise ShowWorkLimitError("artifact byte limit")
    parent_fd: int | None = None
    directory_fd: int | None = None
    artifact_fd: int | None = None
    temporary_name: str | None = None
    try:
        parent_fd = _open_directory(_ARTIFACT_DIR.parent)
        os.mkdir(_ARTIFACT_DIR.name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise ShowWorkLimitError("artifact parent") from exc
    try:
        if parent_fd is None:
            return None
        directory_fd = _open_directory(_ARTIFACT_DIR.name, dir_fd=parent_fd)
        name = _artifact_name(task_id, markdown)
        try:
            existing_fd = _open_regular(name, dir_fd=directory_fd)
        except FileNotFoundError:
            replacing = None
        else:
            try:
                if _artifact_matches(existing_fd, encoded):
                    return f"artifact:{name}"
            finally:
                os.close(existing_fd)
            replacing = name
        _enforce_artifact_retention(directory_fd, len(encoded), replacing=replacing)
        temporary_name = f".{name}.{secrets.token_hex(16)}.tmp"
        artifact_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        _write_bytes(artifact_fd, encoded)
        os.fsync(artifact_fd)
        os.close(artifact_fd)
        artifact_fd = None
        os.replace(temporary_name, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        temporary_name = None
        os.fsync(directory_fd)
        return f"artifact:{name}"
    except (OSError, UnicodeError):
        return None
    finally:
        if artifact_fd is not None:
            os.close(artifact_fd)
        if temporary_name is not None and directory_fd is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except OSError:
                pass
        if directory_fd is not None:
            os.close(directory_fd)
        if parent_fd is not None:
            os.close(parent_fd)


def _journal_file_fds(path: Path) -> Iterator[int]:
    """Yield verified journal file descriptors without traversing symlinks."""
    if not _NOFOLLOW:
        raise ShowWorkLimitError("safe filesystem open unavailable")
    before = os.stat(path, follow_symlinks=False)
    if stat.S_ISREG(before.st_mode):
        yield _open_regular(path)
        return
    if not stat.S_ISDIR(before.st_mode):
        raise ShowWorkLimitError("journal path is not a regular file or directory")
    directory_fd = _open_directory(path)
    try:
        for name in sorted(os.listdir(directory_fd)):
            if name.endswith(".jsonl"):
                yield _open_regular(name, dir_fd=directory_fd)
    finally:
        os.close(directory_fd)


def _iter_bounded_journal_lines(path: Path, task_id: str) -> Iterator[str]:
    """Scan the bounded trace ring while bounding individual frames, never unrelated bytes."""
    matched = 0
    task_marker = f'"task_id": {json.dumps(task_id)}'.encode("utf-8", "strict")
    for journal_fd in _journal_file_fds(path):
        with os.fdopen(journal_fd, "rb") as handle:
            while line := handle.readline(_MAX_LINE_BYTES + 1):
                if len(line) > _MAX_LINE_BYTES:
                    trailing = line[-(len(task_marker) - 1) :]
                    if task_marker in line:
                        raise ShowWorkLimitError("oversized requested trace frame")
                    while not line.endswith(b"\n"):
                        line = handle.readline(_MAX_LINE_BYTES + 1)
                        if not line:
                            break
                        if task_marker in trailing + line:
                            raise ShowWorkLimitError("oversized requested trace frame")
                        trailing = (trailing + line)[-(len(task_marker) - 1) :]
                    continue
                decoded = line.decode("utf-8", "strict")
                frame = parse_frame(decoded)
                if frame is not None and frame.task_id == task_id:
                    matched += 1
                    if matched > _MAX_MATCHING_FRAMES:
                        raise ShowWorkLimitError
                yield decoded


def _render(task_id: str) -> str:
    """Use the production trace default and shared reconstruction engine."""
    journal = _journal_path()
    lines = _iter_bounded_journal_lines(journal, task_id)
    summary, frames = reconstruct(lines, task_id)
    if summary.frame_count == 0:
        raise LookupError
    full = render_markdown(summary, frames)
    artifact = _write_full(task_id, full.markdown) if full.full_len > _MAX_RESPONSE_BYTES else None
    rendered = render_markdown(
        summary, frames, max_chars=_MAX_RESPONSE_BYTES, full_output_path=artifact
    ).markdown
    if len(rendered.encode("utf-8")) > _MAX_RESPONSE_BYTES:
        raise ShowWorkLimitError
    return rendered


def _journal_path() -> Path:
    """Resolve the hub journal from the trace sink's production owning constant."""
    return Path(os.environ.get("FLEET_TRACE_PATH") or PRODUCTION_TRACE_PATH)


def _task_id(request: Envelope) -> str | None:
    """Strictly accept only a bounded ``show_work`` request payload."""
    if request.kind != "show_work" or request.model_extra or set(request.payload) != {"task_id"}:
        return None
    task_id = request.payload.get("task_id")
    if not isinstance(task_id, str) or not task_id.strip() or len(task_id) > _MAX_TASK_ID_CHARS:
        return None
    if len(request.to_bytes()) > _MAX_REQUEST_BYTES:
        return None
    return task_id.strip()


def _reply(
    *,
    request: Envelope,
    sender: str,
    new_id: Callable[[], str],
    now: Callable[[], str],
    payload: dict[str, str],
) -> Envelope:
    return Envelope.new(
        msg_id=new_id(),
        ts=now(),
        sender=sender,
        kind="show_work",
        correlation_id=request.id,
        payload=payload,
    )


async def serve_show_work(
    bus: FleetBus,
    *,
    env: str,
    sender: str,
    new_id: Callable[[], str],
    now: Callable[[], str],
) -> Subscription:
    """Wire the one hub-wide raw responder; callers own its lifecycle handle."""
    subject = subjects.hub_show_work(env)
    retrieval = asyncio.Lock()

    async def on_request(msg: Msg) -> None:
        try:
            if not msg.reply:
                logger.warning("orchestrator.show_work | rejected request without reply subject")
                return
            if len(msg.data) > _MAX_REQUEST_BYTES:
                logger.warning("orchestrator.show_work | rejected oversized request")
                return
            request = Envelope.from_bytes(msg.data)
            task_id = _task_id(request)
            if task_id is None:
                payload = {"error": "show_work: invalid request"}
            elif retrieval.locked():
                payload = {"error": "show_work: busy"}
            else:
                async with retrieval:
                    try:
                        payload = {"markdown": await asyncio.to_thread(_render, task_id)}
                    except LookupError:
                        payload = {"error": "show_work: no deliberation frames found"}
                    except (OSError, UnicodeError, ValueError):
                        payload = {"error": "show_work: response unavailable"}
            reply = _reply(request=request, sender=sender, new_id=new_id, now=now, payload=payload)
            await bus.reply(msg, reply)
        except Exception as exc:  # noqa: BLE001 - raw callbacks must remain alive
            logger.warning("orchestrator.show_work | request failed class={}", type(exc).__name__)

    logger.info("orchestrator.show_work | serving subject={}", subject)
    return await bus.raw_subscribe(subject, on_request)
