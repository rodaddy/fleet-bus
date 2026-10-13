#!/usr/bin/env python3
"""Verify an immutable fleet hub release without changing deployment state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path


REQUIRED_DISTRIBUTIONS = {
    "fleet-core": "fleet_core",
    "fleet-nats": "fleet_nats",
    "fleet-subscriber": "fleet_subscriber",
    "fleet-orchestrator": "fleet_orchestrator",
}
REQUIRED_MANIFEST_KEYS = {
    "schema_version",
    "sha",
    "lock_sha256",
    "distributions",
    "files",
    "symlinks",
}
REQUIRED_RELEASE_FILES = {
    ".fleet-release-sha",
    "src/uv.lock",
    ".venv/bin/fleet-orchestrator",
}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
PYTHON_LIBRARY_PATTERN = re.compile(r"^python[0-9]+\.[0-9]+$")


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of one regular file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fail(message: str) -> None:
    """Stop verification with a stable operator-facing error."""
    raise SystemExit(f"release verification failed: {message}")


def parse_args() -> argparse.Namespace:
    """Parse the trusted source, release path, and expected source SHA."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("--sha", required=True)
    return parser.parse_args()


def is_python_bytecode(path: Path, relative: str) -> bool:
    """Return whether *relative* is derived CPython bytecode, not sealed content.

    A ``__pycache__`` directory (or a stray top-level ``*.pyc``) holds bytecode
    that CPython regenerates from the ``.py`` source it caches. It is DERIVED,
    not authored: the running orchestrator writes fresh ``.pyc`` into the venv on
    import (the service unit does not set ``PYTHONDONTWRITEBYTECODE``), which
    would otherwise mutate the sealed release out from under its own manifest and
    fail the next deploy (#208). Excluding it is safe because the authoritative
    ``.py`` source IS sealed — CPython only trusts a cached ``.pyc`` whose
    embedded source hash/mtime matches the ``.py``, so a tampered ``.pyc`` beside
    an authentic ``.py`` is ignored or regenerated, never silently executed.
    """
    return "__pycache__" in path.parts or relative.endswith(".pyc")


def release_inventory(release: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Return every sealed regular file hash and every symlink target in *release*.

    Derived CPython bytecode (``__pycache__``/``*.pyc``) is excluded — see
    :func:`is_python_bytecode`. The seal producer applies the SAME exclusion, so
    the manifest and this inventory always agree.
    """
    files: dict[str, str] = {}
    symlinks: dict[str, str] = {}
    for path in sorted(release.rglob("*")):
        relative = path.relative_to(release).as_posix()
        try:
            mode = path.lstat().st_mode
        except OSError as exc:
            fail(f"could not inspect release entry {relative}: {exc}")
        if stat.S_ISDIR(mode):
            continue
        if is_python_bytecode(path, relative):
            continue
        if stat.S_ISLNK(mode):
            try:
                symlinks[relative] = os.readlink(path)
            except OSError as exc:
                fail(f"could not read release symlink {relative}: {exc}")
            continue
        if not stat.S_ISREG(mode):
            fail(f"release entry is not regular or a documented symlink: {relative}")
        if relative not in {"release-manifest.json", "release-seal"}:
            files[relative] = sha256_file(path)
    return files, symlinks


def source_file_map(source: Path) -> dict[str, str]:
    """Return the trusted source files that must be copied into the release."""
    files: dict[str, str] = {}
    for root_name in ("packages", "scripts"):
        root = source / root_name
        if not root.is_dir() or root.is_symlink():
            fail(f"trusted source is missing a regular {root_name} directory")
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(source).as_posix()
            if {".venv", "__pycache__"} & set(path.relative_to(source).parts):
                continue
            mode = path.lstat().st_mode
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                fail(f"trusted source entry is not regular: {relative}")
            files[relative] = sha256_file(path)
    for relative in ("pyproject.toml", "uv.lock"):
        path = source / relative
        if not path.is_file() or path.is_symlink():
            fail(f"trusted source file is missing or is a symlink: {relative}")
        files[relative] = sha256_file(path)
    return files


def load_manifest(path: Path) -> dict[str, object]:
    """Load and validate the top-level release manifest shape."""
    if not path.is_file() or path.is_symlink():
        fail("release manifest is missing or is a symlink")
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"invalid release manifest: {exc}")
    if not isinstance(manifest, dict) or set(manifest) != REQUIRED_MANIFEST_KEYS:
        fail("release manifest schema is incomplete or has unexpected fields")
    return manifest


def regular_directory(path: Path) -> bool:
    """Return whether *path* is a non-symlink directory."""
    try:
        return stat.S_ISDIR(path.lstat().st_mode)
    except OSError:
        return False


def regular_file(path: Path) -> bool:
    """Return whether *path* is a non-symlink regular file."""
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except OSError:
        return False


def candidate_site_packages(release: Path) -> Path:
    """Locate the candidate venv site-packages without running candidate code."""
    library_root = release / ".venv" / "lib"
    if not regular_directory(library_root):
        fail("candidate venv library directory is missing or is not regular")
    candidates: list[Path] = []
    for path in sorted(library_root.iterdir()):
        if not PYTHON_LIBRARY_PATTERN.fullmatch(path.name):
            continue
        site_packages = path / "site-packages"
        if regular_directory(site_packages):
            candidates.append(site_packages)
        elif site_packages.exists() or site_packages.is_symlink():
            fail("candidate site-packages path is not a regular directory")
    if len(candidates) != 1:
        fail("candidate site-packages layout is missing or ambiguous")
    return candidates[0]


def metadata_name(path: Path) -> str:
    """Read the exact distribution Name field from a regular metadata file."""
    if not regular_file(path):
        fail(
            f"candidate distribution metadata is missing or not regular: {path.parent.name}"
        )
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        fail(
            f"could not read candidate distribution metadata {path.parent.name}: {exc}"
        )
    for line in lines:
        if not line:
            break
        field, separator, value = line.partition(":")
        if separator and field.lower() == "name" and value.strip():
            return value.strip()
    fail(f"candidate distribution metadata has no Name field: {path.parent.name}")


def distribution_directory_name(path: Path) -> str | None:
    """Return the required distribution implied by a metadata directory name."""
    for distribution_name in REQUIRED_DISTRIBUTIONS:
        normalized = distribution_name.replace("-", "_")
        for prefix in (distribution_name, normalized):
            if path.name == f"{prefix}.egg-info" or (
                path.name.startswith(f"{prefix}-") and path.name.endswith(".dist-info")
            ):
                return distribution_name
    return None


def verify_candidate_distributions(release: Path) -> None:
    """Verify required installed distributions and modules without candidate imports."""
    site_packages = candidate_site_packages(release)
    found: dict[str, list[Path]] = {name: [] for name in REQUIRED_DISTRIBUTIONS}
    for path in sorted(site_packages.iterdir()):
        is_dist_info = path.name.endswith(".dist-info")
        is_egg_info = path.name.endswith(".egg-info")
        if not (is_dist_info or is_egg_info):
            continue
        implied_name = distribution_directory_name(path)
        if not regular_directory(path):
            if implied_name is not None:
                fail(
                    f"candidate distribution metadata directory is not regular: {implied_name}"
                )
            continue
        metadata_path = path / ("METADATA" if is_dist_info else "PKG-INFO")
        observed_name = metadata_name(metadata_path)
        if implied_name is not None and observed_name != implied_name:
            fail(f"candidate distribution metadata name mismatch: {implied_name}")
        if observed_name in REQUIRED_DISTRIBUTIONS:
            if implied_name != observed_name:
                fail(f"candidate distribution metadata path mismatch: {observed_name}")
            found[observed_name].append(path)
    for distribution_name, module_name in REQUIRED_DISTRIBUTIONS.items():
        if len(found[distribution_name]) != 1:
            fail(
                f"candidate distribution metadata missing or duplicate: {distribution_name}"
            )
        package_path = site_packages / module_name
        module_file = site_packages / f"{module_name}.py"
        if regular_directory(package_path):
            if regular_file(package_path / "__init__.py"):
                continue
        elif regular_file(module_file):
            continue
        fail(f"candidate module path missing or not regular: {distribution_name}")


def verify_release(source_input: Path, release_input: Path, expected_sha: str) -> None:
    """Fail closed unless *release_input* is the exact sealed release for SHA."""
    if not SHA_PATTERN.fullmatch(expected_sha):
        fail("expected SHA must be a full lowercase commit SHA")
    if not source_input.is_dir() or source_input.is_symlink():
        fail("trusted source directory is missing or is a symlink")
    if not release_input.is_dir() or release_input.is_symlink():
        fail("release directory is missing or is a symlink")
    source = source_input.resolve()
    release = release_input.resolve()
    marker_path = release / ".fleet-release-sha"
    manifest_path = release / "release-manifest.json"
    seal_path = release / "release-seal"
    lock_path = release / "src" / "uv.lock"
    venv_python = release / ".venv" / "bin" / "python"
    executable = release / ".venv" / "bin" / "fleet-orchestrator"

    if not marker_path.is_file() or marker_path.is_symlink():
        fail("release marker is missing or is a symlink")
    if marker_path.read_text() != f"{expected_sha}\n":
        fail("release marker SHA mismatch")
    if not seal_path.is_file() or seal_path.is_symlink():
        fail("release seal is missing or is a symlink")
    if not lock_path.is_file() or lock_path.is_symlink():
        fail("release lock file is missing or is a symlink")
    if not venv_python.is_file() or not (venv_python.stat().st_mode & 0o111):
        fail("release venv Python is missing")
    if (
        not executable.is_file()
        or executable.is_symlink()
        or not executable.stat().st_mode & 0o111
    ):
        fail("release executable is missing or is not executable")

    manifest = load_manifest(manifest_path)
    if manifest["schema_version"] != 2 or manifest["sha"] != expected_sha:
        fail("manifest schema version or SHA mismatch")
    if manifest["distributions"] != REQUIRED_DISTRIBUTIONS:
        fail("manifest distribution set is incomplete or mismatched")
    lock_digest = manifest["lock_sha256"]
    if not isinstance(lock_digest, str) or not SHA256_PATTERN.fullmatch(lock_digest):
        fail("manifest lock digest is invalid")
    if sha256_file(lock_path) != lock_digest:
        fail("manifest lock digest mismatch")

    files = manifest["files"]
    if not isinstance(files, dict) or not files:
        fail("manifest file set is empty or invalid")
    if any(
        not isinstance(path, str)
        or not isinstance(digest, str)
        or not SHA256_PATTERN.fullmatch(digest)
        for path, digest in files.items()
    ):
        fail("manifest file map contains an invalid path or digest")
    if any(
        "/" not in path and path in {"release-manifest.json", "release-seal"}
        for path in files
    ):
        fail("manifest file map must not contain manifest or seal")
    symlinks = manifest["symlinks"]
    if (
        any(
            not isinstance(path, str) or not isinstance(target, str)
            for path, target in symlinks.items()
        )
        if isinstance(symlinks, dict)
        else True
    ):
        fail("manifest symlink map contains an invalid path or target")
    actual_files, actual_symlinks = release_inventory(release)
    if files != actual_files:
        fail("manifest file set is incomplete or content mismatched")
    if symlinks != actual_symlinks:
        fail("manifest symlink set is incomplete or mismatched")
    if not REQUIRED_RELEASE_FILES.issubset(files):
        fail("manifest omits required release content")
    expected_source_files = source_file_map(source)
    actual_source_files = {
        relative.removeprefix("src/"): digest
        for relative, digest in files.items()
        if relative.startswith("src/")
    }
    if actual_source_files != expected_source_files:
        fail("release source content does not match the trusted source checkout")
    verify_candidate_distributions(release)

    expected_seal = f"{sha256_file(manifest_path)}  release-manifest.json\n"
    if seal_path.read_text() != expected_seal:
        fail("release seal is not the exact manifest digest")


def main() -> None:
    """Run the verification command."""
    sys.dont_write_bytecode = True
    args = parse_args()
    verify_release(args.source, args.release, args.sha)


if __name__ == "__main__":
    main()
