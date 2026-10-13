"""Import and metadata tests for the fleet-core skeleton."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

import fleet_core
import fleet_core.config
import fleet_core.spec
import fleet_core.transport

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_package_version_matches_distribution_metadata() -> None:
    assert fleet_core.__version__ == version("fleet-core")


def test_c1_stub_modules_import() -> None:
    assert fleet_core.spec is not None
    assert fleet_core.config is not None
    assert fleet_core.transport is not None


def test_release_docs_pin_tag_and_subdirectory_install_to_project_version() -> None:
    pyproject = tomllib.loads((PACKAGE_ROOT / "pyproject.toml").read_text())
    project_version = pyproject["project"]["version"]
    release_tag = f"fleet-core/v{project_version}"
    install_url = (
        "fleet-core @ git+https://github.com/rodaddy/fleet-bus.git@"
        f"{release_tag}#subdirectory=packages/fleet-core"
    )

    releasing = (PACKAGE_ROOT / "RELEASING.md").read_text()
    readme = (PACKAGE_ROOT / "README.md").read_text()

    assert release_tag in releasing
    assert install_url in releasing
    assert release_tag in readme
    assert install_url in readme
