# fleet-core release procedure

`fleet-core` releases are immutable Git tags on this monorepo. Downstream
clients install the package from the `packages/fleet-core` subdirectory at a
specific tag, so the tag name, package version, and install URL must stay in
lockstep.

## Immutable tag convention

The release tag for `fleet-core` is:

```text
fleet-core/v{project.version}
```

For the current package version in `pyproject.toml`, the tag is:

```text
fleet-core/v0.1.0
```

The tag must point at the reviewed commit that contains the matching
`packages/fleet-core/pyproject.toml` version. Do not create the tag before the
final reviewed head is known, and do not move or reuse a published tag. If a
release needs a correction, bump `project.version` and publish a new tag.

## Downstream install URL

Downstream clients pin the exact tag and package subdirectory:

```sh
uv add "fleet-core @ git+https://github.com/rodaddy/fleet-bus.git@fleet-core/v0.1.0#subdirectory=packages/fleet-core"
```

Replace only the version suffix when cutting a newer `fleet-core` release. The
`#subdirectory=packages/fleet-core` fragment is required because `fleet-core` is
a package inside the fleet-bus monorepo, not the repository root package.

## Release checklist

1. Confirm `packages/fleet-core/pyproject.toml` has the intended
   `project.version`.
2. Confirm the release tag would be `fleet-core/v{project.version}`.
3. Run package validation:

   ```sh
   cd packages/fleet-core
   uv run ruff format --check .
   uv run ruff check .
   uv run mypy src tests
   uv run pytest
   uv build
   ```

4. From the repository root, prove the Git-subdirectory install path from the
   exact reviewed head or tag:

   ```sh
   uv run --with "fleet-core @ git+file://$PWD@$(git rev-parse HEAD)#subdirectory=packages/fleet-core" python -c "import fleet_core; print(fleet_core.__version__)"
   ```

5. After final review passes, create the immutable tag:

   ```sh
   git tag fleet-core/v0.1.0 <reviewed-head-sha>
   git push origin fleet-core/v0.1.0
   ```

6. Have at least one downstream client pin the HTTPS install URL above before
   relying on the release for client cadence separation.

## Fleet-core hard rules

1. `fleet-core` is the base; import it, do not reinvent. Capability gaps become
   fleet-core issues, not forked transports or hand-formatted subjects.
2. The spec is code: signals are typed Pydantic fields, never prose trailers.
   `[[done]]` and `[[stance:]]` are deleted at the root.
3. Durable-by-default; the only ephemeral exception is heartbeat. Losing
   durability is an explicit, justified opt-out.
4. `extra="allow"` is for client-specific signal; agreed fields validate hard.
5. `mypy --strict`, ruff, and pytest must stay green; lanes keep disjoint write
   sets.
6. Backward compat: re-pointing a package keeps existing consumers working, with
   no silent break across the fleet.
