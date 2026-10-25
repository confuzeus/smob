# Changelog for Contributors

Developer-facing release history: repository tooling, tests, CI, build,
documentation, and process. User-visible features, behavior changes, bug fixes,
and upgrade notes live in [RELEASE_NOTES.md](RELEASE_NOTES.md). Every tagged
release has one entry here, newest first, and the version matches the git tag.

## v0.2.3 — Split User-Facing Release Notes

No user-visible changes; the binary, configuration, metrics, and storage formats
are unchanged from v0.2.2.

- `docs(release)`: `RELEASE_NOTES.md` now records only user-visible changes;
  the developer-facing history moved to this file.
- `.agents/skills/release/SKILL.md` requires one entry here for every release,
  a `RELEASE_NOTES.md` section only for user-visible releases, and a short
  stub there for internal-only ones. It also drops the old requirement to
  maintain a release-history list inside `RELEASE_NOTES.md`.
- `AGENTS.md` lists this file and splits the release-record rule by audience.

## v0.2.2 — Release Workflow

No user-visible changes; the binary, configuration, metrics, and storage formats
are unchanged from v0.2.1.

- Added `.agents/skills/release/SKILL.md`: verify the tree, choose the next
  SEMVER version from the latest tag, update the release record, commit, and
  create an annotated `vX.Y.Z` tag on that commit.
- Recorded that the `justfile` derives `VERSION` from `git describe`, so there
  is no version constant to edit and no second place for it to drift.
- `AGENTS.md` links the skill, makes the workflow a working rule, and checks the
  notes, commit, and tag in its definition of done.

## v0.2.1 — Initial Release

User-visible changes; see [RELEASE_NOTES.md](RELEASE_NOTES.md).

- Packaged the v0.1 and v0.2 development line for publication: system, process,
  and container metrics; disk and S3 backends with spool replay; `collect`,
  `query`, and `stats`; disk retention; aggregation; and operator docs, the
  systemd unit, and the Compose example.
- Audit fixes across storage, sources, CLI, documentation, and CI.

## v0.2.0 — Development Tag

User-visible changes; their capabilities shipped in v0.2.1. Per-release notes
are in [docs/releases/v0.2.0.md](docs/releases/v0.2.0.md).

- S3 backend, Docker container metrics, disk retention, `stats`, aggregation,
  and operator documentation.
- CI extended suite requires MinIO and Docker.

## v0.1.0 — Development Tag

User-visible changes; their capabilities shipped in v0.2.1.

- System and process metrics, disk backend, `collect`, and `query`.
