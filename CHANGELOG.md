# Changelog for Contributors

Developer-facing release history: repository tooling, tests, CI, build,
documentation, and process. User-visible features, behavior changes, bug fixes,
and upgrade notes live in [RELEASE_NOTES.md](RELEASE_NOTES.md). Every tagged
release has one entry here, newest first, and the version matches the git tag.

## v0.2.8 — Flaky Collector Test Fix

Nothing user-visible changed; the binary is identical to v0.2.7. See
[RELEASE_NOTES.md](RELEASE_NOTES.md).

- `internal/collector`: `TestSlowSourceTimesOutAndRunContinues` bounded the run
  with a 400 ms parent context and then asserted that every tick carried a
  positive per-tick deadline. Near the parent's expiry a tick could start with
  its clamped deadline already in the past, which the assertion read as a
  missing deadline, so the test failed intermittently (about 1 in 20 runs
  locally). The test now cancels the run when the slow source retries, so the
  run has no deadline of its own, every tick carries the full `tickTimeout`
  budget, and the retried sample is still flushed to the store and asserted.

## v0.2.7 — Copy-Paste Recipes

Documentation only; the binary, configuration, metrics, and storage formats are
unchanged from v0.2.6. See [RELEASE_NOTES.md](RELEASE_NOTES.md).

- Added `docs/recipes.md`: complete copy-paste setups for systemd on local disk,
  systemd on AWS S3, Docker Compose on local disk, Docker Compose with container
  metrics, MinIO, and query-only hosts, with variants for process filtering,
  retention, shared buckets, static credentials, bind mounts, and S3 in Compose.
- Cross-linked the recipes from `README.md`, `docs/quickstart.md`,
  `docs/configuration.md`, `docs/s3.md`, `docs/docker.md`, and the repository
  layout in `docs/IMPLEMENTATION.md`.

## v0.2.6 — One-Line Installation

User-visible distribution changed: smob now ships a `curl` installer that
verifies the release checksum and can install and start the systemd service.
The binary is unchanged. See [RELEASE_NOTES.md](RELEASE_NOTES.md).

- Added `install.sh`, a POSIX `sh` installer published as a release asset at
  `releases/latest/download/install.sh`. It resolves the latest tag from the
  release redirect, validates `vX.Y.Z`, selects linux/amd64 or linux/arm64,
  verifies the binary against `SHA256SUMS`, installs atomically to
  `/usr/local/bin/smob`, creates `/etc/smob/smob.yaml` only when absent,
  validates it, and installs and restarts the systemd unit. Non-root use
  requires `--no-service --install-dir DIR`; the unit's fixed paths are
  rejected otherwise. `SMOB_RELEASES_BASE_URL` overrides the download prefix for
  mirrors and CI.
- `release.yml` publishes `install.sh` and `contrib/systemd/smob.service` (as
  `smob.service`) with each release, and installs the built binaries through the
  installer from a local mirror before publishing.
- Added `test/integration/install_test.go` covering a successful install, a
  checksum mismatch, a missing asset, and the non-root refusal. The Alpine lane
  skips it with a reason when curl is absent.
- Added `just lint-shell` (shellcheck), run by the CI lint job.
- Docs: README and quickstart lead with the installer; IMPLEMENTATION records
  the release assets, repository layout, and the distribution item.

## v0.2.5 — Workflow Lint Fix

Nothing user-visible changed; the binary is identical to v0.2.4. See
[RELEASE_NOTES.md](RELEASE_NOTES.md).

- CI: the `wait for MinIO` step used `for i in $(seq 1 30)` without reading `i`,
  which shellcheck reported as SC2034 and actionlint treated as an error, so
  `just lint-workflows` failed. The counter is now the throwaway name `_`.

## v0.2.4 — Release Automation and CI Hardening

User-visible distribution changed: every annotated tag now publishes the release
binaries and a multi-arch container image. See
[RELEASE_NOTES.md](RELEASE_NOTES.md).

- Added `.github/workflows/release.yml`. A `v*` tag push, or a manual dispatch
  naming a tag, verifies the tag is annotated and that its commit is on
  `master`, runs the fast gate, extracts the notes, resolves
  `VERSION`/`COMMIT`/`DATE` from the tagged commit, writes `SHA256SUMS`, and
  pushes the GHCR multi-arch image before it creates the release. A re-run edits
  the existing release and re-uploads its assets with `--clobber`, so a failed
  run converges instead of duplicating.
- Added `cmd/release-notes` and `internal/releasenotes` for extracting one
  release section, with table tests; `just release-notes <tag>` previews the
  same title and body locally.
- `Dockerfile` now builds on `$BUILDPLATFORM` and cross-compiles with
  `$TARGETOS`/`$TARGETARCH`, so the arm64 image is produced without emulation.
- CI: corrected the master concurrency comment; raised the test job cap to 30m
  and gave `test` and `test-race` an explicit 10m timeout; raised the extended
  cap to 60m. The Alpine lane runs as the invoking user, so the read-only parent
  directory permission test no longer skips, derives and pins its toolchain
  image with a `go.mod` guard, and the process RSS floor now accepts busybox
  `yes`. The busybox fixture is pinned by digest in the workflow and both test
  packages. The image test asserts `/data` ownership and the CA bundle, the
  build matrix no longer fails fast, and the image banner checks use substring
  matching. `just fmt-check` and `just lint-workflows` (actionlint v1.7.12) are
  the lint sources; the golden gate detects untracked files and the goldens job
  gets the Docker socket; the `setup-just` pin comment names v4.0.0.
- Docs: the release skill, `AGENTS.md`, the README install and download path,
  quickstart, implementation doc, and integration-testing skill wording.

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
