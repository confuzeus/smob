# AGENTS.md — smob

Agent and contributor guide for the smob repository. Read the linked documents
before changing behavior; this file only orients you and defines the rules that
apply across the codebase.

## Start here

| Read | For |
| --- | --- |
| [README.md](README.md) | User-facing contract: features, metric catalogue, CLI reference, storage behavior, and the documentation index. |
| [docs/quickstart.md](docs/quickstart.md) | Operator entry point: build, first collection, daemon and service deployment. |
| [docs/configuration.md](docs/configuration.md) | Canonical YAML field reference: type, default, validation, and effect. |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | Internal design: principles, sample schema, chunk/storage layout, delivery phases, risks, testing strategy, repository layout, and TODO list. |
| [.agents/skills/go-best-practices/SKILL.md](.agents/skills/go-best-practices/SKILL.md) | Go conventions, dependency policy, error handling, concurrency, and the definition of done. |
| [.agents/skills/integration-testing/SKILL.md](.agents/skills/integration-testing/SKILL.md) | Test contract, harness rules, and what may be tested as a pure function. |
| [.agents/skills/release/SKILL.md](.agents/skills/release/SKILL.md) | Commit, SEMVER tag, and release-record workflow for every completed change. |
| [CHANGELOG.md](CHANGELOG.md) | Developer-facing history of every release; user-visible changes are in `RELEASE_NOTES.md`. |
| [justfile](justfile) | Authoritative development commands. |

`README.md` is canonical for user-visible behavior. `docs/IMPLEMENTATION.md` is
canonical for internal design and the roadmap. Keep them consistent when a
change affects both.

## Project status

System, process, and Docker container metrics, the disk and S3 backends
(including spool replay), `collect`, `query`, `stats`, disk retention,
aggregation, and the operator documentation are implemented. The Docker source
is registered with the collector but disabled by default; enabling it fails
startup when the socket is unreachable. `tail` remains a stub; live tail, chunk
indexing, and multi-host aggregation are v0.3 items. The README describes the
user-facing contract, and `docs/quickstart.md` and `docs/configuration.md` are
the operator entry points. Pushing an annotated tag publishes the GitHub release
(binaries, checksums, and notes) and the GHCR image through
`.github/workflows/release.yml`; release notes come from `RELEASE_NOTES.md`.

## Commands

`just` is the task runner; there is no Makefile.

| Task | Command |
| --- | --- |
| Build the static binary | `just build` (`CGO_ENABLED=0`) |
| Format Go source | `just fmt` |
| Check formatting without writing | `just fmt-check` |
| Vet and static analysis | `just vet`, `just lint` |
| Lint the workflow files | `just lint-workflows` (actionlint) |
| Pure-function tests | `just test-unit` |
| Fast integration suite | `just test` |
| Extended suite (MinIO and Docker, strict mode) | `just test-all` |
| Race-enabled integration suite | `just test-race` (requires cgo) |
| Build the container image | `just docker` |
| Preview one release's notes | `just release-notes vX.Y.Z` (prints its title and body) |
| Cross-compile release binaries | `just release` |

Release binaries stay `CGO_ENABLED=0`. The race build is a CI/testing artifact
only.

## Repository map

| Path | Purpose |
| --- | --- |
| `cmd/smob/` | Cobra root and subcommand wiring. |
| `cmd/release-notes/` | Release-notes extractor command used by the release workflow. |
| `internal/model/` | Samples, queries, label matchers, and time ranges. |
| `internal/config/` | Strict YAML configuration and validation. |
| `internal/store/` | `Store`/`Chunk` contract, chunk encoding, and disk/S3 backends. |
| `internal/source/` | System, process, and Docker metric sources. |
| `internal/collector/` | Scheduler and chunk assembler. |
| `internal/query/` | Filtering, aggregation, and output formatting. |
| `internal/releasenotes/` | Release-notes section extraction and its tests. |
| `test/integration/` | Black-box CLI tests against real dependencies. |
| `docs/` | Design docs and operator guides (quickstart, configuration, S3, Docker). |
| `compose.yaml` | Docker Compose deployment example. |
| `contrib/systemd/smob.service` | systemd unit for a host install. |
| `.github/workflows/release.yml` | Tag-triggered release and GHCR image publish. |

See `docs/IMPLEMENTATION.md` for the full repository layout.

## Working rules

- Load `go-best-practices` before writing or reviewing Go, and
  `integration-testing` before writing or changing tests.
- Load `release` before landing a completed change: every change is committed,
  tagged with a SEMVER version, and recorded in `CHANGELOG.md`. A change users
  can observe also updates `RELEASE_NOTES.md`; repository and process detail
  stays in `CHANGELOG.md`. The tag is the only version source; do not hand-edit
  a version constant. Do not push or publish; that is the maintainer's action.
- Pushing the annotated tag is the publish action and a maintainer decision; the
  agent never pushes. The release body and the image both derive from the tagged
  tree, and `RELEASE_NOTES.md` is the single source for the release body. Do not
  create or edit a GitHub release or image tag by hand; re-run the workflow
  instead.
- User-visible changes update `README.md`. Internal design changes update
  `docs/IMPLEMENTATION.md` and its TODO list.
- The metric catalogue and CLI reference live in `README.md`. Do not duplicate
  them in the implementation doc; link to them instead.
- Integration tests are the public contract. Build and drive the `smob` binary
  against real `/proc`, temporary data directories, and optional MinIO/Docker
  services. Do not mock `/proc`, the filesystem, the clock, stores, Docker, or
  S3.
- Linux only. Use UTC internally and the monotonic clock for durations and rate
  deltas.
- Fail loud at startup when an enabled source or the selected backend is
  unusable. There is no silent degradation.
- Do not add a third-party dependency without agreement. Approved dependencies
  are documented in the Go skill.
- Keep changes consistent with the surrounding package. A new pattern is a
  decision, not a style preference.

## Definition of done

- [ ] `gofmt -l .` is empty; `just vet`, `just lint`, and `just test` pass.
- [ ] `just test-all` passes with MinIO and Docker available. It runs in strict
      mode, so a missing service fails rather than skips; CI's `extended` job
      enforces both.
- [ ] New observable behavior has an integration test through the CLI.
- [ ] New configuration fields are validated with field-named errors.
- [ ] `README.md` and `docs/IMPLEMENTATION.md` reflect the change, including
      the TODO list when a phase item is completed.
- [ ] `CHANGELOG.md` has an entry for the new version, and `RELEASE_NOTES.md`
      has a section for it (a short stub when nothing user-visible changed).
- [ ] `just release-notes vX.Y.Z` prints the new section's title and body.
- [ ] One conventional commit contains the change and the notes, and an
      annotated `vX.Y.Z` tag points at it. Nothing was pushed; the maintainer's
      push publishes the release and image automatically.
- [ ] Release builds remain `CGO_ENABLED=0`.
