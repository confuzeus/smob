# AGENTS.md — smob

Agent and contributor guide for the smob repository. Read the linked documents
before changing behavior; this file only orients you and defines the rules that
apply across the codebase.

## Start here

| Read | For |
| --- | --- |
| [README.md](README.md) | User-facing contract: features, quick start, metric catalogue, CLI reference, and storage behavior. |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | Internal design: principles, sample schema, chunk/storage layout, delivery phases, risks, testing strategy, repository layout, and TODO list. |
| [.agents/skills/go-best-practices/SKILL.md](.agents/skills/go-best-practices/SKILL.md) | Go conventions, dependency policy, error handling, concurrency, and the definition of done. |
| [.agents/skills/integration-testing/SKILL.md](.agents/skills/integration-testing/SKILL.md) | Test contract, harness rules, and what may be tested as a pure function. |
| [justfile](justfile) | Authoritative development commands. |

`README.md` is canonical for user-visible behavior. `docs/IMPLEMENTATION.md` is
canonical for internal design and the roadmap. Keep them consistent when a
change affects both.

## Project status

smob is pre-v0.1. The CLI scaffolding, configuration loader, `version`, and
`config` commands are implemented. `collect`, `query`, `stats`, and `tail` are
stubs in the current build. The README describes the target v0.1 contract, not
only current behavior.

## Commands

`just` is the task runner; there is no Makefile.

| Task | Command |
| --- | --- |
| Build the static binary | `just build` (`CGO_ENABLED=0`) |
| Format Go source | `just fmt` |
| Vet and static analysis | `just vet`, `just lint` |
| Pure-function tests | `just test-unit` |
| Fast integration suite | `just test` |
| Extended suite (MinIO/Docker) | `just test-all` |
| Race-enabled integration suite | `just test-race` (requires cgo) |
| Build the container image | `just docker` |
| Cross-compile release binaries | `just release` |

Release binaries stay `CGO_ENABLED=0`. The race build is a CI/testing artifact
only.

## Repository map

| Path | Purpose |
| --- | --- |
| `cmd/smob/` | Cobra root and subcommand wiring. |
| `internal/model/` | Samples, queries, label matchers, and time ranges. |
| `internal/config/` | Strict YAML configuration and validation. |
| `internal/store/` | `Store`/`Chunk` contract and backend factory. |
| `internal/source/` | System, process, and Docker metric sources (planned). |
| `internal/collector/` | Scheduler and chunk assembler (planned). |
| `internal/query/` | Filtering, aggregation, and output formatting (planned). |
| `test/integration/` | Black-box CLI tests against real dependencies. |
| `docs/` | Contributor design docs and future user guides. |

See `docs/IMPLEMENTATION.md` for the full planned layout.

## Working rules

- Load `go-best-practices` before writing or reviewing Go, and
  `integration-testing` before writing or changing tests.
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
- [ ] `just test-all` passes when MinIO/Docker services are available.
- [ ] New observable behavior has an integration test through the CLI.
- [ ] New configuration fields are validated with field-named errors.
- [ ] `README.md` and `docs/IMPLEMENTATION.md` reflect the change, including
      the TODO list when a phase item is completed.
- [ ] Release builds remain `CGO_ENABLED=0`.
