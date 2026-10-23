# smob — Simple Monitoring Observability Tool

## Executive Summary

**smob** is a lightweight observability tool delivered as a single Go binary, designed to record CPU, memory, and network usage over time on a VPS and store the data either on local disk or on any S3-compatible object storage. It supports three collection scopes: system-wide, individual processes, and Docker containers. Users interact with the tool through a straightforward CLI (`smob`) to collect metrics and query historical data. The tool ships with a Docker image for container-based deployment, while its static binary and minimal dependencies make it equally at home running directly on the host under systemd.

The design favors simplicity and correctness over feature breadth. Metrics are captured by directly parsing Linux `/proc` (for system and process scopes) and by using the Docker Engine API (for container scope), avoiding heavy instrumentation libraries. A pluggable storage interface backed by a compact, human-inspectable on-disk format (JSONL-gzipped chunks) and a matching S3 layout ensures the two backends are interchangeable — the same data can be written to disk today and migrated to S3 tomorrow by changing config. Sampling is driven by a single ticker with in-memory ring buffering and batching, so writes to S3 are efficient and resilient to transient backend failures.

The MVP can be delivered in four increments, each of which produces a usable tool: (1) system metrics with a disk backend, (2) process metrics with filtering, (3) an S3 backend, and (4) Docker container metrics. Later increments add retention policies, tailing, and aggregation options. Development is scoped at roughly 4–6 weeks for a single engineer working at a sustainable pace.

---

## Implementation Plan

### 1. Guiding Principles

- **One binary, two modes.** `smob collect` runs the daemon; every other subcommand is a short-lived CLI. Shared code, no forking.
- **Direct `/proc` parsing.** Avoid `gopsutil`. Rates (CPU %, B/s) are computed in the collector and stored as final values, not raw counters.
- **Pluggable storage.** The `Store` interface is the only thing the collector knows about. Disk and S3 are peer implementations behind it.
- **Label-based samples.** Every sample carries a metric name, value, timestamp, and labels (scope, PID, container, etc.). The CLI filters on labels, not on hard-coded scopes.
- **Fail soft, then fail loud.** A missing Docker socket disables the container source with a warning; an unreadable `/proc` fails startup immediately.

### 2. Phase Breakdown

**Phase 0 — Foundations (Week 1)**

- Initialize Go module, repository layout, justfile, and CI (build, vet, integration tests, race).
- Define `model` package: `Sample`, `Query`, `TimeRange`, label helpers.
- Define `store.Store` interface and the `config` package (YAML or TOML — pick one, validate on load).
- Wire up `cobra` root command and stub subcommands: `collect`, `query`, `tail`, `stats`, `config`, `version`.
- Cross-compile test with `CGO_ENABLED=0` for Linux amd64 and arm64; produce a `FROM scratch` Dockerfile.

**Phase 1 — MVP: System Metrics + Disk Backend (Week 2)**

- Implement `internal/source/system`: parsers for `/proc/stat`, `/proc/meminfo`, `/proc/net/dev`, verified end-to-end against the test host's real `/proc`.
- Implement the collector: single ticker, ring buffer (bounded by time), batch flush, structured logging via `slog`.
- Implement `internal/store/disk`: chunked JSONL-gzip with atomic write via `.tmp` + `rename(2)`, directory layout `<data>/<scope>/YYYY/MM/DD/HH/<chunk>.jsonl.gz`, per-chunk index file.
- Implement `query` subcommand: table and JSON output, filters on metric, time range, and labels.
- **Exit criterion:** `smob collect` writes real samples to disk for a full day without gaps; `smob query --since 1h` returns correct CPU, memory, and network figures.

**Phase 2 — Process Metrics (Week 3)**

- Implement `internal/source/process`: enumerator over `/proc/[pid]`, parser for `stat` (jiffies, starttime) and `statm`/`status` (RSS), filter by regex, top-N mode.
- Handle PID reuse (key by `(pid, starttime)`), counter reset detection, and re-scan cadence.
- Add config fields: `process.filter`, `process.top_n`, `process.enabled`.
- Extend CLI with `--label pid=...` and `--label comm=...` filtering.
- **Exit criterion:** given a config filtering `nginx|postgres`, `smob query` shows per-process CPU and RSS at the configured resolution.

**Phase 3 — S3 Backend (Week 4)**

- Implement `internal/store/s3` using `aws-sdk-go-v2` (works against any S3-compatible service; configure endpoint, region, credentials from env or config).
- Mirror the disk chunk layout in the object key namespace. Batch size derived from a target chunk duration (default 1m) rather than byte count.
- Add retry with exponential backoff; on sustained failure and if disk is also configured, fall back to disk.
- CLI queries: `LIST` with a prefix derived from the time range, then concurrent `GET` with a worker pool.
- Add `--dry-run` on `collect` for validation.
- **Exit criterion:** collect to MinIO (dev) and to a real S3 bucket; query results match the disk backend byte-for-byte after decompression.

**Phase 4 — Docker Container Metrics (Week 5)**

- Implement `internal/source/docker` using the Docker Engine client over `/var/run/docker.sock`. Prefer persistent `ContainerStats` streams with fan-in for many containers; fall back to short-lived `ContainerStatsOneShot` calls for small deployments.
- Label samples with `container=<name>` and `image=<image>`.
- Graceful disable if the socket is absent; reconnect on Docker restarts.
- Config: `docker.socket`, `docker.enabled`, `docker.exclude` regex.
- Extend the Dockerfile to make `smob` runnable in a container with `/var/run/docker.sock` and `/proc` mounted. Document the `--pid=host` caveat for host-level process visibility.
- **Exit criterion:** inside a Docker container, `smob collect` records per-container CPU and network; `smob query --label container=web` returns correct values.

**Phase 5 — Polish & Release (Week 6)**

- Add retention: `retention.disk` (bytes / days) enforced during write; `retention.s3` delegated to bucket lifecycle policy with a documented recommendation.
- Add `smob tail` (follow last N samples) and `smob stats` (quick summary over a range).
- Aggregation options in `query`: `--agg avg|min|max|sum` with `--group-by metric,label`.
- Documentation: README, `smob config --validate`, sample systemd unit, sample compose file, migration note (disk → S3 by changing config and re-running with a fresh `--data-dir` and bucket).
- Release: GitHub releases with `smob` binaries and the Docker image; tag `v0.1.0`.

### 3. Non-Goals (v0.1)

- No alerting, dashboards, or Prometheus exposition.
- No Windows or macOS support — Linux only.
- No multi-host aggregation or remote query API.
- No distributed collection or sharding.

### 4. Risks and Mitigations

| Risk                                                | Mitigation                                                                               |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `/proc` parsing breaks on unusual kernels           | Integration tests on Alpine and Debian kernels; fail loud at startup on missing files    |
| S3 cost from many small objects                     | Default 1-minute chunk duration; document size at target retention                       |
| Docker stats stream per container is heavy at scale | Config knob to switch to one-shot; document scale guidance                               |
| Clock adjustments produce bogus rates               | Store the delta-time used with each rate; CLI flags anomalies                            |
| Disk fills up                                       | Retention enforcement at write time; explicit exit code + log on disk-full               |

### 5. Testing Strategy

All testing is black-box integration testing; there are no unit tests. Every test builds the `smob` binary and drives it as a subprocess against real dependencies.

- **Fast suite** (every change): config validation, `collect` → `query` against the host's real `/proc` and a temporary data directory, output formats, signal handling, and error paths that need no external service.
- **Extended suite** (CI and `just test-all`): process scope with a CPU burner, MinIO for the S3 backend, a Docker daemon for container scope, retention, `tail`, and `stats`.
- MinIO and Docker are compose/CI services configured through environment variables. Tests skip with a clear reason when a service is absent; CI runs in strict mode where a skip is a failure.
- Tests assert schema, labels, ordering, and plausible ranges, not exact live metric values. `/proc`, storage, the clock, and the Docker API are never mocked.
- Golden files cover only stable output (chunk schema, help text), with an explicit update flag.
- The race detector is applied to the `smob` binary under test, not only to the Go test process.

### 6. Repository Layout

```
cmd/smob/main.go               # cobra root, subcommand wiring
internal/model/                # Sample, Query, TimeRange
internal/config/               # config parsing + validation
internal/collector/            # ticker, ring buffer, batching, source registry
internal/source/system/        # /proc/stat, meminfo, net/dev
internal/source/process/       # /proc/[pid]/...
internal/source/docker/        # docker client wrapper
internal/store/                # Store interface, factory
internal/store/disk/           # local fs implementation
internal/store/s3/             # S3 implementation
internal/query/                # filtering, aggregation
internal/cli/                  # shared CLI helpers (table output, exit codes)
docs/                          # README, examples, systemd unit, compose file
Dockerfile                     # FROM scratch, static binary
```

---

## TODO List

### Phase 0 — Foundations

- [ ] `go mod init github.com/<org>/smob`; choose module path and license.
- [ ] Create repository layout per the plan.
- [ ] Add justfile recipes: `build`, `test`, `test-all`, `test-race`, `test-update`, `vet`, `docker`, `release`.
- [ ] Set up CI: build on `linux/amd64` and `linux/arm64` with `CGO_ENABLED=0`; run `go vet -tags=integration` and the integration suite with a `-race` binary.
- [ ] Write `FROM scratch` (or distroless-static) Dockerfile producing an image with the static binary.
- [ ] Define `model.Sample`, `model.Query`, `model.TimeRange` with doc comments.
- [ ] Define `store.Store` interface (`Write`, `Query`) and `store.New(cfg)` factory.
- [ ] Define config schema (YAML): `interval`, `data_dir`, `sources.{system,process,docker}`, `storage.{disk,s3}`, `retention`.
- [ ] Implement config loading with strict unknown-field rejection and `smob config --validate`.
- [ ] Wire `cobra` root and stub subcommands: `collect`, `query`, `tail`, `stats`, `config`, `version`.
- [ ] Write `version` subcommand printing version, commit, build date (via `-ldflags`).

### Phase 1 — MVP: System Metrics + Disk

- [ ] Implement `/proc/stat` parser (per-CPU and aggregate jiffies), covered by the system integration test.
- [ ] Implement `/proc/meminfo` parser (focus on `MemTotal`, `MemAvailable`), covered by the system integration test.
- [ ] Implement `/proc/net/dev` parser (per-interface counters), covered by the system integration test.
- [ ] Implement `system` source that diffs snapshots to produce CPU %, used memory bytes, and net RX/TX B/s.
- [ ] Detect counter wrap / reboot and emit marker samples instead of bogus rates.
- [ ] Implement collector: `time.Ticker` with jitter, bounded ring buffer, batch flush.
- [ ] Add structured logging via `log/slog`; include a `--log-level` flag.
- [ ] Implement `disk` store: chunked JSONL-gzip, atomic write (`.tmp` + `rename`), per-chunk index file.
- [ ] Add a golden-file integration test for the disk chunk layout and schema.
- [ ] Implement `query` subcommand: filters (`--metric`, `--since`, `--until`, `--label`), table output by default.
- [ ] Add `--format json` and `--format csv` to `query`.
- [ ] End-to-end smoke test in CI: `collect` for 10s, then `query`, assert results.
- [ ] Manual soak test: run `collect` for 24h, verify no gaps and reasonable memory usage.

### Phase 2 — Process Metrics

- [ ] Implement `/proc/[pid]/stat` parser (fields 14, 15, 22 = utime, stime, starttime), covered by the process integration test.
- [ ] Implement `/proc/[pid]/statm` and `/proc/[pid]/status` (VmRSS) parsers, covered by the process integration test.
- [ ] Implement PID enumeration with configurable re-scan cadence.
- [ ] Implement process source: filter by regex (`process.filter`), top-N mode (`process.top_n`).
- [ ] Handle PID reuse by keying on `(pid, starttime)`.
- [ ] Emit per-process labels: `pid`, `comm`, `scope=process`.
- [ ] Extend `query` to support `--label pid=` and `--label comm=` with regex match.
- [ ] Add process source config fields and validation.
- [ ] Benchmark: measure per-tick cost on a host with ~1000 processes; document.

### Phase 3 — S3 Backend

- [ ] Add `aws-sdk-go-v2` dependency; implement config (endpoint, region, bucket, prefix, credentials).
- [ ] Implement `s3` store with the same chunk layout as disk; final-key PUT (no rename needed).
- [ ] Implement retry with exponential backoff and jitter.
- [ ] Implement fallback-to-disk when configured and S3 is failing after retry budget.
- [ ] Implement `LIST`-prefix query path derived from the time range.
- [ ] Implement concurrent `GET` with a bounded worker pool in the query path.
- [ ] Add `minio` service to CI; run S3 integration tests.
- [ ] Verify: `disk` and `s3` backends produce the same key layout and parse to equivalent samples on identical input.
- [ ] Document S3 IAM permissions (`s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `s3:DeleteObject` if retention is used).

### Phase 4 — Docker Container Metrics

- [ ] Add Docker Engine client dependency; connect over `docker.socket` config.
- [ ] Implement `ContainerStats` stream fan-in for persistent collection.
- [ ] Implement `ContainerStatsOneShot` path as an alternative for small deployments.
- [ ] Compute per-container CPU % and net B/s correctly (using `precpu_stats`).
- [ ] Emit labels: `container`, `image`, `scope=container`.
- [ ] Implement graceful disable when socket is absent; log at info level.
- [ ] Implement reconnect on Docker daemon restart.
- [ ] Add `docker.exclude` regex config.
- [ ] Update Dockerfile and docs: mount `/var/run/docker.sock`; document `--pid=host` caveat.
- [ ] Integration test in CI using a `dind` service; run a container and assert metrics appear.

### Phase 5 — Polish & Release

- [ ] Implement disk retention: delete oldest chunks when size or age exceeds config.
- [ ] Document S3 retention via bucket lifecycle policy (recommendation, not code).
- [ ] Implement `smob tail`: follow the last N samples, streaming output.
- [ ] Implement `smob stats`: summary over a range (count, min/max/mean per metric).
- [ ] Add `--agg avg|min|max|sum` and `--group-by` to `query`.
- [ ] Write README: quickstart, config reference, disk↔S3 migration note.
- [ ] Provide sample `systemd` unit and `docker-compose.yml`.
- [ ] Add `--version` output test; add `--help` snapshot tests for each subcommand.
- [ ] Tag `v0.1.0`; build release artifacts (binaries for amd64/arm64, Docker image).
- [ ] Post-release: collect user feedback, prioritize `v0.2.0` backlog (alerting, Prometheus endpoint, non-Linux targets are candidates to explicitly reject or accept).

### Cross-Cutting (run throughout)

- [ ] Every parser and store backend is covered by an integration test; CI builds the binary under test with `-race`.
- [ ] Every CLI subcommand has a `--format` option and a smoke test.
- [ ] Every config field is validated with a clear error message.
- [ ] Every external failure mode (disk full, S3 auth error, missing socket, `/proc` unreadable) is exercised by a test or explicitly documented.
- [ ] Keep the binary lean: audit dependencies at each phase; remove unused ones.
