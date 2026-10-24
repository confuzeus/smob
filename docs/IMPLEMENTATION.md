# smob — Design and Implementation Plan

This document is for contributors. It covers the design rationale, storage
internals, delivery phases, testing strategy, and TODO list. User-facing
documentation — installation, quick start, the metric catalogue, and the CLI
reference — lives in the [README](../README.md).

## Design Context

**smob** is a lightweight observability tool delivered as a single Go binary. It records CPU, memory, and network usage over time on a Linux host and stores the data either on local disk or in any S3-compatible object storage. It supports three collection scopes: system-wide, individual processes, and Docker containers. Users interact with the tool through a straightforward CLI (`smob`) to collect metrics and query historical data. The tool ships with a Docker image for container-based deployment, while its static binary and minimal dependencies make it equally at home running directly on the host under systemd.

The design favors simplicity and correctness over feature breadth. Metrics are captured by directly parsing Linux `/proc` (for system and process scopes) and through the Docker Engine API (for container scope), avoiding heavy instrumentation libraries. A pluggable storage interface is backed by one time-aligned chunk format — JSONL-gzip files on disk and objects with identical keys in S3 — so the backends are interchangeable. Existing data can be copied from disk to S3 with `aws s3 sync`; switching the active backend changes only the destination. Every sample carries a `host` label, so several machines may share a bucket, but v0.1 does not aggregate across them.

Sampling is driven by one scheduler with per-source cadences. Samples are assembled into time-aligned chunks, and each completed chunk is written once. A crash loses at most one chunk interval (five minutes by default); `SIGTERM` flushes the partial chunk before exit. Completed chunks are immutable, which makes writes atomic, queries reproducible, and retention a matter of deleting whole chunks.

The MVP ships in four increments: (1) system metrics with a disk backend, (2) process metrics with filtering, (3) an S3 backend, and (4) Docker container metrics. Retention, summaries, and aggregation follow in the polish phase. Live tailing is deferred until its transport is defined. The plan targets roughly three weeks to a usable v0.1 (system + process + disk + query) and seven weeks to v0.2 (full scope). These are estimates, not commitments.

## 1. Guiding Principles

- **One binary, two modes.** `smob collect` runs the daemon; every other subcommand is a short-lived CLI. Shared code, no forking.
- **Direct `/proc` parsing.** Avoid `gopsutil`. Parsers are pure functions over an `io.Reader`; rate math is a pure function of `(previous, current, elapsed)`.
- **Rates are stored as final values.** CPU % and B/s are computed in the collector, and every rate sample records the measurement window so aggregation stays correct. Raw counters are not persisted.
- **One chunk clock.** A chunk is a UTC-aligned time window. Disk and S3 use the same chunk shape and key layout. A chunk is closed before it is written; the only partial chunks are written on shutdown or by `collect --once`.
- **Pluggable storage.** The collector hands complete chunks to a `Store`. Disk and S3 are peer implementations behind the same interface.
- **Label-based samples.** Every sample carries `host`, `scope`, and metric-specific labels. Scope partitioning in the key layout is a query-pruning detail, not part of the query contract.
- **Explicit schema and units.** Metric names encode their unit (`.percent`, `.bytes`, `.bytes_per_second`) and never change meaning between versions. Aggregation weights rates by their recorded interval.
- **Fail loud at startup.** Every enabled source and the selected backend must be usable before collection begins. Disabled sources are simply not started; there is no silent degradation of an enabled component.
- **Streaming queries.** Filters, scope, and limits are pushed into the store; results are yielded sample by sample. Nothing reads an unbounded corpus into a slice.
- **One writer per host and prefix.** The `host` label identifies the writer. Concurrent collectors writing the same host and prefix are unsupported.

## 2. Sample Schema and Rate Semantics

On the wire, each sample is one JSON object per line:

```json
{
  "timestamp": "2026-10-23T16:00:00.123456789Z",
  "metric": "system.cpu.percent",
  "value": 12.5,
  "interval_ms": 10000,
  "labels": { "host": "web-1", "scope": "system", "cpu": "all" }
}
```

In Go:

```go
type Sample struct {
	Timestamp  time.Time         `json:"timestamp"`
	Metric     string            `json:"metric"`
	Value      float64           `json:"value"`
	IntervalMS int64             `json:"interval_ms,omitempty"` // rate window; 0 for gauges
	Labels     map[string]string `json:"labels,omitempty"`
}
```

- Timestamps are UTC. `interval_ms` is the measured window between the two snapshots used for a rate; it is zero for gauges. It is not a label, so it does not split a series.
- `host` is `os.Hostname()` unless overridden in config. `scope` is `system`, `process`, or `container`.
- Labels are strings. Labels that identify a series (`pid`, `comm`, `interface`, `container`, `image`) are stable for the life of that series; labels that change per sample are not used.

The canonical metric catalogue — names, types, units, and labels — lives in the
[README's metrics reference](../README.md#metrics-reference). Update that table
first when a metric is added or changed.

CPU conventions:

- System CPU busy time is `total − (idle + iowait)`, where `total` sums the `/proc/stat` fields excluding `guest` and `guest_nice` (they are already included in `user` and `nice`).
- Aggregate `cpu=all` is derived from the aggregate `cpu` line, not by summing per-CPU percentages (they can drift when a CPU is offlined).
- Process and container percentages follow `top` and `docker stats`: 100 means one core fully busy, so a multi-threaded workload may exceed 100.
- UI tools may present these as 0–100 normalized; the stored value is always per-core percent.

Counter resets are not samples. If a counter decreases, or `/proc/sys/kernel/random/boot_id` changes, the collector drops the delta (no rate is emitted for that tick), logs once per series at `warn`, and resumes from the new snapshot. Marker samples are deliberately not written: they complicate queries and aggregations for no operational benefit.

## 3. Storage Format and Chunking

A **chunk** is a half-open UTC window `[start, end)`, one collection scope, one host, and a sorted list of samples. The default chunk interval is **5 minutes**, shared by both backends and configurable as `storage.chunk_interval`. Five minutes is a deliberate compromise: one-minute chunks produce ~525k objects per year per host and scope, while hourly chunks make crash loss and query latency worse. The interval must be a whole number of seconds of at least 1s and divide one hour evenly, so a chunk never crosses an hour directory boundary and its filename stays second-precision; validation rejects values such as `500ms`, `1500ms`, and `7m`.

Key layout:

```text
<root>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
```

- `<root>` is `data_dir` for disk and `<prefix>` for S3.
- Times in the path are UTC; file names use `YYYYMMDDTHHMMSSZ`.
- Example: `web-1/system/2026/10/23/16/20261023T160000Z-20261023T160500Z.jsonl.gz`.
- If a window is written twice (clock step or restart), the writer appends `-1`, `-2`, … before the extension instead of overwriting, producing `20261023T160000Z-20261023T160500Z-1.jsonl.gz`.
- Samples are sorted by timestamp, then metric, then canonical label order, so identical inputs produce identical chunk contents regardless of backend. Map keys are sorted by `encoding/json`; gzip headers carry no file name or modification time. Key derivation and encoding live in `internal/store/chunk` and are shared by both backends.

Write semantics:

- A completed chunk is immutable. On disk it is written to `<key>.tmp`, fsynced, and renamed into place. On S3 it is one conditional `PUT` (`If-None-Match: *`) to the final key; a `412` or `409` means that window already exists, so the writer retries the same body under the next `-1`, `-2`, … suffix. This matches the disk collision guard on endpoints that implement conditional writes (AWS S3 and current MinIO); a service that ignores the condition can still overwrite.
- The S3 store retries transient failures — network errors and timeouts, `408`, `429`, and `5xx` — on `PUT`, `HEAD`, `LIST`, and `GET` with exponential backoff and jitter: six attempts, a 250ms base, a 4s cap, and equal jitter, so at most roughly 7.75s of delay. Terminal errors (`400`, `401`, `403`, and `404` where absence is not tolerated) fail immediately. The chunk is encoded once and every attempt sends the same bytes. A `PUT` that times out or returns a transient error may still have landed; if a later attempt gets `412`/`409`, the writer downloads that key and compares it byte for byte with its body. Equal bytes mean the chunk is already stored; different bytes mean another writer owns the window, so the next suffix is tried. Only a conflict on a key with no ambiguous attempt, or a byte mismatch, suffixes, so an ambiguous retry cannot create a duplicate chunk. When the retry budget is exhausted, `Write` atomically spools the same encoded bytes under the chunk key and returns success; only a failed spool write is a persistent store failure (exit 3).
- The spool mirrors the relative chunk key below `spool_dir`: `<spool_dir>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>[-N].jsonl.gz`. A spool write uses a directory-local temp file, fsync, rename, and directory fsync; a restart or clock step reserves a `-N` suffix in the spool rather than overwriting an earlier pending chunk. Startup creates the spool if needed, probes it with a temp file, removes stale `.spool-*` temps, and fails with exit 3 if the directory is not writable.
- A background replayer, started by `collect` only and stopped and waited for on `SIGINT`/`SIGTERM`, uploads spool entries in lexicographic key order (chronological within a host and scope). Replay uses an exact-key conditional `PUT`: identical existing bytes are treated as success and the spool entry is deleted, while an existing object with different bytes is a permanent conflict that is logged at `error` and left for an operator. Replay never suffixes, so a successful upload whose delete failed cannot become a `-1` duplicate. Transient failures keep the file and back off from 30s up to 5m; successes are logged at `info`.
- `spool_max_bytes` bounds the total size of spooled chunks. The size index is maintained in memory; after each spool write the replayer-protected and just-written entries are skipped and the lexicographically oldest entries are removed until the total fits, with each drop logged at `error`. Eviction is best effort under concurrent replay, never waits on the network, and may leave the spool above the cap when every remaining entry is in flight or a single chunk exceeds the cap.
- `query` unions bucket and spool listings when the spool directory is readable. A relative key present in both is read from the bucket only, so an object whose spool deletion failed is emitted once. Missing spool directories are empty, a chunk that disappears between listing and opening is skipped, and the existing hour ordering and `Limit` behavior are preserved.
- A crash loses the chunk currently being assembled, at most `storage.chunk_interval`.
- `SIGINT`/`SIGTERM` closes the active chunk with its actual end time, writes it once, and exits 0. A second signal exits 1 without waiting.
- No per-chunk index is written in v0.1 or v0.2. Queries prune by host, scope, and hour, then scan the chunks in the range. A sidecar index (min/max timestamp, metric set, counts) is a v0.3 optimization if real query latency demands it; adding it later does not change the chunk format.
- Retention deletes only closed chunks. It never deletes the chunk being assembled. The disk implementation is a metadata-only `Sweep` pass: age from the chunk end first, then total size, oldest first, considering only files in the documented seven-component chunk layout. `.tmp` files, the S3 spool, and stray files are ignored. `collect` runs one pass after the store check and before the free-space warning, then starts an hourly ticker that calls the same `Sweep`; `--once` runs the startup pass, `--dry-run` runs none, and shutdown cancels and joins the ticker. A chunk ending after the sweep start is spared by the size pass so a concurrently flushed shutdown chunk is not deleted in the same instant. The startup pass is covered by the CLI integration suite; the hourly tick reuses the same function and is not independently integration-tested, because waiting an hour or adding a test-only interval would violate the no-test-only-flags rule.
- Readers tolerate a chunk disappearing between listing and opening (`ENOENT`) and check the context between chunks.

Store contract:

```go
type Chunk struct {
	Host    string
	Scope   string
	Start   time.Time
	End     time.Time
	Samples []model.Sample
}

type Store interface {
	Write(ctx context.Context, chunk Chunk) error
	Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error
	Close() error
}
```

The collector owns chunk assembly. The store never buffers across `Write` calls, which keeps crash semantics and disk/S3 parity obvious.

Exactly one store is active. `storage.primary: disk|s3` selects the write target, and `query` reads the same backend. There is no mirroring or dual-write. The S3 spool is temporary staging for failed uploads, not a second store. Moving from disk to S3 is an `aws s3 sync` followed by changing `storage.primary`.

`data_dir` is required for the disk backend and, unless `spool_dir` is set, for the S3 spool. `spool_dir` defaults to `<data_dir>/spool` and may be overridden.

Query model:

```go
type LabelOp int

const (
	LabelEqual LabelOp = iota
	LabelRegex
)

type LabelMatcher struct {
	Name  string
	Op    LabelOp
	Value string
}

type Query struct {
	Range  TimeRange
	Host   string
	Scope  string
	Metric string
	Labels []LabelMatcher
	Limit  int // 0 means unlimited
}
```

A missing label is treated as the empty string when matching, so `name=` matches
an absent label and `name~.*` matches every sample. Regex matchers use Go's
unanchored `MatchString` semantics; anchor with `^` and `$` for a full-value
match. Query results are globally ordered by timestamp, then metric, then
canonical label order, and a non-zero `Limit` stops the scan as soon as it is
reached.

`query` and `collect` share one configuration schema, so validation requires at
least one enabled source even for a query-only host. That is accepted for v0.1;
a dedicated query mode can relax it later if it causes friction.

## 4. Phase Breakdown

**Phase 0 — Foundations (Week 1)**

- Go module (`github.com/confuzeus/smob`), repository layout, license, justfile, and CI.
- CI builds `linux/amd64` and `linux/arm64` with `CGO_ENABLED=0`, runs `go vet` and staticcheck, the fast suite, a race-enabled binary, and the Alpine kernel job. The workflow triggers on the repository's actual default branch (`master`).
- Define `model.Sample` (including `IntervalMS`), `model.Query` (matchers, host/scope, limit), and `model.TimeRange`.
- Define the streaming `store.Store` interface, `store.Chunk`, and the config package.
- Config schema (YAML, strict decoding): `data_dir`, `proc_path`, `host`, `log_level`, `log_format`, `sources.{system,process,docker}` (each with `enabled` and `interval`), `storage.{chunk_interval,primary,s3}`, `retention`. `storage.primary` is the single active backend; per-backend `enabled` flags are not needed. `storage.chunk_interval` must be a whole number of seconds and divide one hour evenly, and `host` must be a single path component.
- Wire `cobra` root and stub subcommands: `collect`, `query`, `tail`, `stats`, `config`, `version`.
- `version` prints version, commit, and build date injected via `-ldflags`.
- `FROM scratch` Dockerfile with the CA bundle copied in (already present) and a non-root user.
- Pure parser signatures: `parseStat(r io.Reader) (Snapshot, error)`, `parseMeminfo(r io.Reader)`, `parseNetDev(r io.Reader)`. No global paths and no `os.ReadFile` inside a parser.

**Phase 1 — System Metrics, Disk, Query (Weeks 2–3)**

- Implement `internal/source/system` parsers for `/proc/stat`, `/proc/meminfo`, and `/proc/net/dev`, with the proc root supplied by config (`proc_path`, default `/proc`).
- Implement the system source: diff snapshots to produce CPU %, memory bytes, and per-interface RX/TX B/s. Read `boot_id` at startup; skip deltas on reset; drop state for interfaces that disappear and skip the first sample for interfaces that appear.
- Implement the scheduler: one goroutine per enabled source, its own ticker with jitter, a per-tick context timeout, and no unbounded goroutines. Sources write snapshots to the chunk assembler.
- Implement the chunk assembler: UTC-aligned windows per `(host, scope)`, sorted samples, partial flush on shutdown.
- Implement `internal/store/disk`: the layout above, `.tmp` + `rename(2)`, gzip, and a guard that refuses to overwrite an existing chunk.
- Implement `query`: streaming table output by default, plus `--format json`, `jsonl`, and `csv`. Filters: `--since`, `--until`, `--host`, `--scope`, `--metric`, `--label name=value` (exact), `--label name~regex`, and `--limit` (default 10,000; `0` disables).
- `collect` flags: `--dry-run` (validate config, `/proc` inputs, and store; write no samples), `--once` (collect one snapshot from every enabled source, write one partial chunk per scope, exit), and `--data-dir` (override `data_dir`, also honored by `query`).
- Add structured logging via `log/slog`, a `--log-level` flag, and a startup line with the effective config (redacted) and the resolved chunk key root.
- Exit codes: 0 success, 1 runtime failure, 2 usage/config error, 3 persistent store failure (including `ENOSPC`). A store failure after retries stops collection rather than silently dropping data.
- **Exit criterion:** `smob collect` writes real samples for at least an hour with no gaps; `smob query --since 1h` returns correct CPU, memory, and network figures; `collect --once` followed by `query` works in the fast test suite; a 24-hour soak shows flat memory and no missing chunks.

**Phase 2 — Process Metrics (Week 4) → v0.1 feature-complete**

- Implement `internal/source/process`: enumerate `/proc/[pid]`, parse `stat` (find the last `)` to handle `comm` containing spaces or parentheses) and `status` (`VmRSS`).
- Key rate state by `(pid, starttime)`. On PID reuse, discard the old state and skip the first delta. Treat `ENOENT` during a scan as a normal exit, not an error.
- Filter matches `comm`, which is truncated to 15 bytes by the kernel; document this and do not pretend it matches the command line. A future `cmdline` filter is a separate option.
- Top-N is by CPU over RSS, with ties broken by RSS then PID. `top_n` defaults to 20; `0` means all matching processes. This bounds the volume: at 10s and 1,000 processes the tool would write ~17M samples/day; the process source defaults to a 30s interval and top-20, which is ~115k samples/day.
- Config: `sources.process.{enabled,filter,top_n,interval,rescan_interval}`; the `/proc` root is the global `proc_path`.
- Labels: `pid`, `comm`, `scope=process`. `starttime` is internal state, not a label.
- Extend `query` label matching for `pid` and `comm` (exact and regex).
- Benchmark per-tick cost on a host with ~1,000 processes and document the
  result. Measured on 2026-10-23 on an AMD Ryzen 5 PRO 5650GE (12 threads) with
  1,001 live processes: ~37 ms per tick, from
  `go test -run '^$' -bench BenchmarkCollect -benchtime 100x ./internal/source/process`.
  At the default 30s cadence that is ~0.12% of one core, and the default top-20
  writes 115,200 samples/day (two metrics per selected process).
- **Exit criterion:** with a filter of `nginx|postgres`, `smob query` shows per-process CPU and RSS at the configured resolution, with no bogus first samples and no crashes while processes exit during a scan.

**Phase 3 — S3 Backend (Week 5) → v0.2**

- Implement `internal/store/s3` with `aws-sdk-go-v2`. Config: `endpoint`, `region`, `bucket`, `prefix`, `path_style`, `spool_dir`, and `spool_max_bytes`. Credentials come from the standard AWS chain (environment, shared config, instance role); no secrets are stored in `smob.yaml`, and `smob config` must redact anything sensitive if that ever changes.
- Custom endpoints (MinIO) default to path-style addressing; expose `path_style` for services that need it explicitly.
- Reuse the same chunk assembler and write each chunk with one `PUT` to the same key layout as disk. Deterministic JSON means disk and S3 contain equivalent samples; gzip bytes need not be identical across library versions.
- Retry with exponential backoff and jitter. When the retry budget is exhausted, write the chunk to `spool_dir` under its final key and return success; a background replayer uploads spooled chunks in key order and deletes them after a successful `PUT`. Queries merge the spool with the bucket when the spool directory is readable on the querying host, so local data is never invisible.
- Bound the spool by `spool_max_bytes` (default 1 GiB). On overflow, drop the oldest chunks and log at `error`; never block collection forever.
- CLI queries: derive hour prefixes from the time range, `LIST` with a delimiter to discover hosts and scopes when unfiltered, and `GET` concurrently with a bounded worker pool. Respect context cancellation.
- Document IAM permissions: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, and `s3:DeleteObject` only if lifecycle or tooling deletion is used.
- **Exit criterion:** collecting to MinIO and to a real S3 bucket yields the same keys and equivalent samples as the disk backend for identical input; killing the network mid-run and restoring it drains the spool without losing chunks.

**Phase 4 — Docker Container Metrics (Week 6) → v0.2**

- Implement `internal/source/docker` using the official Docker Engine client (already an approved dependency). If its transitive weight proves unacceptable, the stats endpoints are simple enough to call directly over the Unix socket; decide once, with a binary-size measurement. Measured on 2026-10-24 with `CGO_ENABLED=0 go build -trimpath -ldflags '-s -w'`: `github.com/docker/docker v28.5.2+incompatible` grows the binary from 11,808,928 to 12,034,208 bytes (+225,280, +1.9%) and the module count from 27 to 87. The newer `github.com/moby/moby/client v0.6.2` costs 442,368 bytes (+3.7%) and 46 modules; the v28 client is the smaller, stable official client, so it is kept. The client pins Engine API 1.41 (Docker 20.10, the first version with one-shot stats) and probes the daemon with a versioned `/info` call; API negotiation stays disabled so daemon upgrades cannot change response shapes.
- Default to `ContainerStatsOneShot` polling on the source interval (`sources.docker.interval`, default 10s). Persistent streams are opt-in via `sources.docker.stream_stats` for deployments that need sub-interval resolution; document that one stream per container is expensive at scale.
- Discover containers with `/containers/json` and a `rescan_interval` (default 30s); optionally subscribe to `/events` later. Reconnect with backoff after a daemon restart, and skip the first CPU sample per container because there is no previous observation. On Docker 29.9.1 with API 1.41, `ContainerStatsOneShot` leaves `precpu_stats` empty on every call, so CPU is computed from smob's own baseline rather than the daemon's previous payload.
- Compute CPU % as `(cpu_delta / system_delta) × online_cpus × 100` from smob's own consecutive observations, keeping the previous `cpu_stats.total_usage` and `system_cpu_usage` per container. The ratio and `interval_ms` cover the same monotonic window in both one-shot and stream modes, including when stream backpressure drops a payload. Read network counters per interface and diff each one. For memory, use `usage − inactive_file` (cgroup v1 `total_inactive_file`, v2 `inactive_file`) and floor at zero.
- Labels: `container`, `image`, `scope=container`. Container memory is collected, not just CPU and network.
- The Docker source is off by default. When `sources.docker.enabled: true` and the socket is missing or unreachable, fail startup with a clear error; an operator who does not want the source disables it in config.
- Add a configurable `proc_path` so host-level system and process metrics work inside a container. Document the mounts and flags:
  - `/proc:/host/proc:ro` with `proc_path: /host/proc` for host system metrics.
  - `--pid=host` for host process visibility.
  - `--network=host` if host network interfaces are desired; otherwise system network metrics describe the container namespace.
- Document the Docker socket permission problem: `/var/run/docker.sock` is usually `root:docker` with mode `0660`, so the image's non-root UID cannot read it without `--group-add` (and a matching GID) or running as root. The data directory must also be writable by the container UID.
- **Exit criterion:** inside a container, `smob collect` records per-container CPU, memory, and network; `smob query --label container=web` returns plausible values; removing and recreating a container produces a new series, not a corrupted rate.

Stats acquisition lives in `internal/source/docker/stats.go`. One-shot mode requests one `ContainerStatsOneShot` sample per container per collector tick, with at most eight requests in flight; per-container failures are logged and omitted rather than failing the tick. Stream mode is opt-in through `sources.docker.stream_stats`: it opens one stats stream per container and fans decoded payloads into a bounded channel (one pending payload per container; new payloads are dropped when the consumer lags), and a reader stops when its stream ends or errors. The supervisor in `internal/source/docker/supervisor.go` owns discovery and reconnect: its constructor connects, probes the daemon, and lists running containers; `Run` rescans at `sources.docker.rescan_interval`, starts and stops streams as containers appear and disappear without touching unchanged streams, and reopens a stream that ends with exponential backoff and equal jitter (500ms base, 30s cap, reset after a payload). A failed discovery keeps the previous container set and is retried on the next rescan. `sources.docker.exclude` is an unanchored regular expression matched against the container name without its leading slash; IDs are not matched because a short pattern would accidentally match hex IDs. Stopped containers are not discovered because they have no live stats. Both modes return raw payloads only.

Metric computation lives in `internal/source/docker/metrics.go`. A long-lived accumulator keyed by container ID keeps the previous `cpu_stats` counters, the observation time, and per-interface RX/TX counters. `observe` uses the local monotonic interval since the previous consumed payload for both the CPU ratio and network rates, so `interval_ms` and the CPU counter delta always describe the same window, including when stream backpressure drops a payload; `StatsResponse.Read` is never used. The first observation, a window shorter than a millisecond, non-advancing system CPU time, or a counter decrease produces no CPU sample, and a decrease advances the baseline. Network deltas are per interface and direction: a decrease drops only that direction, logs once per `(container, interface, direction)` at `warn` until a valid delta resumes, and removed interfaces are forgotten so a reappearing interface starts fresh. Memory is `usage − inactive_file` (`total_inactive_file` first for cgroup v1, then `inactive_file` for v2), floored at zero; when neither key exists, usage is reported unchanged and may include reclaimable page cache. `prune` drops state and warning flags for containers no longer discovered, and a re-created container has a new ID and therefore new state.

Sample construction lives in `internal/source/docker/source.go`. `New` owns the supervisor (and therefore the client) and one accumulator for the source lifetime. It emits only `container` and `image` series labels, adding `interface` to network samples; the collector adds `host` and `scope`. One-shot `Collect` polls every discovered container, skips containers that fail or disappear, and prunes state using the discovered ID set rather than the successful fetch subset. With `stream_stats`, `Start` runs the supervisor and a bridge goroutine that timestamps each payload on receipt, observes it, buffers rate samples in a bounded drop-oldest FIFO (16,384 samples), and coalesces memory to one gauge per container per tick; `Collect` drains that buffer without waiting on the stream, and unknown container IDs are dropped rather than labelled with a guess. `Close` cancels, joins the supervisor and bridge goroutines, then releases the client. The source is registered with the collector and disabled by default; `collect` constructs and probes it before the store check, starts it before collection, and closes it after collection on every path. `collect --once` forces one-shot stats and logs the downgrade when `stream_stats` is configured.

**Phase 5 — Polish and Release (Week 7) → v0.2.0**

- Disk retention: a startup + hourly sweeper (not a write-path scan) deletes closed chunks by age (`max_age`) or total size (`max_bytes`). It skips the active chunk, tolerates `ENOENT`, logs every deletion at `info`, and never fails collection. A preflight free-space warning is logged at startup.
- `smob stats`: count, min, max, mean per metric over a range, using the same range defaults and filters as `query` and the same streaming path.
- `query --agg avg|min|max|sum --group-by metric,label...`: rate metrics are weighted by `interval_ms`; gauges are treated as instantaneous samples. `sum` is the arithmetic total of sample values in the group over the scanned range: it is not interval-weighted, not a time integral, not CPU-seconds, and not a per-timestamp cross-series total. For CPU percentages it sums across series and also accumulates each scanned sample over time, so a range-wide sum is count-sensitive.
- Documentation: quickstart, full config reference, systemd unit, compose file, IAM policy, and a disk → S3 migration note. Migration is `aws s3 sync <data_dir>/ s3://<bucket>/<prefix>/` followed by a config change — not "change the config and lose the old data."
- Add `--version` output and `--help` snapshot tests for each subcommand.
- Tag `v0.1.0` after Phase 2 and `v0.2.0` after this phase; publish binaries for amd64/arm64 and the Docker image.
- Post-release: collect feedback and prioritize v0.3 (live tail, chunk index, multi-host query).

## 5. Deferred to v0.3

- **Live tail.** `smob tail` is not a store-polling command in v0.2: with five-minute chunks, a poller would be up to five minutes behind, which is not a tail. A real tail needs a live spool or a local IPC endpoint from the collector, and that design is deferred rather than guessed. The subcommand stays stubbed until then.
- **Chunk index/compaction.** Add a sidecar index (metric set, min/max timestamp, counts) only if measured query latency requires it. Compaction of small chunks is likewise deferred.
- **Multi-host aggregation and remote query.** The `host` label and path make shared buckets safe. Queries can filter or merge hosts, but v0.2 has no cross-host series aggregation and no server.

## 6. Non-Goals (v0.2)

- No alerting, dashboards, or Prometheus exposition.
- No Windows or macOS support — Linux only.
- No multi-host aggregation, remote query API, distributed collection, or sharding.
- No live tail in v0.2; see Deferred.
- No chunk index, compaction, or downsampling in v0.2.
- No support for multiple concurrent collectors writing the same host and prefix.

## 7. Risks and Mitigations

| Risk                                                                         | Mitigation                                                                                                                                                           |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/proc` parsing breaks on unusual kernels or fields containing spaces/parens | Pure parsers with table-driven fixture tests; Alpine and Debian integration jobs; `comm` parsed from the last `)`; fail loud at startup on unreadable required files |
| S3 cost from many small objects                                              | Shared 5-minute chunks (288 objects/day per host and scope); document object counts and bytes/day from the soak test; lifecycle policy for retention                 |
| Per-process data volume explodes                                             | 30s default process interval, top-N default 20, and a documented sample/day budget; `top_n: 0` is opt-in                                                             |
| Queries exhaust memory on long ranges                                        | Streaming `Store.Query`, `--limit`, JSONL/CSV output, hour-prefix pruning; no whole-corpus slices                                                                    |
| Clock steps produce bogus rates or overlapping chunks                        | Rates use the monotonic clock and record `interval_ms`; `boot_id` detects reboot; a backward wall-clock step > chunk interval starts a new chunk and logs a warning  |
| Disk fills up                                                                | Hourly retention sweeper, startup free-space warning, exit code 3 on `ENOSPC`, systemd restart policy                                                                |
| S3 outage grows the spool without bound                                      | `spool_max_bytes` cap with oldest-first drop and `error` logs; replay in key order; queries merge the spool locally                                                  |
| Docker stats stream is heavy at scale                                        | One-shot polling is the default; streams are opt-in; container rescan rather than a goroutine per container                                                          |
| Docker socket is root-equivalent and unreadable by the image's UID           | Document `--group-add`, socket mode, data-dir ownership, and the security trade-off; never mount the socket unless the container source is enabled                   |
| Host system metrics inside a container describe the wrong namespace          | Configurable `proc_path`; documented `/host/proc`, `--pid=host`, and `--network=host` mounts                                                                         |
| Shared buckets interleave hosts                                              | `host` label and `<host>` path component; one writer per host/prefix is explicit                                                                                     |
| Schedule slips                                                               | v0.1 ships after Phase 2 with system, process, disk, and query; S3, Docker, retention, and docs are v0.2; tail is already out of scope                               |

## 8. Testing Strategy

Integration tests are the public contract. Every behavior a user can observe is exercised by building the `smob` binary and driving it as a subprocess against real `/proc`, a temporary data directory, and optional MinIO or Docker services. The default `go test ./...` stays fast; the tagged integration suite runs explicitly.

A narrow exception is allowed for pure functions whose edge cases cannot be reached through the CLI: `/proc` parsers over byte fixtures, rate/reset math, and chunk key derivation. Those tests live beside the code, use table-driven inputs (`io.Reader` and snapshot values), and never mock a service, clock, filesystem, or store. They are a complement to the contract, not a replacement. Store parity is the second exception: `internal/store/equivalence_integration_test.go` feeds one fixed chunk through the real disk and S3 stores because the CLI cannot reproduce identical `/proc` input; it runs in the extended suite and compares decoded samples, not compressed bytes. The Docker client and supervisor suite is the third: `internal/source/docker/*_integration_test.go` connects to and drives the real daemon beside the package, while `test/integration/docker_test.go` covers the CLI contract for an enabled source. No test-only production flags or exported internals.

- **Fast suite** (every change): config validation, `collect --once` → `query`, output formats, help/version snapshots, signal handling, `--dry-run`, and error paths that need no external service. Net rate tests generate loopback traffic rather than assuming a busy interface.
- **Extended suite** (CI and `just test-all`): process scope with a CPU burner,
  MinIO for S3 including exact disk/S3 chunk equivalence for one fixed chunk,
  the Docker socket for container scope, retention, spool replay, `stats`, and
  aggregation. Services are configured through environment variables
  (`SMOB_TEST_S3_ENDPOINT`, `SMOB_TEST_S3_BUCKET`, `SMOB_TEST_DOCKER_SOCKET`, …).
  Tests skip with a clear reason when a service is absent; `SMOB_TEST_STRICT=1`
  turns every skip into a failure. CI's `extended` job sets both the MinIO and
  Docker configuration with strict mode, so a missing MinIO or Docker daemon
  fails the job instead of skipping to a pass.
- **Assertions** cover schema, labels, ordering, chunk boundaries, and plausible ranges — not exact live values. Use wide tolerances for rates and never require nonzero traffic on an idle runner.
- **Golden files** cover stable output: help text, config dumps, and parser output from fixtures. Live `/proc` samples are validated structurally, not compared byte-for-byte.
- **Race detector** applies to the `smob` binary under test (`SMOB_TEST_RACE=1`), not only to the Go test process. The race binary requires cgo/gcc and is a CI artifact only; release binaries remain `CGO_ENABLED=0`.
- **CI**: lint, fast suite, race suite, and cross-builds on linux/amd64 and linux/arm64. The Alpine job runs the fast suite on a musl kernel. The `extended` job starts pinned MinIO, asserts the runner's Docker daemon and pre-pulls the `busybox:1.36` fixture image, then runs the whole extended suite with `SMOB_TEST_STRICT=1`, `SMOB_TEST_S3_*`, and `SMOB_TEST_DOCKER_SOCKET` set. It covers the S3 suite including the network-partition spool drain and disk/S3 equivalence, plus the Docker CLI and package suites. A missing or unreachable MinIO or Docker service fails the job rather than skipping; failure and cancellation dump container logs and remove leftover `smob-*` containers. The fast and race jobs set the same Docker socket but stay non-strict, so Docker-gated tests run when the runner daemon is reachable and skip with a reason when it is not. Arm64 is build-only; integration runs on amd64.

## 9. Repository Layout

```text
cmd/smob/main.go               # cobra root, subcommand wiring
internal/model/                # Sample, Query, LabelMatcher, TimeRange
internal/config/               # config parsing + validation
internal/collector/            # scheduler, chunk assembler, source registry
internal/source/system/        # /proc/stat, meminfo, net/dev
internal/source/process/       # /proc/[pid]/...
internal/source/docker/        # Engine client, discovery supervisor, stats acquisition
internal/store/                # Store interface, Chunk, factory
internal/store/chunk/          # shared chunk keys and deterministic JSONL-gzip encoding
internal/store/disk/           # local fs implementation
internal/store/s3/             # S3 implementation + spool replay
internal/query/                # filtering, aggregation, formatting
internal/cli/                  # shared CLI helpers (output, exit codes)
test/integration/              # black-box CLI tests
test/integration/testdata/     # fixtures and golden files
docs/IMPLEMENTATION.md         # design, storage internals, phases, TODO
docs/quickstart.md             # operator quickstart
docs/configuration.md          # canonical YAML field reference
docs/s3.md, docs/docker.md     # S3/MinIO and container deployment guides
contrib/systemd/smob.service   # systemd unit
compose.yaml                   # Docker Compose deployment
Dockerfile                     # FROM scratch, static binary, CA bundle
```

---

## TODO List

### Phase 0 — Foundations

- [x] `go mod init github.com/confuzeus/smob`; choose license.
- [x] Create repository layout per the plan.
- [x] Add justfile recipes: `build`, `test-unit`, `test`, `test-all`, `test-race`, `test-update`, `vet`, `docker`, `release`.
- [x] Set up CI: build amd64/arm64 with `CGO_ENABLED=0`; run vet, staticcheck, integration tests, and a race binary.
- [x] Write a `FROM scratch` Dockerfile with the CA bundle and a non-root user.
- [x] Implement config loading with strict unknown-field rejection and `smob config --validate`.
- [x] Wire `cobra` root and stub subcommands.
- [x] `version` subcommand with `-ldflags` metadata.
- [x] Fix the CI trigger to the default branch (`master`).
- [x] Extend `model.Sample` with `IntervalMS`; add `LabelMatcher`, `Query.Host/Scope/Limit`, and the streaming `Store`/`Chunk` contract.
- [x] Add config: `host`, `proc_path`, `storage.chunk_interval` (validated to be whole seconds that divide one hour, replacing `storage.s3.chunk_interval`), per-source `interval`, S3 `path_style`/`spool_dir`/`spool_max_bytes`; remove `flush_interval` and the per-backend `enabled` flags (`storage.primary` selects the backend).
- [x] Add `collect --dry-run`, `collect --once`, and `--data-dir` for `collect`/`query`.
- [x] Define exit code 3 for persistent store failure in `internal/cli`.
- [x] Update the integration-testing guidance to permit pure-function table tests (and add a `test-unit` recipe).

### Phase 1 — System Metrics, Disk, Query (v0.1)

- [x] Implement `/proc/stat`, `/proc/meminfo`, and `/proc/net/dev` parsers as pure `io.Reader` functions, with fixture-based table tests for edge cases.
- [x] Implement the system source: CPU %, memory bytes, per-interface RX/TX B/s; `boot_id` reset detection; interface add/remove handling.
- [x] Implement the scheduler: per-source ticker with jitter, per-tick timeout, clean shutdown.
- [x] Implement the chunk assembler: UTC-aligned 5-minute windows, per `(host, scope)`, sorted samples, partial flush.
- [x] Implement the disk store: `<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz`, `.tmp` + rename, collision guard.
- [x] Implement streaming `query` with table, JSON, JSONL, and CSV output; `--since/--until/--host/--scope/--metric/--label/--limit`.
- [x] Implement `collect --dry-run` and `collect --once` behavior; the flags and `--data-dir` override are already wired in Phase 0, and structured logging is in place.
- [x] Add exit code 3 on persistent store failure and a startup free-space warning.
- [x] Golden tests for help text and config output; structural tests for chunk keys, ordering, and schema.
- [x] Fast suite: `collect --once`, query, formats, signals, dry-run, and error paths against real `/proc`.
- [ ] Manual soak: 24h collection, verify no gaps, flat memory, and realistic chunk counts.

  **Why this is unchecked:** it is a wall-clock acceptance check, not code. It
  needs a real host running `smob collect` for 24 hours against a durable
  `data_dir`, then a human comparing chunk counts, contiguous windows, and
  resident memory. An agent session or CI job cannot hold a process for 24 hours
  or vouch for the resulting data, so the item stays open until an operator runs
  the soak and records the outcome here. It does not block Phase 3. The
  automated substitutes already in place are `collect --once` → `query` in the
  fast suite and the signal-handling tests.

### Phase 2 — Process Metrics (v0.1)

- [x] Implement `/proc/[pid]/stat` (fields 14, 15, 22) and `status` (`VmRSS`) parsers, with fixtures for `comm` containing spaces and parentheses.
- [x] Implement PID enumeration and rescan cadence; treat `ENOENT` as normal.
- [x] Implement process source: regex filter on `comm`, top-N by CPU/RSS/PID, 30s default interval.
- [x] Key rate state by `(pid, starttime)`; reset on PID reuse.
- [x] Emit labels `pid`, `comm`, `scope=process`.
- [x] Extend `query` label matching for `pid` and `comm` (exact and regex).
- [x] Add process config fields and validation.
- [x] Benchmark per-tick cost with ~1,000 processes and document sample/day volume.
- [x] Integration test with a CPU burner; assert plausible process CPU and RSS.
- [x] Tag `v0.1.0`.

### Phase 3 — S3 Backend (v0.2)

- [x] Add `aws-sdk-go-v2`; config endpoint, region, bucket, prefix, `path_style`, `spool_dir`, `spool_max_bytes`; standard credential chain.
- [x] Implement the S3 store with the same chunk keys and a single `PUT` per chunk.
- [x] Implement retry with exponential backoff and jitter.
- [x] Implement the local spool and background replay; cap the spool; merge spooled chunks in queries.
- [x] Implement `LIST` prefix pruning and concurrent `GET` with a bounded worker pool.
- [x] Verify disk/S3 equivalent samples for identical input, not byte-identical gzip.
- [x] Add MinIO to CI; test network interruption and spool drain.
- [x] Document IAM permissions and MinIO path-style configuration.

### Phase 4 — Docker Container Metrics (v0.2)

- [x] Add the Docker Engine client and connect over `docker.socket`.
- [x] Implement one-shot stats polling by default; opt-in stream fan-in.
- [x] Implement container discovery/rescan and reconnect with backoff.
- [x] Compute CPU % by diffing smob's own consecutive observations, memory used excluding `inactive_file`, and per-interface network rates.
- [x] Emit labels `container`, `image`, `scope=container`.
- [x] Keep the source off by default; fail startup when it is enabled and the socket is unreachable.
- [x] Audit the existing `sources.docker.interval`, `sources.docker.exclude`, and `sources.docker.stream_stats`; confirm `sources.docker.rescan_interval` (default 30s).
- [x] Add `proc_path`, `/host/proc`, `--pid=host`, and `--network=host` documentation.
- [x] Document socket permissions, `--group-add`, and data-dir ownership in the container deployment.
- [x] Integration test with Docker; create, remove, and recreate a container and assert series behavior.

### Phase 5 — Polish and Release (v0.2.0)

- [x] Implement the disk retention sweeper (age and size), closed chunks only, tolerant of reader races.
- [x] Implement `smob stats`.
- [x] Add `--agg` and `--group-by`, weighting rates by `interval_ms`.
- [x] Write the quickstart, config reference, systemd unit, compose file, and IAM policy.
- [x] Write the disk → S3 migration note using `aws s3 sync`.
- [x] Add `--version` and `--help` snapshot tests.
- [x] Run the extended suite in CI with MinIO and Docker; keep strict mode.
- [ ] Tag `v0.2.0`; publish binaries and the Docker image.

  **Blocked on a remote and publication credentials.** The annotated `v0.2.0`
  tag exists locally on the commit that contains this note. The ignored `dist/`
  directory holds `smob-linux-amd64` and `smob-linux-arm64`, and the local
  `smob:v0.2.0` image was built and verified. This checkout has no `origin`
  remote, no `gh`, and no registry login, so no tag, asset, or image has been
  pushed or published. To finish, a maintainer must run:

  ```sh
  git remote add origin <repository-url>
  git push origin master
  git push origin v0.2.0
  gh release create v0.2.0 dist/smob-linux-amd64 dist/smob-linux-arm64 \
    --title "smob v0.2.0" --notes-file docs/releases/v0.2.0.md
  docker tag smob:v0.2.0 <registry>/smob:v0.2.0
  docker push <registry>/smob:v0.2.0
  ```

  Rebuild the ignored artifacts with `VERSION=v0.2.0 just release` and the
  image with `VERSION=v0.2.0 just docker` if they are no longer present. Mark
  this item complete only after the push, release, and image publication
  commands succeed. Release notes are in
  [`docs/releases/v0.2.0.md`](releases/v0.2.0.md).

- [ ] Post-release: collect feedback and scope v0.3 (live tail, chunk index, multi-host query).

### Cross-Cutting (run throughout)

- [ ] Every user-visible behavior has an integration test; pure-function tests exist only where the CLI cannot reach the edge case.
- [ ] Every config field is validated with a clear, field-named error.
- [ ] Every external failure mode (disk full, S3 auth failure, missing socket, unreadable `/proc`, spool overflow) is exercised by a test or explicitly documented.
- [ ] `query`, `stats`, and future output commands support the documented formats; `collect` has no `--format`.
- [ ] The release binary stays `CGO_ENABLED=0`; the race binary is CI-only.
- [ ] Audit dependencies at each phase; keep the Docker client only if its size is justified.
