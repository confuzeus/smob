# smob — Simple Monitoring Observability Tool

**smob** is a lightweight observability tool delivered as a single static Go
binary. It records CPU, memory, and network usage over time on a Linux host and
stores the data on local disk or in any S3-compatible object store. It supports
three collection scopes: the whole system, individual processes, and Docker
containers. Everything is driven from one CLI: `smob collect` runs the
collector, and the remaining commands query historical data.

The design favors simplicity and correctness over feature breadth. Metrics come
from Linux `/proc` and the Docker Engine API rather than heavy instrumentation
libraries. Disk and S3 use one time-aligned chunk format, so the backends are
interchangeable and existing data can be copied between them with
`aws s3 sync`.

> **Project status:** System, process, and Docker container metrics, the disk
> and S3 backends (including local spool replay), `collect`, and `query` are
> implemented. The Docker source is disabled by default and is enabled with
> `sources.docker.enabled: true`. S3 IAM and MinIO deployment guidance is in
> [docs/s3.md](docs/s3.md). Retention, aggregation, `stats`, and `tail` remain
> planned. The command and metric reference below describes the current
> contract. See the [design and implementation plan](docs/IMPLEMENTATION.md) for
> the roadmap.

## Features

- **One static binary.** Linux amd64 and arm64, built with `CGO_ENABLED=0`, with
  no runtime dependencies.
- **Three collection scopes.** System-wide CPU, memory, and network; per-process
  CPU and RSS with filtering and top-N selection; per-container CPU, memory, and
  network through the Docker Engine API.
- **Disk or S3.** Write locally or to AWS S3, MinIO, or any S3-compatible
  endpoint. Both backends use the same key layout and sample format.
- **Efficient storage.** Samples are grouped into immutable, UTC-aligned chunks
  (five minutes by default) and compressed with gzip. A crash loses at most the
  chunk being assembled; `SIGINT`/`SIGTERM` flushes it before exit.
- **Streaming queries.** Query by time range, host, scope, metric, and labels;
  emit a table or stream JSON, JSONL, and CSV.
- **Explicit units.** Metric names encode their unit, and rates carry the
  measurement window used to compute them.

## Requirements

- Linux (amd64 or arm64).
- Go 1.27 or newer to build from source.
- Optional: a reachable Docker socket to collect container metrics.
- Optional: an S3-compatible bucket for remote storage.

## Install

Build the static binary with the provided justfile:

```sh
just build
./smob version
```

Without `just`, the equivalent command is:

```sh
CGO_ENABLED=0 go build -trimpath -o smob ./cmd/smob
```

Cross-compiled release binaries can be produced with `just release`. A minimal
container image (`FROM scratch`, non-root user, CA bundle) can be built with
`just docker`.

## Quick start

Create a minimal configuration file, for example `smob.yaml`:

```yaml
data_dir: ./data
log_level: info
sources:
  system:
    enabled: true
storage:
  primary: disk
```

Validate it and inspect the effective configuration:

```sh
./smob config --config ./smob.yaml --validate
./smob config --config ./smob.yaml
```

The workflow is:

```sh
# Run the collector in the foreground.
./smob collect --config ./smob.yaml

# In another shell, query the last hour of samples.
./smob query --config ./smob.yaml --since 1h

# Query a specific metric and label.
./smob query --config ./smob.yaml --since 24h \
  --metric process.cpu.percent --label 'comm~nginx|postgres'
```

By default the configuration is read from `/etc/smob/smob.yaml`; use
`--config` to point at another file. Query commands read the same backend the
collector writes to.

## Metrics reference

Every sample carries a `host` label and a `scope` label (`system`, `process`, or
`container`), plus the series labels listed below. Metric names and units never
change meaning between versions.

| Metric | Type | Unit | Labels | Notes |
| --- | --- | --- | --- | --- |
| `system.cpu.percent` | rate | percent | `cpu=all` or `cpu=N` | 0–100 per logical CPU; `all` is the aggregate |
| `system.memory.total_bytes` | gauge | bytes | — | `MemTotal` |
| `system.memory.available_bytes` | gauge | bytes | — | `MemAvailable` |
| `system.memory.used_bytes` | gauge | bytes | — | `total − available` |
| `system.network.rx_bytes_per_second` | rate | B/s | `interface` | Per interface |
| `system.network.tx_bytes_per_second` | rate | B/s | `interface` | Per interface |
| `process.cpu.percent` | rate | percent | `pid`, `comm` | 0–100 per core; two busy cores report 200 |
| `process.memory.rss_bytes` | gauge | bytes | `pid`, `comm` | `VmRSS` |
| `container.cpu.percent` | rate | percent | `container`, `image` | 0–100 per core, from consecutive observations |
| `container.memory.used_bytes` | gauge | bytes | `container`, `image` | `usage − inactive_file`, floored at zero |
| `container.network.rx_bytes_per_second` | rate | B/s | `container`, `image`, `interface` | Per interface |
| `container.network.tx_bytes_per_second` | rate | B/s | `container`, `image`, `interface` | Per interface |

### Reading the metrics

- Timestamps are UTC. Gauge samples are instantaneous; rate samples record the
  measurement window in `interval_ms`.
- CPU percentages are per core. `100` means one core fully busy, so a
  multi-threaded process or container can exceed `100`. System CPU also reports
  an aggregate series with `cpu=all` alongside per-logical-CPU series.
- System CPU busy time is `total − (idle + iowait)`, following `/proc/stat`
  semantics. The aggregate `cpu=all` series comes from the kernel's aggregate
  line, not from summing individual CPUs.
- A counter reset — for example after a reboot or when an interface is
  recreated — drops that tick rather than reporting a bogus rate. A short gap in
  the series is expected.
- Container CPU is the difference between smob's own consecutive observations,
  so the first payload per container has no CPU sample. The CPU ratio and
  `interval_ms` cover the same measurement window in both one-shot and stream
  modes.
- Container memory subtracts `inactive_file` from `usage` and floors at zero. If
  the daemon reports neither `inactive_file` nor cgroup v1's
  `total_inactive_file`, smob reports `usage` unchanged; that estimate may
  include reclaimable page cache.
- Process labels use `comm`, which the kernel truncates to 15 bytes. Matching
  the full command line is not supported.
- Process series are selected each tick: the top `N` by CPU, then RSS, then PID.
  `top_n` defaults to 20; `0` records every matching process.

## CLI reference

Global flags:

| Flag | Description |
| --- | --- |
| `--config PATH` | Configuration file. Defaults to `/etc/smob/smob.yaml`. |
| `--log-level LEVEL` | Override the configured level: `debug`, `info`, `warn`, `error`. |
| `--log-format FORMAT` | Override the configured format: `text` or `json`. |

Commands:

| Command | Purpose |
| --- | --- |
| `smob collect` | Run the collector and write samples to the configured backend. |
| `smob query` | Query recorded samples. |
| `smob stats` | Summarize matching samples (planned for v0.2). |
| `smob tail` | Live tail (deferred; currently a stub). |
| `smob config` | Print the effective configuration; `--validate` checks it and exits. |
| `smob version` | Print version, commit, and build date. |

### `collect`

- `--dry-run` validates configuration, source inputs, and store access without
  writing samples.
- `--once` collects one snapshot from every enabled source, writes one partial
  chunk per scope, and exits. This is intended for testing and automation.
- `--data-dir DIR` overrides `data_dir` from the configuration.

The collector runs until interrupted. A store failure stops collection rather
than silently dropping data.

### Docker container metrics

Container collection is disabled by default. Enable it when the daemon is
available:

```yaml
sources:
  docker:
    enabled: true
    socket: /var/run/docker.sock
```

`collect` connects to and probes the socket at startup. If the source is
enabled and the socket is missing or unreachable, collection fails with exit
code 1 before any sample is written. A host without Docker keeps the source
disabled (the default) and is unaffected.

`sources.docker.stream_stats: true` keeps one persistent stats stream per
container for sub-interval resolution. `collect --once` always takes one-shot
stats because a single snapshot has no stream history; when `stream_stats` is
enabled, `--once` logs that it used one-shot stats for that run.

### `query`

Query defaults to the last hour when `--since` is omitted. `--until` defaults to
now; `--until` without `--since` means the hour before `--until`. A duration
given to `--since` is measured back from `--until`.

| Flag | Description |
| --- | --- |
| `--since TIME` | Duration (`1h`, `24h`) or an RFC3339 timestamp. |
| `--until TIME` | RFC3339 timestamp. Defaults to now. |
| `--host NAME` | Restrict to one host. Omitting it merges hosts. |
| `--scope SCOPE` | Restrict to `system`, `process`, or `container`. |
| `--metric NAME` | Exact metric name. |
| `--label MATCH` | Repeatable and ANDed. `name=value` is exact; `name~regex` is a regular expression. Quote shell metacharacters, for example `--label 'comm~nginx|postgres'`. |
| `--limit N` | Maximum samples returned. Defaults to 10,000; `0` means unlimited. |
| `--format FORMAT` | `table` (default), `json`, `jsonl`, or `csv`. |
| `--data-dir DIR` | Override `data_dir` from the configuration. |

Output is UTC everywhere. Table columns are `TIME`, `METRIC`, `VALUE`, `UNIT`,
and `LABELS`; units come from the metric suffix. JSON is one array; JSONL
streams one object per line for large ranges; CSV has the columns `timestamp`,
`metric`, `value`, `interval_ms`, and `labels`.
Results are sorted by timestamp, then metric, then canonical labels. `--limit`
is applied after filtering and sorting.

Data goes to stdout; status and errors go to stderr.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Runtime failure |
| `2` | Usage or configuration error |
| `3` | Persistent store failure (including `ENOSPC`) |

## Storage

Set `storage.primary` to `disk` or `s3`. Exactly one backend is active; there is
no mirroring or dual-write.

- **Disk** writes under `data_dir` and is the default. Retention by age
  (`retention.disk.max_age`) or total size (`retention.disk.max_bytes`) is
  planned for v0.2.
- **S3** writes to an AWS or S3-compatible bucket using the standard AWS
  credential chain (environment, shared config, or instance role). Credentials
  are never stored in `smob.yaml`. Transient failures are retried with
  exponential backoff and jitter. A chunk that still cannot be uploaded is
  written atomically to a local spool (`storage.s3.spool_dir`, default
  `<data_dir>/spool`), and a background replayer uploads it in key order. Replay
  uses the exact chunk key: an object already present with identical bytes is
  treated as stored and the spool copy is deleted, so a failed delete cannot
  create a duplicate. Only a failed spool write is a persistent store failure
  (exit code 3). The spool is capped by `storage.s3.spool_max_bytes`; when it
  stays full, the oldest chunks are dropped with an `error` log. `query` merges
  readable spool entries with the bucket, so chunks not yet uploaded are still
  visible on the collecting host. See [docs/s3.md](docs/s3.md) for the IAM
  policy, credential chain, and a local MinIO recipe.

Both backends use the same keys and sample format, so migrating from disk to S3
is a copy followed by a configuration change:

```sh
aws s3 sync ./data/ s3://<bucket>/<prefix>/
# then set storage.primary: s3
```

Completed chunks are immutable. A collector crash loses at most the chunk
currently being assembled, and `SIGINT`/`SIGTERM` flushes that chunk before
exit. Concurrent collectors must not write the same host and prefix. Multiple
hosts may share a bucket; every sample is labelled with its host.

## Development

The [design and implementation plan](docs/IMPLEMENTATION.md) covers architecture,
the storage layout, delivery phases, testing strategy, and the TODO list.

```sh
just test       # fast suite
just test-all   # extended suite; requires MinIO and Docker for full coverage
just lint
just release
```

## License

MIT. See [LICENSE](LICENSE).
