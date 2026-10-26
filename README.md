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
> and S3 backends (including local spool replay), `collect`, `query`, `stats`,
> disk retention, and aggregation are implemented. The Docker source is
> disabled by default and is enabled with `sources.docker.enabled: true`.
> Operator documentation — the [quickstart](docs/quickstart.md),
> [recipes](docs/recipes.md), [configuration reference](docs/configuration.md),
> [S3/MinIO guide](docs/s3.md), [container deployment guide](docs/docker.md),
> systemd unit, and Compose file — is implemented alongside the code. `tail`
> remains a stub; live tail, a chunk index, and multi-host aggregation are
> unscheduled v0.3 candidates. The command and metric reference below describes
> the current contract. See the
> [design and implementation plan](docs/IMPLEMENTATION.md) for architecture,
> phases, and the TODO list.

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
- **Bounded disk usage.** A startup and hourly sweep deletes closed disk chunks
  by age or total size, oldest first, without touching the chunk being
  assembled or any non-chunk file.
- **Streaming queries and summaries.** Query samples, aggregate them by metric
  and label, or summarize count, min, max, and mean per metric over a time
  range, with the same filters and output formats.
- **Explicit units.** Metric names encode their unit, and rates carry the
  measurement window used to compute them.

## Requirements

- Linux (amd64 or arm64).
- Go 1.27 or newer to build from source.
- Optional: a reachable Docker socket to collect container metrics.
- Optional: an S3-compatible bucket for remote storage.

## Install

### Install script

The installer verifies the release checksum, installs the static binary to
`/usr/local/bin/smob`, writes a minimal `/etc/smob/smob.yaml` only when that
file is absent, and installs and starts the hardened systemd unit:

```sh
curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh
```

It prompts for nothing, because standard input carries the script. Pin a
release with `--version vX.Y.Z` (or `SMOB_VERSION`), or install the binary only
with `--no-service`. `--install-dir DIR` selects another prefix and requires
`--no-service`, since the unit runs `/usr/local/bin/smob`. Without those flags,
a non-root run fails and prints the `sudo` form. The script is a release asset,
so the installer and the artifacts it downloads always come from the same tag.
Download and read it first if you prefer:

```sh
curl -fsSLO https://github.com/confuzeus/smob/releases/latest/download/install.sh
less install.sh
sudo sh install.sh
```

Re-running the installer upgrades the binary and restarts the service, and it
never overwrites an existing `/etc/smob/smob.yaml`. The generated configuration
uses `data_dir: /var/lib/smob`, the system source, and the disk backend; the
manual install below remains the fallback and the place to start when you need a
different layout.

### Download a release

Each annotated `vX.Y.Z` tag publishes a GitHub release. The
[releases page](https://github.com/confuzeus/smob/releases) lists them newest
first, and each release carries `smob-linux-amd64`, `smob-linux-arm64`,
`SHA256SUMS`, `install.sh`, `smob.service`, and `LICENSE`. Download the binary
for your architecture and verify it in the directory that holds the binary and
the checksum file:

```sh
sha256sum -c SHA256SUMS
```

The checksum file lists both architectures, so a run with only one binary
downloaded reports the missing one as failed; add `--ignore-missing` in that
case.

The container image is published for linux/amd64 and linux/arm64 as
`ghcr.io/confuzeus/smob:vX.Y.Z` and `ghcr.io/confuzeus/smob:latest`, and runs as
UID 65532. The mounts and permissions it needs are in
[docs/docker.md](docs/docker.md).

### Build from source

Build the static binary with the provided justfile:

```sh
just build
./smob version
```

Without `just`, the equivalent command is:

```sh
CGO_ENABLED=0 go build -trimpath -o smob ./cmd/smob
```

`just build`, `just release`, and `just docker` inject version metadata from the
`VERSION`, `COMMIT`, and `DATE` environment variables. Their defaults are
`git describe`, the short commit hash, and the current UTC timestamp. A plain
`go build` without `-ldflags` injects nothing, so `smob version` reports
`dev` (commit `none`, built `unknown`).

Cross-compiled release binaries can be produced with `just release`. A minimal
container image (`FROM scratch`, non-root user, CA bundle) can be built with
`just docker`.

## Documentation

| Guide | Contents |
| --- | --- |
| [Quickstart](docs/quickstart.md) | Build, minimal config, first `collect --once`/`query`, daemon, service. |
| [Recipes](docs/recipes.md) | Copy-paste setups for systemd, Docker Compose, AWS S3, MinIO, container metrics, and query-only hosts. |
| [Configuration reference](docs/configuration.md) | Every field with type, default, validation rule, and effect. |
| [S3 and MinIO](docs/s3.md) | IAM policy, credential chain, MinIO recipe, spool behavior. |
| [Disk → S3 migration](docs/migration.md) | Quiesce the collector, copy with `aws s3 sync`, verify samples, switch, roll back. |
| [Running in Docker](docs/docker.md) | Host procfs, PID/network namespaces, socket permissions, data ownership. |
| [systemd unit](contrib/systemd/smob.service) | Hardened service with `SIGTERM` flush and journal logging. |
| [Compose file](compose.yaml) | Host `/proc`, data volume, host networking, optional Docker socket. |
| [Design and implementation plan](docs/IMPLEMENTATION.md) | Architecture, storage internals, phases, TODOs. |

The default configuration path is `/etc/smob/smob.yaml`; use `--config` to point
at another file. Query commands read the same backend the collector writes to.

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
| `system.network.rx_bytes_per_second` | rate | B/s | `interface` | Per interface, including `lo` |
| `system.network.tx_bytes_per_second` | rate | B/s | `interface` | Per interface, including `lo` |
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
- A process seen for the first time on a tick has no previous CPU counter, so it
  emits no `process.cpu.percent` until the next tick. `collect --once` takes one
  snapshot, so it records only `process.memory.rss_bytes` for processes.
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
- `container` is the container name without Docker's leading slash, or the full
  container ID when the daemon reports no name. `image` is the reference used
  when the container was created — a tag, digest, or image ID — not the image's
  current tag.
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

`smob --version` prints the same line as `smob version` and exits 0 without
reading the configuration.

Commands:

| Command | Purpose |
| --- | --- |
| `smob collect` | Run the collector and write samples to the configured backend. |
| `smob query` | Query recorded samples. |
| `smob stats` | Summarize matching samples per metric (count, min, max, mean). |
| `smob tail` | Live tail (deferred; currently a stub). |
| `smob config` | Print the effective configuration; `--validate` checks it and exits. |
| `smob version` | Print version, commit, and build date. |

### `collect`

- `--dry-run` validates configuration, source inputs, and store access without
  writing samples.
- `--once` collects one snapshot from every enabled source, writes one partial
  chunk per scope, and exits. This is intended for testing and automation.
- `--data-dir DIR` overrides `data_dir` from the configuration.

The collector runs until interrupted. A source failure, such as an unreadable
`/proc` root or a tick timeout, is logged at `warn` (rate-limited per source) and
collection continues on the next tick. The process source treats a single
unreadable, unparseable, or invalid process as a skipped PID: it logs once per
rescan and keeps sampling the rest. A failed process rescan keeps the previously
discovered PIDs and retries on the next rescan. A store failure stops collection
rather than silently dropping data.

### Docker container metrics

Deploying smob itself in a container — host `/proc`, PID and network namespace
choices, and the data volume — is covered in [docs/docker.md](docs/docker.md).
The repository's [compose.yaml](compose.yaml) is the executable Compose example.

Container collection is disabled by default. Enable it when the daemon is
available:

```yaml
sources:
  docker:
    enabled: true
```

Every Docker field, its default, and its validation rule are in
[docs/configuration.md#docker-source](docs/configuration.md#docker-source).
The daemon is selected only by `sources.docker.socket`; `DOCKER_HOST` and
`DOCKER_API_VERSION` are ignored, so the configuration file is the single source
of truth.

`collect` connects to and probes the socket at startup. If the source is
enabled and the socket is missing or unreachable, collection fails with exit
code 1 before any sample is written. A host without Docker keeps the source
disabled (the default) and is unaffected.

`collect --once` always takes one-shot stats because a single snapshot has no
stream history; when `stream_stats` is enabled, `--once` logs that it used
one-shot stats for that run.

### `query`

Query defaults to the last hour when `--since` is omitted. `--until` defaults to
now; `--until` without `--since` means the hour before `--until`. A duration
given to `--since` is measured back from `--until`. The range is half-open,
`[since, until)`: a sample stamped exactly at `until` is excluded, while one at
`since` is included.

| Flag | Description |
| --- | --- |
| `--since TIME` | Duration (`1h`, `24h`) or an RFC3339 timestamp. |
| `--until TIME` | RFC3339 timestamp. Defaults to now. |
| `--host NAME` | Restrict to one host. Omitting it merges hosts. |
| `--scope SCOPE` | Restrict to `system`, `process`, or `container`. |
| `--metric NAME` | Exact metric name. |
| `--label MATCH` | Repeatable and ANDed. `name=value` is exact; `name~regex` is a regular expression. Quote shell metacharacters, for example `--label 'comm~nginx|postgres'`. A label absent from a sample reads as the empty string. |
| `--limit N` | Maximum samples returned. Defaults to 10,000; `0` means unlimited. |
| `--format FORMAT` | `table` (default), `json`, `jsonl`, or `csv`. |
| `--agg OP` | Aggregate matching samples instead of listing them: `avg`, `min`, `max`, or `sum`. |
| `--group-by LABELS` | Comma-separated label names to group by. The metric is always included. Requires `--agg`. |
| `--data-dir DIR` | Override `data_dir` from the configuration. |

A `--data-dir` (or `data_dir`) path that does not exist, or that is not a
directory, is a runtime failure that names the path and exits 1. A directory
that exists but holds no chunks is an empty result, not an error. `stats` and
`--agg` follow the same rule.

Output is UTC everywhere. Table columns are `TIME`, `METRIC`, `VALUE`, `UNIT`,
and `LABELS`; units come from the metric suffix (`.bytes` and `_bytes` are `B`,
`.bytes_per_second` is `B/s`, and `.percent` is `%`). Table cells escape
backslashes and line breaks (`\\`, `\n`, `\r`, `\t`), so a label value such as
a process `comm` cannot split a row or shift its columns; CSV uses the standard
quoted form. A JSON sample result is one array and, like JSONL, is written as the
scan proceeds rather than buffered; JSONL emits one object per line for large
ranges; CSV has the columns `timestamp`, `metric`, `value`, `interval_ms`, and
`labels`. Results are sorted by timestamp, then metric, then canonical labels.
`--limit` is applied after filtering and sorting, so it bounds emitted samples,
not scan work: to keep output ordered it may read the rest of a chunk window and
open every chunk in the current UTC hour before it stops.

A query that fails or is interrupted closes its output document first, then
exits 1: a truncated result is never reported as success. A `SIGINT` during
`query` is an interruption, not a successful partial result. A non-finite
sample or aggregate value (`NaN`, `+Inf`, `-Inf`) is rejected with the metric
named rather than written to the table or CSV.
#### Aggregation

`--agg` replaces the sample stream with one row per group. `--group-by` lists
additional label names; the metric is always part of the group key and may be
listed explicitly or omitted. Without `--group-by`, there is one group per
metric. `--group-by` requires `--agg` and is a usage error on its own. Samples
that differ only in labels not listed in `--group-by` merge into one group, and
a grouped label that is absent from a sample has the empty string as its value.
Groups are ordered by metric, then by the grouped values in alphabetical label
name order.

`COUNT` is the number of matching samples in the group. Each sample's `avg`
weight is its `interval_ms` when that is positive and `1` otherwise:

| Aggregation | Rate (`interval_ms > 0`) | Gauge (`interval_ms == 0`) |
| --- | --- | --- |
| `avg` | `Σ(value × interval_ms) / Σinterval_ms` | `Σvalue / count` (unweighted) |
| `min` | raw minimum | raw minimum |
| `max` | raw maximum | raw maximum |
| `sum` | `Σvalue` (unweighted) | `Σvalue` (unweighted) |

Weighting changes only `avg`; `min`, `max`, and `sum` always use raw sample
values. A metric is normally entirely a rate or entirely a gauge. If malformed
data mixes both in one group, the same per-sample weight rule applies.

`sum` is the arithmetic total of the recorded sample values in the group over
the scanned range. It is not interval-weighted, not a time integral, and not
CPU-seconds. For CPU percentages it sums across series, and because the range
scan visits every timestamp it also accumulates one contribution per sample
over time. A range-wide `sum` is therefore count-sensitive and is not a
per-timestamp cross-series total; use a one-timestamp range or plain `query`
when that instantaneous total is what you need.

Aggregated table columns are `METRIC`, `COUNT`, `VALUE`, `UNIT`, and `LABELS`;
`LABELS` shows only the grouped labels in canonical order and is blank when
there are none. JSON and JSONL objects have `metric`, `labels` (always an
object, `{}` when empty), `count`, and `value`. CSV columns are `metric`,
`labels`, `count`, and `value`.

`--limit` applies to the store scan before aggregation, so a limited run
aggregates only the first N matching samples. Aggregation keeps one small
accumulator per group instead of the samples, so memory grows with the number
of distinct metric/grouped-label combinations in the range, not with the number
of samples. Grouping by a high-cardinality label such as `pid` can approach the
number of series; `--limit` bounds scanned samples, not the number of groups.

Data goes to stdout; status and errors go to stderr.

### `stats`

`stats` accepts the same time-range defaults, filters, and `--data-dir` override
as `query`, and reads the store through the same streaming path. It reports one
row per metric, aggregating every matching series and host into that metric
unless a filter narrows the set. Metrics are sorted alphabetically.

Table columns are `METRIC`, `COUNT`, `MIN`, `MAX`, `MEAN`, and `UNIT`. JSON is
one array of flat objects with `metric`, `count`, `min`, `max`, and `mean`;
JSONL emits one object per line; CSV has the columns `metric`, `count`, `min`,
`max`, and `mean`. `COUNT` is the number of matching samples. `MEAN` uses the
same weighting as `--agg avg`: `Σ(value × interval_ms) / Σinterval_ms` for rate
metrics and the unweighted arithmetic mean for gauges. For the same range and
filters it therefore equals `query --agg avg`.

`--limit` behaves as in `query`: it stops the scan after the first N matching
samples (default 10,000; `0` means unlimited). A limited run therefore reports
partial statistics for those samples. An empty result exits 0 and prints only
the header (or `[]` for JSON).

Use the `query` flags above (`--since`, `--until`, `--host`, `--scope`,
`--metric`, `--label`, `--limit`, `--format`, and `--data-dir`) with `stats`.
`stats` does not accept `--agg` or `--group-by`; use `query` for grouped
aggregation. An interrupted `stats` scan exits 1 with no rows, so partial
statistics are never presented as a complete summary.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Runtime failure, including a `SIGINT` or `SIGTERM` during `query` or `stats` |
| `2` | Usage or configuration error: an unknown command or flag, an invalid flag value, an unreadable or invalid configuration file, or a configuration validation failure |
| `3` | Persistent store failure (including `ENOSPC`) |

A second `SIGINT` or `SIGTERM` received while the first is being handled exits 1
immediately without flushing the active chunk.

## Storage

Set `storage.primary` to `disk` or `s3`. Exactly one backend is active; there is
no mirroring or dual-write. The field-by-field reference, including the S3 and
retention defaults, is in
[docs/configuration.md#storage](docs/configuration.md#storage).

- **Disk** writes under `data_dir` and is the default. New chunk files are
  `0640` inside directories created `0750`; existing files and directories are
  never chmodded. The data directory must not be world-writable: a
  world-writable parent lets another local user replace chunk paths. A retention
  sweep runs at startup and then hourly. It deletes closed chunks older than
  `retention.disk.max_age` or beyond `retention.disk.max_bytes`; `0` disables
  either limit, and both may be set. Age is measured from the chunk's end
  timestamp, the moment the window is complete, so it is wall-clock based: a
  backward clock step delays deletion and a forward step can delete chunks
  early. A chunk whose end lies in the future is measured from its start
  instead, so a clock jump or a planted filename cannot exempt it forever. Age
  is applied before size, and size is the total on-disk size of chunk files.
  Only files in the documented chunk layout are eligible, and the configured
  `storage.s3.spool_dir` subtree is always skipped — even when it is `data_dir`
  itself — so `.tmp` files, spooled chunks, and stray files are never touched by
  retention. The store check that runs before retention removes crash leftovers
  instead — `.smob-*.tmp`, legacy `<chunk>.tmp`, and `.check-*` files — and logs
  one `info` line with the count; chunk files and directories are never removed.
  The hourly tick calls the same sweep as the startup pass. Deletions are logged
  at `info`; sweep failures are logged at `warn` and never stop collection. The
  startup pass runs before the free-space warning. When the size cap is smaller
  than a single chunk, every deletable chunk is removed, including one just
  written, until nothing eligible remains; the S3 spool is never part of this
  accounting.
- **S3** writes to an AWS or S3-compatible bucket using the standard AWS
  credential chain (environment, shared config, or instance role). Credentials
  are never stored in `smob.yaml`. Transient failures are retried with
  exponential backoff and jitter. A chunk that still cannot be uploaded is
  written atomically to a local spool (`storage.s3.spool_dir`, default
  `<data_dir>/spool`) under the key the failed attempt was using — the base key
  or its `-1`, `-2`, … suffix — so replay targets a name the bucket did not
  already occupy; if every suffix is taken, the base key is the last resort and
  a `warn` names it. A background replayer uploads spooled chunks in key order.
  A terminal rejection — `400`, `401`, `403`, or a `404` that absence does not
  explain — is never spooled: `collect` fails immediately with exit code 3,
  because replay could not fix a revoked credential or a denied permission.
  Replay uses the exact chunk key: an object already present with identical
  bytes is treated as stored and the spool copy is deleted, so a failed delete
  cannot create a duplicate. Before uploading, replay verifies that each spool
  file still decodes as a chunk; a truncated or bit-rotted file is left in place
  with an `error` and is not retried in that process. Transient replay failures
  back off from 30s to 5m with jitter, so hosts do not retry in lockstep. A
  failed spool write is a persistent store failure
  (exit code 3) too. The spool is capped by `storage.s3.spool_max_bytes`; the
  cap is applied at startup before collection writes and again after each
  spooled chunk, and the oldest chunks are dropped with an `error` log. `query`
  merges readable spool entries with the bucket, so chunks not yet uploaded are
  still visible on the collecting host. See [docs/s3.md](docs/s3.md) for the
  IAM policy, credential chain, and a local MinIO recipe.

Both backends use the same keys and sample format, so migrating from disk to S3
is a copy followed by a configuration change. The complete procedure — stopping
the collector, excluding the local spool, verifying the samples, switching, and
rolling back — is in [docs/migration.md](docs/migration.md).

Completed chunks are immutable. A collector crash loses at most the chunk
currently being assembled, and `SIGINT`/`SIGTERM` flushes that chunk before
exit. Concurrent collectors must not write the same host and prefix. Multiple
hosts may share a bucket; every sample is labelled with its host.

### Data-directory trust

`data_dir` and `spool_dir` are part of the trust boundary, not inputs. Both must
be writable only by the user that runs `collect`. smob does not defend against a
local user who can write inside them: chunk writes create and follow directory
components, and the containment check runs only after the directory exists; the
check rejects a path that resolves outside `data_dir`; retention deletes chunk
files; spool eviction deletes the oldest spool entries. The precise rules are in
[configuration.md#trust-boundary](docs/configuration.md#trust-boundary).

What an attacker with write access can do:

- Forge chunk files that `query` and `stats` then report as real samples.
- Delete retention candidates before their age or size limit is reached.
- Plant symlinks in the layout; a write that resolves outside `data_dir` fails
  rather than escaping, so this is a denial of service, not an escape.
- Fill the disk with chunks or spool entries and stop collection.

Metrics are not secret-free: process samples carry `pid` and `comm`, container
samples carry names and image references, and system network samples carry host
interface names. New files are `0640` inside `0750` directories, and existing
files keep their old modes. Group read access is the supported sharing
mechanism; group write is not. Run the collector under a dedicated service
user, keep the parent of `data_dir` and `spool_dir` owned by root or that user
and not group- or world-writable, and do not add other users to the service
group unless they may read every recorded metric.

## Development

The [design and implementation plan](docs/IMPLEMENTATION.md) covers architecture,
the storage layout, delivery phases, testing strategy, and the TODO list.

```sh
just test                  # fast suite
just test-all              # extended suite; needs MinIO and Docker
just lint                  # staticcheck
just lint-workflows        # actionlint over the workflow files
just release-notes vX.Y.Z  # print one release's title and body
just release               # cross-compile release binaries into dist/
```

Pushing an annotated `vX.Y.Z` tag publishes the GitHub release and the container
image through `.github/workflows/release.yml`; agents commit and tag but never
push, so the push is the maintainer's action. The full process is in
[.agents/skills/release/SKILL.md](.agents/skills/release/SKILL.md).

## License

MIT. See [LICENSE](LICENSE).
