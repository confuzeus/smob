# smob v0.2.1 — Initial Release

**smob** (Simple Monitoring Observability Tool) is a lightweight observability
tool delivered as a single static Go binary. It records CPU, memory, and network
usage over time on a Linux host and stores the data on local disk or in any
S3-compatible object store. It collects three scopes — the whole system,
individual processes, and Docker containers — and one CLI both collects and
queries: `smob collect` runs the daemon, while `smob query` and `smob stats`
read historical data.

v0.2.1 is the initial release. It packages the complete product surface built
across the v0.1 and v0.2 development line, plus the audit fixes merged after the
local `v0.2.0` tag.

## Release history

- **v0.1.0** — development tag: system and process metrics, disk backend,
  `collect` and `query`.
- **v0.2.0** — development tag: S3 backend, Docker container metrics, disk
  retention, `stats`, and aggregation.
- **v0.2.1** — first release prepared for publication; includes everything
  above and the storage, source, CLI, documentation, and CI audit fixes.

## Highlights

- **One static binary.** Linux amd64 and arm64, built with `CGO_ENABLED=0`,
  with no runtime dependencies.
- **Three collection scopes.** System-wide CPU, memory, and network; per-process
  CPU and RSS with filtering and top-N selection; per-container CPU, memory, and
  network through the Docker Engine API.
- **Disk or S3.** Write locally or to AWS S3, MinIO, or any S3-compatible
  endpoint. Both backends use the same key layout and sample format.
- **Efficient storage.** Samples are grouped into immutable, UTC-aligned chunks
  (five minutes by default) and compressed with gzip. A crash loses at most the
  chunk being assembled; `SIGINT`/`SIGTERM` flushes it before exit.
- **Bounded disk usage.** A startup and hourly sweep deletes closed disk chunks
  by age or total size, oldest first, without touching the active chunk or any
  non-chunk file.
- **Streaming queries and summaries.** Query samples, aggregate them by metric
  and label, or summarize count, min, max, and mean per metric over a time
  range, using the same filters and output formats.
- **Explicit units.** Metric names encode their unit, and rate samples carry the
  measurement window used to compute them.
- **Operator documentation and deployment artifacts.** Quickstart,
  configuration reference, S3/IAM guide, disk-to-S3 migration guide, Docker
  deployment guide, hardened systemd unit, and Compose example.

## Metrics collected

Every sample carries a `host` label and a `scope` label (`system`, `process`, or
`container`), plus the series labels below.

| Metric | Type | Unit | Series labels |
| --- | --- | --- | --- |
| `system.cpu.percent` | rate | percent | `cpu=all` or `cpu=N` |
| `system.memory.total_bytes` | gauge | bytes | — |
| `system.memory.available_bytes` | gauge | bytes | — |
| `system.memory.used_bytes` | gauge | bytes | — |
| `system.network.rx_bytes_per_second` | rate | B/s | `interface` |
| `system.network.tx_bytes_per_second` | rate | B/s | `interface` |
| `process.cpu.percent` | rate | percent | `pid`, `comm` |
| `process.memory.rss_bytes` | gauge | bytes | `pid`, `comm` |
| `container.cpu.percent` | rate | percent | `container`, `image` |
| `container.memory.used_bytes` | gauge | bytes | `container`, `image` |
| `container.network.rx_bytes_per_second` | rate | B/s | `container`, `image`, `interface` |
| `container.network.tx_bytes_per_second` | rate | B/s | `container`, `image`, `interface` |

CPU percentages are per core: `100` means one core fully busy, so a
multi-threaded process or container may exceed `100`. Gauge samples are
instantaneous and carry `interval_ms: 0`; rate samples record their measurement
window. A counter reset — after a reboot or a recreated interface — drops that
tick instead of reporting a bogus rate, so a short gap is expected.

## Quick start

Requirements: Linux (amd64 or arm64) and Go 1.27.3 or newer to build from
source. Docker and S3 are optional.

```sh
git clone https://github.com/confuzeus/smob.git
cd smob
just build
./smob version
```

Without `just`:

```sh
CGO_ENABLED=0 go build -trimpath -o smob ./cmd/smob
```

Create a minimal configuration:

```yaml
data_dir: ./data
sources:
  system:
    enabled: true
storage:
  primary: disk
```

Then validate, collect, and query:

```sh
./smob config --config ./smob.yaml --validate
./smob collect --once --config ./smob.yaml
./smob query --config ./smob.yaml --since 1h --limit 5
./smob stats --config ./smob.yaml --since 1h
```

Run the collector in the foreground with `./smob collect --config ./smob.yaml`.
`--once` takes a single snapshot, so it records gauges but no CPU or network
rates. See the [quickstart](docs/quickstart.md) for the full walkthrough.

## Storage and durability

Disk and S3 use the same UTC-aligned chunk layout:

```text
<root>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
```

- Chunks are immutable once written; a completed window is never overwritten.
- A crash loses at most the chunk currently being assembled.
- The first `SIGINT`/`SIGTERM` flushes the active chunk and exits `0`; a second
  signal exits `1` without flushing.
- Failed S3 uploads are spooled locally and replayed in the background, bounded
  by `spool_max_bytes`; queries merge readable spool entries with the bucket.
- Retention deletes only closed disk chunks, by age or total size. The active
  chunk, temporary files, and the S3 spool are never touched.

Exactly one backend is active, selected by `storage.primary` (`disk` or `s3`).
There is no mirroring. Migrating from disk to S3 is an `aws s3 sync` followed by
a configuration change; see the [migration guide](docs/migration.md).

## Operations

- **Fail loud at startup.** Every enabled source and the selected backend must
  be usable before collection begins. Docker collection fails startup when
  enabled and the socket is unreachable.
- **Exit codes.** `0` success, `1` runtime failure, `2` usage or configuration
  error, `3` persistent store failure (including `ENOSPC`).
- **Trust boundary.** `data_dir` and `spool_dir` must be writable only by the
  collector's user. New files are `0640` inside `0750` directories; group read
  is the supported sharing mechanism.
- **One writer per host and prefix.** Multiple hosts may share a bucket because
  every sample carries a `host` label, but concurrent collectors must not write
  the same host and prefix.

## Known limitations

- `smob tail` is a stub. Live tail, a chunk index, compaction, and multi-host
  aggregation or remote query are deferred to v0.3.
- Linux only. There is no Windows or macOS support.
- Docker collection requires access to the Docker socket, and the source is
  disabled by default.
- Process filtering matches `comm`, which the kernel truncates to 15 bytes;
  matching the full command line is not supported.
- No alerting, dashboards, Prometheus exposition, or downsampling.

## Documentation

| Guide | Contents |
| --- | --- |
| [README](README.md) | Features, metric catalogue, CLI reference, storage behavior. |
| [Quickstart](docs/quickstart.md) | Build, first collection, daemon and service deployment. |
| [Configuration](docs/configuration.md) | Every field with type, default, validation, and effect. |
| [S3 and MinIO](docs/s3.md) | IAM policy, credential chain, MinIO recipe, spool behavior. |
| [Migration](docs/migration.md) | Disk → S3 copy, verification, switch, and rollback. |
| [Running in Docker](docs/docker.md) | Host procfs, namespaces, socket permissions, data ownership. |
| [Design](docs/IMPLEMENTATION.md) | Architecture, storage internals, phases, TODO list. |

## License

MIT. See [LICENSE](LICENSE).
