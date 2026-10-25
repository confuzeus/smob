# smob v0.2.3 — Release Notes Maintenance

This release is documentation only. The `smob` binary, its configuration, its
metrics, and its storage formats are unchanged from v0.2.2, so there is no
upgrade action.

## Highlights

- **Notes describe user-visible changes only.** Release notes now cover new
  features, behavior changes, bug fixes, and upgrade steps. Contributor-facing
  history is kept separately in [CHANGELOG.md](CHANGELOG.md).

## Upgrade notes

- None.

---

# smob v0.2.2 — Maintenance Release

This release changed only project documentation and tooling. The `smob` binary,
its configuration, its metrics, and its storage formats are unchanged from
v0.2.1, so there is no upgrade action.

## Upgrade notes

- None.

---

# smob v0.2.1 — Initial Release

**smob** (Simple Monitoring Observability Tool) is a lightweight observability
tool delivered as a single static Go binary. It records CPU, memory, and network
usage over time on a Linux host and stores the data on local disk or in any
S3-compatible object store. It collects three scopes — the whole system,
individual processes, and Docker containers — and one CLI both collects and
queries: `smob collect` runs the daemon, while `smob query` and `smob stats`
read historical data.

## Highlights

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
  by age or total size, oldest first, without touching the active chunk or any
  non-chunk file.
- **Streaming queries and summaries.** Query samples, aggregate them by metric
  and label, or summarize count, min, max, and mean per metric over a time
  range, using the same filters and output formats.
- **Explicit units.** Metric names encode their unit, and rate samples carry the
  measurement window used to compute them.
- **Operator documentation.** Quickstart, configuration reference, S3/IAM guide,
  disk-to-S3 migration guide, Docker deployment guide, hardened systemd unit,
  and Compose example.

## Upgrade notes

- This is the first release; there is no earlier version to upgrade from.
- Linux only. Docker and S3 are optional, and every enabled source and the
  selected backend must be usable at startup.

## Known limitations

- `smob tail` is a stub. Live tail, a chunk index, compaction, and multi-host
  aggregation or remote query are not available.
- Linux only. There is no Windows or macOS support.
- Docker collection requires access to the Docker socket, and the source is
  disabled by default.
- Process filtering matches `comm`, which the kernel truncates to 15 bytes;
  matching the full command line is not supported.
- No alerting, dashboards, Prometheus exposition, or downsampling.

See the [README](README.md) for the metric catalogue, CLI reference, and storage
behavior, and the [quickstart](docs/quickstart.md) to get started.
