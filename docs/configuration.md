# Configuration reference

This page is the canonical reference for every `smob.yaml` field: its type,
default, validation rule, and effect. Deployment walkthroughs live in
[quickstart.md](quickstart.md), [s3.md](s3.md), and [docker.md](docker.md); the
design rationale is in [IMPLEMENTATION.md](IMPLEMENTATION.md).

## How configuration is loaded

- The default path is `/etc/smob/smob.yaml`. Override it with `--config PATH`.
- Defaults from `config.Default()` are applied first, then the file is decoded
  on top of them. Omitted fields keep their defaults; omitting a whole section
  is valid.
- Decoding is strict. An unknown field and a second YAML document are errors,
  never silently ignored.
- Durations are Go duration strings such as `10s`, `5m`, or `1h30m`. A
  non-string duration is rejected.
- `Resolve()` fills dependent defaults (S3 region, path style, and spool
  directory) after file values and CLI overrides are applied. `smob config`
  prints the resolved, effective configuration.
- `Validate()` runs after resolution, reports every problem at once, and names
  the offending field in each message.
- `--log-level`, `--log-format`, and `--data-dir` override the corresponding
  file values. `--data-dir` is accepted by `collect`, `query`, and `stats`.
- Credentials are never configuration fields. S3 uses the standard AWS
  credential chain; see [s3.md](s3.md#credentials).

Check a file without contacting any backend:

```sh
smob config --config ./smob.yaml --validate
```

Print the effective configuration, including resolved defaults:

```sh
smob config --config ./smob.yaml
```

## Top-level fields

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `data_dir` | string | `/var/lib/smob` | Required when `storage.primary` is `disk`: `data_dir: required when storage.primary is disk`. May be empty only with S3 when `storage.s3.spool_dir` is set. | Root directory for the disk backend. Also the base for the default S3 spool directory. Must be writable by the user running `collect`; `query` only needs read access. `--data-dir` overrides it. |
| `proc_path` | string | `/proc` | Must not be empty: `proc_path: must not be empty`. | Root of the procfs used by the system and process sources. Set it to `/host/proc` when a container mounts the host procfs there. |
| `host` | string | `os.Hostname()` when empty | Must not be empty after resolution: `host: must not be empty`. Must be a single path component: `host: must be a single path component; got %q`. It cannot be `.` or `..` and cannot contain `/`. | Value of the `host` label on every sample and the first component of every chunk key. It identifies the writer; do not run two collectors with the same host and prefix. |
| `log_level` | string | `info` | One of `debug`, `info`, `warn`, `error`: `log_level: must be one of debug, info, warn, error; got %q`. | Minimum severity written to stderr. `--log-level` overrides it. |
| `log_format` | string | `text` | `text` or `json`: `log_format: must be text or json; got %q`. | Log encoding. `text` is for people; `json` is for structured log collection. `--log-format` overrides it. |

## Sources

`collect` runs every enabled source. At least one source must be enabled:
`sources: at least one source must be enabled`. A disabled source is never
constructed, so it cannot fail startup or touch its input.

### System source

Host-wide CPU, memory, and network metrics from `proc_path`.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `sources.system.enabled` | boolean | `true` | — | Run the system source. |
| `sources.system.interval` | duration | `10s` | When enabled, must be greater than zero: `sources.system.interval: must be greater than zero`. | Collection cadence. The first tick produces gauges only; later ticks also produce CPU and network rates. |

### Process source

Per-process CPU and RSS from `proc_path/[pid]`.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `sources.process.enabled` | boolean | `false` | — | Run the process source. |
| `sources.process.filter` | string | empty | When enabled, must compile as a Go regular expression: `sources.process.filter: <regexp error>`. | Unanchored expression matched against `comm`. Empty matches every process. `comm` is truncated to 15 bytes by the kernel, so match that name, not the command line. |
| `sources.process.top_n` | integer | `20` | When enabled, must not be negative: `sources.process.top_n: must not be negative`. | Maximum number of process series per tick, selected by CPU, then RSS, then PID. `0` records every matching process and can produce a large volume. |
| `sources.process.interval` | duration | `30s` | When enabled, must be greater than zero: `sources.process.interval: must be greater than zero`. | Collection cadence. |
| `sources.process.rescan_interval` | duration | `30s` | When enabled, must be greater than zero: `sources.process.rescan_interval: must be greater than zero`. | How often the proc root is listed to discover new PIDs. It is independent of `interval`; a shorter value makes new processes appear sooner. |

### Docker source

Per-container CPU, memory, and network metrics from the Docker Engine API. The
source is off by default. When it is enabled and the socket is missing or
unreachable, `collect` fails at startup with exit code 1. See
[docker.md](docker.md) for deployment mounts, namespaces, and socket
permissions.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `sources.docker.enabled` | boolean | `false` | — | Run the Docker source and probe the daemon at startup. |
| `sources.docker.socket` | string | `/var/run/docker.sock` | When enabled, must not be empty: `sources.docker.socket: required when the docker source is enabled`. | Docker Engine API socket path. |
| `sources.docker.exclude` | string | empty | When enabled, must compile as a Go regular expression: `sources.docker.exclude: <regexp error>`. | Unanchored expression matched against the container name with Docker's leading slash removed. It never matches container IDs. Empty excludes nothing; anchor with `^` and `$` for an exact name. |
| `sources.docker.interval` | duration | `10s` | When enabled, must be greater than zero: `sources.docker.interval: must be greater than zero`. | One-shot polling cadence, and the cadence at which streamed observations are drained. Any positive duration is accepted, including sub-second values; very small values increase daemon load. |
| `sources.docker.rescan_interval` | duration | `30s` | When enabled, must be greater than zero: `sources.docker.rescan_interval: must be greater than zero`. | How often the running container set is enumerated. New containers start contributing without a restart; stopped containers disappear from later samples. |
| `sources.docker.stream_stats` | boolean | `false` | Requires the source: `sources.docker.stream_stats: requires sources.docker.enabled`. | Keep one persistent stats stream per container instead of polling one-shot stats. Streams cost a goroutine and connection per container, so this is opt-in. `collect --once` always uses one-shot stats because a single snapshot has no stream history. |

## Storage

Exactly one backend is active. `storage.primary` selects the write target, and
`query` and `stats` read the same backend. There is no mirroring or dual-write.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `storage.primary` | string | `disk` | `disk` or `s3`: `storage.primary: must be disk or s3; got %q`. | Active backend for `collect`, `query`, and `stats`. |
| `storage.chunk_interval` | duration | `5m` | Greater than zero: `storage.chunk_interval: must be greater than zero`. A whole number of seconds: `storage.chunk_interval: must be a whole number of seconds (minimum 1s); got %s`. Must divide one hour evenly: `storage.chunk_interval: must divide one hour evenly; got %s`. | Length of a UTC-aligned chunk. A crash loses at most this much collection; a larger value means fewer objects and later query visibility. Values such as `500ms`, `1500ms`, and `7m` are rejected. |

### S3

The S3 section is used when `storage.primary` is `s3`. See
[s3.md](s3.md) for the IAM policy, credential chain, and a local MinIO recipe.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `storage.s3.endpoint` | string | empty | When `primary` is `s3` and this is set, must be an absolute `http` or `https` URL with a host: `storage.s3.endpoint: must be an http or https URL; got %q` or `storage.s3.endpoint: must include a host; got %q`. | Custom S3-compatible endpoint. Empty uses AWS S3. |
| `storage.s3.region` | string | empty; resolved to `us-east-1` when `endpoint` is set | When `primary` is `s3`, required when `endpoint` is empty: `storage.s3.region: required unless storage.s3.endpoint is set`. | Signing region. MinIO and most compatible services ignore the value, but the AWS SDK requires one. |
| `storage.s3.bucket` | string | empty | Required when `primary` is `s3`: `storage.s3.bucket: required when storage.primary is s3`. | Bucket name. The bucket must already exist; smob does not create it. |
| `storage.s3.prefix` | string | empty | — | Key root inside the bucket. Leading and trailing slashes are trimmed. Empty means the bucket root. A non-empty prefix is recommended so the IAM policy can scope object access. |
| `storage.s3.path_style` | boolean | unset; resolved to `true` when `endpoint` is set, otherwise `false` | — | Use path-style addressing (`endpoint/bucket/key`) instead of virtual-hosted style (`bucket.endpoint/key`). Set it explicitly only when the service requires virtual-hosted style and DNS supports the bucket hostname. |
| `storage.s3.spool_dir` | string | empty; resolved to `<data_dir>/spool` | Required when `primary` is `s3` and `data_dir` is empty: `storage.s3.spool_dir: required when data_dir is empty`. | Local directory for chunks that could not be uploaded. Startup creates it if needed, probes it, and fails with exit code 3 when it is not writable. |
| `storage.s3.spool_max_bytes` | integer (bytes) | `1073741824` (1 GiB) | Must be greater than zero: `storage.s3.spool_max_bytes: must be greater than zero`. Validated even when `primary` is `disk`. | Cap on the total size of spooled chunks. Over the cap, the lexicographically oldest unprotected chunks are dropped and each drop is logged at `error`; collection never blocks on the network. A single chunk larger than the cap can leave the spool above the limit. |

## Retention

Retention applies only to the disk backend. S3 retention is a bucket lifecycle
rule. `collect` runs one sweep at startup, before the free-space warning, and
then hourly; `collect --once` runs the startup sweep, and `--dry-run` runs none.
A zero limit disables that limit, and both may be set. Age is applied before
size, and size is the total on-disk size of chunk files.

| Field | Type | Default | Validation | Effect |
| --- | --- | --- | --- | --- |
| `retention.disk.max_age` | duration | `0` (disabled) | Must not be negative: `retention.disk.max_age: must not be negative; zero disables the limit`. | Delete closed chunks whose end is at or before `now - max_age`. Age is measured from the chunk end, the moment the window is complete. |
| `retention.disk.max_bytes` | integer (bytes) | `0` (disabled) | Must not be negative: `retention.disk.max_bytes: must not be negative; zero disables the limit`. | Keep total chunk-file size at or below this cap by deleting the oldest closed chunks first. Only files in the documented chunk layout are eligible; `.tmp` files, the S3 spool, and stray files are ignored. |

## Example

A minimal local disk configuration. The first `collect --once` writes gauges;
run the daemon for at least two system intervals to get rates:

```yaml
data_dir: ./data
sources:
  system:
    enabled: true
storage:
  primary: disk
```

For a complete S3/MinIO configuration, use
[s3.md](s3.md#custom-endpoints-and-minio). For a container deployment that
reads the host procfs and Docker socket, use
[docker.md](docker.md#host-system-and-process-metrics) and the repository
[compose.yaml](../compose.yaml).
