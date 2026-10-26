# Quickstart

This is the shortest path from an installed binary to a first query. It runs
smob in the foreground against a local data directory. For every field and
validation rule, see [configuration.md](configuration.md); for deployment
depth, see the links at the end.

## Requirements

- Linux (amd64 or arm64).
- Go 1.27.3 or newer to build from source. The `go` directive in
  [go.mod](../go.mod) requires that exact patch level: a 1.27.0 toolchain
  succeeds only when Go may fetch a newer one (the default
  `GOTOOLCHAIN=auto`), and with `GOTOOLCHAIN=local` the build stops with a
  version error.
- No Docker or S3 service is needed for this walkthrough.

## Install

### Install script

```sh
curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh
```

On a systemd host this installs the binary to `/usr/local/bin/smob`, writes a
minimal `/etc/smob/smob.yaml` when none exists, installs the hardened unit from
`contrib/systemd/smob.service`, and enables and starts `smob`. The generated
configuration uses `data_dir: /var/lib/smob`, the system source, and the disk
backend. Pass `--no-service` to install the binary only, `--version vX.Y.Z` to
pin a release, or `--install-dir DIR` with `--no-service` for another prefix.
The [README](../README.md#install) describes the script in full. The rest of
this page uses `./smob`; replace it with `smob` when the binary is on `PATH`.

### Download a release

Official `smob-linux-amd64` and `smob-linux-arm64` binaries are attached to
every release on the
[releases page](https://github.com/confuzeus/smob/releases), together with
`SHA256SUMS`, `install.sh`, `smob.service`, and `LICENSE`; the
[README](../README.md#install) covers the release process. Download the binary
for your architecture and verify it from the directory that holds the binary and
the checksum file:

```sh
sha256sum -c SHA256SUMS
```

`SHA256SUMS` lists both architectures, so a run with only one binary downloaded
reports the other as missing; add `--ignore-missing` in that case. Put the
binary on `PATH`, using the file name you downloaded:

```sh
sudo install -m 0755 ./smob-linux-amd64 /usr/local/bin/smob
```

`ghcr.io/confuzeus/smob:vX.Y.Z` is the published container image; see
[docker.md](docker.md) for container deployment.

### Build from source

From a clone of the repository:

```sh
git clone https://github.com/confuzeus/smob.git
cd smob
just build
./smob version
```

`./smob --version` prints the same line and does not read the configuration.

Without `just`, the equivalent build command is:

```sh
CGO_ENABLED=0 go build -trimpath -o smob ./cmd/smob
```

To install the binary system-wide, put it on `PATH`:

```sh
sudo install -m 0755 ./smob /usr/local/bin/smob
```

The rest of this page uses `./smob`; if the binary is on `PATH`, use `smob`
instead.

## Create a minimal configuration

```sh
mkdir -p ./data
cat > smob.yaml <<'YAML'
data_dir: ./data
sources:
  system:
    enabled: true
storage:
  primary: disk
YAML
```

`system.enabled` is already the default, but stating it makes the example
explicit. `host` defaults to the machine hostname, `proc_path` to `/proc`, and
the chunk interval to five minutes. See
[configuration.md](configuration.md#top-level-fields) for those defaults.

Validate the file and inspect the resolved configuration:

```sh
./smob config --config ./smob.yaml --validate
./smob config --config ./smob.yaml
```

`--validate` prints `config valid: ./smob.yaml` and exits 0. Without
`--validate`, `smob config` prints the effective YAML, including the resolved
host and any S3 region and spool defaults. It does not contact the store.

## First collection and query

`--once` takes one snapshot from every enabled source, writes one partial chunk
per scope, and exits. A single snapshot has no previous counters, so it contains
gauges (memory) but no CPU or network rates:

```sh
./smob collect --once --config ./smob.yaml
./smob query --config ./smob.yaml --since 1h --limit 5
```

`query` defaults to the last hour, reads the same backend the collector writes,
and prints a table. Use `--format json`, `jsonl`, or `csv` for machine-readable
output. `--limit 0` removes the default 10,000-sample limit.

## Run the collector

Start the daemon in the foreground:

```sh
./smob collect --config ./smob.yaml
```

Leave it running for at least 25 seconds so the 10-second system source
completes two ticks and therefore produces CPU and network rates (the first
tick has no previous snapshot). Stop it with `Ctrl-C` (or `SIGTERM`); the first
signal flushes the active chunk and exits 0. A second signal exits 1 without
flushing.

Query the collected data from another terminal, or after stopping:

```sh
./smob query --config ./smob.yaml --since 1h --metric system.cpu.percent --limit 5
./smob stats --config ./smob.yaml --since 1h
```

While the collector runs, samples sit in the active chunk and are not visible
to `query` until the five-minute window closes. Stopping with `SIGINT` or
`SIGTERM` flushes that partial chunk, which makes the data visible immediately.
`collect --once` flushes immediately for the same reason.

## Where data lands

Disk data is written under `data_dir` using UTC-aligned keys:

```text
<data_dir>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
```

With the configuration above and a host named `web-1`, a system chunk looks
like:

```text
data/web-1/system/2026/10/24/07/20261024T070000Z-20261024T070500Z.jsonl.gz
```

A partial chunk written by `--once` or shutdown has an end one second or more
after its last sample, so it remains a valid half-open window. Chunks are
gzip-compressed JSONL, one sample per line. The detailed format and collision
suffix rules are in
[IMPLEMENTATION.md](IMPLEMENTATION.md#3-storage-format-and-chunking). Nothing
else in the data directory is part of the format.

## Exit codes

Every subcommand uses the same codes, including `config --validate` and
`version`:

| Code | Meaning |
| --- | --- |
| `0` | Success, including `--help`, `--version`, and a query with no matching samples |
| `1` | Runtime failure that is not a usage or store error, such as a source read error |
| `2` | Usage or configuration error: bad flags or arguments, or a missing, unreadable, or invalid configuration file |
| `3` | Persistent store failure: a full filesystem (`ENOSPC`), an unwritable `data_dir`, or a failed S3 write and spool write |

`collect` checks the store before collecting, so an unusable `data_dir` exits 3
without producing samples. On shutdown, the first `SIGINT`/`SIGTERM` flushes the
active chunk and exits 0; a second signal exits 1 immediately without flushing.

## Run as a service

The repository ships a hardened unit at
[../contrib/systemd/smob.service](../contrib/systemd/smob.service) and a
Compose deployment at [../compose.yaml](../compose.yaml). Both assume the
system configuration at `/etc/smob/smob.yaml`.

For systemd, install the binary and unit, and set `data_dir: /var/lib/smob` in
the service configuration. That is the default path and the unit's
`StateDirectory`, so the dynamic service user can write it without extra
ownership rules:

```sh
sudo install -m 0755 ./smob /usr/local/bin/smob
sudo install -d -m 0750 /etc/smob
sudo install -m 0644 smob.yaml /etc/smob/smob.yaml
sudoedit /etc/smob/smob.yaml    # set data_dir: /var/lib/smob
sudo install -m 0644 contrib/systemd/smob.service /etc/systemd/system/smob.service
sudo systemctl daemon-reload
sudo systemctl enable --now smob
systemctl status smob
journalctl -u smob -f
```

`StateDirectory=smob` creates `/var/lib/smob`, hands it to the service user, and
keeps it writable under `ProtectSystem=strict`. That is why the default
`data_dir` needs no ownership work. A non-default `data_dir` does: with
`DynamicUser=yes` the UID is transient, so a `chown` you run today may no longer
match after a restart, and `ReadWritePaths=` only permits access to a path the
unit can already write; it never changes ownership. Use the unit's
static-account alternative instead:

```sh
sudo useradd --system --user-group --no-create-home \
  --home-dir /var/lib/smob --shell /usr/sbin/nologin smob
sudo install -d -o smob -g smob -m 0750 /srv/smob
sudoedit /etc/systemd/system/smob.service   # apply the block below
sudo systemctl daemon-reload
sudo systemctl restart smob
```

In `[Service]`, replace `DynamicUser=yes` with the static account, keep
`StateDirectory=smob`, and open the new path to the unit:

```ini
User=smob
Group=smob
StateDirectory=smob
ReadWritePaths=/srv/smob
```

Then set `data_dir: /srv/smob` in `/etc/smob/smob.yaml`. `StateDirectory` still
creates and manages `/var/lib/smob` for the static user, which is harmless when
unused. Under `/home`, also change `ProtectHome=yes` to `ProtectHome=read-only`
or `no`, because the unit hides `/home` by default. If you keep
`DynamicUser=yes`, the only writable directory you can name portably is a
`StateDirectory=` path under `/var/lib`: replace `StateDirectory=smob` with
`StateDirectory=smob-data` and set `data_dir: /var/lib/smob-data`. A path such
as `/srv/smob` or `/mnt/data` requires the static account above.

`Restart=on-failure` restarts the unit on a runtime failure (exit code 1) and a
persistent store failure (exit code 3). A usage or configuration error (exit
code 2) is excluded by `RestartPreventExitStatus=2`: it needs a human, so the
service stops in `failed` instead of retrying. Fix `/etc/smob/smob.yaml`, check
it with `smob config --validate --config /etc/smob/smob.yaml`, then
`sudo systemctl start smob`.

### Container deployment

Do **not** copy the local `smob.yaml` from this page into the container
unchanged. The local file uses the default `proc_path: /proc` and
`data_dir: ./data`. Inside a container, `/proc` is the container's own PID and
network namespace, so process and network metrics would silently describe the
container rather than the host, and the relative data path is not the mounted
volume.

The Compose and Docker examples use:

```yaml
data_dir: /data
proc_path: /host/proc
```

and mount `/proc` at `/host/proc`. Start from
[docker.md](docker.md#host-system-and-process-metrics) or
[../compose.yaml](../compose.yaml), which also documents host networking, the
optional Docker socket, and data-directory ownership.

## Next steps

- [Recipes](recipes.md) — complete, copy-paste setups for systemd, Docker
  Compose, S3, MinIO, container metrics, and query-only hosts.
- [Configuration reference](configuration.md) — every field, default, and
  validation rule.
- [S3 and MinIO](s3.md) — IAM policy, credentials, remote storage, and spool
  behavior.
- [Running in Docker](docker.md) — host procfs, namespaces, socket permissions,
  and data ownership.
- [systemd unit](../contrib/systemd/smob.service) and
  [Compose file](../compose.yaml) — production service definitions.
- [README](../README.md) — features, the metric catalogue, and the full CLI
  reference.
