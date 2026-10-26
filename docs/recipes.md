# Recipes

Complete, copy-paste starting points for common smob deployments. Each recipe
gives you a full `smob.yaml`, the commands that start it, and a quick way to
check that data is flowing. The [configuration reference](configuration.md)
remains canonical for every field; the deep deployment guides are linked at the
end of each recipe.

A YAML block that shows a whole file is complete. A block introduced as
"add" or "replace" is a fragment that changes the configuration above it.
Download a release binary or run the installer before starting a recipe that
runs the binary directly; both are covered in the
[README](../README.md#install). The Docker recipes use the published image.

## Which recipe?

| What you have | Store | Recipe |
| --- | --- | --- |
| One Linux host with systemd | Local disk | [Local disk on systemd](#local-disk-on-systemd) |
| One Linux host with systemd | AWS S3 | [AWS S3 on systemd](#aws-s3-on-systemd) |
| Docker Compose | Local disk | [Docker Compose with local disk](#docker-compose-with-local-disk) |
| Docker Compose and container metrics | Local disk or S3 | [Docker Compose with container metrics](#docker-compose-with-container-metrics) |
| A local or S3-compatible endpoint | S3 | [MinIO](#minio) |
| A workstation that only queries | Disk or S3 | [Query-only host](#query-only-host) |

## Before you copy

- **`host` identifies the writer.** It defaults to the machine hostname and is
  both the first chunk-key component and a sample label. Set it explicitly in a
  container, where the default is the container ID, and when several hosts share
  a store. Never run two collectors with the same `host` and prefix.
- **A chunk becomes visible when its window closes.** With the default
  five-minute `storage.chunk_interval`, a running collector's samples are not
  visible to `query` until the window ends. Stopping the collector flushes the
  partial chunk, and `collect --once` flushes immediately.
- **The data directory is trusted.** `data_dir` and `spool_dir` must be writable
  only by the collector's user; anyone who can write there can forge chunks or
  delete retention candidates. See
  [configuration.md#trust-boundary](configuration.md#trust-boundary).
- **The container image is not pinned.** The recipes use
  `ghcr.io/confuzeus/smob:latest`; replace it with a release tag such as
  `ghcr.io/confuzeus/smob:vX.Y.Z` when you need a reproducible deployment.
- **Commands assume a shell with `sudo`, `curl`, and GNU coreutils.** Systemd
  recipes target the paths the release unit expects; Docker recipes target
  Docker Engine with Compose v2.

## Local disk on systemd

**Use when:** one Linux host stores metrics locally and runs the collector as a
service. This is the layout the installer creates by default.

**Prerequisites:** Linux amd64 or arm64, systemd, and root. No Docker or S3
service is needed. The install step replaces `/etc/smob/smob.yaml`; back up an
existing file first.

### Install and configure

```sh
mkdir -p ~/smob-recipe && cd ~/smob-recipe

curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh

cat > smob.yaml <<'YAML'
data_dir: /var/lib/smob

sources:
  system:
    enabled: true

storage:
  primary: disk
YAML

sudo smob config --validate --config ./smob.yaml
sudo install -d -m 0750 /etc/smob
sudo install -m 0640 ./smob.yaml /etc/smob/smob.yaml
sudo systemctl restart smob
```

This replaces `/etc/smob/smob.yaml`. `data_dir: /var/lib/smob` is the unit's
`StateDirectory`, so the dynamic service user can write it without a `chown`.
`host` is omitted because the machine hostname is already the right default; set
it explicitly if you need a different stable name, and do not reuse that name on
another writer.

### Verify

```sh
sudo systemctl status smob --no-pager
sudo journalctl -u smob -n 20 --no-pager
```

Wait at least 20 seconds so the system source completes two ticks; the first
tick has no previous counters and therefore no CPU or network rates. Then flush
the active chunk and query it:

```sh
sudo systemctl stop smob
sudo smob query --config /etc/smob/smob.yaml --since 1h --limit 5
sudo systemctl start smob
```

The first `stop` writes the partial chunk and exits 0. Without stopping, the
first query is empty until the five-minute window closes.

### Variants

**Watch a few processes.** Add this beside `system` in the `sources` block:

```yaml
  process:
    enabled: true
    filter: "^(nginx|postgres|redis-server)$"
    top_n: 20
```

The filter is an unanchored Go regular expression matched against `comm`, which
the kernel truncates to 15 bytes. Anchor it for an exact name. `top_n: 0`
records every matching process.

**Bound disk usage.** Add a top-level `retention` block:

```yaml
retention:
  disk:
    max_age: 720h
    max_bytes: 10737418240
```

Both limits are optional and both may be set; `0` disables one. The sweep runs
at startup and hourly, applies age before size, and deletes the oldest closed
chunks first. See [configuration.md#retention](configuration.md#retention).

**See data sooner.** Replace the `storage` block to use one-minute chunks:

```yaml
storage:
  primary: disk
  chunk_interval: 1m
```

The interval must be a whole number of seconds and divide one hour evenly.
Smaller chunks are visible sooner and create more files.

**Collect container metrics too.** Add the Docker source:

```yaml
  docker:
    enabled: true
```

Then give the service access to the socket with a drop-in, and restart:

```sh
sudo systemctl edit smob
```

```ini
[Service]
SupplementaryGroups=docker
```

```sh
sudo systemctl restart smob
```

The `docker` group must exist on the host. The Docker socket is root-equivalent;
grant access to it only when container metrics are intended. See
[docker.md#why-the-docker-socket-is-special](docker.md#why-the-docker-socket-is-special).

## AWS S3 on systemd

**Use when:** one or more Linux hosts write chunks to an S3 bucket and run the
collector as a service. Credentials come from the AWS credential chain; there
are no credential fields in `smob.yaml`.

**Prerequisites:** the AWS CLI, a bucket, the runtime IAM policy, and a
credential source. The install step replaces `/etc/smob/smob.yaml`; back up an
existing file first. Create the bucket and attach the policy from
[s3.md#iam-policy](s3.md#iam-policy) to the instance role, task role, service
account, or user:

```sh
aws s3 mb s3://smob-metrics --region eu-west-1
```

The runtime identity needs `s3:ListBucket`, `s3:GetObject`, and
`s3:PutObject`. It does not need `s3:DeleteObject`; S3 retention is a bucket
lifecycle rule.

### Install and configure

```sh
mkdir -p ~/smob-recipe && cd ~/smob-recipe

curl -fsSL https://github.com/confuzeus/smob/releases/latest/download/install.sh | sudo sh

cat > smob.yaml <<'YAML'
data_dir: /var/lib/smob
host: web-1

sources:
  system:
    enabled: true

storage:
  primary: s3
  s3:
    region: eu-west-1
    bucket: smob-metrics
    prefix: smob
YAML

sudo smob config --validate --config ./smob.yaml
sudo install -d -m 0750 /etc/smob
sudo install -m 0640 ./smob.yaml /etc/smob/smob.yaml
sudo systemctl restart smob
```

Change `host`, `region`, `bucket`, and `prefix` for the deployment. `host` is
the unique name of this writer. `data_dir` stays because the spool defaults to
`<data_dir>/spool`; the unit's `StateDirectory` keeps it writable. The bucket
must already exist. `prefix` is recommended so the IAM policy can scope object
access.

The service runs the same startup check as `collect --dry-run`: it probes the
spool and calls `HeadBucket` before writing any sample. Start or restart the
service and read its status and journal; a persistent store failure exits 3 with
the failing path in the message. Do not run `sudo smob collect --dry-run`
against this unit's configuration. The unit uses `DynamicUser=yes`, so a root
run would create `/var/lib/smob/spool` as root and the service user could then
no longer write it.

### Verify

```sh
sudo systemctl status smob --no-pager
sudo journalctl -u smob -n 20 --no-pager
sudo systemctl stop smob
sudo smob query --config /etc/smob/smob.yaml --since 1h --limit 5
aws s3 ls --recursive s3://smob-metrics/smob/ | head
sudo systemctl start smob
```

Stopping flushes and uploads the partial chunk. `query` then reads the bucket.
`aws s3 ls` uses your tooling credentials, not smob's runtime identity. Wait at
least two system ticks before stopping if you want CPU and network rates.

### Variants

**Use static credentials.** On a host that is not an EC2 instance, ECS task, or
EKS service account, put the keys in the unit's optional environment file. Do
not add them to `smob.yaml`:

```sh
sudo install -m 0600 /dev/null /etc/smob/smob.env
sudo tee /etc/smob/smob.env >/dev/null <<'ENV'
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_EC2_METADATA_DISABLED=true
ENV
sudo systemctl restart smob
```

Set `AWS_SESSION_TOKEN` too for temporary credentials. Set
`AWS_EC2_METADATA_DISABLED=true` only when the host does not use an instance
profile; it disables that credential source. The unit already reads
`EnvironmentFile=-/etc/smob/smob.env` as root before dropping privileges. See
[s3.md#credentials](s3.md#credentials).

**Share one bucket across hosts.** Keep the same `bucket` and `prefix` on every
host, and give each one a different `host` value:

```yaml
host: web-2
```

Each host writes under its own `<prefix>/<host>/...` key prefix and every
sample carries the `host` label. Query from any host with read credentials;
`--host` narrows the result and omitting it merges hosts. One writer per host
and prefix is still required. See
[README.md#storage](../README.md#storage) and
[s3.md](s3.md) for the shared-bucket rules.

**Use a bucket lifecycle rule.** S3 retention is not part of `smob.yaml` and
smob never deletes bucket objects. Add a lifecycle rule after collection and
verification are working.

## Docker Compose with local disk

**Use when:** one Docker host stores metrics in a named volume and runs the
collector in a container with a host view. The container gets host CPU and
memory from the kernel; the mounted `/proc` and host network namespace make
process and network metrics describe the host instead of the container.

**Prerequisites:** Docker Engine, Compose v2, and `curl`.

### Create the files

```sh
mkdir -p ~/smob && cd ~/smob

curl -fsSLO https://raw.githubusercontent.com/confuzeus/smob/master/compose.yaml

cat > smob.yaml <<'YAML'
data_dir: /data
proc_path: /host/proc
host: web-1

sources:
  system:
    enabled: true
  process:
    enabled: true

storage:
  primary: disk
YAML

printf 'SMOB_IMAGE=ghcr.io/confuzeus/smob:latest\n' > .env
```

Change `host` to a stable, unique name for this machine. It must be set: inside
a container the default is the container hostname, which is the short container
ID. `data_dir: /data` and `proc_path: /host/proc` match the mounts in
[compose.yaml](../compose.yaml). `.env` supplies the published image because the
Compose file defaults to a locally built `smob:local`.

### Start

```sh
docker compose up -d
docker compose ps
docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q smob)"
```

The healthcheck repeats the startup check with `collect --dry-run`; it reports
`starting` until the first check passes and then `healthy`. Logs are available
with `docker compose logs -f smob`.

### Verify

Wait at least 20 seconds so two system ticks have produced rates, then stop the
collector to flush the partial chunk:

```sh
docker compose stop smob
docker compose run --rm smob query --since 1h --limit 5
docker compose start smob
```

`docker compose run` uses the same service definition, including the data volume
and configuration mount. Querying needs neither the host procfs nor the host
network namespace, but reusing the service keeps the paths identical.
`docker compose stop` sends `SIGTERM` and waits for the chunk to flush.

### Variants

**Use a bind-mounted host directory.** Prepare its ownership first, because the
image runs as UID 65532:

```sh
mkdir -p data
docker run --rm -v "$PWD/data:/data" busybox:1.36 chown 65532:65532 /data
SMOB_DATA="$PWD/data" docker compose up -d
```

On an SELinux host, append `:z` to the bind mount in `compose.yaml` so Docker
relabels the directory. A named volume needs none of this, so prefer it unless
you need the files on the host. See
[docker.md#data-directory-ownership](docker.md#data-directory-ownership).

**Store in S3.** Replace the `storage` block in `smob.yaml`:

```yaml
storage:
  primary: s3
  s3:
    region: eu-west-1
    bucket: smob-metrics
    prefix: smob
```

`data_dir: /data` stays: it is the base for the spool at `/data/spool`, which
lives in the same volume. On an EC2 instance with an instance role, nothing else
is needed. For static credentials, use a Compose override and an environment
file instead of putting keys in `smob.yaml`:

```yaml
# compose.s3.yaml
services:
  smob:
    env_file:
      - smob.env
```

```sh
cat > smob.env <<'ENV'
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_EC2_METADATA_DISABLED=true
ENV
chmod 0600 smob.env
docker compose -f compose.yaml -f compose.s3.yaml up -d
```

Set `AWS_EC2_METADATA_DISABLED=true` only when the container does not use an
instance or task role. The healthcheck now costs one `HeadBucket` per interval.
See [s3.md](s3.md) for the IAM policy, credential chain, and spool behavior.

## Docker Compose with container metrics

**Use when:** the Docker Compose deployment above is running and you also want
`container.*` samples for the containers on the host.

**Prerequisites:** a working Compose deployment and access to the Docker socket.
The socket grants root-equivalent control of the daemon; read
[docker.md#why-the-docker-socket-is-special](docker.md#why-the-docker-socket-is-special)
before mounting it.

### Configure

Add the Docker source to the `sources` block in `smob.yaml`:

```yaml
sources:
  system:
    enabled: true
  process:
    enabled: true
  docker:
    enabled: true
```

Create a Compose override that adds the socket and the socket's numeric group:

```yaml
# compose.docker.yaml
services:
  smob:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    group_add:
      - "${DOCKER_SOCKET_GID:?set DOCKER_SOCKET_GID to the numeric GID of the docker socket}"
```

### Start

```sh
echo "DOCKER_SOCKET_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
docker compose -f compose.yaml -f compose.docker.yaml up -d
```

Appending the GID to `.env` keeps the later `logs`, `stop`, `run`, and `start`
commands working without repeating the variable. `group_add` needs the socket's
numeric GID, not the group name: the `scratch` image has no `/etc/group`. If
`sources.docker.socket` points somewhere other than `/var/run/docker.sock`, stat
that path instead. If the socket's GID changes, update the line in `.env`.
Rootless Docker maps UIDs and GIDs differently; see
[docker.md#passing-the-socket-gid](docker.md#passing-the-socket-gid).

If you enabled the off-by-default block inside `compose.yaml` instead, do not
also pass the override file. The two approaches are alternatives.

### Verify

Wait at least 20 seconds for two container observations, then flush and query:

```sh
docker compose -f compose.yaml -f compose.docker.yaml logs -n 20 smob
docker compose -f compose.yaml -f compose.docker.yaml stop smob
docker compose -f compose.yaml -f compose.docker.yaml run --rm smob \
  query --since 1h --scope container --limit 10
docker compose -f compose.yaml -f compose.docker.yaml start smob
```

A single snapshot has no previous counters, so it emits
`container.memory.used_bytes` and no CPU or network rates. The first query after
a fresh start can therefore contain memory only.

| Symptom | Cause |
| --- | --- |
| `docker: connect ... permission denied` | The container lacks the socket's GID. |
| `docker: connect ... Cannot connect` | The socket is not mounted, or `sources.docker.socket` is wrong. |
| `collect` exits 1 before writing samples | The source is enabled but the daemon is unreachable. |

See [docker.md#checking-access](docker.md#checking-access) for the full
troubleshooting table.

## MinIO

**Use when:** you want a local S3-compatible store for a trial, or you run
MinIO on a private network.

The complete walkthrough — pinned image, bucket creation, and an optional
restricted user and policy — is the canonical MinIO recipe in
[s3.md#custom-endpoints-and-minio](s3.md#custom-endpoints-and-minio). Start
MinIO and create the bucket there, then use this complete configuration:

```yaml
data_dir: ./data
host: web-1

sources:
  system:
    enabled: true

storage:
  primary: s3
  s3:
    endpoint: http://127.0.0.1:9000
    region: us-east-1
    bucket: smob-metrics
    prefix: smob
    path_style: true
    spool_dir: ./data/spool
    spool_max_bytes: 1073741824
```

`region` and `path_style` are explicit for clarity; with `endpoint` set they
resolve to `us-east-1` and `true` anyway. Keep the endpoint on loopback or a
private network: plain `http` sends credentials, object keys, and metrics in
clear text ([s3.md#plain-http-is-clear-text](s3.md#plain-http-is-clear-text)).

```sh
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

./smob config --config ./smob.yaml --validate
./smob collect --once --config ./smob.yaml
./smob query --config ./smob.yaml --since 1h
```

Use the restricted writer credentials instead of the MinIO root user if you
created them in the walkthrough. `AWS_EC2_METADATA_DISABLED=true` skips the
instance-metadata probe on a host that is not an EC2 instance.

## Query-only host

**Use when:** a workstation, bastion, or reporting host reads a shared disk tree
or bucket but never collects. `query` and `stats` use the same configuration
schema as `collect`, and validation requires at least one enabled source even
though a query never starts one. Keep `sources.system.enabled: true`.

### Shared disk

```yaml
data_dir: /var/lib/smob

sources:
  system:
    enabled: true

storage:
  primary: disk
```

Mount the collector's `data_dir` read-only at the same path, or change
`data_dir` to the mount point. `query` only needs read access. Chunks are `0640`
inside `0750` directories. With the default `DynamicUser=yes` unit, the
simplest query path is `sudo`; with a static service account, add the query user
to the service group. Group read is the sharing mechanism, and group write is
not. See [configuration.md#trust-boundary](configuration.md#trust-boundary).

```sh
smob query --config ./smob.yaml --since 1h --metric system.cpu.percent --limit 5
smob stats --config ./smob.yaml --since 24h
```

Add `--host web-1` when the tree contains several hosts, or omit it to merge
them.

### S3

```yaml
data_dir: ""

sources:
  system:
    enabled: true

storage:
  primary: s3
  s3:
    region: eu-west-1
    bucket: smob-metrics
    prefix: smob
    spool_dir: /var/lib/smob/spool
```

`data_dir` is empty because this host does not collect; `spool_dir` is required
when it is empty. A local spool is read if it exists and a missing spool is an
empty result, not an error. The read identity needs `s3:ListBucket` and
`s3:GetObject`; use the policy in [s3.md#iam-policy](s3.md#iam-policy) without
the `s3:PutObject` action.

```sh
smob query --config ./smob.yaml --since 1h --host web-1 --limit 5
smob stats --config ./smob.yaml --since 24h
```

`query` has no startup probe: bad credentials, a missing bucket, or an
unreachable endpoint fail on the first list or read with exit code 1. The same
range and filters work with `--format json`, `jsonl`, or `csv`; see the
[CLI reference](../README.md#cli-reference).

## See also

- [Quickstart](quickstart.md) — build, first `collect --once`/`query`, and the
  systemd service in detail.
- [Configuration reference](configuration.md) — every field, default, and
  validation rule.
- [S3 and MinIO](s3.md) — IAM policy, credentials, MinIO, TLS, and the spool.
- [Running in Docker](docker.md) — host procfs, namespaces, socket
  permissions, and data ownership.
- [Disk → S3 migration](migration.md) — copy, verify, and roll back.
- [Compose file](../compose.yaml) — the canonical hardened Compose service.
- [systemd unit](../contrib/systemd/smob.service) — the hardened unit used by
  the installer.
