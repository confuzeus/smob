# Running smob in Docker

smob ships as a `FROM scratch` image that runs as UID 65532 and starts
`collect` with `/etc/smob/smob.yaml` by default. This guide covers the mounts
and namespace flags that give the collector a host view, and how to verify the
result. Socket permissions, `--group-add`, and data-directory ownership are a
separate topic and are not repeated here; this page only assumes the data
volume is writable and, when container scope is enabled, that the Docker socket
is accessible.

The metric catalogue is in the [README](../README.md#metrics-reference), and
S3/MinIO deployment is covered in [s3.md](s3.md).

## What a container's `/proc` shows

The system and process sources read `proc_path` (default `/proc`); the
container source reads the Docker Engine API. In a container, the default
`/proc` is the container's own view:

| Scope | Read from | Inside a default container |
| --- | --- | --- |
| `system.cpu.*` | `<proc_path>/stat` | Host-wide. CPU accounting is not namespaced, so the container already sees every host CPU. |
| `system.memory.*` | `<proc_path>/meminfo` | Host-wide. Memory counters are not namespaced; a cgroup limit does not change them. |
| `system.network.*` | `<proc_path>/net/dev` | The container's network namespace: `eth0` and `lo`, not the host interfaces. |
| `process.*` | `<proc_path>/[pid]/...` | The container's PID namespace: only smob itself. |
| `container.*` | Docker Engine API | Unaffected by mounts and namespaces; requires the Docker socket. |

The namespace-sensitive parts of smob are process enumeration and network
counters. `/proc/stat` and `/proc/meminfo` are kernel-global, which is why
system CPU and memory already describe the host even without a mount.
`/proc/net` is a magic symlink to `self/net`, and `self` resolves to the
process that opens it, so `net/dev` always describes the reader's network
namespace; a host `/proc` mount does not change that target. Only sharing the
host network namespace does. A container's own `/proc` likewise lists only its
PID namespace, which is why the mount or `--pid=host` is needed for host
processes.

## Host system and process metrics

Bind-mount the host procfs read-only and point `proc_path` at it:

```sh
-v /proc:/host/proc:ro
```

A minimal configuration:

```yaml
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
```

Set `host` explicitly. It defaults to `os.Hostname()`, which inside a container
is the container hostname — the short container ID unless `--hostname` is set —
not the machine's name. The label identifies the writer in shared storage, so
use the same name the host would use.

With this mount, `<proc_path>/stat`, `<proc_path>/meminfo`,
`<proc_path>/sys/kernel/random/boot_id`, and `<proc_path>/[pid]` all come from
the host. The mount is what makes host processes visible; CPU and memory were
already host-wide. `:ro` makes the bind mount read-only in the container's
mount namespace, where it appears as `ro` in `/proc/mounts` and refuses writes
with `Read-only file system`. smob only opens these files for reading, so it
needs no write access, and the host's `/proc` cannot be modified through the
container.

## Host PID namespace (`--pid=host`)

The recommended deployment does **not** use `--pid=host`. A bind mount of the
host procfs preserves the host PID namespace: `/host/proc` lists host PIDs and
`/host/proc/1/comm` is the host's PID 1 regardless of the container's own PID
namespace. That is the least-privilege choice.

Use `--pid=host` instead of the mount when you want `proc_path` to stay
`/proc`. Sharing the host PID namespace makes the container's `/proc` the
host's procfs, so the process source sees host processes without a separate
mount. Do not combine it with the mount unless you need both paths to show the
host view.

The trade-off is isolation. With `--pid=host` the container can see host
processes and, subject to UID and capability checks, signal them. A monitoring
agent may accept that, but it is unnecessary with the `/host/proc` mount, so
add the flag only when you need it.

A host that mounts `/proc` with `hidepid=1` or `hidepid=2` hides other users'
processes from the container too. The image runs as UID 65532, so process
coverage is limited to the processes that UID can read. Neither the
`/host/proc` mount nor `--pid=host` changes the mount's `hidepid` setting; use
a UID that can read the processes you need, or adjust the host mount.

## Host network interfaces (`--network=host`)

`system.network.*` reports the reader's network namespace, even with
`/host/proc` mounted, because `/proc/net/dev` is reached through `self`. Add:

```sh
--network=host
```

to make the container share the host network namespace. The interface labels
then match `ls /sys/class/net` on the host instead of the container's `eth0`
and `lo`. The trade-off is that the container loses network isolation: it can
bind host ports and sees host interfaces and traffic. That is usually intended
for a host-monitoring agent. Container-scope network metrics are unaffected;
they come from the Docker API and describe each container's own interfaces.

## Build, data directory, and run

Build the image with the repository task runner. `just docker` tags the image
with the version from `git describe`; `VERSION=local` makes the tag below
stable:

```sh
VERSION=local just docker
```

The image runs as UID 65532, so `data_dir` must be writable by that user.
Create a named volume and initialize its ownership once:

```sh
docker volume create smob-data
docker run --rm -v smob-data:/data busybox:1.36 chown 65532:65532 /data
```

With `storage.primary: s3`, `spool_dir` defaults to `<data_dir>/spool`, so the
same volume also holds chunks waiting to be uploaded. Bind mounts and
group-based ownership belong to the separate permissions topic; this guide
only needs a writable volume.

Save the configuration above as `smob.yaml`, then start the collector:

```sh
docker run -d --name smob \
  -v /proc:/host/proc:ro \
  --network=host \
  -v smob-data:/data \
  -v "$PWD/smob.yaml:/etc/smob/smob.yaml:ro" \
  smob:local
```

`$PWD/smob.yaml` is read from the current directory on the host and mounted at
the image's default configuration path.

## Container scope (optional)

Container metrics come from the Docker Engine API, not `/proc`, so they are
unaffected by the mounts above. The source is off by default. Enable it in the
configuration and mount the socket read-only:

```yaml
sources:
  docker:
    enabled: true
```

```sh
-v /var/run/docker.sock:/var/run/docker.sock:ro
```

If the source is enabled and the socket is missing or unreachable, `collect`
exits with code 1 before writing any sample. The socket is normally not
readable by the image's UID without an additional group; socket permissions and
`--group-add` belong to the separate permissions topic. Do not mount the socket
when the source is disabled.

## Verifying the deployment

Let the collector run for about 35 seconds — long enough for the second
default process tick at 30s — so both gauges and rates are present. Then stop it; `SIGTERM` flushes the
partial chunk, which is necessary because a five-minute chunk is not visible to
`query` until its window closes:

```sh
docker stop smob
docker rm smob
```

Define a query wrapper that reads the same volume and configuration:

```sh
smob_query() {
  docker run --rm \
    -v smob-data:/data:ro \
    -v "$PWD/smob.yaml:/etc/smob/smob.yaml:ro" \
    smob:local query --config /etc/smob/smob.yaml "$@"
}
```

Querying does not need `/host/proc`, `--pid=host`, or `--network=host`.

```sh
# Process samples: host PIDs and comm values.
smob_query --since 1h --scope process --limit 10

# System network: interface labels.
smob_query --since 1h --metric system.network.rx_bytes_per_second --limit 10

# The configured host label.
smob_query --since 1h --host web-1 --limit 5
```

What to expect:

| Check | Host view | Container-only view |
| --- | --- | --- |
| Process PIDs | Host PIDs with host `comm` values (the top 20 by default). | Only `pid=1`, `comm=smob`. |
| Network interfaces | The names from `ls /sys/class/net` on the host, such as `docker0`, `enp2s0f1`, and `lo`. | `eth0` and `lo`. |
| `host` label | The configured `host`, here `web-1`. | The container hostname or ID when `host` is not set. |
| CPU series | `cpu=all` plus `cpu=0` … `cpu=N-1` for every host CPU. | The same; CPU is not namespaced. |
| Memory totals | The host's `MemTotal`. | The same; memory is not namespaced. |

Selected process sets and interface sets vary between samples as processes and
interfaces come and go. The decisive checks are process PIDs and network
interfaces: if process samples contain only `pid=1` with `comm=smob`, the
collector is reading the container's PID namespace, and if interfaces are
`eth0` and `lo`, the network namespace is the container's. CPU count and memory
totals do not distinguish the two views because `/proc/stat` and
`/proc/meminfo` are kernel-global.

To see the contrast directly, run the same image without the host mount and
with `proc_path: /proc`: it reports one PID and the container interfaces. The
recommended configuration above reports the host's processes and, with
`--network=host`, the host's interfaces.
