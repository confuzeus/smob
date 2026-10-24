# Running smob in Docker

smob ships as a `FROM scratch` image that runs as UID 65532 and starts
`collect` with `/etc/smob/smob.yaml` by default. This guide covers the mounts
and namespace flags that give the collector a host view, the permissions that
let it reach the Docker socket and data directory, and how to verify the
result.

Socket permissions, `--group-add`, and data-directory ownership are covered in
[Socket permissions and data ownership](#socket-permissions-and-data-ownership)
below.

The metric catalogue is in the [README](../README.md#metrics-reference),
S3/MinIO deployment is covered in [s3.md](s3.md), the first-run walkthrough is
in [quickstart.md](quickstart.md), and the full field reference is in
[configuration.md](configuration.md).

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

A minimal configuration for the container:

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

This is deliberately not the quickstart's local configuration. The local file
uses the default `proc_path: /proc` and `data_dir: ./data`; copying it here
would collect the container's PID and network namespaces and write to the wrong
path. The container configuration above sets `proc_path: /host/proc` and
`data_dir: /data` to match the mounts.

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
Create and initialize the data volume once, before the `docker run` below, as
described in [Data-directory ownership](#data-directory-ownership).

With `storage.primary: s3`, `spool_dir` defaults to `<data_dir>/spool`, so the
same volume also holds chunks waiting to be uploaded; it needs the same
ownership as `data_dir`. A bind-mounted host directory needs the same
initialization and is covered in the same section.

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
readable by the image's UID without an additional group; see
[Passing the socket GID](#passing-the-socket-gid). Do not mount the socket when
the source is disabled.

`:ro` makes the socket mount read-only, but it does not restrict the Docker
API; see
[Read-only socket mounts do not restrict the API](#read-only-socket-mounts-do-not-restrict-the-api).

## Socket permissions and data ownership

The image runs as UID 65532 with no supplementary groups. Two paths it uses
outside its own filesystem are not writable by that UID by default: the Docker
socket and the data directory. Both need an explicit grant.

### Why the Docker socket is special

On a typical host the socket looks like this:

```text
$ stat -c '%A %U %G %a' /var/run/docker.sock
srw-rw---- root docker 660
```

It is a Unix domain socket owned by `root` and group `docker`, mode `0660`.
Connecting requires write permission on the socket, so only UID 0 and members
of the socket's group can open it. UID 65532 is neither, and a `scratch` image
has no `/etc/group` in which to look the group up by name. With
`sources.docker.enabled: true`, `collect` fails during startup instead of
collecting nothing.

The Docker Engine API is root-equivalent. A client that can open the socket can
create a privileged container, mount the host filesystem, read every
container's environment, and change or remove the daemon's containers. Adding
the socket's group is therefore not a narrow permission: it is a path to host
root through the daemon. Mount the socket only when
`sources.docker.enabled: true`, only on a host where smob is trusted with that
authority, and never as a convenience for a deployment that does not collect
container metrics. A disabled source is not constructed, so its socket is never
opened.

### Passing the socket GID

Docker applies supplementary groups from `--group-add` when it starts the
container. Read the socket's numeric GID on the host and pass that number to
`--group-add` in the `docker run` command:

```sh
--group-add "$(stat -c '%g' /var/run/docker.sock)"
```

The numeric GID is what the kernel checks. A group name is not resolved inside
a `scratch` image; passing the name fails outright:

```text
docker: Error response from daemon: unable to find group docker: no matching entries in group file
```

Even where a name resolves, the kernel checks the numeric GID: a host group with
the same name does not help unless its GID matches the socket. `id -g` reports
your primary group, not the socket's group, so use `stat` on the socket path.
If `sources.docker.socket` points somewhere other than `/var/run/docker.sock`,
stat that path and mount it at the same path.

The repository's [compose.yaml](../compose.yaml) has the socket mount and
`group_add` entries commented out. Enable `sources.docker` in `smob.yaml`,
uncomment both lines, then pass the socket's numeric GID:

```sh
DOCKER_SOCKET_GID="$(stat -c '%g' /var/run/docker.sock)" docker compose up -d
```

**Rootless Docker.** A rootless daemon usually listens on
`$XDG_RUNTIME_DIR/docker.sock` (for example `/run/user/1000/docker.sock`)
rather than `/var/run/docker.sock`, and the socket is owned by the user who
started the daemon, with a mode that may be `0600`. Rootless containers run
inside the daemon's user namespace, so host UIDs and GIDs map to different IDs
inside the container. The value for `--group-add` is the in-container GID that
maps to the socket's owning GID; it is not necessarily the number `stat`
prints, and the rootful command above must not be copied blind. Identify the
actual socket and confirm rootless mode with `docker info`, read the mapping
from the daemon's `gid_map`, and verify the candidate GID with the dry-run
probe in [Checking access](#checking-access). If the probe still reports
permission denied, correct the mapping rather than trying another number.

### Read-only socket mounts do not restrict the API

Mounting the socket with `:ro` is useful hygiene, but it does not restrict the
Docker API. The flag governs the mounted socket inode, not the socket protocol:
`connect(2)` and the HTTP request bytes sent over the connection are not
filesystem writes, and the daemon performs the changes on its own side. The
mount covers only the socket file, not its parent directory. A read-only socket
mount still permits `docker run --privileged`, starting and stopping
containers, and every other daemon operation. Treat `:ro` as a convention, not
a security boundary: the security decision is whether the socket is mounted
and which GID can open it.

### Data-directory ownership

The image runs as UID 65532, so `data_dir` must be owned by that UID (or
otherwise writable by it). A directory owned by root and mode `0755` is not
writable, and a bind-mounted host directory keeps its host ownership.

Initialize a named volume once:

```sh
docker volume create smob-data
docker run --rm -v smob-data:/data busybox:1.36 chown 65532:65532 /data
```

Initialize a bind-mounted host directory the same way:

```sh
mkdir -p ./data
docker run --rm -v "$PWD/data:/data" busybox:1.36 chown 65532:65532 /data
```

Then mount it as `-v "$PWD/data:/data"`. The `chown` has to run as root or
through a container: an ordinary host user cannot give a file to another UID.
Running `chown 65532:65532` as an ordinary user fails. The mode does not need to be
group-writable: owner `65532` with mode `0755` is enough. Docker creates a fresh
named volume from the image's path when that path exists, but a `scratch` image
has no `/data`, so a new volume starts as `root:root` and needs this
initialization once. New files are then created as `65532:65532`.

For the repository [compose.yaml](../compose.yaml), the default volume is
project-scoped (`smob_smob-data` when the project is `smob`). `docker compose
config` prints the actual name; initialize that name once, or set `SMOB_DATA`
to a prepared host directory.

When the directory is not writable, `collect` probes the store before writing
any sample and exits with code **3**:

```text
smob: check /data: persistent store failure: open /data/.check-...: permission denied
```

A named volume that was never initialized fails the same way. With
`storage.primary: s3`, `spool_dir` defaults to `<data_dir>/spool`, so the same
volume and ownership cover the spool. A separate `spool_dir` on a bind mount
needs the same `chown`; a failed spool probe is also exit **3** and names the
spool:

```text
smob: check spool /data/spool: persistent store failure: mkdir /data/spool: permission denied
```

### Checking access

Run the startup probe with the same mounts the collector will use. It checks
the configuration, reads `/host/proc`, probes the store, and connects to the
Docker daemon when the source is enabled:

```sh
docker run --rm \
  --group-add "$(stat -c '%g' /var/run/docker.sock)" \
  -v /proc:/host/proc:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v smob-data:/data \
  -v "$PWD/smob.yaml:/etc/smob/smob.yaml:ro" \
  smob:local collect --dry-run
```

Exit code 0 and a final `configuration is valid; no data written` mean every
component was reachable. Without `--group-add`, or with a GID that does not
match the socket, the probe fails with exit code **1** before the store check:

```text
smob: docker: connect to /var/run/docker.sock: permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get "http://%2Fvar%2Frun%2Fdocker.sock/v1.41/info": dial unix /var/run/docker.sock: connect: permission denied
```

A socket that is not mounted at all reports a different connection error, also
exit **1**:

```text
smob: docker: connect to /var/run/docker.sock: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?
```

Because the Docker probe runs first, a bad socket hides a bad data directory;
fix them in that order. A data-directory failure exits **3**, as shown above.

Then take one snapshot and query it:

```sh
docker run --rm \
  --group-add "$(stat -c '%g' /var/run/docker.sock)" \
  -v /proc:/host/proc:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v smob-data:/data \
  -v "$PWD/smob.yaml:/etc/smob/smob.yaml:ro" \
  smob:local collect --once

docker run --rm \
  -v smob-data:/data:ro \
  -v "$PWD/smob.yaml:/etc/smob/smob.yaml:ro" \
  smob:local query --config /etc/smob/smob.yaml \
  --since 1h --scope container --limit 10
```

`query` needs neither the socket nor the socket group: it only reads the data
volume. A single snapshot has no previous counters, so it emits
`container.memory.used_bytes` for each running container and no CPU or network
rates; run the collector for a couple of intervals to get those. The one-shot
container is itself running, so it appears in the output unless
`sources.docker.exclude` matches it. A stopped container has no live stats and
does not appear.

| Symptom | Cause | Exit |
| --- | --- | --- |
| `docker: connect ... permission denied` | UID 65532 does not have the socket's GID | 1 |
| `docker: connect ... Cannot connect` | socket not mounted or wrong `sources.docker.socket` | 1 |
| `check /data: persistent store failure: ... permission denied` | `data_dir` or named volume not owned by 65532 | 3 |
| `check spool ...: persistent store failure: ... permission denied` | S3 spool directory not writable by 65532 | 3 |

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
