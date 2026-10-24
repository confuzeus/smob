# Migrating from disk to S3

smob's disk and S3 backends use the same chunk format and the same relative key
layout:

```text
<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
```

`<root>` is `data_dir` on disk and `storage.s3.prefix` in the bucket. Migration
is therefore a copy of that tree followed by a configuration change. smob does
not mirror or dual-write: exactly one backend is active, and `query` and `stats`
read the same backend the collector writes.

Run one collector per host and prefix. Stop the old writer before copying.
Completed chunks are immutable, but a running collector can create new chunks
and the disk retention sweeper can delete old ones while the copy is in progress.

## Before you start

1. **Stop the collector and wait for it to exit.** `SIGINT`/`SIGTERM` flushes the
   active chunk and exits 0; a second signal exits without flushing.

   ```sh
   sudo systemctl stop smob
   # or, for a foreground collector:
   # kill -TERM <pid>
   ```

   Do not copy while collection is running. If you cannot stop it, copy from a
   filesystem snapshot and accept that chunks written after the snapshot are not
   in the copy; there is no dual-write mode that fills the gap.

2. **Copy before the next disk start if retention is configured.** The disk
   retention sweeper runs once at `collect` startup and then hourly. Restarting
   the old disk collector before the copy therefore runs a sweep immediately,
   and it may delete exactly the chunks you intended to migrate.

3. **Create or choose the bucket and prefix.** The bucket must already exist;
   smob does not create it. The prefix in the copy destination must match
   `storage.s3.prefix` after smob trims leading and trailing slashes. For the
   credential chain, IAM policy, and MinIO endpoint fields, see
   [s3.md](s3.md). The credentials that run the copy are tooling credentials and
   are separate from the identity smob uses at runtime.

4. **Keep the old data directory.** Do not delete it after the copy. It is the
   rollback copy until you have verified the S3 deployment.

### Retention is frozen while S3 is active

The retention sweeper is wired to the disk store. `collect` runs it only when
the active store implements the sweeper, and the S3 store does not. Once
`storage.primary: s3`, the old disk tree is not swept: it is frozen in place,
which is what makes rollback possible. On rollback, if a retention limit is
configured, the disk collector runs a startup sweep immediately, so check
`retention.disk.max_age` and `retention.disk.max_bytes` before restarting it.

## Copy the chunks

The minimal command uses trailing slashes to make the directory intent explicit:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/
```

`/var/lib/smob/` is the source directory contents. `s3://smob-metrics/smob/` is
the bucket plus `storage.s3.prefix`. The copy preserves the `<host>/<scope>/...`
components; they are part of the key, not a directory to flatten.

In testing with aws-cli/1.46.2, removing either trailing slash produced the same
keys: `s3 sync` treats both sides as directories in either form. The trailing
slashes are kept here to make that intent explicit. The dangerous mistake is
syncing the wrong directory level (see below), not the slash.

A safe form for a data directory that may also contain the S3 spool or leftover
temporary files:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --exclude "spool/*" \
  --exclude "*.tmp" \
  --exclude ".check-*"
```

Add `--dryrun` to either command to preview the uploads without writing objects.

### Why the excludes matter

- `spool/*` is the local S3 spool, not disk chunk data. It lives at
  `<data_dir>/spool` by default and is used by the S3 store for chunks whose
  upload exhausted the retry budget. Without the exclude, its files are copied
  to `<prefix>/spool/...`, a path smob does not treat as disk data; if the
  replayer later uploads such a chunk, the same samples exist under two keys.
- A spool from a previous S3 run is still the new S3 store's spool when
  `spool_dir` resolves to the same `<data_dir>/spool`. Leave it in place and the
  replayer uploads valid pending chunks after the switch. If it is stale,
  remove it deliberately; do not copy it as disk data.
- `*.tmp` files are partial writes left by a crash or interrupted write. They
  are not chunks and are ignored by `query`.
- `.check-*` files are startup probe files.

A host literally named `spool` collides with the default `<data_dir>/spool`
directory, and `--exclude "spool/*"` would also skip that host. Use a different
host name, or point `storage.s3.spool_dir` somewhere outside `data_dir`, before
migrating such a host.

### Collision suffixes and path components

A window written twice is stored under `...-1.jsonl.gz`, `...-2.jsonl.gz`, and so
on. These are valid chunk names, not duplicates to rename or drop. `aws s3 sync`
preserves them, and `query` reads them. Leave the names and the
`<host>/<scope>/` components exactly as they are.

Do not sync `data_dir/<host>/` to the prefix root. That drops the `<host>`
component and the resulting keys do not match the layout, so `query` finds
nothing. Sync `data_dir/` so the host directory is copied as the first key
component.

### AWS and MinIO credentials

For AWS, use the standard profile chain:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --profile smob-migration \
  --region eu-west-1
```

`AWS_PROFILE` and the instance role work the same way. For MinIO or another
custom endpoint:

```sh
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --endpoint-url http://127.0.0.1:9000 \
  --exclude "spool/*" \
  --exclude "*.tmp" \
  --exclude ".check-*"
```

For MinIO, `storage.s3.path_style: true` is the matching setting. If your AWS
CLI profile forces virtual-hosted style, set `s3.addressing_style = path` in
the AWS config so the bucket stays in the URL path.

### Never use --delete for this migration

`aws s3 sync` is additive by default: it uploads missing objects and leaves
unrelated objects in the destination alone. With `--delete`, it removes every
destination object that is not present in the source. Against a real bucket
that can remove:

- chunks belonging to another host that shares the same prefix,
- chunks written by a collector after the copy started,
- a previous partial copy that is newer than the source, and
- objects that a bucket lifecycle rule or another tool placed under the prefix.

It also removes the destination copy of any chunk that disk retention deleted
after an earlier copy. If a partial copy must be cleaned up, verify the key
lists first and remove objects deliberately. Do not use `--delete` against real
data.

`sync` compares object size and modification time, not content. The query
comparison below is the real check that the samples arrived.

## Verify before switching

### Key counts

Compare the number of chunk files in the source with the number of objects in
the destination:

```sh
find /var/lib/smob -type f -name '*.jsonl.gz' \
  -not -path '/var/lib/smob/spool/*' | wc -l
aws s3 ls --recursive s3://smob-metrics/smob/ | wc -l
```

If the bucket and prefix already contain other hosts, compare one host at a
time:

```sh
find /var/lib/smob/web-1 -type f -name '*.jsonl.gz' | wc -l
aws s3 ls --recursive s3://smob-metrics/smob/web-1/ | wc -l
```

The counts include collision-suffixed files. Confirm that the excludes worked:

```sh
aws s3 ls --recursive s3://smob-metrics/smob/ \
  | grep -E '(/spool/|\.tmp$|\.check-)' || true
```

This should print nothing.

### Query the same range from both backends

Use one fixed absolute range that is fully inside the copied data, and disable
the sample limit. `--since 1h` is relative to the current time and can move
between the two commands, so use timestamps:

```sh
START=2026-10-24T00:00:00Z   # replace with the start of the copied range
END=2026-10-24T01:00:00Z     # replace with the end

./smob query --config disk.yaml --since "$START" --until "$END" \
  --limit 0 --format jsonl > /tmp/disk.jsonl
./smob query --config s3.yaml --since "$START" --until "$END" \
  --limit 0 --format jsonl > /tmp/s3.jsonl

wc -l /tmp/disk.jsonl /tmp/s3.jsonl
diff -u /tmp/disk.jsonl /tmp/s3.jsonl
```

An empty diff means the same sample count, timestamps, metric names, label sets,
values, and `interval_ms`, in the same global order. `--format json` with
`diff` also works; JSONL is simply easier to compare line by line. A
`smob stats` run over the same range is a quick cross-check.

Do **not** compare gzip bytes. Different gzip implementations or versions can
produce different compressed bytes for the same samples. The decoded sample
stream is the contract, and `smob query` decodes both backends. The sync copies
the bytes that disk wrote, but a later rewrite need not be byte-identical.

Check that collision suffixes survived:

```sh
find /var/lib/smob -type f -name '*.jsonl.gz' \
  | grep -E -- '-[0-9]+\.jsonl\.gz$'
aws s3 ls --recursive s3://smob-metrics/smob/ \
  | grep -E -- '-[0-9]+\.jsonl\.gz$'
```

The relative names should match. A `-1` chunk contains real samples, so the
query diff, not only the key count, is what proves the copy.

### Wrong prefix and wrong source level

A wrong prefix does not fail loudly. With an empty local spool, `query` returns
an empty result (`[]` for `--format json`, no lines for JSONL) and exits 0.
`collect --dry-run` still succeeds because it checks the bucket with
`HeadBucket` and probes the spool; it does not inspect the prefix contents.
Confirm with `aws s3 ls --recursive s3://<bucket>/<prefix>/` that the keys are
where you expect.

`query` with `primary: s3` also merges the local spool. If that spool still
holds chunks, a wrong-prefix check can return spooled samples instead of an
empty result. Point `spool_dir` at an empty directory when testing a prefix, or
empty the spool first.

Syncing `data_dir/<host>/` instead of `data_dir/` is the other silent failure:
the copied keys start with `<scope>/` and no host, so `query` finds no chunks
for that host.

## Switch the collector

Change the backend and keep `data_dir` for the default spool and the rolled-back
disk copy:

```yaml
data_dir: /var/lib/smob
storage:
  primary: s3
  s3:
    bucket: smob-metrics
    prefix: smob
```

For AWS add `region`; for MinIO add `endpoint`, `region`, and `path_style`. The
full field reference is in [configuration.md](configuration.md#storage) and the
MinIO example is in [s3.md](s3.md#custom-endpoints-and-minio). Do not copy the
S3 block from another deployment without checking the bucket and prefix.

With S3 active, the spool default is `<data_dir>/spool`; an explicit
`storage.s3.spool_dir` still wins.

Validate, probe, and start:

```sh
./smob config --config smob.yaml --validate
./smob collect --config smob.yaml --dry-run
sudo systemctl start smob
./smob query --config smob.yaml --since 1h --limit 5
```

`query` now reads S3. The old disk chunks and the local spool are handled
differently: the S3 store reads the bucket, and it merges a readable local spool
because spooled chunks have not been uploaded yet. The old disk chunk tree is
not read while `primary` is `s3`. Leave it in place for rollback.

`collect --dry-run` proves the bucket is reachable and the spool is writable; it
does not prove that the prefix contains the copied data. Do that check with
`query` or `aws s3 ls` before starting the service.

## Roll back

Stop the S3 collector, change `storage.primary` back to `disk`, and restart:

```sh
sudo systemctl stop smob
# set storage.primary: disk
sudo systemctl start smob
./smob query --config smob.yaml --since 1h --limit 5
```

Keep both copies until the S3 deployment is verified. The old disk tree was not
swept while S3 was active, but if a retention limit is configured, the rollback
start runs the disk sweep immediately. Check the retention limits first. Any
chunks written to S3 after the switch are not on disk, so an S3 query and a disk
query can differ by those later samples; that is expected, not a copy failure.

### If both writers ran

Concurrent collectors are unsupported. If a disk writer and an S3 writer both
ran for the same host and prefix, the same window can exist in both backends,
and collision suffixes can appear in either one:

- `query` emits every copy it finds, so the same timestamp, metric, and labels
  can appear more than once and the sample count is higher than expected.
- The disk and S3 key listings can show the same `<start>-<end>` with different
  `-1`/`-2` suffixes.
- A sync started after the S3 writer began can contain a window that the disk
  writer also produced, or can miss a window the S3 writer created next.

Compare the key lists and the sample sets before deleting anything. Decide
which copy is authoritative per window; do not assume a suffix is safe to drop.
There is no automatic merge or deduplication.

## Layout-specific caveats

- **Suffixes are part of the name.** `-1`, `-2`, and so on are valid chunks and
  must be preserved. Do not rename them to the unsuffixed name.
- **Host and scope are path components and labels.** Sync `data_dir/`, never
  `data_dir/<host>/`. Do not rename the host or scope directories. `host` is
  also the first key component and a sample label; `query --host NAME` filters
  the label and prunes by path, so data migrated under a different host name is
  not found under the new name.
- **Prefix must match.** `storage.s3.prefix` is trimmed of leading and trailing
  slashes. The copy destination must resolve to the same prefix. An empty prefix
  means the bucket root and changes the IAM policy; see
  [s3.md](s3.md#iam-policy).
- **IAM for the copy is separate.** smob's runtime identity needs the policy in
  [s3.md](s3.md#iam-policy). The identity that runs `aws s3 sync` is a tooling
  identity and needs `s3:ListBucket` and `s3:PutObject`, plus `s3:GetObject` for
  verification and head requests. Do not reuse the runtime policy without
  checking those permissions.
- **No `--delete`.** Migration is additive; see the `--delete` section above.
- **Shared buckets need per-host checks.** A prefix can contain several hosts.
  Compare one host at a time, and never run a destination-wide delete.
- **S3 retention is a lifecycle rule.** smob does not call `DeleteObject`.
  Configure a bucket lifecycle rule only after the copy and verification are
  complete; lifecycle deletion is independent of smob's disk retention.
- **One writer.** The copy is a snapshot, not a second writer. Do not start an
  S3 collector against the same host and prefix until the disk collector has
  stopped.
