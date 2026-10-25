# Migrating from disk to S3

smob's disk and S3 backends use the same chunk format and the same relative key
layout:

```text
<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
```

`<root>` is `data_dir` on disk and `storage.s3.prefix` in the bucket. Migration
is therefore a copy of that tree followed by a configuration change. smob does
not mirror or dual-write: exactly one backend is active, and `query` and `stats`
read the same backend the collector writes. The reverse direction copies the
same tree with source and destination swapped; see
[Migrating back from S3 to disk](#migrating-back-from-s3-to-disk).

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

`data_dir` also holds the S3 spool and may hold leftover temporary files, so the
copy excludes them. Trailing slashes make the directory intent explicit:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --exclude "spool/*" \
  --exclude "*.tmp" \
  --exclude ".check-*"
```

`/var/lib/smob/` is the source directory contents. `s3://smob-metrics/smob/` is
the bucket plus `storage.s3.prefix`. The copy preserves the `<host>/<scope>/...`
components; they are part of the key, not a directory to flatten.

If `storage.s3.spool_dir` points at a different directory inside `data_dir`,
exclude that relative path instead of `spool/*`. The exclude follows the
configured spool, not the default name.

In testing with aws-cli/1.46.2, removing either trailing slash produced the same
keys: `s3 sync` treats both sides as directories in either form. The trailing
slashes are kept here to make that intent explicit. The dangerous mistake is
syncing the wrong directory level (see below), not the slash.

Add `--dryrun` to preview the uploads without writing objects.

### Re-running a sync

A resumed copy should use `--size-only`. Without it, `aws s3 sync` compares each
source file's size and modification time against the destination object's
`LastModified`, which is the time of the upload, not the time the chunk was
written. gzip rewrites also change the source mtime without changing the
samples. Object ETags identify compressed bytes, not samples, and `sync` does
not compare them by default, so neither timestamp nor ETag tells you whether a
run is complete. Expect a bare re-run to re-upload files that are already
correct.

For a re-run against the same, unchanged source, add `--size-only` to the
command above:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --size-only \
  --exclude "spool/*" \
  --exclude "*.tmp" \
  --exclude ".check-*"
```

With `--size-only`, a second run over the same source is a no-op: nothing is
uploaded and the key listing is unchanged. That is the expected result after an
interrupted copy, not a sign that the copy was skipped. `--size-only` compares
size alone, so it will also skip a destination object of the right size with
different bytes; it is a resumption aid, not a correctness check. The query
comparison below is what proves the samples arrived.

### Why the excludes matter

- `spool/*` is the local S3 spool, not disk chunk data. It lives at
  `<data_dir>/spool` by default and is used by the S3 store for chunks whose
  upload exhausted the retry budget. Without the exclude, its files are copied
  to `<prefix>/spool/...`, which is outside the host/scope chunk layout: `query`
  does not treat those objects as chunk data, and the replayer uploads the real
  spool entry from the local spool, not from the bucket copy. The copy is junk
  under the prefix — it contributes no samples, but it does inflate the object
  count, so the count check below no longer matches.
- Spool contents are never disposable. A spool from a previous S3 run is still
  the new S3 store's spool when `spool_dir` resolves to the same
  `<data_dir>/spool`; leave it in place and the replayer uploads the valid
  pending chunks after the switch. A spooled chunk may exist nowhere else, so
  never delete entries to tidy up. If a test needs a clean spool, point
  `spool_dir` at a new empty directory instead.
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

### Write semantics: sync overwrites, smob does not

smob uploads a chunk with a conditional `PUT` (`If-None-Match: *`). If an object
already exists at that host/window key, the write does not replace it: the
collector retries the same body under `-1`, `-2`, and so on. smob therefore
never overwrites an existing object for a window. The disk store enforces the
same rule with a filesystem primitive: it hard-links the finished chunk into
place and `link(2)` fails with `EEXIST` when the name is taken, so it suffixes
instead of replacing. Neither backend replaces an existing window.

`aws s3 sync` has no such guard. It uploads every source file whose destination
key is missing or differs in size or modification time, replacing the existing
object unconditionally. Syncing into a prefix that already holds chunks for the
same host overwrites them with the disk copy, and a window that exists only in
the bucket stays beside the new one under its own key.

The reverse copy behaves the same way. Syncing the bucket back into `data_dir`
replaces local chunk files whose keys already exist in the bucket. That is fine
when the bucket copy is the newer, authoritative one, but it is still an
overwrite: check the destination before you copy, not after.

Before syncing, check the destination for each host you are migrating. Forward,
that is the bucket prefix; reverse, it is the local tree you are about to
overwrite:

```sh
aws s3 ls --recursive s3://smob-metrics/smob/<host>/
find /var/lib/smob/<host> -type f -name '*.jsonl.gz' | wc -l
```

An empty listing is the expected forward state. A non-zero local count is
expected in reverse, because `data_dir` usually still holds the pre-migration
tree; the point is to know what you are overwriting. If the destination already
holds data, decide per key whether the existing chunks are intentional before
copying over them; smob will not restore what `sync` replaced.

### AWS and MinIO credentials

For AWS, use the standard profile chain:

```sh
aws s3 sync /var/lib/smob/ s3://smob-metrics/smob/ \
  --profile smob-migration \
  --region eu-west-1 \
  --exclude "spool/*" \
  --exclude "*.tmp" \
  --exclude ".check-*"
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
after an earlier copy. In the reverse direction `--delete` is worse: every local
file the bucket lacks is deleted, including the spool and any chunks the disk
collector wrote before the cutover. If a partial copy must be cleaned up, verify
the key lists first and remove objects or files deliberately. Do not use
`--delete` against real data in either direction.

`sync` compares object size and modification time, not content; add
`--size-only` for a re-run, as described above. The query comparison below is
the real check that the samples arrived.

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

### Checklist for one hour

A short pass that avoids hardcoded clock times and does not compare file
metadata. Do it for one host and one hour you know is fully inside the copied
range.

1. **Take the hour from the source path, not from the clock.** Pick one hour
   directory under `<root>/<host>/<scope>/YYYY/MM/DD/` (the disk tree for a
   forward copy, the bucket key for a reverse copy). The trailing
   `YYYY/MM/DD/HH/` components are the UTC hour the chunks cover, so
   `.../system/2026/10/24/00/` gives `START=2026-10-24T00:00:00Z` and
   `END=2026-10-24T01:00:00Z`. Verification never needs the exact timestamps of
   individual samples, only that hour boundary.
2. **Query both backends with the limit disabled.** Same `--host`, same hour,
   with `--limit 0 --format jsonl`: once with the disk config (before) and once
   with a config that sets `storage.primary: s3` (after).

   ```sh
   ./smob query --config disk.yaml --host web-1 --since "$START" --until "$END" \
     --limit 0 --format jsonl | sort > /tmp/disk.jsonl
   ./smob query --config s3.yaml --host web-1 --since "$START" --until "$END" \
     --limit 0 --format jsonl | sort > /tmp/s3.jsonl
   ```

3. **Compare sorted lines and require equal counts.** `wc -l` on both files must
   agree, and `diff -u /tmp/disk.jsonl /tmp/s3.jsonl` must be empty. Sorting
   removes scan order from the comparison, so an empty diff means the same
   sample set regardless of how each backend walked its store; a non-empty diff
   names the samples that differ or went missing. The counts catch a whole hour
   missing, the diff catches a single wrong sample.
4. **Do not check mtimes or `LastModified`.** `sync` stamps the destination at
   copy time and a gzip rewrite moves the source mtime, so neither says anything
   about samples. The sorted sample diff is the check.

### Wrong prefix and wrong source level

A wrong prefix does not fail loudly. With an empty local spool, `query` returns
an empty result (`[]` for `--format json`, no lines for JSONL) and exits 0.
`collect --dry-run` still succeeds because it checks the bucket with
`HeadBucket` and probes the spool; it does not inspect the prefix contents.
Confirm with `aws s3 ls --recursive s3://<bucket>/<prefix>/` that the keys are
where you expect.

`query` with `primary: s3` also merges the local spool. If that spool still
holds chunks, a wrong-prefix check can return spooled samples instead of an
empty result. Point `spool_dir` at a new empty directory when testing a prefix,
or let the collector drain the spool before the cutover. Never empty the spool
to get a clean result: a spooled chunk may exist nowhere else, so deleting it
loses samples.

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

## Migrating back from S3 to disk

The layout is identical in both directions, so the reverse move is a download
followed by the same configuration change. Use it when the bucket holds chunks
the old disk tree does not, for example when retiring S3 without losing the
samples written since the cutover.

1. **Drain the spool while the S3 store is still active.** Replay is started by
   `collect` under `primary: s3`; the disk store does not read
   `<data_dir>/spool`. Start the S3 collector once more, let the spool directory
   empty, and only then stop it. That run may also collect new chunks; they land
   in the bucket and are included in the download. A spool entry left behind
   stays local and is invisible to the disk store, because a spool entry is not
   a chunk in the layout: its name is a candidate key, but it lives under
   `<data_dir>/spool`, not under `<host>/<scope>/YYYY/MM/DD/HH/`. Never copy
   spool files into a host directory to "restore" them; replay is the only path
   from a spool entry to a published chunk.

2. **Stop the collector, then copy the bucket down.** Do not copy while it runs.

   ```sh
   aws s3 sync s3://smob-metrics/smob/ /var/lib/smob/ \
     --exclude "spool/*" \
     --exclude "*.tmp"
   ```

   The `<host>/<scope>/...` components are keys, so they arrive as directories.
   Do not flatten a host's prefix into `data_dir/<host>/`, and do not add
   `--delete`: on the download side it removes every local file the bucket
   lacks, including the spool and any pre-cutover chunks. A re-run takes
   `--size-only` for the same reason as above. This copy needs `s3:GetObject` as
   well as `s3:ListBucket` on the tooling identity, unlike the upload direction;
   see the IAM note below.

   The `--exclude "spool/*"` guards the destination: a bucket copy of spool
   objects is not chunk data, and restoring it into `<data_dir>/spool` would
   place junk where the next S3 run expects replayable entries.

3. **Check the destination and retention.** `data_dir` usually still holds the
   pre-migration tree, and `sync` overwrites same-key chunk files with the
   bucket copy. Either accept that overwrite (the bucket copy is the newer one)
   or download into a fresh directory and point `data_dir` at it. If a retention
   limit is configured, the first `collect` start runs the disk sweep
   immediately, so check `retention.disk.max_age` and
   `retention.disk.max_bytes` before starting it.

4. **Switch back.** Set `storage.primary: disk`, validate, and query one hour:

   ```yaml
   data_dir: /var/lib/smob
   storage:
     primary: disk
   ```

   The spool stays where it is; it is not part of the disk chunk tree and does
   not need to move with the data. If you later switch to S3 again, the same
   `spool_dir` makes a drained spool a no-op and keeps an undrained one
   replayable.

   Verify this copy with the [one-hour checklist](#checklist-for-one-hour)
   before relying on it: the disk config is the new state, and a config that
   still points at S3 is the old one.

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
  identity and needs `s3:ListBucket` and `s3:PutObject` for a disk-to-S3 copy,
  plus `s3:GetObject` for an S3-to-disk copy. The upload does not need
  `s3:GetObject`: `sync` only lists and uploads, and its size and modification
  time comparison comes from the listing. Verification reads go through
  `query`, which uses the runtime identity, or through `aws s3 ls`, which is
  `s3:ListBucket`. Do not reuse the runtime policy without checking those
  permissions.
- **No `--delete`.** Migration is additive; see the `--delete` section above.
- **Shared buckets need per-host checks.** A prefix can contain several hosts.
  Compare one host at a time, and never run a destination-wide delete.
- **S3 retention is a lifecycle rule.** smob does not call `DeleteObject`.
  Configure a bucket lifecycle rule only after the copy and verification are
  complete; lifecycle deletion is independent of smob's disk retention.
- **One writer.** The copy is a snapshot, not a second writer. Do not start an
  S3 collector against the same host and prefix until the disk collector has
  stopped.
