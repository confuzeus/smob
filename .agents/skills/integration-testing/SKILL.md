---
name: integration-testing
description: Use when writing, running, reviewing, or debugging tests in smob. smob has no unit tests; every test builds the smob binary and drives it as a black box against real /proc, a temporary data directory, and optional MinIO or Docker services.
---

# Integration Testing for smob

## Stance

- Every test is an integration test. Build the `smob` binary and run it as a
  subprocess. Assert on the exit code, stdout, stderr, files in the data
  directory, and objects in S3. Nothing else.
- There are no unit tests. Do not create `_test.go` files under `internal/` or
  `cmd/`, and do not call unexported functions from a test.
- Do not mock, fake, or stub `/proc`, the filesystem, the clock, `store.Store`,
  the Docker API, or the S3 API. If a behavior cannot be exercised through the
  binary against real dependencies, the design is wrong, not the test.
- Do not add production code that exists only for tests: no hidden `--test-*`
  flags, no injectable clock, no exported internals.
- Linux only. A test that cannot run on the current platform skips with a
  reason; it does not silently pass.
- Tests live in `test/integration` and carry `//go:build integration`. Run them
  with `go test -tags=integration ./test/integration/...`.

## What counts

| Covered by an integration test              | Not covered here                        |
| ------------------------------------------- | --------------------------------------- |
| CLI behavior: flags, exit codes, formats    | Unexported helpers                      |
| Config decoding and validation via the CLI  | Individual parser functions             |
| `/proc` collection on the real host         | Malformed `/proc` fixtures              |
| Disk chunk layout and JSONL schema          | Byte-exact snapshots of internals       |
| S3/MinIO and Docker sources and stores      | Third-party library behavior            |
| Shutdown, signals, retries, and fallbacks   | Timing internals that are unobservable  |

The CLI and the on-disk/S3 formats are the contract. Do not import `internal/...`
or `cmd/...` from the test package; mirror the documented wire types locally.
That duplication is intentional: a schema change should break the contract test
and force a deliberate update.

## Layout

```text
test/integration/
  main_test.go       # TestMain: builds the binary once
  harness_test.go    # run/start/wait/read helpers
  config_test.go
  system_test.go
  process_test.go
  s3_test.go
  docker_test.go
  retention_test.go
  testdata/
    configs/         # valid and invalid config bodies
    golden/          # stable CLI and chunk-format output
  compose.yaml       # MinIO and any Docker fixtures
```

Every Go file in this package starts with `//go:build integration` and declares
`package integration`. The package uses the standard library only; no assertion
library, no testcontainers, no mock framework.

## Harness

### Build the binary once

```go
var smobBin string

func TestMain(m *testing.M) {
	root := moduleRoot()
	dir, err := os.MkdirTemp("", "smob-it-*")
	if err != nil {
		panic(err)
	}
	smobBin = filepath.Join(dir, "smob")

	args := []string{"build", "-o", smobBin}
	if os.Getenv("SMOB_TEST_RACE") == "1" {
		args = append(args, "-race")
	}
	args = append(args, "./cmd/smob")

	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build smob: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found above test directory")
		}
		dir = parent
	}
}
```

Build with the repository's normal `-ldflags` for version output. Build once per
`go test` invocation; never rebuild inside a test.

### Run one-shot commands

```go
type result struct {
	Stdout, Stderr string
	ExitCode       int
}

func runSmob(t *testing.T, ctx context.Context, args ...string) result {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()
	res := result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run smob %v: %v", args, err)
		}
		res.ExitCode = exitErr.ExitCode()
	}
	return res
}
```

### Run a daemon

Every `collect` test starts a daemon, waits for observable output, sends
`SIGINT`, and waits for a clean exit. Never leave a process behind.

```go
type daemon struct {
	cmd     *exec.Cmd
	stderr  bytes.Buffer
	done    chan struct{}
	waitErr error
}

func startSmob(t *testing.T, ctx context.Context, args ...string) *daemon {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }

	d := &daemon{cmd: cmd, done: make(chan struct{})}
	cmd.Stderr = &d.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start smob: %v", err)
	}
	go func() {
		d.waitErr = cmd.Wait()
		close(d.done)
	}()

	t.Cleanup(func() {
		select {
		case <-d.done:
		default:
			_ = cmd.Process.Kill()
			<-d.done
		}
		if t.Failed() {
			t.Logf("smob stderr:\n%s", d.stderr.String())
		}
	})
	return d
}

func (d *daemon) stop(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := d.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal smob: %v", err)
	}
	select {
	case <-d.done:
		if d.waitErr != nil {
			t.Fatalf("smob did not exit cleanly: %v\n%s", d.waitErr, d.stderr.String())
		}
	case <-time.After(timeout):
		t.Fatalf("smob did not exit within %s\n%s", timeout, d.stderr.String())
	}
}
```

### Wait on observable state

```go
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
```

That poll interval is the only `time.Sleep` allowed in a test. Wait on something
observable: a chunk file appears, a line is printed, a container reports stats.
Never sleep "long enough" for a flush.

### Arrange state

- `t.TempDir()` per test, with a unique data directory.
- `writeConfig` writes a complete config body to `<tmp>/smob.yaml`; never rely
  on a developer's real config or environment.
- Seeding old chunk files, pre-existing data dirs, and invalid config files is
  legitimate arrangement. Faking the systems that produce them is not.
- For S3, create a unique bucket per test and delete it in `t.Cleanup`.
- For Docker, use a unique container name and remove it in `t.Cleanup`.

### Read output

- Decode JSON with `encoding/json` into local structs that mirror the documented
  schema.
- Read a chunk by walking the data dir, opening the `.jsonl.gz` file with
  `compress/gzip`, and scanning one `json.Unmarshal` per line.
- Configure JSON logs and parse fields. Do not string-match prose log messages.
- Parse table and CSV output by column. Whole-string comparison is only for
  golden `--help` text.

### Optional services

```go
func requireS3(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("SMOB_TEST_S3_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal("SMOB_TEST_STRICT=1 but SMOB_TEST_S3_ENDPOINT is not set")
		}
		t.Skip("SMOB_TEST_S3_ENDPOINT not set; run `make test-all`")
	}
	return endpoint
}
```

`requireDocker` does the same for the Docker socket. Skip messages must say how
to provide the dependency. In strict mode a missing dependency is a failure, not
a skip.

## Writing a test

Arrange, act, assert:

```go
func TestSystemCollectThenQuery(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := writeConfig(t, dir, fmt.Sprintf(`
interval: 200ms
flush_interval: 200ms
data_dir: %s
sources:
  system:
    enabled: true
storage:
  disk:
    enabled: true
`, dataDir))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "first chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	d.stop(t, 10*time.Second)

	samples := readSamples(t, dataDir)
	if len(samples) == 0 {
		t.Fatal("collect wrote no samples")
	}
	assertWellFormed(t, samples)

	out := runSmob(t, ctx, "query", "--config", cfg, "--since", "1m", "--format", "json")
	if out.ExitCode != 0 {
		t.Fatalf("query failed: exit %d\n%s", out.ExitCode, out.Stderr)
	}
	var queried []sample
	if err := json.Unmarshal([]byte(out.Stdout), &queried); err != nil {
		t.Fatalf("decode query output: %v\n%s", err, out.Stdout)
	}
	if len(queried) == 0 {
		t.Fatal("query returned no samples")
	}
}
```

Field names follow the documented config and wire schema. Update this file when
the schema changes; the friction is the point.

Useful helpers to keep in `harness_test.go`:

```go
func writeConfig(t *testing.T, dir, body string) string
func countChunks(t *testing.T, dataDir string) int
func readSamples(t *testing.T, dataDir string) []sample
func assertWellFormed(t *testing.T, samples []sample)
```

## Assertions that survive real data

- Never assert exact CPU %, RSS, or B/s. Assert finite, non-negative, and inside
  a broad bound (CPU at most `100 * runtime.NumCPU()` for system scope, memory at
  most `MemTotal`).
- Assert relationships, not magnitudes: loopback traffic raises RX/TX; a CPU
  burner raises that process's CPU above zero; a stopped container produces no
  new samples.
- Timestamps are UTC and non-decreasing per series. Sample spacing is roughly
  the configured interval; allow for scheduling delay and jitter.
- Assert labels exactly: `scope`, `pid`, `comm`, `container`, `image` where
  documented. An unexpected label is a failure.
- Assert counts as ranges (`>= 2` samples), not exact counts.
- For disk/S3 parity, compare key layout, metric names, label sets, and
  ordering. Do not compare live values.
- Golden files only for deterministic output: `--help`, table headers, chunk
  schema. Update them with an explicit `-update` flag and review the diff.

## Failure paths

Cover these through the CLI wherever they are observable:

- Invalid config: table of bodies → non-zero exit, stderr names the field.
- Unknown config field → rejected, never ignored.
- Unwritable data dir → startup failure naming the path. Skip when
  `os.Geteuid() == 0`; root bypasses permissions.
- Bad S3 endpoint or credentials → clear error or the documented fallback;
  assert exit code and structured log fields.
- S3 outage mid-collect with disk fallback configured → collection continues and
  chunks land on disk.
- Missing Docker socket → only the Docker source is disabled with a warning;
  system collection still works.
- Docker daemon restart → reconnect, or a documented gap; never a crash.
- `SIGINT` and `SIGTERM` → final flush and exit 0.
- Disk full, unreadable `/proc`, and clock jumps: document them as manual or
  untested. Do not pretend a fake covers them.

## Flake control

- No `time.Sleep` outside `waitFor`. Poll for observable state with a deadline.
- No retry loops around assertions. A test that needs three attempts is broken.
- Unique temp dirs, buckets, container names, and config paths per test. No
  shared globals.
- Do not depend on ambient state: other processes, existing containers, host
  network traffic, CPU load, or a pre-existing data dir.
- Metric-sensitive tests run serially. Use `t.Parallel()` only for tests that
  share no service and assert structural output.
- Run CI with `-count=1 -shuffle=on` to catch order dependence.
- Set per-test contexts and a suite timeout. A hung daemon must fail fast with
  its stderr attached.
- Never widen a bound or add a sleep to silence a flaky test. Find the race or
  the wrong assumption.

## CI and commands

```sh
go vet -tags=integration ./...
staticcheck -tags=integration ./...
go test -tags=integration -count=1 ./test/integration/...

# Race coverage for the binary under test, not just the test process.
SMOB_TEST_RACE=1 go test -tags=integration -count=1 ./test/integration/...

# Full suite with compose services already running.
SMOB_TEST_STRICT=1 SMOB_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  go test -tags=integration -count=1 -timeout=10m ./test/integration/...
```

Required Make targets (Phase 0):

| Target        | Behavior                                              |
| ------------- | ----------------------------------------------------- |
| `test`        | Fast suite; service tests skip with a reason          |
| `test-all`    | Compose services up, strict mode, full suite          |
| `test-race`   | Suite with `SMOB_TEST_RACE=1`                         |
| `test-update` | Suite with the golden update flag                     |

CI runs `test-all` and `test-race` on Linux, and the fast suite on both Debian
and Alpine kernels. `go test -race` by itself only instruments the test binary;
`SMOB_TEST_RACE=1` is what makes `TestMain` build `smob` with the race detector.
The race build needs `CGO_ENABLED=1`, so run it on the Debian runner and keep
release builds `CGO_ENABLED=0`.

Do not gate on coverage percentages. The phase exit criteria in the README are
the gate.

## Scenario map

**Phase 0 — foundations**

- `smob version` prints version, commit, and build date from ldflags.
- `smob config --validate` accepts valid configs and rejects a table of invalid
  ones.
- `--help` for every subcommand matches a golden file.

**Phase 1 — system and disk**

- Collect for a few intervals, stop cleanly, then query CPU, memory, and network
  for the host.
- Chunk paths match `<data>/<scope>/YYYY/MM/DD/HH/...jsonl.gz`; every line
  decodes into the documented schema.
- Rates are finite and sane, timestamps are monotonic, and no gap exceeds the
  interval tolerance.
- Table, JSON, and CSV output all parse and carry the same samples.
- An unwritable data dir fails startup.

**Phase 2 — process**

- A CPU-burning helper appears under `comm`/`pid` labels with CPU above zero.
- The regex filter includes matching processes and excludes others; top-N
  returns at most N series.
- Restarting the helper produces a new series keyed by start time instead of a
  corrupted rate.

**Phase 3 — S3**

- MinIO round-trip: collect, query, and get the same metric names and labels as
  the disk backend.
- Object keys mirror the disk layout.
- Bad credentials fail clearly; an outage with disk fallback configured keeps
  collecting.
- A bounded time range reads only the expected key prefixes.

**Phase 4 — Docker**

- A labeled test container produces `scope=container`, `container`, and `image`
  samples.
- `docker.exclude` removes matching containers.
- A missing socket disables only the Docker source.
- A Docker restart either reconnects or records a documented gap.

**Phase 5 — polish**

- Retention deletes chunks beyond age or size and keeps fresh ones.
- `tail` streams new samples until interrupted.
- `stats` and `query --agg/--group-by` return expected aggregates over seeded
  data.

## Anti-patterns

- A `_test.go` file under `internal/` "just for this parser".
- A `fakeStore`, `fakeClock`, `fakeProc`, or `httptest` server standing in for
  MinIO.
- Asserting `cpu == 12.5` or `rx == 4096`.
- Sleeping for the flush interval instead of polling for the chunk.
- Sharing one data dir or bucket across tests.
- Retrying a failing assertion.
- Updating golden files in CI or without reading the diff.
- Chasing coverage or testing stdlib and third-party behavior.
- Leaving a `collect` process, container, or bucket behind on failure.

## Definition of done

- [ ] New or changed behavior is reachable through the CLI and has an
      integration test.
- [ ] At least one observable failure path is covered: bad config, bad
      dependency, signal, or fallback.
- [ ] The test waits on observable state; no fixed sleeps or retries.
- [ ] Temp dirs, buckets, containers, and processes are cleaned up.
- [ ] `make test` passes; `make test-all` passes when services are available.
- [ ] `SMOB_TEST_RACE=1` passes for collector or concurrency changes.
- [ ] Golden changes were reviewed, not auto-accepted.
- [ ] No production code was added solely to make testing easier.
