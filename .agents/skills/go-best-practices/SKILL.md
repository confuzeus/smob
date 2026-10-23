---
name: go-best-practices
description: Use when writing, reviewing, refactoring, or debugging Go code in smob. Enforces lean, idiomatic Go — stdlib-first, small interfaces, explicit errors, testable /proc parsers — and bans premature abstraction.
---

# Go Best Practices for smob

## Stance

- Write idiomatic Go before clever Go. The standard library is the default; reach
  for a dependency only when it clearly earns its weight.
- Prefer the simplest implementation that is correct and testable. Optimize for
  the next reader, not for hypothetical reuse.
- Follow the Go version declared in `go.mod`. Do not use language features newer
  than it, and do not raise the version without the user's agreement.
- Match the conventions already in the package you are editing. A new pattern is
  a decision, not a style preference.

## Baseline gate

Every change must pass:

```sh
gofmt -l .            # must print nothing
go vet ./...
staticcheck ./...
go test -race ./...
go mod tidy
```

- Never hand-format code; run `gofmt`.
- Do not silence a staticcheck finding without a `//lint:ignore <check> <reason>`
  comment that explains why.
- Every exported identifier gets a doc comment that starts with its name.
- Do not add a third-party dependency without stating why in the change
  description. Already approved: `cobra`, `aws-sdk-go-v2`, the Docker Engine
  client, and a YAML decoder. Anything else needs a conversation first.

## Errors

- Wrap with `%w` and enough context to locate the failure:
  `fmt.Errorf("parse %s: %w", path, err)`. Include the operation and the path or
  key. Do not write `failed to` or repeat the same context at every layer.
- Error strings are lowercase, without trailing punctuation, and do not embed
  the function name.
- Log an error **or** return it, never both. Log once at the process boundary
  (`main`, the collect loop, the CLI runner). Library code returns.
- Use sentinel errors (`var ErrNotFound = errors.New(...)`) or test against
  stdlib types with `errors.Is` / `errors.As`. Do not build custom error
  hierarchies or wrapper types with fields.
- `panic` is for programmer invariants only. `os.Exit` belongs in `main` and the
  exit-code mapping in `internal/cli`; return a typed error from everything
  below and let the boundary decide the code.
- Aggregate independent validation failures with `errors.Join`, not by returning
  early on the first one.

## Functions and types

- Accept interfaces, return concrete types. Keep interfaces at the consumer and
  to 1–3 methods.
- Do not introduce an interface for a single implementation unless it is a real
  test seam. `store.Store` is justified because disk and S3 are peers; a new
  one-method interface is not.
- Constructors (`NewX`) exist to establish an invariant. If the zero value is
  usable, skip the constructor.
- Avoid `any` except at decode/serialization boundaries. If a value can be a
  string or an int, model it; do not store `map[string]any` in domain code.
- Preallocate slices and maps when the size is known. Otherwise leave it alone —
  no speculative micro-optimization.
- Keep functions short enough to read in one screen. Extract when there is a
  name for the piece, not to hit a line count.
- Make the zero value useful, and prefer value receivers unless the method
  mutates or the struct contains a mutex/map/channel.

## State, context, and concurrency

- No package-level mutable state, no `init()` with side effects, no global
  logger or config. Wire dependencies in `main` and pass them in.
- `context.Context` is the first parameter of anything that can block, and is
  never stored in a struct. Honor cancellation in collectors, stores, and
  network calls.
- Every goroutine has an owner and a shutdown path. Never start a
  fire-and-forget goroutine. If you cannot wait for it, do not start it.
- Prefer a mutex for shared state and channels for transferring ownership. Do
  not use channels as a general-purpose queue.
- Size buffered channels deliberately and document why. An arbitrary buffer is
  a hidden assumption.
- Never copy a value containing a `sync.Mutex`, and never read a shared field
  without the lock that guards it.
- Time is UTC internally. Use the monotonic clock for durations and rate
  deltas; never subtract wall-clock timestamps to compute a rate.

## smob-specific rules

- `/proc` parsers are pure functions over a reader:
  `func parseStat(r io.Reader) (Snapshot, error)`. No global paths, no
  `os.ReadFile` inside the parser, no hidden state. Test them from fixtures in
  `testdata/`.
- Rate math is a pure function of `(prev, cur, elapsed)`. Do not call
  `time.Now()` inside it; the collector supplies the clock.
- Sources produce snapshots. The collector owns the ticker, jitter, buffering,
  and flush cadence.
- Keep `store.Store` minimal: `Write`, `Query`, `Close`. Add `List`/`Delete`
  only when retention actually needs them. No speculative methods.
- Config is one struct tree decoded strictly (`yaml.Decoder.KnownFields(true)`).
  `Validate() error` reports every problem at once with `errors.Join`, and each
  message names the field.
- CLI commands use `cobra` `RunE` and return errors. Write to
  `cmd.OutOrStdout()` / `cmd.ErrOrStderr()`, never to `os.Stdout` directly, so
  output stays testable.
- Logging is `log/slog` with structured key/value pairs. No `fmt.Printf` outside
  the CLI's user-facing output.
- When a required input is missing (unreadable `/proc`), fail at startup. When
  an optional one is missing (Docker socket), disable that source with a
  warning — do not fail the process.

## Do not build

- Repository/service/handler layering. This is a CLI, not an enterprise
  application.
- Custom error hierarchies, error codes enums, or exception-like control flow.
- Functional options before a constructor has four or more optional parameters.
- `util`, `common`, `helpers`, or `types` packages. Name the package after what
  it does.
- Mocking frameworks or generated mocks. Write a small fake against a small
  interface.
- Reflection, plugin registries, event buses, or generic `Manager` types.
- Premature generics. Use them when they remove real, existing duplication.
- `time.Sleep` in tests. Inject a clock or call the pure function directly.
- Wrappers whose only job is to call another function unchanged.

## Testing

- Table-driven tests for parsers and rate math. One case per behavior, including
  malformed input and counter resets.
- Use `t.TempDir()` for filesystem tests and `testdata/` for fixtures. Do not
  depend on the host's `/proc`.
- Golden files are allowed only with an explicit `-update` flag and a diff on
  failure. Never rewrite a golden file silently.
- Test error paths. A parser that only has a happy-path test is untested.
- Call `t.Parallel()` when the test has no shared state; do not force it.
- Gate integration tests (MinIO, Docker-in-Docker) behind an environment
  variable or build tag so `go test ./...` stays fast and offline.
- Prefer deterministic tests over retries and generous timeouts.

## Definition of done

- [ ] `gofmt -l .` is empty; `go vet`, `staticcheck`, and `go test -race` pass.
- [ ] New behavior has a test; parser changes have fixtures.
- [ ] No new dependency, or its justification is stated.
- [ ] Errors carry context; nothing logs and returns the same error.
- [ ] Every goroutine has a shutdown path.
- [ ] Exported API has doc comments; config is validated.
- [ ] The diff is the smallest one that solves the problem.
