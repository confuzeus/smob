# Build and test smob.

binary := "smob"
staticcheck_version := "v0.8.2"
version := env_var_or_default("VERSION", `git describe --tags --always --dirty 2>/dev/null || echo dev`)
commit := env_var_or_default("COMMIT", `git rev-parse --short HEAD 2>/dev/null || echo none`)
date := env_var_or_default("DATE", `date -u +%Y-%m-%dT%H:%M:%SZ`)

# VERSION is also the Docker image tag, so it may only contain the tag alphabet:
# letters, digits, "_", ".", and "-" (at most 128 characters). A space makes the
# tag invalid and ":" would separate the repository from the tag. COMMIT is a git
# short hash and DATE is an RFC 3339 UTC timestamp. A space in any of the three
# splits the single -ldflags string the linker parses. Under --dry-run just
# prints backtick expressions instead of running them, so an unevaluated default
# is stripped here and validated by the real run.
metadata_guard := \
    if replace_regex(replace_regex(version, "`[^`]*`", ""), "^([A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?$", "") != "" { \
        error("VERSION " + quote(version) + " is not a valid Docker image tag: use letters, digits, '_', '.' and '-', with no whitespace or ':'") \
    } else if replace_regex(replace_regex(commit + date, "`[^`]*`", ""), "[^[:space:]]", "") != "" { \
        error("COMMIT " + quote(commit) + " and DATE " + quote(date) + " must not contain whitespace: the linker splits its -ldflags string on spaces") \
    } else { "" }

# The whole -ldflags string is quoted once at the command line. Values are not
# quoted individually: the linker takes everything after "=" literally, so inner
# quotes would become part of the version string. The guard rejects whitespace
# (the linker splits its flags on spaces) and VERSION's non-tag characters.
ldflags := "-s -w -X main.version=" + version + " -X main.commit=" + commit + " -X main.date=" + date

# Build the static binary (default).
default: build

# Build the static smob binary.
build:
    CGO_ENABLED=0 go build -trimpath -ldflags {{quote(ldflags)}} -o {{binary}} ./cmd/smob {{metadata_guard}}

# Format Go source.
fmt:
    gofmt -l -w .

# Run go vet with integration tags.
vet:
    go vet -tags=integration ./...

# Run staticcheck.
lint:
    go run honnef.co/go/tools/cmd/staticcheck@{{staticcheck_version}} -tags=integration ./...

# Run pure-function tests (no build tag).
test-unit:
    go test -count=1 ./...

# Run the fast integration suite.
test: test-unit
    go test -tags=integration -count=1 ./test/integration/...

# Run the extended integration suite in strict mode.
test-all:
    SMOB_TEST_STRICT=1 go test -tags=integration -count=1 -shuffle=on -timeout=10m ./test/integration/... ./internal/store/... ./internal/source/docker/...

# Run integration tests with a race-enabled smob binary.
test-race:
    SMOB_TEST_RACE=1 go test -tags=integration -count=1 -shuffle=on ./test/integration/...

# Run the storage and Docker source integration tests under the race detector.
# CI runs this in the extended job, where MinIO and Docker are required.
test-race-extended:
    CGO_ENABLED=1 go test -race -tags=integration -count=1 ./internal/store/... ./internal/source/docker/...

# Update golden test files; update mode is forced over any inherited value.
test-update:
    SMOB_TEST_UPDATE=1 go test -tags=integration -count=1 ./test/integration/...

# Scoped to testdata/golden, the only fixture tree update mode writes.
# Regenerate goldens and fail if the committed snapshots change; CI runs this.
test-golden-check: test-update
    git diff --exit-code -- test/integration/testdata/golden

# Build the Docker image.
docker:
    docker build --build-arg VERSION={{quote(version)}} --build-arg COMMIT={{quote(commit)}} --build-arg DATE={{quote(date)}} -t {{quote(binary + ":" + version)}} . {{metadata_guard}}

# Cross-compile release binaries into dist/.
release:
    rm -rf dist
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags {{quote(ldflags)}} -o dist/smob-linux-amd64 ./cmd/smob {{metadata_guard}}
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags {{quote(ldflags)}} -o dist/smob-linux-arm64 ./cmd/smob {{metadata_guard}}

# Remove build artifacts.
clean:
    rm -rf {{binary}} dist
