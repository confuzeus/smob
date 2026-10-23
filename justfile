# Build and test smob.

binary := "smob"
staticcheck_version := "v0.8.2"
version := env_var_or_default("VERSION", `git describe --tags --always --dirty 2>/dev/null || echo dev`)
commit := env_var_or_default("COMMIT", `git rev-parse --short HEAD 2>/dev/null || echo none`)
date := env_var_or_default("DATE", `date -u +%Y-%m-%dT%H:%M:%SZ`)
ldflags := "-s -w -X main.version=" + version + " -X main.commit=" + commit + " -X main.date=" + date

# Build the static binary (default).
default: build

# Build the static smob binary.
build:
    CGO_ENABLED=0 go build -trimpath -ldflags '{{ldflags}}' -o {{binary}} ./cmd/smob

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
    SMOB_TEST_STRICT=1 go test -tags=integration -count=1 -shuffle=on -timeout=10m ./test/integration/...

# Run integration tests with a race-enabled smob binary.
test-race:
    SMOB_TEST_RACE=1 go test -tags=integration -count=1 -shuffle=on ./test/integration/...

# Update golden test files.
test-update:
    SMOB_TEST_UPDATE=1 go test -tags=integration -count=1 ./test/integration/...

# Build the Docker image.
docker:
    docker build --build-arg VERSION={{version}} --build-arg COMMIT={{commit}} --build-arg DATE={{date}} -t {{binary}}:{{version}} .

# Cross-compile release binaries into dist/.
release:
    rm -rf dist
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '{{ldflags}}' -o dist/smob-linux-amd64 ./cmd/smob
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '{{ldflags}}' -o dist/smob-linux-arm64 ./cmd/smob

# Remove build artifacts.
clean:
    rm -rf {{binary}} dist
