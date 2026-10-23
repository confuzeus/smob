BINARY  := smob
STATICCHECK_VERSION := v0.8.2
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: all build fmt vet lint test test-all test-race test-update docker release clean

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/smob

fmt:
	gofmt -l -w .

vet:
	go vet -tags=integration ./...

lint:
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -tags=integration ./...

test:
	go test -tags=integration -count=1 ./test/integration/...

test-all:
	SMOB_TEST_STRICT=1 go test -tags=integration -count=1 -shuffle=on -timeout=10m ./test/integration/...

test-race:
	SMOB_TEST_RACE=1 go test -tags=integration -count=1 -shuffle=on ./test/integration/...

test-update:
	SMOB_TEST_UPDATE=1 go test -tags=integration -count=1 ./test/integration/...

docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(BINARY):$(VERSION) .

release:
	rm -rf dist
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/smob-linux-amd64 ./cmd/smob
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/smob-linux-arm64 ./cmd/smob

clean:
	rm -rf $(BINARY) dist
