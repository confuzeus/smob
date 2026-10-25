# Pinned to the toolchain in go.mod (go 1.27.3): the floating 1.27-alpine tag
# moves with each patch release, so a rebuild could silently change compilers.
FROM golang:1.27.3-alpine AS build

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /smob ./cmd/smob

# Docker copies a new named volume from the image path, including its ownership.
# The empty directory gives /data to UID 65532, so an uninitialized volume is
# writable instead of root:root.
RUN mkdir -p /data

FROM scratch

# Redeclared because a build argument is only in scope in the stage that
# declares it; VERSION defaults above, so no new required build arg.
ARG VERSION=dev

LABEL org.opencontainers.image.title="smob" \
      org.opencontainers.image.description="Host, process, and container metrics collector" \
      org.opencontainers.image.source="https://github.com/confuzeus/smob" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /smob /smob
COPY --from=build --chown=65532:65532 /data /data

# MIT requires the notice to accompany copies of the binary.
COPY LICENSE /LICENSE

VOLUME ["/data"]

USER 65532:65532
ENTRYPOINT ["/smob"]
CMD ["collect", "--config", "/etc/smob/smob.yaml"]
