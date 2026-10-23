FROM golang:1.27-alpine AS build

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

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /smob /smob

USER 65532:65532
ENTRYPOINT ["/smob"]
CMD ["collect", "--config", "/etc/smob/smob.yaml"]
