// Package s3 connects smob to S3-compatible object storage.
//
// The client uses the standard AWS credential chain: environment variables,
// shared configuration and credentials files, and instance or container roles.
// smob never stores secrets in its configuration file.
package s3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/confuzeus/smob/internal/config"
)

// Transport bounds. A blackholed endpoint that accepts the connection and
// never answers must fail the exchange, so smob's retry budget and spool engage
// instead of blocking the writer forever.
const (
	dialTimeout           = 5 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 30 * time.Second
	idleConnTimeout       = 90 * time.Second
	maxIdleConnsPerHost   = 16
)

// NewClient returns an S3 client for cfg. The configuration must already be
// resolved with config.Config.Resolve. NewClient does not contact the endpoint;
// Store.Check validates the bucket at startup. SDK log lines are routed through
// logger so --log-format json stays parseable; a nil logger discards them.
func NewClient(ctx context.Context, cfg config.S3Storage, logger *slog.Logger) (*awss3.Client, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("s3: bucket must not be empty")
	}
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("s3: region must not be empty")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithLogger(sdkLogger{logger: logger}),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}

	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle != nil && *cfg.PathStyle
		o.HTTPClient = newHTTPClient(awsCfg.HTTPClient)
		// smob owns retries so it can treat an ambiguous PutObject specially
		// and bound the budget exactly. The SDK retryer would multiply attempts
		// invisibly.
		o.Retryer = aws.NopRetryer{}
	}), nil
}

// sdkLogger adapts the AWS SDK's unstructured logger to slog. The SDK writes
// its own "SDK ... WARN ..." prefixes to stderr by default, which breaks JSON
// log consumers, so every line becomes one structured record at debug.
type sdkLogger struct {
	logger *slog.Logger
}

// Logf implements logging.Logger.
func (l sdkLogger) Logf(class logging.Classification, format string, v ...any) {
	l.logger.Debug("aws sdk", "classification", string(class), "message", fmt.Sprintf(format, v...))
}

// newHTTPClient returns the HTTP client the SDK uses. base is the client the
// SDK resolved during LoadDefaultConfig, which may already carry the custom CA
// bundle from AWS_CA_BUNDLE; replacing it outright would silently discard that
// trust configuration. When base is a buildable client, only the transport
// bounds are added; an unknown client is kept as-is because its timeouts cannot
// be injected safely.
func newHTTPClient(base aws.HTTPClient) aws.HTTPClient {
	if b, ok := base.(*awshttp.BuildableClient); ok {
		return withBounds(b).Freeze()
	}
	if base != nil {
		return base
	}
	return newHTTPBuilder().Freeze()
}

// newHTTPBuilder returns the builder behind newHTTPClient. It is separate so
// the package's tests can read the transport bounds, which the frozen client
// does not expose.
func newHTTPBuilder() *awshttp.BuildableClient {
	return withBounds(awshttp.NewBuildableClient())
}

// withBounds adds the transport bounds to an existing buildable client without
// disturbing its TLS configuration (including a custom CA bundle).
func withBounds(b *awshttp.BuildableClient) *awshttp.BuildableClient {
	dialer := newDialer()
	return b.
		WithDialerOptions(func(d *net.Dialer) {
			d.Timeout = dialer.Timeout
			d.KeepAlive = dialer.KeepAlive
		}).
		WithTransportOptions(func(t *http.Transport) {
			t.TLSHandshakeTimeout = tlsHandshakeTimeout
			t.ResponseHeaderTimeout = responseHeaderTimeout
			t.IdleConnTimeout = idleConnTimeout
			t.MaxIdleConnsPerHost = maxIdleConnsPerHost
		})
}

// newDialer returns the dialer carrying the connect bound. Keeping it in one
// place lets the package's tests assert the dial timeout, which net/http does
// not expose on the transport.
func newDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: awshttp.DefaultDialKeepAliveTimeout,
	}
}
