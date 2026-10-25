//go:build integration

// This file constructs the real S3 client, so it belongs to the extended suite
// with its siblings. The transport-timeout and AWS_CA_BUNDLE assertions stay
// because they are security properties the CLI cannot exercise
// deterministically: there is no observable knob for a dial timeout, and a
// bundle-only TLS endpoint cannot be arranged behind the binary.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/confuzeus/smob/internal/config"
)

// isolateAWS keeps client construction away from the developer's shared AWS
// configuration and the instance metadata service. No credentials are resolved
// until a request is signed.
func isolateAWS(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
}

func TestNewClientOptions(t *testing.T) {
	isolateAWS(t)

	pathStyle, virtualHost := true, false
	cases := []struct {
		name          string
		cfg           config.S3Storage
		wantRegion    string
		wantPathStyle bool
		wantEndpoint  string
	}{
		{
			name: "aws endpoint",
			cfg: config.S3Storage{
				Region:    "eu-west-1",
				Bucket:    "bucket",
				PathStyle: &virtualHost,
			},
			wantRegion:    "eu-west-1",
			wantPathStyle: false,
		},
		{
			name: "custom endpoint",
			cfg: config.S3Storage{
				Region:    "us-east-1",
				Bucket:    "bucket",
				Endpoint:  "http://127.0.0.1:9000",
				PathStyle: &pathStyle,
			},
			wantRegion:    "us-east-1",
			wantPathStyle: true,
			wantEndpoint:  "http://127.0.0.1:9000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient(context.Background(), tc.cfg, nil)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			opts := client.Options()
			if opts.Region != tc.wantRegion {
				t.Errorf("region = %q, want %q", opts.Region, tc.wantRegion)
			}
			if opts.UsePathStyle != tc.wantPathStyle {
				t.Errorf("path style = %t, want %t", opts.UsePathStyle, tc.wantPathStyle)
			}
			var endpoint string
			if opts.BaseEndpoint != nil {
				endpoint = *opts.BaseEndpoint
			}
			if endpoint != tc.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, tc.wantEndpoint)
			}
		})
	}
}

func TestNewClientTimeouts(t *testing.T) {
	isolateAWS(t)

	client, err := NewClient(context.Background(), config.S3Storage{
		Region: "us-east-1",
		Bucket: "bucket",
	}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, ok := client.Options().HTTPClient.(*http.Client); !ok {
		t.Fatalf("HTTP client = %T, want *http.Client", client.Options().HTTPClient)
	}

	transport := newHTTPBuilder().GetTransport()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"dial", newDialer().Timeout, 5 * time.Second},
		{"TLS handshake", transport.TLSHandshakeTimeout, 10 * time.Second},
		{"response header", transport.ResponseHeaderTimeout, 30 * time.Second},
		{"idle connection", transport.IdleConnTimeout, 90 * time.Second},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s timeout = %s, want %s", tc.name, tc.got, tc.want)
		}
	}
	if transport.MaxIdleConnsPerHost <= 0 {
		t.Errorf("max idle connections per host = %d, want a positive bound", transport.MaxIdleConnsPerHost)
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		t.Error("TLS verification is disabled")
	}
}

func TestNewClientRequiresBucketAndRegion(t *testing.T) {
	isolateAWS(t)

	cases := []struct {
		name    string
		cfg     config.S3Storage
		wantErr string
	}{
		{
			name:    "missing bucket",
			cfg:     config.S3Storage{Region: "us-east-1"},
			wantErr: "s3: bucket must not be empty",
		},
		{
			name:    "missing region",
			cfg:     config.S3Storage{Bucket: "bucket"},
			wantErr: "s3: region must not be empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewClient(context.Background(), tc.cfg, nil); err == nil || err.Error() != tc.wantErr {
				t.Fatalf("NewClient error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestClientRoutesSDKLogThroughSlog(t *testing.T) {
	isolateAWS(t)

	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := NewClient(context.Background(), config.S3Storage{
		Region: "us-east-1",
		Bucket: "bucket",
	}, logger)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	client.Options().Logger.Logf(logging.Warn, "unstructured %s line", "sdk")

	var record struct {
		Level          string `json:"level"`
		Msg            string `json:"msg"`
		Classification string `json:"classification"`
		Message        string `json:"message"`
	}
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatalf("SDK log line is not JSON: %v\nline: %s", err, out.String())
	}
	if record.Level != "DEBUG" || record.Msg != "aws sdk" {
		t.Errorf("record = %+v, want a DEBUG aws sdk record", record)
	}
	if record.Classification != "WARN" || record.Message != "unstructured sdk line" {
		t.Errorf("record = %+v, want the original classification and message", record)
	}
}

// TestClientPreservesCustomCABundle proves the transport bounds do not replace
// the HTTP client the SDK built from AWS_CA_BUNDLE: a TLS endpoint whose
// certificate is only in that bundle must verify.
func TestClientPreservesCustomCABundle(t *testing.T) {
	isolateAWS(t)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	cert := server.Certificate()
	bundle := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	t.Setenv("AWS_CA_BUNDLE", bundle)

	pathStyle := true
	client, err := NewClient(context.Background(), config.S3Storage{
		Region:    "us-east-1",
		Bucket:    "bucket",
		Endpoint:  server.URL,
		PathStyle: &pathStyle,
	}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := client.HeadBucket(context.Background(), &awss3.HeadBucketInput{
		Bucket: aws.String("bucket"),
	}); err != nil {
		t.Fatalf("HeadBucket with AWS_CA_BUNDLE: %v", err)
	}
}
