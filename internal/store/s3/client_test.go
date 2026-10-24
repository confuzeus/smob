package s3

import (
	"context"
	"path/filepath"
	"testing"

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
			client, err := NewClient(context.Background(), tc.cfg)
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
			if _, err := NewClient(context.Background(), tc.cfg); err == nil || err.Error() != tc.wantErr {
				t.Fatalf("NewClient error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
