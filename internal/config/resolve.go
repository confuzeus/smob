package config

import (
	"path/filepath"
	"strings"
)

// defaultS3Region is the signing region used for custom S3 endpoints when the
// configuration does not set one. MinIO and most S3-compatible services ignore
// the region but the AWS SDK still requires a value.
const defaultS3Region = "us-east-1"

// Resolve fills in defaults that depend on other fields. It runs after the
// configuration file and CLI overrides have been applied and before Validate,
// so the effective configuration is what the backend sees and what
// `smob config` prints.
func (c *Config) Resolve() {
	s3 := &c.Storage.S3

	// Normalise the S3 strings once so validation, the config dump, and the
	// backend all see the same value. Surrounding whitespace is never part of a
	// bucket, region, endpoint, or directory, and a key prefix is relative to
	// the bucket root.
	s3.Endpoint = strings.TrimSpace(s3.Endpoint)
	s3.Region = strings.TrimSpace(s3.Region)
	s3.Bucket = strings.TrimSpace(s3.Bucket)
	s3.SpoolDir = strings.TrimSpace(s3.SpoolDir)
	s3.Prefix = strings.Trim(strings.TrimSpace(s3.Prefix), "/")

	if s3.Region == "" && s3.Endpoint != "" {
		s3.Region = defaultS3Region
	}
	if s3.PathStyle == nil {
		pathStyle := s3.Endpoint != ""
		s3.PathStyle = &pathStyle
	}
	if s3.SpoolDir == "" && c.DataDir != "" {
		s3.SpoolDir = filepath.Join(c.DataDir, "spool")
	}
}
