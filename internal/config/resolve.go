package config

import "path/filepath"

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
