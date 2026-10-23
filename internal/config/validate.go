package config

import (
	"errors"
	"fmt"
	"regexp"
)

// Validate reports every problem in the configuration at once. Each message
// names the offending field so a user can fix the file without guessing.
func (c Config) Validate() error {
	var errs []error

	if c.Interval <= 0 {
		errs = append(errs, errors.New("interval: must be greater than zero"))
	}
	if c.FlushInterval <= 0 {
		errs = append(errs, errors.New("flush_interval: must be greater than zero"))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log_level: must be one of debug, info, warn, error; got %q", c.LogLevel))
	}
	switch c.LogFormat {
	case "text", "json":
	default:
		errs = append(errs, fmt.Errorf("log_format: must be text or json; got %q", c.LogFormat))
	}

	switch c.Storage.Primary {
	case "disk":
		if !c.Storage.Disk.Enabled {
			errs = append(errs, errors.New("storage.primary: disk is not enabled"))
		}
	case "s3":
		if !c.Storage.S3.Enabled {
			errs = append(errs, errors.New("storage.primary: s3 is not enabled"))
		}
	default:
		errs = append(errs, fmt.Errorf("storage.primary: must be disk or s3; got %q", c.Storage.Primary))
	}
	if c.Storage.Disk.Enabled && c.DataDir == "" {
		errs = append(errs, errors.New("data_dir: required when disk storage is enabled"))
	}
	if c.Storage.S3.Enabled {
		if c.Storage.S3.Bucket == "" {
			errs = append(errs, errors.New("storage.s3.bucket: required when s3 storage is enabled"))
		}
		if c.Storage.S3.Region == "" && c.Storage.S3.Endpoint == "" {
			errs = append(errs, errors.New("storage.s3.region: required unless storage.s3.endpoint is set"))
		}
		if c.Storage.S3.ChunkInterval <= 0 {
			errs = append(errs, errors.New("storage.s3.chunk_interval: must be greater than zero"))
		}
		errs = append(errs, errors.New("storage.s3: not implemented in this build"))
	}

	if !c.Sources.System.Enabled && !c.Sources.Process.Enabled && !c.Sources.Docker.Enabled {
		errs = append(errs, errors.New("sources: at least one source must be enabled"))
	}
	if c.Sources.Process.Enabled {
		if c.Sources.Process.TopN < 0 {
			errs = append(errs, errors.New("sources.process.top_n: must not be negative"))
		}
		if c.Sources.Process.RescanInterval <= 0 {
			errs = append(errs, errors.New("sources.process.rescan_interval: must be greater than zero"))
		}
		if c.Sources.Process.Filter != "" {
			if _, err := regexp.Compile(c.Sources.Process.Filter); err != nil {
				errs = append(errs, fmt.Errorf("sources.process.filter: %w", err))
			}
		}
		errs = append(errs, errors.New("sources.process: not implemented in this build"))
	}
	if c.Sources.Docker.Enabled {
		if c.Sources.Docker.Socket == "" {
			errs = append(errs, errors.New("sources.docker.socket: required when the docker source is enabled"))
		}
		if c.Sources.Docker.Exclude != "" {
			if _, err := regexp.Compile(c.Sources.Docker.Exclude); err != nil {
				errs = append(errs, fmt.Errorf("sources.docker.exclude: %w", err))
			}
		}
		errs = append(errs, errors.New("sources.docker: not implemented in this build"))
	}

	if c.Retention.Disk.MaxAge < 0 {
		errs = append(errs, errors.New("retention.disk.max_age: must not be negative"))
	}
	if c.Retention.Disk.MaxBytes < 0 {
		errs = append(errs, errors.New("retention.disk.max_bytes: must not be negative"))
	}

	return errors.Join(errs...)
}
