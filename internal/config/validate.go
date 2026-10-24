package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Validate reports every problem in the configuration at once. Each message
// names the offending field so a user can fix the file without guessing.
func (c Config) Validate() error {
	var errs []error

	switch {
	case c.Host == "":
		errs = append(errs, errors.New("host: must not be empty"))
	case !validHost(c.Host):
		errs = append(errs, fmt.Errorf("host: must be a single path component; got %q", c.Host))
	}
	if c.ProcPath == "" {
		errs = append(errs, errors.New("proc_path: must not be empty"))
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

	switch {
	case c.Storage.ChunkInterval <= 0:
		errs = append(errs, errors.New("storage.chunk_interval: must be greater than zero"))
	case c.Storage.ChunkInterval.Std()%time.Second != 0:
		errs = append(errs, fmt.Errorf("storage.chunk_interval: must be a whole number of seconds (minimum 1s); got %s", c.Storage.ChunkInterval))
	case time.Hour%c.Storage.ChunkInterval.Std() != 0:
		errs = append(errs, fmt.Errorf("storage.chunk_interval: must divide one hour evenly; got %s", c.Storage.ChunkInterval))
	}

	switch c.Storage.Primary {
	case "disk":
		if c.DataDir == "" {
			errs = append(errs, errors.New("data_dir: required when storage.primary is disk"))
		}
	case "s3":
		if c.Storage.S3.Bucket == "" {
			errs = append(errs, errors.New("storage.s3.bucket: required when storage.primary is s3"))
		}
		if c.Storage.S3.Region == "" && c.Storage.S3.Endpoint == "" {
			errs = append(errs, errors.New("storage.s3.region: required unless storage.s3.endpoint is set"))
		}
		if c.Storage.S3.Endpoint != "" {
			if err := validS3Endpoint(c.Storage.S3.Endpoint); err != nil {
				errs = append(errs, fmt.Errorf("storage.s3.endpoint: %w", err))
			}
		}
		if c.Storage.S3.SpoolDir == "" && c.DataDir == "" {
			errs = append(errs, errors.New("storage.s3.spool_dir: required when data_dir is empty"))
		}
	default:
		errs = append(errs, fmt.Errorf("storage.primary: must be disk or s3; got %q", c.Storage.Primary))
	}
	if c.Storage.S3.SpoolMaxBytes <= 0 {
		errs = append(errs, errors.New("storage.s3.spool_max_bytes: must be greater than zero"))
	}

	if !c.Sources.System.Enabled && !c.Sources.Process.Enabled && !c.Sources.Docker.Enabled {
		errs = append(errs, errors.New("sources: at least one source must be enabled"))
	}
	if c.Sources.System.Enabled && c.Sources.System.Interval <= 0 {
		errs = append(errs, errors.New("sources.system.interval: must be greater than zero"))
	}
	if c.Sources.Process.Enabled {
		if c.Sources.Process.TopN < 0 {
			errs = append(errs, errors.New("sources.process.top_n: must not be negative"))
		}
		if c.Sources.Process.Interval <= 0 {
			errs = append(errs, errors.New("sources.process.interval: must be greater than zero"))
		}
		if c.Sources.Process.RescanInterval <= 0 {
			errs = append(errs, errors.New("sources.process.rescan_interval: must be greater than zero"))
		}
		if c.Sources.Process.Filter != "" {
			if _, err := regexp.Compile(c.Sources.Process.Filter); err != nil {
				errs = append(errs, fmt.Errorf("sources.process.filter: %w", err))
			}
		}
	}
	if c.Sources.Docker.StreamStats && !c.Sources.Docker.Enabled {
		errs = append(errs, errors.New("sources.docker.stream_stats: requires sources.docker.enabled"))
	}
	if c.Sources.Docker.Enabled {
		if c.Sources.Docker.Socket == "" {
			errs = append(errs, errors.New("sources.docker.socket: required when the docker source is enabled"))
		}
		if c.Sources.Docker.Interval <= 0 {
			errs = append(errs, errors.New("sources.docker.interval: must be greater than zero"))
		}
		if c.Sources.Docker.Exclude != "" {
			if _, err := regexp.Compile(c.Sources.Docker.Exclude); err != nil {
				errs = append(errs, fmt.Errorf("sources.docker.exclude: %w", err))
			}
		}
		// Keep rejecting an enabled Docker source until it is registered with
		// the collector. Otherwise collect would start and silently emit no
		// container samples.
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

// validHost reports whether host can be used unchanged as a single path
// component in the chunk key layout.
func validHost(host string) bool {
	return host != "." && host != ".." && !strings.ContainsRune(host, '/')
}

// validS3Endpoint requires an absolute http or https URL for a custom S3
// endpoint.
func validS3Endpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("must be an http or https URL; got %q", endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("must include a host; got %q", endpoint)
	}
	return nil
}
