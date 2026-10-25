package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/confuzeus/smob/internal/store/chunk"
)

// Validate reports every problem in the configuration at once. Each message
// names the offending field so a user can fix the file without guessing.
func (c Config) Validate() error {
	var errs []error

	switch {
	case c.Host == "":
		errs = append(errs, errors.New("host: must not be empty"))
	case !chunk.IsComponent(c.Host):
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
		} else if err := validS3Bucket(c.Storage.S3.Bucket); err != nil {
			errs = append(errs, fmt.Errorf("storage.s3.bucket: %w", err))
		}
		if c.Storage.S3.Region == "" && c.Storage.S3.Endpoint == "" {
			errs = append(errs, errors.New("storage.s3.region: required unless storage.s3.endpoint is set"))
		}
		if c.Storage.S3.Endpoint != "" {
			if err := validS3Endpoint(c.Storage.S3.Endpoint); err != nil {
				errs = append(errs, fmt.Errorf("storage.s3.endpoint: %w", err))
			}
		}
		if err := validS3Prefix(c.Storage.S3.Prefix); err != nil {
			errs = append(errs, fmt.Errorf("storage.s3.prefix: %w", err))
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
	if c.Sources.System.Enabled {
		if err := validateInterval("sources.system.interval", c.Sources.System.Interval); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Sources.Process.Enabled {
		if c.Sources.Process.TopN < 0 {
			errs = append(errs, errors.New("sources.process.top_n: must not be negative"))
		}
		if err := validateInterval("sources.process.interval", c.Sources.Process.Interval); err != nil {
			errs = append(errs, err)
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
		if err := validateInterval("sources.docker.interval", c.Sources.Docker.Interval); err != nil {
			errs = append(errs, err)
		}
		if c.Sources.Docker.RescanInterval <= 0 {
			errs = append(errs, errors.New("sources.docker.rescan_interval: must be greater than zero"))
		}
		if c.Sources.Docker.Exclude != "" {
			if _, err := regexp.Compile(c.Sources.Docker.Exclude); err != nil {
				errs = append(errs, fmt.Errorf("sources.docker.exclude: %w", err))
			}
		}
	}

	if c.Retention.Disk.MaxAge < 0 {
		errs = append(errs, errors.New("retention.disk.max_age: must not be negative; zero disables the limit"))
	}
	if c.Retention.Disk.MaxBytes < 0 {
		errs = append(errs, errors.New("retention.disk.max_bytes: must not be negative; zero disables the limit"))
	}

	return errors.Join(errs...)
}

// validateInterval checks a source collection interval. The floor of one
// millisecond keeps every rate window represented by a positive interval_ms
// instead of rounding to zero, which would make the sample look like a gauge.
func validateInterval(field string, d Duration) error {
	switch {
	case d <= 0:
		return fmt.Errorf("%s: must be greater than zero", field)
	case d < Duration(time.Millisecond):
		return fmt.Errorf("%s: must be at least 1ms; got %s", field, d)
	}
	return nil
}

// validS3Endpoint requires an absolute http or https URL for a custom S3
// endpoint. Userinfo, a query, and a fragment are rejected because the AWS SDK
// silently drops them: the endpoint would validate here and then fail at
// HeadBucket with a confusing authentication or routing error.
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
	if u.User != nil {
		return fmt.Errorf("must not include userinfo; got %q", endpoint)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return fmt.Errorf("must not include a query string; got %q", endpoint)
	}
	if u.Fragment != "" {
		return fmt.Errorf("must not include a fragment; got %q", endpoint)
	}
	return nil
}

// validS3Bucket rejects a bucket name that no endpoint can use. It does not
// implement the AWS DNS rule set: MinIO and other compatible services accept
// names AWS itself would reject, and smob must keep working with them.
func validS3Bucket(bucket string) error {
	if len(bucket) > 255 {
		return fmt.Errorf("must be at most 255 bytes; got %d", len(bucket))
	}
	for i := 0; i < len(bucket); i++ {
		if c := bucket[i]; c < 0x20 || c == 0x7f {
			return fmt.Errorf("must not contain control characters; got %q", bucket)
		}
	}
	if strings.ContainsFunc(bucket, unicode.IsSpace) {
		return fmt.Errorf("must not contain whitespace; got %q", bucket)
	}
	return nil
}

// validS3Prefix rejects a key prefix that a normalising gateway could rewrite
// into a different object key. Surrounding slashes are trimmed during
// resolution, so an empty segment here means a repeated slash.
func validS3Prefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	for _, segment := range strings.Split(prefix, "/") {
		switch segment {
		case "":
			return fmt.Errorf("must not contain empty segments or repeated slashes; got %q", prefix)
		case ".", "..":
			return fmt.Errorf("must not contain %q segments; got %q", segment, prefix)
		}
	}
	return nil
}
