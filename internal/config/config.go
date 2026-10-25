// Package config defines smob's YAML configuration, its defaults, and its
// validation rules.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultPath is where smob looks for its configuration when --config is not
// set.
const DefaultPath = "/etc/smob/smob.yaml"

// Duration is a time.Duration that decodes from YAML strings such as "10s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var text string
	if err := value.Decode(&text); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("line %d, column %d: %w", value.Line, value.Column, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler so dumps stay human-readable.
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// Std returns the duration as a time.Duration.
func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// String returns the duration in Go syntax, such as "5m0s".
func (d Duration) String() string {
	return time.Duration(d).String()
}

// Config is the root of the YAML configuration.
type Config struct {
	DataDir   string    `yaml:"data_dir"`
	ProcPath  string    `yaml:"proc_path"`
	Host      string    `yaml:"host"`
	LogLevel  string    `yaml:"log_level"`
	LogFormat string    `yaml:"log_format"`
	Sources   Sources   `yaml:"sources"`
	Storage   Storage   `yaml:"storage"`
	Retention Retention `yaml:"retention"`
}

// Sources selects which metric sources the collector runs.
type Sources struct {
	System  SystemSource  `yaml:"system"`
	Process ProcessSource `yaml:"process"`
	Docker  DockerSource  `yaml:"docker"`
}

// SystemSource configures host-wide metrics from /proc.
type SystemSource struct {
	Enabled  bool     `yaml:"enabled"`
	Interval Duration `yaml:"interval"`
}

// ProcessSource configures per-process metrics from /proc/[pid].
type ProcessSource struct {
	Enabled        bool     `yaml:"enabled"`
	Filter         string   `yaml:"filter"`
	TopN           int      `yaml:"top_n"`
	Interval       Duration `yaml:"interval"`
	RescanInterval Duration `yaml:"rescan_interval"`
}

// DockerSource configures per-container metrics from the Docker Engine API.
type DockerSource struct {
	Enabled  bool     `yaml:"enabled"`
	Socket   string   `yaml:"socket"`
	Exclude  string   `yaml:"exclude"`
	Interval Duration `yaml:"interval"`
	// RescanInterval is how often the running container set is enumerated for
	// new and removed containers.
	RescanInterval Duration `yaml:"rescan_interval"`
	// StreamStats keeps one persistent stats stream per container instead of
	// polling one-shot stats on each interval. It is opt-in because each stream
	// costs a goroutine and an HTTP connection for the life of the container.
	StreamStats bool `yaml:"stream_stats"`
}

// Storage selects and configures the persistence backend.
type Storage struct {
	Primary       string    `yaml:"primary"`
	ChunkInterval Duration  `yaml:"chunk_interval"`
	S3            S3Storage `yaml:"s3"`
}

// S3Storage configures the S3-compatible backend.
type S3Storage struct {
	Endpoint      string `yaml:"endpoint"`
	Region        string `yaml:"region"`
	Bucket        string `yaml:"bucket"`
	Prefix        string `yaml:"prefix"`
	PathStyle     *bool  `yaml:"path_style"`
	SpoolDir      string `yaml:"spool_dir"`
	SpoolMaxBytes int64  `yaml:"spool_max_bytes"`
}

// Retention bounds the data kept by the disk backend. S3 retention is left to
// a bucket lifecycle policy.
type Retention struct {
	Disk DiskRetention `yaml:"disk"`
}

// DiskRetention limits chunks by age or total size. A zero value disables that
// limit.
type DiskRetention struct {
	MaxAge   Duration `yaml:"max_age"`
	MaxBytes int64    `yaml:"max_bytes"`
}

// Default returns the configuration applied before the file is decoded, so a
// file only needs to state what differs from these values.
func Default() Config {
	return Config{
		DataDir:   "/var/lib/smob",
		ProcPath:  "/proc",
		LogLevel:  "info",
		LogFormat: "text",
		Sources: Sources{
			System: SystemSource{
				Enabled:  true,
				Interval: Duration(10 * time.Second),
			},
			Process: ProcessSource{
				TopN:           20,
				Interval:       Duration(30 * time.Second),
				RescanInterval: Duration(30 * time.Second),
			},
			Docker: DockerSource{
				Socket:         "/var/run/docker.sock",
				Interval:       Duration(10 * time.Second),
				RescanInterval: Duration(30 * time.Second),
				StreamStats:    false,
			},
		},
		Storage: Storage{
			Primary:       "disk",
			ChunkInterval: Duration(5 * time.Minute),
			S3: S3Storage{
				SpoolMaxBytes: 1 << 30,
			},
		},
	}
}

// maxConfigBytes caps how much of a configuration file is read. A real
// configuration is a few kilobytes; the cap keeps a misdirected --config, such
// as a large log or a device, from consuming memory proportional to the file.
const maxConfigBytes = 4 << 20 // 4 MiB

// Load reads and decodes the configuration at path. The file is decoded
// strictly: an unknown field is an error, never silently ignored, and a file
// larger than 4 MiB is a usage error. Host defaults to the machine's hostname
// when the file does not set one.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()

	// Read one byte past the cap so a file of exactly maxConfigBytes still
	// decodes, while anything larger leaves the reader exhausted.
	r := &io.LimitedReader{R: f, N: maxConfigBytes + 1}
	cfg, err := Parse(r)
	if r.N <= 0 {
		return Config{}, fmt.Errorf("config: file %s exceeds %d MiB limit", path, maxConfigBytes>>20)
	}
	if err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Host == "" {
		host, err := os.Hostname()
		if err != nil {
			return Config{}, fmt.Errorf("resolve host: %w", err)
		}
		cfg.Host = host
	}
	return cfg, nil
}

// Parse decodes a configuration from r on top of Default. It does not validate;
// callers apply flag overrides first and then call Validate.
func Parse(r io.Reader) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return cfg, nil
		}
		return Config{}, err
	}

	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("multiple YAML documents are not supported")
		}
		return Config{}, err
	}
	return cfg, nil
}
