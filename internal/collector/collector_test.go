package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store/disk"
)

// scriptedSource is a real source contract implementation whose failures are
// scripted. A mid-run source failure, such as a /proc file disappearing while
// the collector runs, cannot be produced on demand against the host's /proc.
type scriptedSource struct {
	scope    string
	interval time.Duration
	failures int
	calls    int
}

func (s *scriptedSource) Scope() string { return s.scope }

func (s *scriptedSource) Interval() time.Duration { return s.interval }

func (s *scriptedSource) Collect(_ context.Context, now time.Time) ([]model.Sample, error) {
	s.calls++
	if s.calls <= s.failures {
		return nil, errors.New("read /proc/stat: permission denied")
	}
	return []model.Sample{{
		Timestamp: now.UTC(),
		Metric:    "test.value",
		Value:     float64(s.calls),
	}}, nil
}

func newTestCollector(t *testing.T, sources []Source, logger *slog.Logger) (*Collector, *disk.Store) {
	t.Helper()
	st, err := disk.New(t.TempDir(), "", nil)
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	cfg := config.Config{
		Host:    "test-host",
		Storage: config.Storage{ChunkInterval: config.Duration(time.Second)},
	}
	return New(cfg, sources, st, logger), st
}

func TestRunContinuesAfterSourceError(t *testing.T) {
	src := &scriptedSource{scope: "system", interval: 5 * time.Millisecond, failures: 2}
	c, st := newTestCollector(t, []Source{src}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v after a source failure, want clean stop", err)
	}
	if src.calls <= src.failures {
		t.Fatalf("source called %d times, want retries after %d failures", src.calls, src.failures)
	}

	var samples int
	if err := st.Query(context.Background(), model.Query{}, func(model.Sample) error {
		samples++
		return nil
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if samples == 0 {
		t.Fatal("collector wrote no samples after a source failure")
	}
}

func TestRunRateLimitsSourceErrorLog(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	sources := []Source{
		&scriptedSource{scope: "system", interval: 5 * time.Millisecond, failures: 1 << 30},
		&scriptedSource{scope: "process", interval: 5 * time.Millisecond, failures: 1 << 30},
	}
	c, _ := newTestCollector(t, sources, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v, want clean stop", err)
	}

	logged := make(map[string]int)
	scanner := bufio.NewScanner(&logs)
	for scanner.Scan() {
		var record struct {
			Msg   string `json:"msg"`
			Scope string `json:"scope"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record.Msg == "source collection failed; continuing" {
			logged[record.Scope]++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read log: %v", err)
	}
	for _, scope := range []string{"system", "process"} {
		if got := logged[scope]; got != 1 {
			t.Errorf("%s failures logged %d times in 200ms, want 1 (rate-limited)", scope, got)
		}
	}
}
