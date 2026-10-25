package process

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// recordingHandler keeps log records so a source test can assert the skip
// warning policy. It is the only way to observe the once-per-rescan rule; the
// log is not part of the CLI's structured output for the process scope.
type recordingHandler struct {
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, rec slog.Record) error {
	h.records = append(h.records, rec)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// warns returns the records at warn level.
func (h *recordingHandler) warns() []slog.Record {
	var out []slog.Record
	for _, rec := range h.records {
		if rec.Level == slog.LevelWarn {
			out = append(out, rec)
		}
	}
	return out
}

// warnedPIDs returns the pid attribute of every warn record, in order.
func (h *recordingHandler) warnedPIDs() []int {
	var pids []int
	for _, rec := range h.warns() {
		rec.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "pid" {
				pids = append(pids, int(attr.Value.Int64()))
			}
			return true
		})
	}
	return pids
}

const testStatus = "Name:\tprobe\nState:\tS (sleeping)\nVmRSS:\t  1024 kB\n"

// writeProcFile writes one file below dir, creating parents.
func writeProcFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", filepath.Join(dir, name), err)
	}
}

// writeProcSelf creates the self entries probe requires.
func writeProcSelf(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "self")
	writeProcFile(t, dir, "stat", statLine("1", "smob", statTail))
	writeProcFile(t, dir, "status", testStatus)
}

// writeProcPID creates one process directory with the given stat and status
// bodies. An empty body omits the file.
func writeProcPID(t *testing.T, root string, pid int, stat, status string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if stat != "" {
		writeProcFile(t, dir, "stat", stat)
	}
	if status != "" {
		writeProcFile(t, dir, "status", status)
	}
	if stat == "" && status == "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
}

func TestValidateStat(t *testing.T) {
	tests := []struct {
		name    string
		stat    statSnapshot
		wantErr string
	}{
		{"live process", statSnapshot{pid: 42, starttime: 5555}, ""},
		{"zero starttime", statSnapshot{pid: 42}, "starttime is zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStat(tt.stat)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateStat() = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("validateStat() = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestSameProcess(t *testing.T) {
	tests := []struct {
		name          string
		before, after statSnapshot
		want          bool
	}{
		{"same starttime", statSnapshot{starttime: 10}, statSnapshot{starttime: 10}, true},
		{"reused pid", statSnapshot{starttime: 10}, statSnapshot{starttime: 11}, false},
		{"zero before", statSnapshot{}, statSnapshot{}, false},
		{"zero after", statSnapshot{starttime: 10}, statSnapshot{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameProcess(tt.before, tt.after); got != tt.want {
				t.Fatalf("sameProcess(%d, %d) = %v, want %v",
					tt.before.starttime, tt.after.starttime, got, tt.want)
			}
		})
	}
}

// TestCollectSkipsPerPIDFailures covers the per-process error policy that real
// /proc cannot produce on demand: one bad PID must not fail the tick or drop
// samples for the others.
func TestCollectSkipsPerPIDFailures(t *testing.T) {
	root := t.TempDir()
	writeProcSelf(t, root)
	writeProcPID(t, root, 100, statLine("100", "good", statTail), testStatus)
	writeProcPID(t, root, 101, "not a stat line\n", testStatus)
	zeroStart := statLine("102", "zero", "S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 0")
	writeProcPID(t, root, 102, zeroStart, testStatus)
	// A directory where a file is expected opens but fails on read.
	writeProcPID(t, root, 103, "", testStatus)
	if err := os.Mkdir(filepath.Join(root, "103", "stat"), 0o755); err != nil {
		t.Fatalf("mkdir stat dir: %v", err)
	}
	writeProcPID(t, root, 104, statLine("104", "nostatus", statTail), "")
	if err := os.Mkdir(filepath.Join(root, "104", "status"), 0o755); err != nil {
		t.Fatalf("mkdir status dir: %v", err)
	}
	// The filter excludes 105, so it must be silent even though it is readable.
	writeProcPID(t, root, 105, statLine("105", "other", statTail), testStatus)

	handler := &recordingHandler{}
	logger := slog.New(handler)
	src, err := New(Options{
		ProcPath:       root,
		Filter:         "good|zero|nostatus",
		Interval:       time.Second,
		RescanInterval: time.Hour,
	}, logger)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	now := time.Now()
	samples, err := src.Collect(context.Background(), now)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(samples) != 1 || samples[0].Labels["pid"] != "100" {
		t.Fatalf("samples = %+v, want one RSS sample for pid 100", samples)
	}

	wantPIDs := []int{101, 102, 103, 104}
	gotPIDs := handler.warnedPIDs()
	if len(gotPIDs) != len(wantPIDs) {
		t.Fatalf("warned pids = %v, want %v", gotPIDs, wantPIDs)
	}
	for i := range wantPIDs {
		if gotPIDs[i] != wantPIDs[i] {
			t.Fatalf("warned pids = %v, want %v", gotPIDs, wantPIDs)
		}
	}

	// No rescan, no repeated warnings.
	if _, err := src.Collect(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := len(handler.warns()); got != len(wantPIDs) {
		t.Fatalf("warns = %d, want %d before the next rescan", got, len(wantPIDs))
	}

	// A rescan re-arms the warnings for PIDs that are still failing.
	if _, err := src.Collect(context.Background(), now.Add(2*time.Hour)); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := len(handler.warns()); got != 2*len(wantPIDs) {
		t.Fatalf("warns = %d, want %d after the rescan", got, 2*len(wantPIDs))
	}
}

// TestProbeRejectsBadLayout covers startup validation of the files the source
// parses on every tick.
func TestProbeRejectsBadLayout(t *testing.T) {
	tests := []struct {
		name    string
		stat    string
		status  string
		wantErr string
	}{
		{
			name:    "unparseable stat",
			stat:    "garbage\n",
			status:  testStatus,
			wantErr: "parse",
		},
		{
			name:    "zero starttime",
			stat:    statLine("1", "smob", "S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 0"),
			status:  testStatus,
			wantErr: "starttime is zero",
		},
		{
			name:    "unparseable status",
			stat:    statLine("1", "smob", statTail),
			status:  "VmRSS: 12 MB\n",
			wantErr: "unexpected unit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeProcFile(t, filepath.Join(root, "self"), "stat", tt.stat)
			writeProcFile(t, filepath.Join(root, "self"), "status", tt.status)

			_, err := New(Options{
				ProcPath:       root,
				Interval:       time.Second,
				RescanInterval: time.Hour,
			}, nil)
			if err == nil {
				t.Fatal("New() = nil error, want a startup failure")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New() = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
