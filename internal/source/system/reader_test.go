package system

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// errReader returns err on every read. Paired with a prefix via io.MultiReader
// it simulates a truncated /proc read that fails mid-line.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// truncated returns data followed by a read error rather than EOF.
func truncated(data string, err error) io.Reader {
	return io.MultiReader(strings.NewReader(data), errReader{err: err})
}

// TestParseReaderErrorPropagates checks that a reader failure in the middle of
// a line is not swallowed or turned into a partial snapshot. The stat parser
// can name the failing line because it counts completed lines; meminfo and
// netdev report the read that failed.
func TestParseReaderErrorPropagates(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		parse   func(io.Reader) error
		wantMsg string
	}{
		{
			name:  "stat names the failing line",
			input: "cpu 1 2 3 4\nintr 1 2",
			parse: func(r io.Reader) error {
				_, err := parseStat(r)
				return err
			},
			// bufio.Scanner consumes the partial final token before surfacing the
			// read error, so the parser reports the following line.
			wantMsg: "line ",
		},
		{
			name:  "meminfo names the failing read",
			input: "MemTotal: 1024 kB\nMemAvail",
			parse: func(r io.Reader) error {
				_, err := parseMeminfo(r)
				return err
			},
			wantMsg: "read meminfo",
		},
		{
			name:  "netdev names the failing read",
			input: "Inter-|   Rece",
			parse: func(r io.Reader) error {
				_, err := parseNetDev(r)
				return err
			},
			wantMsg: "read net/dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.parse(truncated(tt.input, io.ErrUnexpectedEOF))
			if err == nil {
				t.Fatal("parse error = nil, want io.ErrUnexpectedEOF")
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("parse error = %v, want it to wrap io.ErrUnexpectedEOF", err)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("parse error = %q, want it to contain %q", err, tt.wantMsg)
			}
		})
	}
}

// TestTruncatedDataLineNeverReturnsSnapshot covers truncation landing inside a
// data line. The parser may notice the malformed tail before it reaches the
// read error, but it must never return a usable snapshot.
func TestTruncatedDataLineNeverReturnsSnapshot(t *testing.T) {
	tests := []struct {
		name  string
		input string
		parse func(io.Reader) error
	}{
		{
			name:  "stat",
			input: "cpu 1 2 3 4\ncpu0 5 6",
			parse: func(r io.Reader) error {
				_, err := parseStat(r)
				return err
			},
		},
		{
			name:  "meminfo",
			input: "MemTotal: 1024 kB\nMemAvail",
			parse: func(r io.Reader) error {
				_, err := parseMeminfo(r)
				return err
			},
		},
		{
			name:  "netdev",
			input: netdevHeader + netdevLine("lo", "7", "8") + "\neth0: 1 2 3",
			parse: func(r io.Reader) error {
				_, err := parseNetDev(r)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.parse(truncated(tt.input, io.ErrUnexpectedEOF)); err == nil {
				t.Fatal("parse error = nil, want a truncated-input error")
			}
		})
	}
}

// TestReadSnapshotErrorsNamePath checks that the file-reading wrappers name the
// path they failed to parse, so a startup error points at the offending proc
// file rather than only at its contents.
func TestReadSnapshotErrorsNamePath(t *testing.T) {
	tests := []struct {
		name string
		body string
		read func(string) error
	}{
		{
			name: "stat",
			body: "cpu 1 2 x 4\n",
			read: func(path string) error {
				_, err := readStatSnapshot(path)
				return err
			},
		},
		{
			name: "meminfo",
			body: "MemTotal: nope kB\nMemAvailable: 1024 kB\n",
			read: func(path string) error {
				_, err := readMeminfoSnapshot(path)
				return err
			},
		},
		{
			name: "netdev",
			body: "lo: 1 2 3\n",
			read: func(path string) error {
				_, err := readNetDevSnapshot(path)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.name)
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}

			err := tt.read(path)
			if err == nil {
				t.Fatal("read error = nil, want a parse error")
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("read error = %q, want it to contain path %q", err, path)
			}
		})
	}
}
