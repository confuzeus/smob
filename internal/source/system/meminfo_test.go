package system

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestParseMeminfo(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    meminfoSnapshot
		wantErr string
	}{
		{
			name:  "kilobytes",
			input: "MemTotal:       16384000 kB\nMemAvailable:    8192000 kB\n",
			want: meminfoSnapshot{
				totalBytes:     16384000 * 1024,
				availableBytes: 8192000 * 1024,
			},
		},
		{
			name: "extra whitespace and ignored fields",
			input: "MemFree:\t1000 kB\n" +
				"MemTotal:\t\t2048\tkB\n" +
				"Buffers: 500 kB\n" +
				"MemAvailable:   1024   kB\n",
			want: meminfoSnapshot{
				totalBytes:     2048 * 1024,
				availableBytes: 1024 * 1024,
			},
		},
		{
			name:  "unit omitted",
			input: "MemTotal: 1024\nMemAvailable: 512\n",
			want: meminfoSnapshot{
				totalBytes:     1024 * 1024,
				availableBytes: 512 * 1024,
			},
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: "missing MemTotal",
		},
		{
			name:    "missing available",
			input:   "MemTotal: 1024 kB\n",
			wantErr: "missing MemAvailable",
		},
		{
			name:    "wrong unit",
			input:   "MemTotal: 1024 MB\nMemAvailable: 512 kB\n",
			wantErr: "unexpected unit",
		},
		{
			name:    "missing value",
			input:   "MemTotal:\nMemAvailable: 512 kB\n",
			wantErr: "missing value",
		},
		{
			name:    "malformed value",
			input:   "MemTotal: lots kB\nMemAvailable: 512 kB\n",
			wantErr: "MemTotal",
		},
		{
			name:    "duplicate field",
			input:   "MemTotal: 1024 kB\nMemTotal: 2048 kB\nMemAvailable: 512 kB\n",
			wantErr: "duplicate MemTotal",
		},
		{
			name:    "duplicate available field",
			input:   "MemTotal: 1024 kB\nMemAvailable: 512 kB\nMemAvailable: 256 kB\n",
			wantErr: "duplicate MemAvailable",
		},
		{
			name:  "largest value that fits in bytes",
			input: "MemTotal: 18014398509481983 kB\nMemAvailable: 1 kB\n",
			want: meminfoSnapshot{
				totalBytes:     math.MaxUint64 / 1024 * 1024,
				availableBytes: 1024,
			},
		},
		{
			name:    "overflow by one kilobyte",
			input:   "MemTotal: 18014398509481984 kB\nMemAvailable: 1 kB\n",
			wantErr: "overflows bytes",
		},
		{
			name:    "overflow",
			input:   "MemTotal: 18446744073709551615 kB\nMemAvailable: 512 kB\n",
			wantErr: "overflows bytes",
		},
		{
			name:    "available overflows",
			input:   "MemTotal: 1024 kB\nMemAvailable: 18446744073709551615 kB\n",
			wantErr: "overflows bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMeminfo(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseMeminfo() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseMeminfo() error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMeminfo() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseMeminfo() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseMeminfoFixture(t *testing.T) {
	got, err := parseMeminfo(bytes.NewReader(readFixture(t, "proc_meminfo.txt")))
	if err != nil {
		t.Fatalf("parseMeminfo() error = %v", err)
	}
	if got.totalBytes == 0 {
		t.Fatal("MemTotal is zero")
	}
	if got.availableBytes == 0 {
		t.Fatal("MemAvailable is zero")
	}
	if got.availableBytes > got.totalBytes {
		t.Errorf("MemAvailable = %d, greater than MemTotal = %d", got.availableBytes, got.totalBytes)
	}
}
