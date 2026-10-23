package process

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseStatus(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint64
		wantErr string
	}{
		{
			name: "kilobytes among other fields",
			input: "Name:\tnginx\nState:\tS (sleeping)\n" +
				"VmSize:\t  120000 kB\n" +
				"VmRSS:\t    4321 kB\n" +
				"Threads:\t4\n",
			want: 4321 * 1024,
		},
		{
			name:  "extra whitespace",
			input: "VmRSS:\t\t 2048\tkB\n",
			want:  2048 * 1024,
		},
		{
			name:  "unit omitted",
			input: "VmRSS: 1024\n",
			want:  1024 * 1024,
		},
		{
			name:  "zero resident set",
			input: "VmRSS:\t0 kB\n",
			want:  0,
		},
		{
			// A zombie has no resident set; that is a zero gauge, not an error.
			name:  "missing VmRSS",
			input: "Name:\tzombie\nState:\tZ (zombie)\nThreads:\t1\n",
			want:  0,
		},
		{
			name:    "wrong unit",
			input:   "VmRSS: 12 MB\n",
			wantErr: "unexpected unit",
		},
		{
			name:    "missing value",
			input:   "VmRSS:\n",
			wantErr: "missing value",
		},
		{
			name:    "malformed value",
			input:   "VmRSS: lots kB\n",
			wantErr: "VmRSS",
		},
		{
			name:    "duplicate field",
			input:   "VmRSS: 1024 kB\nVmRSS: 2048 kB\n",
			wantErr: "duplicate VmRSS",
		},
		{
			name:    "overflow",
			input:   "VmRSS: 18446744073709551615 kB\n",
			wantErr: "overflows bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStatus(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseStatus() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseStatus() error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStatus() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseStatus() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseStatusFixture(t *testing.T) {
	got, err := parseStatus(bytes.NewReader(readFixture(t, "proc_pid_status.txt")))
	if err != nil {
		t.Fatalf("parseStatus() error = %v", err)
	}
	if got != 4321*1024 {
		t.Errorf("parseStatus() = %d, want %d", got, 4321*1024)
	}
}
