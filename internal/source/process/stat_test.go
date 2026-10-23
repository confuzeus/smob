package process

import (
	"bytes"
	"strings"
	"testing"
)

// statTail is a valid run of /proc/[pid]/stat fields from field 3 (state)
// through field 22 (starttime): utime 10, stime 20, starttime 5555.
const statTail = "S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 5555"

func statLine(pid, comm, tail string) string {
	return pid + " (" + comm + ") " + tail
}

func TestParseStat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    statSnapshot
		wantErr string
	}{
		{
			name:  "process",
			input: statLine("4242", "nginx", statTail),
			want:  statSnapshot{pid: 4242, comm: "nginx", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:  "comm with spaces",
			input: statLine("7", "Web Content", statTail),
			want:  statSnapshot{pid: 7, comm: "Web Content", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:  "comm with parentheses",
			input: statLine("9", "proc (test)", statTail),
			want:  statSnapshot{pid: 9, comm: "proc (test)", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:  "comm ending in parenthesis",
			input: statLine("9", "weird)", statTail),
			want:  statSnapshot{pid: 9, comm: "weird)", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:  "empty comm",
			input: statLine("9", "", statTail),
			want:  statSnapshot{pid: 9, comm: "", utime: 10, stime: 20, starttime: 5555},
		},
		{
			// A newline inside comm splits the file across two lines, which is
			// why the parser reads the whole file instead of scanning lines.
			name:  "comm with newline",
			input: statLine("9", "two\nlines", statTail),
			want:  statSnapshot{pid: 9, comm: "two\nlines", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:  "extra trailing fields",
			input: statLine("9", "nginx", statTail+" 0 0"),
			want:  statSnapshot{pid: 9, comm: "nginx", utime: 10, stime: 20, starttime: 5555},
		},
		{
			name:    "missing open parenthesis",
			input:   "9 nginx S 1\n",
			wantErr: "missing '('",
		},
		{
			name:    "missing close parenthesis",
			input:   "9 (nginx S 1\n",
			wantErr: "missing ')'",
		},
		{
			name:    "malformed pid",
			input:   statLine("x", "nginx", statTail),
			wantErr: "field 1 (pid)",
		},
		{
			name:    "non-positive pid",
			input:   statLine("0", "nginx", statTail),
			wantErr: "not positive",
		},
		{
			name:    "truncated after comm",
			input:   statLine("9", "nginx", "S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0"),
			wantErr: "want at least 20",
		},
		{
			name:    "malformed utime",
			input:   statLine("9", "nginx", "S 1 1 1 0 -1 0 0 0 0 0 x 20 0 0 20 0 1 0 5555"),
			wantErr: "field 14 (utime)",
		},
		{
			name:    "malformed stime",
			input:   statLine("9", "nginx", "S 1 1 1 0 -1 0 0 0 0 0 10 x 0 0 20 0 1 0 5555"),
			wantErr: "field 15 (stime)",
		},
		{
			name:    "malformed starttime",
			input:   statLine("9", "nginx", "S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 x"),
			wantErr: "field 22 (starttime)",
		},
		{
			name:    "negative utime",
			input:   statLine("9", "nginx", "S 1 1 1 0 -1 0 0 0 0 0 -1 20 0 0 20 0 1 0 5555"),
			wantErr: "field 14 (utime)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStat(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseStat() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseStat() error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStat() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseStat() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseStatFixture(t *testing.T) {
	tests := []struct {
		fixture string
		want    statSnapshot
	}{
		{
			fixture: "proc_pid_stat.txt",
			want:    statSnapshot{pid: 4242, comm: "nginx", utime: 120, stime: 34, starttime: 99887766},
		},
		{
			fixture: "proc_pid_stat_space_comm.txt",
			want:    statSnapshot{pid: 7777, comm: "Web Content", utime: 200, stime: 50, starttime: 1234567},
		},
		{
			fixture: "proc_pid_stat_paren_comm.txt",
			want:    statSnapshot{pid: 31337, comm: "proc (test)", utime: 3, stime: 4, starttime: 5555},
		},
	}

	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			got, err := parseStat(bytes.NewReader(readFixture(t, tt.fixture)))
			if err != nil {
				t.Fatalf("parseStat() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseStat() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
