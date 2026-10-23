package process

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseStatm(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint64
		wantErr string
	}{
		{
			name:  "seven fields",
			input: "2048 1080 300 120 0 0 0\n",
			want:  1080,
		},
		{
			name:  "extra fields are ignored",
			input: "1 2 3 4 5 6 7 8 9\n",
			want:  2,
		},
		{
			name:  "surrounding whitespace",
			input: "\n\n  1024\t512 10 0 0 0 0  \n",
			want:  512,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: "want at least 2",
		},
		{
			name:    "single field",
			input:   "1024\n",
			wantErr: "want at least 2",
		},
		{
			name:    "malformed resident",
			input:   "1024 x 10 0 0 0 0\n",
			wantErr: "resident",
		},
		{
			name:    "negative resident",
			input:   "1024 -1 10 0 0 0 0\n",
			wantErr: "resident",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStatm(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseStatm() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseStatm() error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStatm() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseStatm() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseStatmFixture(t *testing.T) {
	got, err := parseStatm(bytes.NewReader(readFixture(t, "proc_pid_statm.txt")))
	if err != nil {
		t.Fatalf("parseStatm() error = %v", err)
	}
	if got != 1080 {
		t.Errorf("parseStatm() = %d, want 1080", got)
	}
}
