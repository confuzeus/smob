package system

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParseStat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    statSnapshot
		wantErr string
	}{
		{
			name: "aggregate and per-cpu lines",
			input: "cpu  10 20 30 40 50 60 70 80 90 100\n" +
				"cpu0 1 2 3 4 5 6 7 8 9 10\n" +
				"cpu2 11 12 13 14 15 16 17 18 19 20\n" +
				"intr 1 2 3\nctxt 42\nbtime 1700000000\n",
			want: statSnapshot{
				all: cpuTimes{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
				cpus: map[int]cpuTimes{
					0: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
					2: {11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
				},
			},
		},
		{
			name:  "non-contiguous cpu set",
			input: "cpu 1 2 3 4\ncpu0 1 2 3 4\ncpu15 5 6 7 8\n",
			want: statSnapshot{
				all: cpuTimes{user: 1, nice: 2, system: 3, idle: 4},
				cpus: map[int]cpuTimes{
					0:  {user: 1, nice: 2, system: 3, idle: 4},
					15: {user: 5, nice: 6, system: 7, idle: 8},
				},
			},
		},
		{
			name:  "legacy four fields",
			input: "cpu 1 2 3 4\ncpu0 5 6 7 8\n",
			want: statSnapshot{
				all:  cpuTimes{user: 1, nice: 2, system: 3, idle: 4},
				cpus: map[int]cpuTimes{0: {user: 5, nice: 6, system: 7, idle: 8}},
			},
		},
		{
			name:  "ignores non-cpu and cpu-prefixed keys",
			input: "cpufreq 123\ncpu 0 0 0 100\nintr 7\n",
			want: statSnapshot{
				all:  cpuTimes{idle: 100},
				cpus: map[int]cpuTimes{},
			},
		},
		{
			name:  "blank lines and surrounding whitespace",
			input: "\n  cpu   1 2 3 4  \n\n cpu0 5 6 7 8 \n",
			want: statSnapshot{
				all:  cpuTimes{user: 1, nice: 2, system: 3, idle: 4},
				cpus: map[int]cpuTimes{0: {user: 5, nice: 6, system: 7, idle: 8}},
			},
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: "missing aggregate cpu line",
		},
		{
			name:    "no aggregate line",
			input:   "cpu0 1 2 3 4\nintr 7\n",
			wantErr: "missing aggregate cpu line",
		},
		{
			name:    "too few fields",
			input:   "cpu 1 2 3\n",
			wantErr: "want at least 4",
		},
		{
			name:  "twelve fields ignore trailing counters",
			input: "cpu 1 2 3 4 5 6 7 8 9 10 11 12\ncpu0 13 14 15 16 17 18 19 20 21 22 23 24\n",
			want: statSnapshot{
				all:  cpuTimes{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				cpus: map[int]cpuTimes{0: {13, 14, 15, 16, 17, 18, 19, 20, 21, 22}},
			},
		},
		{
			name:    "malformed counter",
			input:   "cpu 1 2 x 4\n",
			wantErr: "field 3",
		},
		{
			name:    "malformed per-cpu counter names the line",
			input:   "cpu 1 2 3 4\ncpu0 1 2 x 4\n",
			wantErr: "line 2: cpu0: field 3",
		},
		{
			name:    "counter above uint64 range",
			input:   "cpu 18446744073709551616 2 3 4\n",
			wantErr: "field 1",
		},
		{
			name:    "negative counter",
			input:   "cpu -1 2 3 4\n",
			wantErr: "field 1",
		},
		{
			name:    "duplicate aggregate",
			input:   "cpu 1 2 3 4\ncpu 5 6 7 8\n",
			wantErr: "duplicate cpu line",
		},
		{
			name:    "duplicate per-cpu",
			input:   "cpu 1 2 3 4\ncpu0 1 2 3 4\ncpu0 5 6 7 8\n",
			wantErr: "duplicate cpu0 line",
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseStat() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCPUTimesTotalExcludesGuest(t *testing.T) {
	base := cpuTimes{user: 10, nice: 20, system: 30, idle: 40, iowait: 50, irq: 60, softirq: 70, steal: 80}
	withGuest := base
	withGuest.guest = 1000
	withGuest.guestNice = 2000

	if got, want := withGuest.total(), base.total(); got != want {
		t.Errorf("total() with guest = %d, want %d", got, want)
	}
	if got, want := base.total(), uint64(360); got != want {
		t.Errorf("total() = %d, want %d", got, want)
	}
	if got, want := base.busy(), uint64(270); got != want {
		t.Errorf("busy() = %d, want %d", got, want)
	}
}

// TestParseStatLongIntrLine covers hosts with thousands of IRQs, where the intr
// line exceeds bufio.Scanner's 64 KiB default token limit.
func TestParseStatLongIntrLine(t *testing.T) {
	var b strings.Builder
	b.WriteString("cpu 1 2 3 4 5 6 7 8 9 10\n")
	b.WriteString("intr")
	for i := range 13_000 {
		fmt.Fprintf(&b, " %d", i)
	}
	b.WriteString("\nctxt 42\n")
	if b.Len() <= 64*1024 {
		t.Fatalf("intr line is %d bytes, want more than 64 KiB", b.Len())
	}

	got, err := parseStat(strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("parseStat() error = %v", err)
	}
	if want := (cpuTimes{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); got.all != want {
		t.Errorf("aggregate = %+v, want %+v", got.all, want)
	}
}

// TestParseStatOversizedLineError checks that a line beyond the raised limit
// still fails loudly and names the line.
func TestParseStatOversizedLineError(t *testing.T) {
	input := "cpu 1 2 3 4\nintr " + strings.Repeat("1 ", 1<<20) + "\n"

	_, err := parseStat(strings.NewReader(input))
	if err == nil {
		t.Fatal("parseStat() error = nil, want an oversized-line error")
	}
	for _, want := range []string{"line 2", "token too long"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("parseStat() error = %q, want it to contain %q", err, want)
		}
	}
}

func TestParseStatFixture(t *testing.T) {
	got, err := parseStat(bytes.NewReader(readFixture(t, "proc_stat.txt")))
	if err != nil {
		t.Fatalf("parseStat() error = %v", err)
	}
	if got.all.total() == 0 {
		t.Fatal("aggregate cpu line has no ticks")
	}
	if len(got.cpus) == 0 {
		t.Fatal("no per-cpu lines parsed")
	}
	for index, times := range got.cpus {
		if times.busy() > times.total() {
			t.Errorf("cpu%d busy = %d, total = %d", index, times.busy(), times.total())
		}
	}
}
