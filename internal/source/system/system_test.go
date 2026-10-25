package system

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func cpuWarningKey(name string) string {
	return seriesKey(metricCPUPercent, map[string]string{"cpu": name})
}

func netWarningKey(metric, name string) string {
	return seriesKey(metric, map[string]string{"interface": name})
}

func TestPruneResetWarnings(t *testing.T) {
	tests := []struct {
		name   string
		warned map[string]bool
		cur    *snapshot
		want   map[string]bool
	}{
		{
			name: "keeps live series",
			warned: map[string]bool{
				cpuWarningKey("all"): true,
				cpuWarningKey("0"):   true,
			},
			cur: &snapshot{
				stat: statSnapshot{cpus: map[int]cpuTimes{0: {}}},
				net:  netDevSnapshot{"lo": {}},
			},
			want: map[string]bool{
				cpuWarningKey("all"): true,
				cpuWarningKey("0"):   true,
			},
		},
		{
			name: "drops a vanished interface in both directions",
			warned: map[string]bool{
				netWarningKey(metricNetworkRXBytesPerSecond, "veth0"): true,
				netWarningKey(metricNetworkTXBytesPerSecond, "veth0"): true,
				netWarningKey(metricNetworkRXBytesPerSecond, "eth0"):  true,
			},
			cur: &snapshot{
				stat: statSnapshot{cpus: map[int]cpuTimes{}},
				net:  netDevSnapshot{"eth0": {}},
			},
			want: map[string]bool{
				netWarningKey(metricNetworkRXBytesPerSecond, "eth0"): true,
			},
		},
		{
			name: "drops a vanished cpu",
			warned: map[string]bool{
				cpuWarningKey("all"): true,
				cpuWarningKey("3"):   true,
			},
			cur: &snapshot{
				stat: statSnapshot{cpus: map[int]cpuTimes{0: {}}},
				net:  netDevSnapshot{"lo": {}},
			},
			want: map[string]bool{cpuWarningKey("all"): true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{warned: tt.warned}
			s.pruneResetWarnings(tt.cur)

			if len(s.warned) != len(tt.want) {
				t.Fatalf("warned = %v, want %v", s.warned, tt.want)
			}
			for key := range tt.want {
				if !s.warned[key] {
					t.Errorf("warning for %q was pruned", key)
				}
			}
		})
	}
}

// TestWarnResetAfterPrune covers the leak's user-visible effect: after an
// interface disappears, its entry no longer suppresses the first reset warning
// when a same-named interface reappears.
func TestWarnResetAfterPrune(t *testing.T) {
	var buf bytes.Buffer
	s := &Source{
		logger: slog.New(slog.NewTextHandler(&buf, nil)),
		warned: map[string]bool{
			netWarningKey(metricNetworkRXBytesPerSecond, "veth0"): true,
		},
	}

	s.pruneResetWarnings(&snapshot{
		stat: statSnapshot{cpus: map[int]cpuTimes{}},
		net:  netDevSnapshot{"eth0": {}},
	})
	if len(s.warned) != 0 {
		t.Fatalf("warned = %v, want empty after the interface vanished", s.warned)
	}

	s.warnReset(metricNetworkRXBytesPerSecond, map[string]string{"interface": "veth0"})
	if !strings.Contains(buf.String(), "veth0") {
		t.Errorf("no reset warning after veth0 reappeared; log = %q", buf.String())
	}
}
