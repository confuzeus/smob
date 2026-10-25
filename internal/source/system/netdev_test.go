package system

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// netdevLine builds a /proc/net/dev data line with sixteen counters.
func netdevLine(name, rx, tx string) string {
	fields := []string{
		rx, "1", "0", "0", "0", "0", "0", "0",
		tx, "2", "0", "0", "0", "0", "0", "0",
	}
	return fmt.Sprintf("%s: %s", name, strings.Join(fields, " "))
}

const netdevHeader = "Inter-|   Receive                                                |  Transmit\n" +
	" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"

func TestParseNetDev(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    netDevSnapshot
		wantErr string
	}{
		{
			name:  "two interfaces",
			input: netdevHeader + netdevLine("lo", "1000", "2000") + "\n" + netdevLine("eth0", "3000", "4000") + "\n",
			want: netDevSnapshot{
				"lo":   {rxBytes: 1000, txBytes: 2000},
				"eth0": {rxBytes: 3000, txBytes: 4000},
			},
		},
		{
			name:  "leading whitespace and blank lines",
			input: "\n  " + netdevLine("lo", "7", "8") + "\n\n",
			want:  netDevSnapshot{"lo": {rxBytes: 7, txBytes: 8}},
		},
		{
			name:  "extra counters are ignored",
			input: netdevLine("lo", "7", "8") + " 99 98\n",
			want:  netDevSnapshot{"lo": {rxBytes: 7, txBytes: 8}},
		},
		{
			name: "maximum counters do not overflow",
			input: netdevHeader + netdevLine("lo",
				strconv.FormatUint(math.MaxUint64, 10),
				strconv.FormatUint(math.MaxUint64, 10)) + "\n",
			want: netDevSnapshot{"lo": {rxBytes: math.MaxUint64, txBytes: math.MaxUint64}},
		},
		{
			name:    "counter above uint64 range",
			input:   netdevHeader + netdevLine("lo", "18446744073709551616", "1") + "\n",
			wantErr: "rx_bytes",
		},
		{
			name:    "header only",
			input:   netdevHeader,
			wantErr: "no interfaces",
		},
		{
			name:    "mangled data line after headers",
			input:   netdevHeader + netdevLine("lo", "7", "8") + "\neth0 9 10 11 12\n",
			wantErr: "line 4: missing ':' separator",
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: "no interfaces",
		},
		{
			name:    "too few counters",
			input:   "lo: 1 2 3\n",
			wantErr: "want at least 16",
		},
		{
			name:    "malformed rx",
			input:   netdevLine("lo", "x", "8") + "\n",
			wantErr: "rx_bytes",
		},
		{
			name:    "malformed tx",
			input:   netdevLine("lo", "7", "x") + "\n",
			wantErr: "tx_bytes",
		},
		{
			name:    "duplicate interface",
			input:   netdevLine("lo", "1", "2") + "\n" + netdevLine("lo", "3", "4") + "\n",
			wantErr: "duplicate interface",
		},
		{
			name:    "empty interface name",
			input:   netdevLine("", "1", "2") + "\n",
			wantErr: "empty interface name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNetDev(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseNetDev() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseNetDev() error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNetDev() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseNetDev() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseNetDevFixture(t *testing.T) {
	got, err := parseNetDev(bytes.NewReader(readFixture(t, "proc_net_dev.txt")))
	if err != nil {
		t.Fatalf("parseNetDev() error = %v", err)
	}
	if len(got) == 0 {
		t.Fatal("no interfaces parsed")
	}
	lo, ok := got["lo"]
	if !ok {
		t.Fatal("loopback interface missing")
	}
	if lo.rxBytes == 0 && lo.txBytes == 0 {
		t.Error("loopback counters are both zero")
	}
}
