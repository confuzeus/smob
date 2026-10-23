package disk

import (
	"path/filepath"
	"testing"
	"time"
)

func TestChunkKey(t *testing.T) {
	cases := []struct {
		name    string
		host    string
		scope   string
		start   time.Time
		end     time.Time
		want    string
		wantErr bool
	}{
		{
			name:  "five minute window",
			host:  "web-1",
			scope: "system",
			start: time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			want:  "web-1/system/2026/10/23/16/20261023T160000Z-20261023T160500Z.jsonl.gz",
		},
		{
			name:  "partial shutdown chunk",
			host:  "web-1",
			scope: "process",
			start: time.Date(2026, 10, 23, 16, 55, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 23, 16, 57, 27, 0, time.UTC),
			want:  "web-1/process/2026/10/23/16/20261023T165500Z-20261023T165727Z.jsonl.gz",
		},
		{
			name:  "non UTC input is normalized",
			host:  "web-1",
			scope: "system",
			start: time.Date(2026, 10, 23, 18, 0, 0, 0, time.FixedZone("CEST", 2*60*60)),
			end:   time.Date(2026, 10, 23, 18, 5, 0, 0, time.FixedZone("CEST", 2*60*60)),
			want:  "web-1/system/2026/10/23/16/20261023T160000Z-20261023T160500Z.jsonl.gz",
		},
		{
			name:  "one second window across midnight",
			host:  "web-1",
			scope: "system",
			start: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 24, 0, 0, 1, 0, time.UTC),
			want:  "web-1/system/2026/10/24/00/20261024T000000Z-20261024T000001Z.jsonl.gz",
		},
		{
			name:    "empty host",
			host:    "",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "host traversal",
			host:    "../other",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "host is dot",
			host:    ".",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "empty scope",
			host:    "web-1",
			scope:   "",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "zero start",
			host:    "web-1",
			scope:   "system",
			start:   time.Time{},
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "zero end",
			host:    "web-1",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Time{},
			wantErr: true,
		},
		{
			name:    "end equals start",
			host:    "web-1",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "end before start",
			host:    "web-1",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "sub second start",
			host:    "web-1",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 500_000_000, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "sub second end",
			host:    "web-1",
			scope:   "system",
			start:   time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:     time.Date(2026, 10, 23, 16, 5, 0, 1, time.UTC),
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chunkKey(tc.host, tc.scope, tc.start, tc.end)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("chunkKey() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("chunkKey() error: %v", err)
			}
			if got != filepath.FromSlash(tc.want) {
				t.Fatalf("chunkKey() = %q, want %q", got, filepath.FromSlash(tc.want))
			}
		})
	}
}

func TestParseChunkName(t *testing.T) {
	cases := []struct {
		name      string
		file      string
		start     time.Time
		end       time.Time
		wantError bool
	}{
		{
			name:  "plain name",
			file:  "20261023T160000Z-20261023T160500Z.jsonl.gz",
			start: time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
		},
		{
			name:  "collision suffix",
			file:  "20261023T160000Z-20261023T160500Z-1.jsonl.gz",
			start: time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
		},
		{
			name:  "large collision suffix",
			file:  "20261023T160000Z-20261023T160500Z-37.jsonl.gz",
			start: time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
		},
		{
			name:      "missing suffix",
			file:      "20261023T160000Z-20261023T160500Z.jsonl",
			wantError: true,
		},
		{
			name:      "not a chunk",
			file:      ".hidden",
			wantError: true,
		},
		{
			name:      "too many parts",
			file:      "20261023T160000Z-20261023T160500Z-1-2.jsonl.gz",
			wantError: true,
		},
		{
			name:      "bad start",
			file:      "notatime-20261023T160500Z.jsonl.gz",
			wantError: true,
		},
		{
			name:      "bad end",
			file:      "20261023T160000Z-notatime.jsonl.gz",
			wantError: true,
		},
		{
			name:      "end before start",
			file:      "20261023T160500Z-20261023T160000Z.jsonl.gz",
			wantError: true,
		},
		{
			name:      "zero collision suffix",
			file:      "20261023T160000Z-20261023T160500Z-0.jsonl.gz",
			wantError: true,
		},
		{
			name:      "negative collision suffix",
			file:      "20261023T160000Z-20261023T160500Z--1.jsonl.gz",
			wantError: true,
		},
		{
			name:      "non numeric collision suffix",
			file:      "20261023T160000Z-20261023T160500Z-copy.jsonl.gz",
			wantError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := parseChunkName(tc.file)
			if tc.wantError {
				if err == nil {
					t.Fatalf("parseChunkName(%q) = %s, %s, want an error", tc.file, start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseChunkName(%q) error: %v", tc.file, err)
			}
			if !start.Equal(tc.start) || !end.Equal(tc.end) {
				t.Fatalf("parseChunkName(%q) = %s, %s, want %s, %s", tc.file, start, end, tc.start, tc.end)
			}
		})
	}
}

func TestChunkKeyParseRoundTrip(t *testing.T) {
	start := time.Date(2026, 10, 23, 16, 55, 0, 0, time.UTC)
	end := time.Date(2026, 10, 23, 17, 0, 0, 0, time.UTC)

	key, err := chunkKey("web-1", "system", start, end)
	if err != nil {
		t.Fatalf("chunkKey() error: %v", err)
	}
	for n := 0; n < 3; n++ {
		candidate := key
		if n > 0 {
			candidate = collisionKey(key, n)
		}
		parsedStart, parsedEnd, err := parseChunkName(filepath.Base(candidate))
		if err != nil {
			t.Fatalf("parseChunkName(%q) error: %v", candidate, err)
		}
		if !parsedStart.Equal(start) || !parsedEnd.Equal(end) {
			t.Fatalf("round trip %q = %s, %s, want %s, %s", candidate, parsedStart, parsedEnd, start, end)
		}
	}
}

func TestParseHourPath(t *testing.T) {
	cases := []struct {
		name string
		rel  string
		want time.Time
		ok   bool
	}{
		{
			name: "valid",
			rel:  "web-1/system/2026/10/23/16",
			want: time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			ok:   true,
		},
		{
			name: "midnight",
			rel:  "web-1/process/2026/01/01/00",
			want: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ok:   true,
		},
		{name: "too shallow", rel: "web-1/system/2026/10/23"},
		{name: "too deep", rel: "web-1/system/2026/10/23/16/extra"},
		{name: "empty host", rel: "/system/2026/10/23/16"},
		{name: "host traversal", rel: "../system/2026/10/23/16"},
		{name: "short year", rel: "web-1/system/26/10/23/16"},
		{name: "short month", rel: "web-1/system/2026/1/23/16"},
		{name: "month out of range", rel: "web-1/system/2026/13/23/16"},
		{name: "day out of range", rel: "web-1/system/2026/10/32/16"},
		{name: "hour out of range", rel: "web-1/system/2026/10/23/24"},
		{name: "not a number", rel: "web-1/system/twenty/10/23/16"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseHourPath(tc.rel)
			if ok != tc.ok {
				t.Fatalf("parseHourPath(%q) ok = %v, want %v", tc.rel, ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Fatalf("parseHourPath(%q) = %s, want %s", tc.rel, got, tc.want)
			}
		})
	}
}
