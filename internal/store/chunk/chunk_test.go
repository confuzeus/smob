package chunk

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

func TestKey(t *testing.T) {
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
			got, err := Key(tc.host, tc.scope, tc.start, tc.end)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Key() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Key() error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Key() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsComponent(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "hostname", value: "web-1", want: true},
		{name: "255 bytes", value: strings.Repeat("a", 255), want: true},
		{name: "256 bytes", value: strings.Repeat("a", 256)},
		{name: "empty", value: ""},
		{name: "dot", value: "."},
		{name: "dot dot", value: ".."},
		{name: "slash", value: "a/b"},
		{name: "newline", value: "a\nb"},
		{name: "tab", value: "a\tb"},
		{name: "NUL", value: "a\x00b"},
		{name: "DEL", value: "a\x7fb"},
		{name: "leading space", value: " a"},
		{name: "trailing space", value: "a "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsComponent(tc.value); got != tc.want {
				t.Fatalf("IsComponent(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestParseName(t *testing.T) {
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
			start, end, err := ParseName(tc.file)
			if tc.wantError {
				if err == nil {
					t.Fatalf("ParseName(%q) = %s, %s, want an error", tc.file, start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseName(%q) error: %v", tc.file, err)
			}
			if !start.Equal(tc.start) || !end.Equal(tc.end) {
				t.Fatalf("ParseName(%q) = %s, %s, want %s, %s", tc.file, start, end, tc.start, tc.end)
			}
		})
	}
}

func TestKeyParseRoundTrip(t *testing.T) {
	start := time.Date(2026, 10, 23, 16, 55, 0, 0, time.UTC)
	end := time.Date(2026, 10, 23, 17, 0, 0, 0, time.UTC)

	key, err := Key("web-1", "system", start, end)
	if err != nil {
		t.Fatalf("Key() error: %v", err)
	}
	for n := 0; n < 3; n++ {
		candidate := key
		if n > 0 {
			candidate = CollisionKey(key, n)
		}
		parsedStart, parsedEnd, err := ParseName(path.Base(candidate))
		if err != nil {
			t.Fatalf("ParseName(%q) error: %v", candidate, err)
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
			got, ok := ParseHourPath(tc.rel)
			if ok != tc.ok {
				t.Fatalf("ParseHourPath(%q) ok = %v, want %v", tc.rel, ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Fatalf("ParseHourPath(%q) = %s, want %s", tc.rel, got, tc.want)
			}
		})
	}
}

func TestOverlaps(t *testing.T) {
	start := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	cases := []struct {
		name  string
		start time.Time
		end   time.Time
		want  bool
	}{
		{name: "unbounded", want: true},
		{name: "starts inside", start: start.Add(time.Minute), want: true},
		{name: "ends inside", end: end.Add(-time.Minute), want: true},
		{name: "touches start", end: start, want: false},
		{name: "starts at end", start: end, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := model.TimeRange{Start: tc.start, End: tc.end}
			if got := Overlaps(start, end, r); got != tc.want {
				t.Fatalf("Overlaps() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEncodeDeterministic(t *testing.T) {
	samples := []model.Sample{
		{
			Timestamp:  time.Date(2026, 10, 23, 16, 0, 1, 0, time.UTC),
			Metric:     "system.memory.used_bytes",
			Value:      1024,
			IntervalMS: 0,
			Labels:     map[string]string{"scope": "system", "host": "web-1"},
		},
		{
			Timestamp:  time.Date(2026, 10, 23, 16, 0, 2, 0, time.UTC),
			Metric:     "system.cpu.percent",
			Value:      12.5,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "web-1", "scope": "system", "cpu": "all"},
		},
	}

	var first, second bytes.Buffer
	if err := Encode(&first, samples); err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if err := Encode(&second, samples); err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("encoding the same samples twice produced different bytes")
	}

	gz, err := gzip.NewReader(bytes.NewReader(first.Bytes()))
	if err != nil {
		t.Fatalf("open encoded chunk: %v", err)
	}
	if gz.Name != "" || !gz.ModTime.IsZero() {
		t.Fatalf("gzip header carries name %q and mtime %s, want neither", gz.Name, gz.ModTime)
	}
	dec := json.NewDecoder(gz)
	for i, want := range samples {
		var got model.Sample
		if err := dec.Decode(&got); err != nil {
			t.Fatalf("decode sample %d: %v", i, err)
		}
		if got.Metric != want.Metric || got.Value != want.Value || got.IntervalMS != want.IntervalMS ||
			!got.Timestamp.Equal(want.Timestamp) || len(got.Labels) != len(want.Labels) {
			t.Fatalf("sample %d = %+v, want %+v", i, got, want)
		}
	}
	var extra model.Sample
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("decode after the last sample = %v, want EOF", err)
	}
	_ = gz.Close()
}
