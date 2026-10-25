package chunk

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

func window() (time.Time, time.Time) {
	start := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	return start, start.Add(5 * time.Minute)
}

// sampleID describes a sample in the canonical order, so a test can state the
// expected sequence without repeating whole structs.
func sampleID(s model.Sample) string {
	labels := make([]string, 0, len(s.Labels))
	for _, key := range slices.Sorted(maps.Keys(s.Labels)) {
		labels = append(labels, key+"="+s.Labels[key])
	}
	return fmt.Sprintf("%s@%s[%s]=%s",
		s.Metric,
		s.Timestamp.UTC().Format(time.RFC3339Nano),
		strings.Join(labels, ","),
		strconv.FormatFloat(s.Value, 'g', -1, 64),
	)
}

func sampleIDs(samples []model.Sample) []string {
	ids := make([]string, len(samples))
	for i, sample := range samples {
		ids[i] = sampleID(sample)
	}
	return ids
}

func TestPrepareOrdering(t *testing.T) {
	start, end := window()
	at := func(offset time.Duration, metric string, value float64, labels map[string]string) model.Sample {
		return model.Sample{Timestamp: start.Add(offset), Metric: metric, Value: value, Labels: labels}
	}

	cases := []struct {
		name    string
		samples []model.Sample
		want    []string
	}{
		{
			name: "timestamp then metric",
			samples: []model.Sample{
				at(time.Second, "system.z", 1, nil),
				at(2*time.Second, "system.b", 2, nil),
				at(2*time.Second, "system.a", 3, nil),
			},
			want: sampleIDs([]model.Sample{
				at(time.Second, "system.z", 1, nil),
				at(2*time.Second, "system.a", 3, nil),
				at(2*time.Second, "system.b", 2, nil),
			}),
		},
		{
			name: "label key order",
			samples: []model.Sample{
				at(0, "m", 1, map[string]string{"b": "0"}),
				at(0, "m", 2, map[string]string{"a": "1"}),
				at(0, "m", 3, nil),
			},
			want: sampleIDs([]model.Sample{
				at(0, "m", 3, nil),
				at(0, "m", 2, map[string]string{"a": "1"}),
				at(0, "m", 1, map[string]string{"b": "0"}),
			}),
		},
		{
			name: "label key order beats label value",
			samples: []model.Sample{
				at(0, "m", 1, map[string]string{"b": "1"}),
				at(0, "m", 2, map[string]string{"a": "9"}),
			},
			want: sampleIDs([]model.Sample{
				at(0, "m", 2, map[string]string{"a": "9"}),
				at(0, "m", 1, map[string]string{"b": "1"}),
			}),
		},
		{
			name: "label value order",
			samples: []model.Sample{
				at(0, "m", 1, map[string]string{"a": "2"}),
				at(0, "m", 2, map[string]string{"a": "1"}),
			},
			want: sampleIDs([]model.Sample{
				at(0, "m", 2, map[string]string{"a": "1"}),
				at(0, "m", 1, map[string]string{"a": "2"}),
			}),
		},
		{
			name: "equal samples keep input order",
			samples: []model.Sample{
				at(0, "m", 2, map[string]string{"a": "1"}),
				at(0, "m", 1, map[string]string{"a": "1"}),
				at(0, "m", 3, map[string]string{"a": "1"}),
			},
			want: sampleIDs([]model.Sample{
				at(0, "m", 2, map[string]string{"a": "1"}),
				at(0, "m", 1, map[string]string{"a": "1"}),
				at(0, "m", 3, map[string]string{"a": "1"}),
			}),
		},
		{
			name:    "empty",
			samples: nil,
			want:    nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Prepare(start, end, tc.samples)
			if err != nil {
				t.Fatalf("Prepare() error: %v", err)
			}
			if ids := sampleIDs(got); !slices.Equal(ids, tc.want) {
				t.Fatalf("Prepare() = %v, want %v", ids, tc.want)
			}
		})
	}
}

func TestPrepareWindow(t *testing.T) {
	start, end := window()
	at := func(ts time.Time) []model.Sample {
		return []model.Sample{{Timestamp: ts, Metric: "system.cpu.percent", Value: 1}}
	}

	cases := []struct {
		name        string
		start       time.Time
		end         time.Time
		samples     []model.Sample
		wantErr     bool
		wantMessage []string
	}{
		{
			name:    "sample at start is inside",
			start:   start,
			end:     end,
			samples: at(start),
		},
		{
			name:    "sample just before end is inside",
			start:   start,
			end:     end,
			samples: at(end.Add(-time.Nanosecond)),
		},
		{
			name:        "sample before start is rejected",
			start:       start,
			end:         end,
			samples:     at(start.Add(-time.Second)),
			wantErr:     true,
			wantMessage: []string{"system.cpu.percent", start.Add(-time.Second).Format(time.RFC3339Nano)},
		},
		{
			name:        "sample at end is rejected",
			start:       start,
			end:         end,
			samples:     at(end),
			wantErr:     true,
			wantMessage: []string{"system.cpu.percent", end.Format(time.RFC3339Nano)},
		},
		{
			name:        "sample after end is rejected",
			start:       start,
			end:         end,
			samples:     at(end.Add(time.Second)),
			wantErr:     true,
			wantMessage: []string{"system.cpu.percent", end.Add(time.Second).Format(time.RFC3339Nano)},
		},
		{
			name:  "every offending sample is reported",
			start: start,
			end:   end,
			samples: []model.Sample{
				{Timestamp: start.Add(-time.Second), Metric: "early.metric", Value: 1},
				{Timestamp: end, Metric: "late.metric", Value: 2},
			},
			wantErr:     true,
			wantMessage: []string{"early.metric", "late.metric"},
		},
		{
			name:        "end equal to start is rejected",
			start:       start,
			end:         start,
			wantErr:     true,
			wantMessage: []string{"not after"},
		},
		{
			name:        "end before start is rejected",
			start:       start,
			end:         start.Add(-time.Minute),
			wantErr:     true,
			wantMessage: []string{"not after"},
		},
		{
			name:        "same instant in another zone is rejected",
			start:       start,
			end:         start.In(time.FixedZone("CEST", 2*60*60)),
			wantErr:     true,
			wantMessage: []string{"not after"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Prepare(tc.start, tc.end, tc.samples)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Prepare() = %v, want an error", sampleIDs(got))
				}
				for _, want := range tc.wantMessage {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("Prepare() error = %q, want it to mention %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Prepare() error: %v", err)
			}
			if len(got) != len(tc.samples) {
				t.Fatalf("Prepare() returned %d samples, want %d", len(got), len(tc.samples))
			}
		})
	}
}

func TestPrepareNormalizesUTC(t *testing.T) {
	start, end := window()
	cest := time.FixedZone("CEST", 2*60*60)

	// 18:00 CEST is 16:00 UTC: inside the window only after normalization.
	samples := []model.Sample{{
		Timestamp: time.Date(2026, 10, 23, 18, 0, 0, 0, cest),
		Metric:    "system.cpu.percent",
		Value:     1,
	}}
	got, err := Prepare(start.In(cest), end.In(cest), samples)
	if err != nil {
		t.Fatalf("Prepare() error: %v", err)
	}
	if loc := got[0].Timestamp.Location(); loc != time.UTC {
		t.Fatalf("timestamp location = %v, want UTC", loc)
	}
	if !got[0].Timestamp.Equal(samples[0].Timestamp) {
		t.Fatalf("timestamp = %s, want the same instant as %s", got[0].Timestamp, samples[0].Timestamp)
	}
}

func TestPrepareDoesNotMutateInput(t *testing.T) {
	start, end := window()
	cest := time.FixedZone("CEST", 2*60*60)
	samples := []model.Sample{
		{Timestamp: start.Add(2 * time.Second), Metric: "system.b", Value: 1},
		{Timestamp: time.Date(2026, 10, 23, 18, 0, 0, 0, cest), Metric: "system.a", Value: 2},
	}
	before := make([]model.Sample, len(samples))
	copy(before, samples)

	got, err := Prepare(start, end, samples)
	if err != nil {
		t.Fatalf("Prepare() error: %v", err)
	}
	for i := range samples {
		if samples[i].Metric != before[i].Metric ||
			!samples[i].Timestamp.Equal(before[i].Timestamp) ||
			samples[i].Timestamp.Location() != before[i].Timestamp.Location() {
			t.Fatalf("input sample %d = %+v, want it untouched at %+v", i, samples[i], before[i])
		}
	}

	// The result must be a separate slice, not the caller's backing array.
	got[0].Metric = "mutated"
	if samples[0].Metric != before[0].Metric || samples[1].Metric != before[1].Metric {
		t.Fatal("mutating the result changed the input slice")
	}
}

func TestPreparePreservesEncoding(t *testing.T) {
	start, end := window()
	samples := []model.Sample{
		{
			Timestamp: start.Add(time.Second),
			Metric:    "system.cpu.percent",
			Value:     12.5,
			Labels:    map[string]string{"host": "web-1", "scope": "system", "cpu": "all"},
		},
		{
			Timestamp: start.Add(2 * time.Second),
			Metric:    "system.memory.used_bytes",
			Value:     1024,
			Labels:    map[string]string{"host": "web-1", "scope": "system"},
		},
	}

	prepared, err := Prepare(start, end, samples)
	if err != nil {
		t.Fatalf("Prepare() error: %v", err)
	}

	var direct, sorted bytes.Buffer
	if err := Encode(&direct, samples); err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if err := Encode(&sorted, prepared); err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if !bytes.Equal(direct.Bytes(), sorted.Bytes()) {
		t.Fatal("canonical input encoded differently after Prepare")
	}
}
