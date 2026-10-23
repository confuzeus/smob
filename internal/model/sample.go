// Package model defines the metric samples and queries shared by the
// collector, the stores, and the CLI.
package model

import (
	"maps"
	"slices"
	"sort"
	"time"
)

// Sample is a single metric observation at a point in time.
//
// Timestamps are UTC. Every series is identified by Metric plus Labels; labels
// describe the collection scope (for example scope=system) and any per-series
// dimensions such as the network interface or process ID. IntervalMS records
// the measurement window for rate samples and is zero for gauges.
type Sample struct {
	Timestamp  time.Time         `json:"timestamp"`
	Metric     string            `json:"metric"`
	Value      float64           `json:"value"`
	IntervalMS int64             `json:"interval_ms,omitempty"` // rate window; 0 for gauges
	Labels     map[string]string `json:"labels,omitempty"`
}

// Label returns the value of the named label, or the empty string when it is
// absent.
func (s Sample) Label(name string) string {
	return s.Labels[name]
}

// SortSamples orders samples by timestamp, then metric, then canonical label
// order. Identical inputs therefore produce identical chunk contents and query
// output regardless of backend.
func SortSamples(samples []Sample) {
	sort.SliceStable(samples, func(i, j int) bool {
		return LessSample(samples[i], samples[j])
	})
}

// LessSample reports whether a sorts before b by timestamp, then metric, then
// canonical label order. It is the single ordering rule shared by the chunk
// writer and the query reader.
func LessSample(a, b Sample) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if a.Metric != b.Metric {
		return a.Metric < b.Metric
	}
	return labelsLess(a.Labels, b.Labels)
}

// labelsLess compares two label sets as sorted key/value pairs.
func labelsLess(a, b map[string]string) bool {
	aKeys := slices.Sorted(maps.Keys(a))
	bKeys := slices.Sorted(maps.Keys(b))

	for i := 0; i < len(aKeys) && i < len(bKeys); i++ {
		if aKeys[i] != bKeys[i] {
			return aKeys[i] < bKeys[i]
		}
		if a[aKeys[i]] != b[bKeys[i]] {
			return a[aKeys[i]] < b[bKeys[i]]
		}
	}
	return len(aKeys) < len(bKeys)
}
