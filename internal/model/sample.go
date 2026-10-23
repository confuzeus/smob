// Package model defines the metric samples and queries shared by the
// collector, the stores, and the CLI.
package model

import "time"

// Sample is a single metric observation at a point in time.
//
// Timestamps are UTC. Every series is identified by Metric plus Labels; labels
// describe the collection scope (for example scope=system) and any per-series
// dimensions such as the network interface or process ID.
type Sample struct {
	Timestamp time.Time         `json:"timestamp"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Label returns the value of the named label, or the empty string when it is
// absent.
func (s Sample) Label(name string) string {
	return s.Labels[name]
}
