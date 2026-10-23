package model

import "time"

// TimeRange is a half-open time interval [Start, End). A zero Start or End
// leaves that side of the interval unbounded.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// Contains reports whether t falls within the range.
func (r TimeRange) Contains(t time.Time) bool {
	if !r.Start.IsZero() && t.Before(r.Start) {
		return false
	}
	if !r.End.IsZero() && !t.Before(r.End) {
		return false
	}
	return true
}

// Query selects samples from a store.
type Query struct {
	Range  TimeRange
	Metric string
	Labels map[string]string
}
