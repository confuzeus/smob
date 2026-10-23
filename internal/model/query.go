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

// LabelOp is the comparison applied by a LabelMatcher.
type LabelOp int

const (
	// LabelEqual requires the label value to equal the matcher value.
	LabelEqual LabelOp = iota
	// LabelRegex requires the label value to match the matcher value as a
	// regular expression.
	LabelRegex
)

// LabelMatcher constrains one sample label.
type LabelMatcher struct {
	Name  string
	Op    LabelOp
	Value string
}

// Query selects samples from a store. Empty Host, Scope, and Metric fields are
// unconstrained. Matchers in Labels are ANDed, and Limit is applied after
// filtering and sorting. A zero Limit means unlimited.
type Query struct {
	Range  TimeRange
	Host   string
	Scope  string
	Metric string
	Labels []LabelMatcher
	Limit  int
}
