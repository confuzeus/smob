package query

import (
	"maps"
	"slices"

	"github.com/confuzeus/smob/internal/model"
)

// Summary is one metric's aggregate over the samples that matched a query.
// Count is the number of samples, Min and Max are the smallest and largest
// sample values, and Mean is their arithmetic mean.
type Summary struct {
	Metric string  `json:"metric"`
	Count  int64   `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
}

// Summarizer accumulates one Summary per metric from a stream of samples. It
// holds only running totals, never the samples themselves. The zero value is
// ready to use.
type Summarizer struct {
	metrics map[string]*accumulator
}

// Add folds sample into its metric's accumulator.
func (s *Summarizer) Add(sample model.Sample) {
	if s.metrics == nil {
		s.metrics = make(map[string]*accumulator)
	}
	acc := s.metrics[sample.Metric]
	if acc == nil {
		acc = &accumulator{}
		s.metrics[sample.Metric] = acc
	}
	acc.add(sample)
}

// Summaries returns one Summary per metric, sorted by metric name.
func (s *Summarizer) Summaries() []Summary {
	metrics := slices.Sorted(maps.Keys(s.metrics))
	summaries := make([]Summary, 0, len(metrics))
	for _, metric := range metrics {
		summaries = append(summaries, s.metrics[metric].summary(metric))
	}
	return summaries
}

// accumulator folds sample values into a count, unweighted sum, minimum,
// maximum, and interval-weighted totals. It backs both stats' unweighted
// Summary and query --agg, which reads the weighted totals directly.
type accumulator struct {
	count       int64
	sum         float64
	weightedSum float64
	weightSum   float64
	min         float64
	max         float64
}

func (a *accumulator) add(sample model.Sample) {
	value := sample.Value
	weight := 1.0
	if sample.IntervalMS > 0 {
		weight = float64(sample.IntervalMS)
	}

	if a.count == 0 {
		a.min, a.max = value, value
	} else {
		a.min = min(a.min, value)
		a.max = max(a.max, value)
	}
	a.count++
	a.sum += value
	a.weightedSum += value * weight
	a.weightSum += weight
}

// mean is the unweighted arithmetic mean used by stats.
func (a *accumulator) mean() float64 {
	return a.sum / float64(a.count)
}

// value returns the selected aggregation for query --agg. Weighting applies
// only to avg; min, max, and sum use the raw sample values.
func (a *accumulator) value(op Aggregation) float64 {
	switch op {
	case AggAvg:
		return a.weightedSum / a.weightSum
	case AggMin:
		return a.min
	case AggMax:
		return a.max
	default:
		return a.sum
	}
}

func (a *accumulator) summary(metric string) Summary {
	return Summary{
		Metric: metric,
		Count:  a.count,
		Min:    a.min,
		Max:    a.max,
		Mean:   a.mean(),
	}
}
