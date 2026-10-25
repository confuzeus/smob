package query

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/confuzeus/smob/internal/model"
)

// Summary is one metric's aggregate over the samples that matched a query.
// Count is the number of samples, Min and Max are the smallest and largest
// raw sample values, and Mean is interval-weighted for rate samples and
// unweighted for gauges, matching query --agg avg.
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

// Summaries returns one Summary per metric, sorted by metric name. A metric
// with a non-finite aggregate is reported as an error naming the metric, so the
// caller can fail before writing any output.
func (s *Summarizer) Summaries() ([]Summary, error) {
	metrics := slices.Sorted(maps.Keys(s.metrics))
	summaries := make([]Summary, 0, len(metrics))
	for _, metric := range metrics {
		summary := s.metrics[metric].summary(metric)
		if err := summary.validate(); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

// accumulator folds sample values into a count, unweighted sum, minimum,
// maximum, and interval-weighted totals. It backs both stats' Summary and
// query --agg, which read the same weighted totals directly.
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

// mean is the interval-weighted mean that stats reports and that query
// --agg avg returns for the same samples.
func (a *accumulator) mean() float64 {
	return a.weightedSum / a.weightSum
}

// value returns the selected aggregation for query --agg. Weighting applies
// only to avg; min, max, and sum use the raw sample values.
func (a *accumulator) value(op Aggregation) float64 {
	switch op {
	case AggAvg:
		return a.mean()
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

// validate rejects a summary whose aggregate overflowed float64. Decoded sample
// values are finite, so a non-finite mean can only come from the weighted total.
func (s Summary) validate() error {
	if s.Count == 0 {
		return nil
	}
	for _, field := range [...]struct {
		name  string
		value float64
	}{
		{"min", s.Min},
		{"max", s.Max},
		{"mean", s.Mean},
	} {
		if math.IsNaN(field.value) || math.IsInf(field.value, 0) {
			return fmt.Errorf("metric %q: %s is not finite", s.Metric, field.name)
		}
	}
	return nil
}
