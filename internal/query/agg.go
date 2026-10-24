package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/confuzeus/smob/internal/model"
)

// Aggregation selects the value an Aggregator reports for each group.
type Aggregation int

const (
	// AggAvg is the interval-weighted mean for rates and the unweighted mean
	// for gauges.
	AggAvg Aggregation = iota
	// AggMin is the smallest raw sample value.
	AggMin
	// AggMax is the largest raw sample value.
	AggMax
	// AggSum is the unweighted sum of sample values.
	AggSum
)

// ParseAggregation maps an --agg value to an Aggregation.
func ParseAggregation(value string) (Aggregation, error) {
	switch value {
	case "avg":
		return AggAvg, nil
	case "min":
		return AggMin, nil
	case "max":
		return AggMax, nil
	case "sum":
		return AggSum, nil
	default:
		return 0, fmt.Errorf("agg: must be one of avg, min, max, sum; got %q", value)
	}
}

// Aggregate is one query --agg row: the metric, the grouped labels, the number
// of contributing samples, and the selected aggregate.
type Aggregate struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Count  int64             `json:"count"`
	Value  float64           `json:"value"`
}

// Aggregator accumulates one value per (metric, group labels) key from a
// stream of samples. It retains only running totals, never the samples
// themselves. Memory is proportional to the number of groups.
type Aggregator struct {
	op     Aggregation
	keys   []string
	groups map[groupKey]*aggregateGroup
}

type groupKey struct {
	metric string
	values string
}

type aggregateGroup struct {
	metric string
	labels map[string]string
	values []string
	acc    accumulator
}

// NewAggregator returns an Aggregator for op that groups by metric plus the
// named labels. Metric is always part of the group key and may be listed in
// groupBy; label names are deduplicated and sorted into canonical order.
func NewAggregator(op Aggregation, groupBy []string) *Aggregator {
	seen := make(map[string]struct{}, len(groupBy))
	keys := make([]string, 0, len(groupBy))
	for _, key := range groupBy {
		if key == "metric" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	slices.Sort(keys)

	return &Aggregator{
		op:     op,
		keys:   keys,
		groups: make(map[groupKey]*aggregateGroup),
	}
}

// Add folds sample into its group.
func (a *Aggregator) Add(sample model.Sample) {
	var encoded strings.Builder
	for _, key := range a.keys {
		value := sample.Label(key)
		encoded.WriteString(strconv.Itoa(len(value)))
		encoded.WriteByte(':')
		encoded.WriteString(value)
	}

	key := groupKey{metric: sample.Metric, values: encoded.String()}
	group := a.groups[key]
	if group == nil {
		values := make([]string, len(a.keys))
		labels := make(map[string]string, len(a.keys))
		for i, name := range a.keys {
			values[i] = sample.Label(name)
			labels[name] = values[i]
		}
		group = &aggregateGroup{
			metric: sample.Metric,
			labels: labels,
			values: values,
		}
		a.groups[key] = group
	}
	group.acc.add(sample)
}

// Aggregates returns one Aggregate per group, sorted by metric and then by the
// grouped label values in canonical key order.
func (a *Aggregator) Aggregates() []Aggregate {
	groups := make([]*aggregateGroup, 0, len(a.groups))
	for _, group := range a.groups {
		groups = append(groups, group)
	}
	slices.SortFunc(groups, compareGroups)

	aggregates := make([]Aggregate, 0, len(groups))
	for _, group := range groups {
		aggregates = append(aggregates, Aggregate{
			Metric: group.metric,
			Labels: group.labels,
			Count:  group.acc.count,
			Value:  group.acc.value(a.op),
		})
	}
	return aggregates
}

// compareGroups orders groups by metric, then by each grouped label value in
// the canonical key order. Every group has the same number of values.
func compareGroups(a, b *aggregateGroup) int {
	if a.metric != b.metric {
		return strings.Compare(a.metric, b.metric)
	}
	for i := range a.values {
		if a.values[i] != b.values[i] {
			return strings.Compare(a.values[i], b.values[i])
		}
	}
	return 0
}
