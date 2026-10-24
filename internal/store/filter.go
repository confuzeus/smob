package store

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/confuzeus/smob/internal/model"
)

// Filter narrows samples to the range, host, scope, metric, and label
// predicates of one query. It is compiled once per query and used by every
// store backend, so filtering cannot drift between disk and S3.
type Filter struct {
	query    model.Query
	matchers []compiledMatcher
}

// NewFilter compiles q's label matchers. Limit is not a predicate; stores apply
// it while streaming results.
func NewFilter(q model.Query) (*Filter, error) {
	if q.Limit < 0 {
		return nil, errors.New("query: limit must not be negative")
	}

	matchers := make([]compiledMatcher, 0, len(q.Labels))
	for _, label := range q.Labels {
		if label.Name == "" {
			return nil, errors.New("query: label matcher has an empty name")
		}

		matcher := compiledMatcher{name: label.Name, op: label.Op, value: label.Value}
		switch label.Op {
		case model.LabelEqual:
		case model.LabelRegex:
			re, err := regexp.Compile(label.Value)
			if err != nil {
				return nil, fmt.Errorf("query: label %s: %w", label.Name, err)
			}
			matcher.re = re
		default:
			return nil, fmt.Errorf("query: label %s: unknown matcher operation %d", label.Name, label.Op)
		}
		matchers = append(matchers, matcher)
	}
	return &Filter{query: q, matchers: matchers}, nil
}

// Match reports whether sample satisfies the query predicates. A missing label
// reads as the empty string, so an exact empty match and a regex such as .* both
// match an absent label.
func (f *Filter) Match(sample model.Sample) bool {
	if !f.query.Range.Contains(sample.Timestamp) {
		return false
	}
	if f.query.Host != "" && sample.Labels["host"] != f.query.Host {
		return false
	}
	if f.query.Scope != "" && sample.Labels["scope"] != f.query.Scope {
		return false
	}
	if f.query.Metric != "" && sample.Metric != f.query.Metric {
		return false
	}
	for _, matcher := range f.matchers {
		value := sample.Labels[matcher.name]
		switch matcher.op {
		case model.LabelEqual:
			if value != matcher.value {
				return false
			}
		case model.LabelRegex:
			if !matcher.re.MatchString(value) {
				return false
			}
		}
	}
	return true
}

// compiledMatcher is a label matcher with its regular expression precompiled.
type compiledMatcher struct {
	name  string
	op    model.LabelOp
	value string
	re    *regexp.Regexp
}
