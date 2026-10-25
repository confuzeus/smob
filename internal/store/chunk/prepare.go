package chunk

import (
	"errors"
	"fmt"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

// Prepare returns a validated copy of samples for the half-open chunk window
// [start, end): sorted by timestamp, then metric, then canonical label order,
// with every timestamp normalized to UTC. It never mutates samples.
//
// The chunk contract requires that ordering, the half-open window, and UTC
// timestamps, and Query relies on all three. The stores call Prepare before
// encoding so a caller cannot silently write a chunk that reads back out of
// order or outside its own key. An out-of-range sample or a reversed window is
// a programming error, not a data problem, so it is reported rather than
// repaired.
func Prepare(start, end time.Time, samples []model.Sample) ([]model.Sample, error) {
	start, end = start.UTC(), end.UTC()
	if !end.After(start) {
		return nil, fmt.Errorf("chunk window: end %s is not after start %s",
			end.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano))
	}

	prepared := make([]model.Sample, len(samples))
	for i, sample := range samples {
		sample.Timestamp = sample.Timestamp.UTC()
		prepared[i] = sample
	}
	model.SortSamples(prepared)

	var errs []error
	for _, sample := range prepared {
		if sample.Timestamp.Before(start) || !sample.Timestamp.Before(end) {
			errs = append(errs, fmt.Errorf("sample %q at %s: outside chunk window [%s, %s)",
				sample.Metric,
				sample.Timestamp.Format(time.RFC3339Nano),
				start.Format(time.RFC3339Nano),
				end.Format(time.RFC3339Nano),
			))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return prepared, nil
}
