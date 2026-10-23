// Package store defines the persistence contract used by the collector and the
// query commands. Disk and S3 are peer implementations behind it.
package store

import (
	"context"

	"github.com/confuzeus/smob/internal/model"
)

// Store persists samples and reads them back.
//
// Write is called with a batch that shares a collection tick. Query returns the
// matching samples in non-decreasing timestamp order. Close releases backend
// resources and flushes anything still buffered.
type Store interface {
	Write(ctx context.Context, samples []model.Sample) error
	Query(ctx context.Context, q model.Query) ([]model.Sample, error)
	Close() error
}
