// Package store defines the persistence contract used by the collector and the
// query commands. Disk and S3 are peer implementations behind it.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

// ErrPersistent marks a failure to durably persist a chunk, such as a full
// disk. The CLI reports exit code 3 when an error wraps it.
var ErrPersistent = errors.New("persistent store failure")

// Chunk is one closed, immutable time window of samples for a single collection
// scope and host. Samples are stored sorted by timestamp, then metric, then
// canonical label order.
type Chunk struct {
	Host    string
	Scope   string
	Start   time.Time
	End     time.Time
	Samples []model.Sample
}

// Checker is implemented by stores that can verify they are ready to accept
// writes without persisting a chunk. The collector probes it at startup so an
// unusable backend fails before collection begins.
type Checker interface {
	Check(ctx context.Context) error
}

// Store persists chunks and reads samples back.
//
// Write stores a complete chunk. The collector owns chunk assembly, so a store
// never buffers across Write calls. Query yields matching samples in
// non-decreasing timestamp order and stops when yield returns an error.
// Close releases backend resources.
type Store interface {
	Write(ctx context.Context, chunk Chunk) error
	Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error
	Close() error
}
