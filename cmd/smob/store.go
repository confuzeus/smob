package main

import (
	"context"
	"fmt"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/disk"
	"github.com/confuzeus/smob/internal/store/s3"
)

// newStore returns the active backend. Both collect and query use the same
// helper, so they always agree on where data lives.
func newStore(ctx context.Context, cfg config.Config) (store.Store, error) {
	switch cfg.Storage.Primary {
	case "disk":
		return disk.New(cfg.DataDir)
	case "s3":
		return s3.New(ctx, cfg.Storage.S3)
	default:
		return nil, fmt.Errorf("storage.primary: %q is not supported in this build", cfg.Storage.Primary)
	}
}
