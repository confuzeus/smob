package main

import (
	"fmt"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/disk"
)

// newStore returns the active backend. Both collect and query use the same
// helper, so they always agree on where data lives.
func newStore(cfg config.Config) (store.Store, error) {
	switch cfg.Storage.Primary {
	case "disk":
		return disk.New(cfg.DataDir)
	default:
		return nil, fmt.Errorf("storage.primary: %q is not supported in this build", cfg.Storage.Primary)
	}
}
