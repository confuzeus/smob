package system

import (
	"os"
	"path/filepath"
	"testing"
)

// readFixture returns a captured /proc file used as realistic parser input.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}
