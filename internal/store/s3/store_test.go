package s3

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/confuzeus/smob/internal/store/chunk"
)

func TestSpoolable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, false},
		{"terminal", fmt.Errorf("put key: %w: %w", errTerminalUpload, statusError(http.StatusForbidden)), false},
		{"wrapped terminal", fmt.Errorf("write: %w", fmt.Errorf("%w: %w", errTerminalUpload, statusError(http.StatusUnauthorized))), false},
		{"retry budget exhausted", fmt.Errorf("put key after %d attempts: %w", retryAttempts, statusError(http.StatusServiceUnavailable)), true},
		{"verification read failed", fmt.Errorf("verify key: %w", statusError(http.StatusServiceUnavailable)), true},
		{"collision exhaustion", fmt.Errorf("%w after %d collisions for key", errCollisionExhausted, maxCollisions), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := spoolable(tc.err); got != tc.want {
				t.Errorf("spoolable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestCandidateKey(t *testing.T) {
	const key = "host/system/2026/10/25/15/20261025T150000Z-20261025T150010Z.jsonl.gz"

	cases := []struct {
		name      string
		n         int
		want      string
		exhausted bool
	}{
		{"base", 0, key, false},
		{"first suffix", 1, chunk.CollisionKey(key, 1), false},
		{"last suffix", maxCollisions, chunk.CollisionKey(key, maxCollisions), false},
		{"past the cap falls back to the base key", maxCollisions + 1, key, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, exhausted := candidateKey(key, tc.n)
			if got != tc.want || exhausted != tc.exhausted {
				t.Errorf("candidateKey(%q, %d) = (%q, %t), want (%q, %t)", key, tc.n, got, exhausted, tc.want, tc.exhausted)
			}
		})
	}
}
