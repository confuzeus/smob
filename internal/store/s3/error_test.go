package s3

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/smithy-go"
)

// TestConditionalErrorClassification pins the 409/412 matching. Depending on
// the transport the SDK surfaces these as an error code, an HTTP status, or
// both, and the writer's decision must not depend on which shape arrives.
func TestConditionalErrorClassification(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		precondition bool
		conflict     bool
	}{
		{"precondition status", statusError(http.StatusPreconditionFailed), true, false},
		{"precondition code", &smithy.GenericAPIError{Code: "PreconditionFailed"}, true, false},
		{"conflict status", statusError(http.StatusConflict), false, true},
		{"conflict code", &smithy.GenericAPIError{Code: "ConditionalRequestConflict"}, false, true},
		{"wrapped precondition", fmt.Errorf("put: %w", statusError(http.StatusPreconditionFailed)), true, false},
		{"nil", nil, false, false},
		{"unrelated transient", statusError(http.StatusServiceUnavailable), false, false},
		{"unrelated terminal", statusError(http.StatusForbidden), false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPreconditionFailed(tc.err); got != tc.precondition {
				t.Errorf("isPreconditionFailed(%v) = %t, want %t", tc.err, got, tc.precondition)
			}
			if got := isConditionalRequestConflict(tc.err); got != tc.conflict {
				t.Errorf("isConditionalRequestConflict(%v) = %t, want %t", tc.err, got, tc.conflict)
			}
		})
	}
}

// TestVerifyReadFailureClassification covers the byte-compare GET that follows
// an ambiguous conditional PUT. A transient read failure stays retryable so
// Write spools the chunk; a terminal one (a revoked credential, a denied
// permission) fails the write with store.ErrPersistent instead of growing a
// spool replay could never drain. A real 412-then-503 exchange needs a service
// that impersonates MinIO, so the classifier is exercised with the error
// shapes the SDK returns.
func TestVerifyReadFailureClassification(t *testing.T) {
	const key = "host/system/2026/10/25/15/20261025T150000Z-20261025T150500Z.jsonl.gz"

	cases := []struct {
		name          string
		readErr       error
		wantSpoolable bool
	}{
		{"service unavailable", statusError(http.StatusServiceUnavailable), true},
		{"throttled", statusError(http.StatusTooManyRequests), true},
		{"network timeout", timeoutError{}, true},
		{"forbidden", statusError(http.StatusForbidden), false},
		{"unauthorized", statusError(http.StatusUnauthorized), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// verifyObject wraps the GET failure; putCandidate keeps a
			// retryable one as-is and marks a terminal one errTerminalUpload.
			err := fmt.Errorf("verify %s: %w", key, tc.readErr)
			if !isRetryable(err) {
				err = fmt.Errorf("%w: %w", errTerminalUpload, err)
			}

			if got := spoolable(err); got != tc.wantSpoolable {
				t.Errorf("spoolable(%v) = %t, want %t", err, got, tc.wantSpoolable)
			}
		})
	}
}
