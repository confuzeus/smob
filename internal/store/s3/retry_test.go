package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// statusError builds the error shape the AWS SDK returns for an HTTP response,
// so the classifier can be tested without contacting a service.
func statusError(status int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      errors.New("response"),
	}
}

func TestBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 250 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{4, 2 * time.Second},
		{5, 4 * time.Second},
		{6, 4 * time.Second},
		{100, 4 * time.Second},
	}

	for _, tc := range cases {
		if got := backoff(tc.attempt); got != tc.want {
			t.Errorf("backoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestJitter(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		u    float64
		want time.Duration
	}{
		{"minimum", time.Second, 0, 500 * time.Millisecond},
		{"midpoint", time.Second, 0.5, 750 * time.Millisecond},
		{"below maximum", time.Second, 0.999, 999500 * time.Microsecond},
		{"base minimum", 250 * time.Millisecond, 0, 125 * time.Millisecond},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := jitter(tc.d, tc.u); got != tc.want {
				t.Errorf("jitter(%s, %v) = %s, want %s", tc.d, tc.u, got, tc.want)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"deadline", fmt.Errorf("get: %w", context.DeadlineExceeded), false},
		{"bad request", statusError(http.StatusBadRequest), false},
		{"unauthorized", statusError(http.StatusUnauthorized), false},
		{"forbidden", statusError(http.StatusForbidden), false},
		{"not found", statusError(http.StatusNotFound), false},
		{"conflict", statusError(http.StatusConflict), false},
		{"precondition failed", statusError(http.StatusPreconditionFailed), false},
		{"no response", statusError(0), true},
		{"request timeout", statusError(http.StatusRequestTimeout), true},
		{"too many requests", statusError(http.StatusTooManyRequests), true},
		{"internal error", statusError(http.StatusInternalServerError), true},
		{"not implemented", statusError(http.StatusNotImplemented), true},
		{"unavailable", statusError(http.StatusServiceUnavailable), true},
		{"unknown server error", statusError(599), true},
		{"network", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"plain", errors.New("boom"), false},
		{"wrapped unavailable", fmt.Errorf("put: %w", statusError(http.StatusServiceUnavailable)), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Errorf("isRetryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyPut(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		ambiguous bool
		want      putAction
	}{
		{"success", nil, true, putSucceeded},
		{"clean conflict", statusError(http.StatusPreconditionFailed), false, putCollision},
		{"ambiguous conflict", statusError(http.StatusPreconditionFailed), true, putVerify},
		{"clean conditional conflict", statusError(http.StatusConflict), false, putCollision},
		{"ambiguous conditional conflict", statusError(http.StatusConflict), true, putVerify},
		{"transient", statusError(http.StatusServiceUnavailable), false, putRetry},
		{"throttled", statusError(http.StatusTooManyRequests), true, putRetry},
		{"forbidden", statusError(http.StatusForbidden), true, putFailed},
		{"bad request", statusError(http.StatusBadRequest), false, putFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPut(tc.err, tc.ambiguous); got != tc.want {
				t.Errorf("classifyPut(%v, %t) = %d, want %d", tc.err, tc.ambiguous, got, tc.want)
			}
		})
	}
}

func TestRetryStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := retry(ctx, func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("operation called %d times, want 0", calls)
	}
}

func TestRetryStopsWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	calls := 0
	err := retry(ctx, func(context.Context) error {
		calls++
		return statusError(http.StatusServiceUnavailable)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry error = %v, want context.DeadlineExceeded", err)
	}
	if calls != 1 {
		t.Fatalf("operation called %d times, want 1", calls)
	}
}

func TestRetryStopsOnTerminalError(t *testing.T) {
	terminal := statusError(http.StatusForbidden)
	calls := 0
	err := retry(context.Background(), func(context.Context) error {
		calls++
		return terminal
	})
	if !errors.Is(err, terminal) {
		t.Fatalf("retry error = %v, want the terminal error", err)
	}
	if calls != 1 {
		t.Fatalf("operation called %d times, want 1", calls)
	}
}
