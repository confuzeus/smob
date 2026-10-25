package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// statusError builds the error shape the AWS SDK returns for an HTTP response,
// so the classifier can be tested without contacting a service.
func statusError(status int) error {
	return statusErrorWithHeader(status, nil)
}

// statusErrorWithHeader is statusError carrying response headers, which the
// Retry-After assertion needs.
func statusErrorWithHeader(status int, header http.Header) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status, Header: header}},
		Err:      errors.New("response"),
	}
}

// timeoutError is the net.Error shape a socket read deadline produces.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

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

// TestRetryBudget pins the arithmetic behind the documented budget: six
// attempts pay for five waits, from the 250ms base up to the 4s cap, and equal
// jitter holds each wait in [d/2, d). A fully failed exchange therefore waits
// less than 7.75s, so it reaches the spool well inside one chunk interval.
func TestRetryBudget(t *testing.T) {
	waits := make([]time.Duration, 0, retryAttempts-1)
	for attempt := 1; attempt < retryAttempts; attempt++ {
		waits = append(waits, backoff(attempt))
	}
	if len(waits) != 5 {
		t.Fatalf("%d attempts produce %d waits, want 5", retryAttempts, len(waits))
	}
	if waits[0] != retryBaseDelay {
		t.Errorf("first wait = %s, want the %s base", waits[0], retryBaseDelay)
	}

	var uncapped time.Duration
	for i, d := range waits {
		if d > retryMaxDelay {
			t.Errorf("wait %d = %s, want at most the %s cap", i+1, d, retryMaxDelay)
		}
		if i > 0 && d < waits[i-1] {
			t.Errorf("wait %d = %s, want at least the previous %s", i+1, d, waits[i-1])
		}
		uncapped += d
	}

	const budget = 7750 * time.Millisecond
	if uncapped != budget {
		t.Fatalf("uncapped wait total = %s, want %s", uncapped, budget)
	}

	// u is in [0, 1); Nextafter gives the largest float64 below 1.
	var fastest, slowest time.Duration
	for _, d := range waits {
		fastest += jitter(d, 0)
		slowest += jitter(d, math.Nextafter(1, 0))
	}
	if fastest != budget/2 {
		t.Errorf("fastest jittered total = %s, want half the uncapped total (%s)", fastest, budget/2)
	}
	if slowest >= budget {
		t.Errorf("slowest jittered total = %s, want below %s: jitter never reaches the full delay", slowest, budget)
	}
	for _, d := range waits {
		for _, u := range []float64{0, 0.25, 0.5, 0.999999} {
			if got := jitter(d, u); got < d/2 || got >= d {
				t.Errorf("jitter(%s, %v) = %s, want in [%s, %s)", d, u, got, d/2, d)
			}
		}
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
		{"moved permanently", statusError(http.StatusMovedPermanently), false},
		{"found", statusError(http.StatusFound), false},
		{"not modified", statusError(http.StatusNotModified), false},
		{"temporary redirect", statusError(http.StatusTemporaryRedirect), false},
		{"permanent redirect", statusError(http.StatusPermanentRedirect), false},
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
		{"bad gateway", statusError(http.StatusBadGateway), true},
		{"unavailable", statusError(http.StatusServiceUnavailable), true},
		{"gateway timeout", statusError(http.StatusGatewayTimeout), true},
		{"unknown server error", statusError(599), true},
		{"network", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		{"network timeout", timeoutError{}, true},
		{"internal error code", &smithy.GenericAPIError{Code: "InternalError"}, true},
		{"slow down", &smithy.GenericAPIError{Code: "SlowDown"}, true},
		{"throttling code", &smithy.GenericAPIError{Code: "ThrottlingException"}, true},
		{"access denied code", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"attempt timeout", fmt.Errorf("%w: %w", errAttemptTimeout, context.DeadlineExceeded), true},
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
	// A real 409 needs concurrent conditional writers and is not reliably
	// reproducible against MinIO, so the classifier is exercised with the
	// error shapes the AWS SDK returns.
	cases := []struct {
		name      string
		err       error
		ambiguous bool
		want      putAction
	}{
		{"success", nil, true, putSucceeded},
		{"key exists", statusError(http.StatusPreconditionFailed), false, putCollision},
		{"key exists after ambiguous attempt", statusError(http.StatusPreconditionFailed), true, putVerify},
		{"concurrent write", statusError(http.StatusConflict), false, putRetryConflict},
		{"concurrent write after ambiguous attempt", statusError(http.StatusConflict), true, putRetryConflict},
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

// TestRetryIgnoresRetryAfter documents that smob does not honor Retry-After:
// the SDK retryer is disabled and retry waits only its own jittered backoff. A
// service asking for a minute would otherwise stall a five-minute chunk cadence
// for most of an interval. If this behavior changes, TestRetryBudget has to be
// revisited deliberately as well.
func TestRetryIgnoresRetryAfter(t *testing.T) {
	throttled := statusErrorWithHeader(http.StatusTooManyRequests, http.Header{"Retry-After": {"60"}})
	var responseErr *smithyhttp.ResponseError
	if !errors.As(throttled, &responseErr) || responseErr.HTTPResponse().Header.Get("Retry-After") != "60" {
		t.Fatalf("test error does not carry Retry-After: %v", throttled)
	}

	calls := 0
	start := time.Now()
	err := retry(context.Background(), func(context.Context) error {
		calls++
		if calls == 1 {
			return throttled
		}
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("retry error = %v, want success on the second attempt", err)
	}
	if calls != 2 {
		t.Fatalf("operation called %d times, want 2", calls)
	}
	if elapsed >= time.Second {
		t.Fatalf("retry took %s with Retry-After: 60, want the jittered budget only", elapsed)
	}
}

func TestWaitRetryCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := waitRetry(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitRetry on a canceled context = %v, want context.Canceled", err)
	}
}

// TestWaitRetryCancellationIsPrompt proves an armed timer is interrupted
// instead of ridden out. The last wait the retry loop can use follows the 4s
// cap, so its jittered delay is at least 2s; a prompt return is far below the
// bound, while a timer-only wait would need at least 2s.
func TestWaitRetryCancellationIsPrompt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- waitRetry(ctx, retryAttempts-1) }()
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("waitRetry error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Errorf("waitRetry returned after %s, want a prompt cancellation return", elapsed)
	}
}
