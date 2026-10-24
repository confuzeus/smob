package s3

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Retry bounds. A five-minute chunk cadence tolerates a few seconds of
// transient failure, but a longer outage is the spool's job.
// Six attempts with equal jitter add at most about 7.75s of delay, so a broken
// backend reaches the spool promptly instead of stalling for a whole chunk
// interval.
const (
	retryAttempts  = 6
	retryBaseDelay = 250 * time.Millisecond
	retryMaxDelay  = 4 * time.Second
)

// retry runs op until it succeeds, a terminal error occurs, or the attempt
// budget is exhausted. It returns the context error as soon as ctx is done and
// never waits after the final attempt.
func retry(ctx context.Context, op func(context.Context) error) error {
	var err error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		err = op(ctx)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !isRetryable(err) || attempt == retryAttempts {
			return err
		}
		if err := waitRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return err
}

// waitRetry waits for the jittered delay after a failed attempt, returning the
// context error if the wait is interrupted.
func waitRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(jitter(backoff(attempt), rand.Float64()))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// backoff returns the capped exponential delay between attempts. attempt is
// one-based, so the first retry waits retryBaseDelay.
func backoff(attempt int) time.Duration {
	delay := retryBaseDelay
	for i := 1; i < attempt && delay < retryMaxDelay; i++ {
		delay *= 2
	}
	if delay > retryMaxDelay {
		return retryMaxDelay
	}
	return delay
}

// jitter spreads a delay over the second half of its range, from d/2 up to but
// not including d. The fixed half keeps retries progressing while the random
// half desynchronizes independent writers and hosts. u must be in [0, 1).
func jitter(d time.Duration, u float64) time.Duration {
	half := d / 2
	return half + time.Duration(u*float64(half))
}

// isRetryable reports whether err is a transient S3 failure worth another
// attempt. Authentication, authorization, malformed requests, and a missing
// bucket are terminal because retrying cannot fix them. Conditional-write
// conflicts are handled by the writer, not here.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		status := responseErr.HTTPStatusCode()
		if status == 0 {
			// The request produced no HTTP response, so the SDK reports
			// status 0 for a connection failure or a truncated exchange.
			return true
		}
		return status == http.StatusRequestTimeout ||
			status == http.StatusTooManyRequests ||
			(status >= 500 && status <= 599)
	}

	// Some transports surface throttling or service errors as API errors
	// without an HTTP response.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InternalError", "RequestTimeout", "RequestTimeoutException",
			"ServiceUnavailable", "SlowDown", "Throttling", "ThrottlingException",
			"TooManyRequests", "TooManyRequestsException":
			return true
		}
		return false
	}

	// Network failures and a truncated response body may succeed on a fresh
	// connection. A request that may have reached the service is treated as
	// retryable for reads and as ambiguous for conditional writes.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}
