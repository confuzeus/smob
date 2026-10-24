package s3

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// isPreconditionFailed reports whether err means the target key already exists.
// S3 returns PreconditionFailed for If-None-Match: *, and
// ConditionalRequestConflict when a concurrent write prevents the check. Both
// cases are resolved by trying the next collision suffix.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}

	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		status := responseErr.HTTPStatusCode()
		return status == http.StatusPreconditionFailed || status == http.StatusConflict
	}
	return false
}

// isNotFound reports whether err means the object is gone. A chunk may
// disappear between listing and opening, which is not a query failure.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}

	var responseErr *smithyhttp.ResponseError
	return errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusNotFound
}
