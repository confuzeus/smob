package s3

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// isPreconditionFailed reports whether err means the target key already exists.
// S3 returns PreconditionFailed (412) for If-None-Match: * when another object
// owns the key, which is resolved by trying the next collision suffix.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "PreconditionFailed" {
			return true
		}
	}

	var responseErr *smithyhttp.ResponseError
	return errors.As(err, &responseErr) &&
		responseErr.HTTPStatusCode() == http.StatusPreconditionFailed
}

// isConditionalRequestConflict reports whether err means a concurrent write
// prevented the If-None-Match check. ConditionalRequestConflict (409) does not
// mean the key exists: this write did not land, so it is retried on the same
// key instead of advancing to a collision suffix.
func isConditionalRequestConflict(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "ConditionalRequestConflict" {
			return true
		}
	}

	var responseErr *smithyhttp.ResponseError
	return errors.As(err, &responseErr) &&
		responseErr.HTTPStatusCode() == http.StatusConflict
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
