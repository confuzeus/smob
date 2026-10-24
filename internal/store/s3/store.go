package s3

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// maxCollisions bounds the suffix search so a writer bug cannot spin.
const maxCollisions = 1000

// Store reads and writes chunks in one S3 bucket under an optional key prefix.
// It uses the same relative chunk keys as the disk backend.
type Store struct {
	client *awss3.Client
	bucket string
	prefix string
}

// New returns an S3 store for cfg. The configuration must already be resolved
// with config.Config.Resolve. New does not contact the endpoint; Check validates
// the bucket at startup.
func New(ctx context.Context, cfg config.S3Storage) (*Store, error) {
	client, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: strings.Trim(cfg.Prefix, "/"),
	}, nil
}

// Write persists one completed chunk with a conditional PUT, retrying
// transient failures with the same encoded bytes. If the window already
// exists, the writer appends -1, -2, and so on instead of overwriting,
// matching the disk backend.
func (s *Store) Write(ctx context.Context, c store.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := chunk.Key(c.Host, c.Scope, c.Start, c.End)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}

	var body bytes.Buffer
	if err := chunk.Encode(&body, c.Samples); err != nil {
		return fmt.Errorf("encode %s: %w: %w", s.objectKey(key), store.ErrPersistent, err)
	}
	if err := s.put(ctx, key, body.Bytes()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("write %s: %w: %w", s.objectKey(key), store.ErrPersistent, err)
	}
	return nil
}

// Check implements store.Checker. It verifies that the bucket is reachable and
// that the configured credentials can read it. Write permission is proven by
// the first chunk write.
func (s *Store) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := retry(ctx, func(ctx context.Context) error {
		_, err := s.client.HeadBucket(ctx, &awss3.HeadBucketInput{
			Bucket: aws.String(s.bucket),
		})
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("check %s: %w: %w", s.location(), store.ErrPersistent, err)
	}
	return nil
}

// Close implements store.Store. An S3 store holds no resources between calls.
func (s *Store) Close() error { return nil }

// put writes body to key without replacing an existing object. A conditional
// write is the only way S3 can express the disk backend's collision guard; it
// relies on the endpoint honoring If-None-Match. If a key already exists, the
// next collision suffix is tried.
//
// An ambiguous PUT — a timeout or transient error that may still have been
// applied — is resolved by reading the object and comparing it with body.
// Equal bytes mean this writer's chunk is already stored; different bytes mean
// another writer owns the window, so the next collision suffix is tried. The
// comparison is safe because chunks are immutable and deterministically
// encoded: identical bytes describe the same window content. It runs only
// after an ambiguous attempt, so a clean conflict still suffixes directly.
func (s *Store) put(ctx context.Context, key string, body []byte) error {
	for n := 0; n <= maxCollisions; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidate := key
		if n > 0 {
			candidate = chunk.CollisionKey(key, n)
		}
		done, err := s.putCandidate(ctx, s.objectKey(candidate), body)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return fmt.Errorf("no free name after %d collisions for %s", maxCollisions, s.objectKey(key))
}

// putAction is the next step after one conditional PUT attempt.
type putAction int

const (
	putSucceeded putAction = iota
	putCollision
	putVerify
	putRetry
	putFailed
)

// classifyPut decides how to react to a conditional PUT result. A precondition
// failure is ambiguous only when an earlier attempt on the same key may have
// landed; otherwise another writer definitely owns the key.
func classifyPut(err error, ambiguous bool) putAction {
	switch {
	case err == nil:
		return putSucceeded
	case isPreconditionFailed(err):
		if ambiguous {
			return putVerify
		}
		return putCollision
	case isRetryable(err):
		return putRetry
	default:
		return putFailed
	}
}

// putCandidate attempts one object key. It returns done == false with a nil
// error when another writer owns the key and the caller should try the next
// suffix.
func (s *Store) putCandidate(ctx context.Context, target string, body []byte) (bool, error) {
	ambiguous := false
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(target),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			IfNoneMatch:   aws.String("*"),
		})
		action := classifyPut(err, ambiguous)
		if action == putSucceeded {
			return true, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}

		switch action {
		case putCollision:
			return false, nil
		case putVerify:
			same, found, err := s.verifyObject(ctx, target, body)
			if err != nil {
				return false, err
			}
			switch {
			case !found && attempt == retryAttempts:
				return false, fmt.Errorf("put %s: object disappeared after %d attempts", target, attempt)
			case !found:
				// The object vanished between the conflict and the read, so
				// retry the same key rather than suffixing.
				continue
			case same:
				return true, nil
			default:
				return false, nil
			}
		case putRetry:
			ambiguous = true
			if attempt == retryAttempts {
				return false, fmt.Errorf("put %s after %d attempts: %w", target, attempt, err)
			}
			if err := waitRetry(ctx, attempt); err != nil {
				return false, err
			}
		default:
			return false, fmt.Errorf("put %s: %w", target, err)
		}
	}
	return false, fmt.Errorf("put %s: retry budget exhausted", target)
}

// verifyObject reports whether target already holds exactly body. A missing
// object is reported as found == false so the caller can retry the same key.
func (s *Store) verifyObject(ctx context.Context, target string, body []byte) (same, found bool, err error) {
	existing, found, err := s.getObject(ctx, target)
	if err != nil {
		return false, false, fmt.Errorf("verify %s: %w", target, err)
	}
	if !found {
		return false, false, nil
	}
	return bytes.Equal(existing, body), true, nil
}

// objectKey returns the full object key for a relative chunk key.
func (s *Store) objectKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// rootPrefix returns the listing prefix that contains every host, with a
// trailing slash so a partial prefix cannot match a neighbouring root.
func (s *Store) rootPrefix() string {
	if s.prefix == "" {
		return ""
	}
	return s.prefix + "/"
}

// location returns the configured store location for error messages.
func (s *Store) location() string {
	if s.prefix == "" {
		return "s3://" + s.bucket
	}
	return "s3://" + s.bucket + "/" + s.prefix
}
