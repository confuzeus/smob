package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// maxCollisions bounds the suffix search so a writer bug cannot spin.
const maxCollisions = 1000

// errSpoolConflict means a spooled chunk names a key that already holds
// different bytes. Replay must not suffix: the chunk is left in the spool for
// an operator instead of creating a duplicate window.
var errSpoolConflict = errors.New("spooled chunk conflicts with an existing object")

// errSpoolCorrupt means a spooled file does not decode as the chunk encoding.
// Replay leaves the bytes on disk, logs at error, and blocks the key for the
// rest of the process: retrying an upload cannot repair the local file.
var errSpoolCorrupt = errors.New("spooled chunk failed verification")

// errTerminalUpload marks a rejected upload that retrying cannot fix: a
// revoked credential, a denied permission, or a malformed request. Write fails
// with store.ErrPersistent instead of spooling, because no later replay can
// succeed and a growing spool would evict recoverable data.
var errTerminalUpload = errors.New("terminal upload error")

// errCollisionExhausted means every key from the base name through
// maxCollisions is occupied. Write still spools the chunk under the base key so
// the bytes survive locally; this is not itself an upload failure, because no
// retry or replay can find a free name.
var errCollisionExhausted = errors.New("no free object key")

// spoolable reports whether a failed upload may be staged in the local spool.
// Only a failure that outlived the retry budget is: a terminal rejection or a
// missing upload error fails the write instead, because replay cannot fix it.
func spoolable(err error) bool {
	return err != nil && !errors.Is(err, errTerminalUpload)
}

// Store reads and writes chunks in one S3 bucket under an optional key prefix.
// It uses the same relative chunk keys as the disk backend. A local spool stages
// writes that exhaust the retry budget.
type Store struct {
	client *awss3.Client
	bucket string
	prefix string
	spool  *spool
	logger *slog.Logger
}

// New returns an S3 store for cfg. The configuration must already be resolved
// with config.Config.Resolve. New does not contact the endpoint; Check validates
// the bucket and the spool at startup.
func New(ctx context.Context, cfg config.S3Storage, logger *slog.Logger) (*Store, error) {
	client, err := NewClient(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.SpoolDir) == "" {
		return nil, errors.New("s3: spool directory must not be empty")
	}
	if cfg.SpoolMaxBytes <= 0 {
		return nil, errors.New("s3: spool_max_bytes must be greater than zero")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: strings.Trim(cfg.Prefix, "/"),
		spool:  newSpool(cfg.SpoolDir, cfg.SpoolMaxBytes, logger),
		logger: logger,
	}, nil
}

// Write persists one completed chunk with a conditional PUT, retrying
// transient failures with the same encoded bytes. Samples are sorted and
// validated by chunk.Prepare before encoding, so the stored chunk satisfies the
// Query ordering contract regardless of the caller. If the window already
// exists, the writer appends -1, -2, and so on instead of overwriting,
// matching the disk backend.
//
// When the upload cannot be persisted because the retry budget was exhausted,
// the same encoded bytes are written atomically to the local spool under the
// candidate key the PUT path was attempting, so replay's exact-key upload
// targets a name the bucket did not already own. The chunk is never silently
// dropped: if the spool write also fails, the error wraps store.ErrPersistent
// and collection exits 3. An upload rejected as terminal — bad credentials,
// denied permission, or a malformed request — is not spooled; it fails
// immediately with store.ErrPersistent, because replay could never succeed.
// When every collision suffix is occupied, the chunk is spooled under the base
// key as a last resort and the condition is logged at warn.
func (s *Store) Write(ctx context.Context, c store.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := chunk.Key(c.Host, c.Scope, c.Start, c.End)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	samples, err := chunk.Prepare(c.Start, c.End, c.Samples)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}

	var body bytes.Buffer
	if err := chunk.Encode(&body, samples); err != nil {
		return fmt.Errorf("encode %s: %w: %w", s.objectKey(key), store.ErrPersistent, err)
	}
	candidate, err := s.put(ctx, key, body.Bytes())
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !spoolable(err) {
			return fmt.Errorf("write %s: %w: %w", s.objectKey(candidate), store.ErrPersistent, err)
		}
		spooled, spoolErr := s.spool.write(ctx, candidate, body.Bytes())
		if spoolErr != nil {
			return fmt.Errorf("write %s: %w", s.objectKey(candidate), errors.Join(store.ErrPersistent, err, spoolErr))
		}
		if errors.Is(err, errCollisionExhausted) {
			s.logger.Warn("every object key is occupied; chunk spooled under the base key",
				"key", candidate, "spool_key", spooled, "error", err)
		} else {
			s.logger.Warn("upload failed; chunk spooled",
				"key", candidate, "spool_key", spooled, "error", err)
		}
		s.spool.enforceCap(ctx, spooled)
		return nil
	}
	return nil
}

// Check implements store.Checker. It verifies that the bucket is reachable and
// that the configured credentials can read it, and that the spool is a writable
// directory. Write permission to the bucket is proven by the first chunk write.
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
	if err := s.spool.check(ctx); err != nil {
		return fmt.Errorf("check spool %s: %w: %w", s.spool.dir, store.ErrPersistent, err)
	}
	return nil
}

// Close implements store.Store. An S3 store holds no resources between calls.
// Any background replay is owned by collect, which waits for it before the
// process exits; an in-flight request that outlives this call is harmless.
func (s *Store) Close() error { return nil }

// put writes body to the first free key starting at key and returns the
// relative candidate key it was attempting. On failure that candidate is what
// Write spools under, so replay's exact-key upload never targets a name this
// attempt already saw occupied.
//
// A conditional write is the only way S3 can express the disk backend's
// collision guard; it relies on the endpoint honoring If-None-Match. If a key
// already exists, the next collision suffix is tried.
//
// An ambiguous PUT — a timeout or transient error that may still have been
// applied — is resolved by reading the object and comparing it with body.
// Equal bytes mean this writer's chunk is already stored; different bytes mean
// another writer owns the window, so the next collision suffix is tried. The
// comparison is safe because chunks are immutable and deterministically
// encoded: identical bytes describe the same window content. It runs only
// after an ambiguous attempt, so a clean conflict still suffixes directly.
func (s *Store) put(ctx context.Context, key string, body []byte) (string, error) {
	for n := 0; ; n++ {
		candidate, exhausted := candidateKey(key, n)
		if exhausted {
			return candidate, fmt.Errorf("%w after %d collisions for %s", errCollisionExhausted, maxCollisions, s.objectKey(key))
		}
		if err := ctx.Err(); err != nil {
			return candidate, err
		}
		done, err := s.putCandidate(ctx, s.objectKey(candidate), body)
		if err != nil {
			return candidate, err
		}
		if done {
			return candidate, nil
		}
	}
}

// candidateKey returns the relative chunk key for collision attempt n and
// reports whether the suffix search is exhausted. Attempt 0 is the base key and
// attempts 1 through maxCollisions append the numeric suffix. Past the cap the
// base key is the last resort: no free name exists, but a spooled chunk must
// still be named so its bytes survive locally.
func candidateKey(key string, n int) (candidate string, exhausted bool) {
	switch {
	case n > maxCollisions:
		return key, true
	case n == 0:
		return key, false
	default:
		return chunk.CollisionKey(key, n), false
	}
}

// putAction is the next step after one conditional PUT attempt.
type putAction int

const (
	putSucceeded putAction = iota
	putCollision
	putVerify
	putRetry
	putRetryConflict
	putFailed
)

// classifyPut decides how to react to a conditional PUT result. A precondition
// failure is ambiguous only when an earlier attempt on the same key may have
// landed; otherwise another writer definitely owns the key. A conditional
// request conflict means a concurrent write prevented the check and this PUT
// did not land, so it retries the same key without becoming ambiguous.
func classifyPut(err error, ambiguous bool) putAction {
	switch {
	case err == nil:
		return putSucceeded
	case isPreconditionFailed(err):
		if ambiguous {
			return putVerify
		}
		return putCollision
	case isConditionalRequestConflict(err):
		return putRetryConflict
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

		err := attemptWithTimeout(ctx, func(ctx context.Context) error {
			_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
				Bucket:        aws.String(s.bucket),
				Key:           aws.String(target),
				Body:          bytes.NewReader(body),
				ContentLength: aws.Int64(int64(len(body))),
				IfNoneMatch:   aws.String("*"),
			})
			return err
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
				if ctxErr := ctx.Err(); ctxErr != nil {
					return false, ctxErr
				}
				if !isRetryable(err) {
					return false, fmt.Errorf("%w: %w", errTerminalUpload, err)
				}
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
		case putRetryConflict:
			// A concurrent writer prevented the condition, so this PUT did not
			// land. Retry the same key; do not mark the attempt ambiguous and
			// do not advance to a suffix.
			if attempt == retryAttempts {
				return false, fmt.Errorf("put %s after %d attempts: %w", target, attempt, err)
			}
			if err := waitRetry(ctx, attempt); err != nil {
				return false, err
			}
		default:
			return false, fmt.Errorf("put %s: %w: %w", target, errTerminalUpload, err)
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

// exactAttempts bounds putExact's outer loop when a conflicting object
// disappears between the conditional PUT and the comparison read.
const exactAttempts = 3

// putExact uploads body to exactly key. Unlike put, it never picks a collision
// suffix. An existing object with identical bytes is treated as success, which
// is what a replay needs when a previous upload landed but deleting the spool
// entry failed. Different bytes are a permanent conflict: replay leaves the
// spool entry for an operator rather than creating a -1 duplicate.
func (s *Store) putExact(ctx context.Context, key string, body []byte) error {
	target := s.objectKey(key)
	for attempt := 1; attempt <= exactAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := s.putCandidate(ctx, target, body)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// putCandidate reports false when the key is owned by another object.
		// Compare the bytes directly, because replay must distinguish "already
		// uploaded" from a genuine collision.
		same, found, err := s.verifyObject(ctx, target, body)
		if err != nil {
			return err
		}
		switch {
		case found && same:
			return nil
		case found:
			return fmt.Errorf("%s: %w", target, errSpoolConflict)
		}
		// The object vanished between the conflict and the read. Retry the
		// same key rather than suffixing.
	}
	return fmt.Errorf("put %s: retry budget exhausted", target)
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
