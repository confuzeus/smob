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

// Write persists one completed chunk with a single conditional PUT. If the
// window already exists, the writer appends -1, -2, and so on instead of
// overwriting, matching the disk backend.
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
	if _, err := s.client.HeadBucket(ctx, &awss3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	}); err != nil {
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
func (s *Store) put(ctx context.Context, key string, body []byte) error {
	for n := 0; n <= maxCollisions; n++ {
		candidate := key
		if n > 0 {
			candidate = chunk.CollisionKey(key, n)
		}
		target := s.objectKey(candidate)

		_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(target),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			IfNoneMatch:   aws.String("*"),
		})
		switch {
		case err == nil:
			return nil
		case isPreconditionFailed(err):
			continue
		default:
			return fmt.Errorf("put %s: %w", target, err)
		}
	}
	return fmt.Errorf("no free name after %d collisions for %s", maxCollisions, s.objectKey(key))
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
