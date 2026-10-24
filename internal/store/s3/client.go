// Package s3 connects smob to S3-compatible object storage.
//
// The client uses the standard AWS credential chain: environment variables,
// shared configuration and credentials files, and instance or container roles.
// smob never stores secrets in its configuration file.
package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/confuzeus/smob/internal/config"
)

// NewClient returns an S3 client for cfg. The configuration must already be
// resolved with config.Config.Resolve. NewClient does not contact the endpoint;
// Store.Check validates the bucket at startup.
func NewClient(ctx context.Context, cfg config.S3Storage) (*awss3.Client, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("s3: bucket must not be empty")
	}
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("s3: region must not be empty")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}

	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle != nil && *cfg.PathStyle
	}), nil
}
