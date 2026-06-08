package filesystem

import (
	"context"
	"time"
)

type TemporaryURLGenerator interface {
	ProvidesTemporaryURLs() bool
	TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error)
}

type TemporaryUploadURLGenerator interface {
	ProvidesTemporaryUploadURLs() bool
	TemporaryUploadURL(ctx context.Context, key string, expiry time.Time, opts ...TemporaryUploadURLOptions) (TemporaryUploadURLResult, error)
}
