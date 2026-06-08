package filesystem

import "time"

const (
	VisibilityPublic  = "public"
	VisibilityPrivate = "private"
)

type PutOptions struct {
	Visibility  string
	ContentType string
}

type FileInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
	ContentType  string
	IsDir        bool
}

type ChecksumOptions struct {
	Algorithm string
}

type TemporaryUploadURLOptions struct {
	ContentType string
	Visibility  string
	Headers     map[string]string
}

type TemporaryUploadURLResult struct {
	URL     string
	Method  string
	Headers map[string]string
	Fields  map[string]string
	Expires time.Time
}
