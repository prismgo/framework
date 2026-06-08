package filesystem

import (
	"context"
	"io"
	"mime/multipart"
	"time"
)

type Filesystem interface {
	Path(key string) string
	Exists(ctx context.Context, key string) (bool, error)
	Missing(ctx context.Context, key string) (bool, error)
	FileExists(ctx context.Context, key string) (bool, error)
	DirectoryExists(ctx context.Context, dir string) (bool, error)

	Get(ctx context.Context, key string) ([]byte, error)
	JSON(ctx context.Context, key string, out any) error
	ReadStream(ctx context.Context, key string) (io.ReadCloser, error)
	WriteStream(ctx context.Context, key string, reader io.Reader, opts ...PutOptions) error
	Put(ctx context.Context, key string, value any, opts ...PutOptions) error
	PutReader(ctx context.Context, key string, reader io.Reader, opts ...PutOptions) error
	PutFile(ctx context.Context, dir string, file *multipart.FileHeader, opts ...PutOptions) (string, error)
	PutFileAs(ctx context.Context, dir string, file *multipart.FileHeader, name string, opts ...PutOptions) (string, error)

	Prepend(ctx context.Context, key string, data string, opts ...PutOptions) error
	Append(ctx context.Context, key string, data string, opts ...PutOptions) error
	Delete(ctx context.Context, keys ...string) error
	Copy(ctx context.Context, src, dst string) error
	Move(ctx context.Context, src, dst string) error

	Size(ctx context.Context, key string) (int64, error)
	LastModified(ctx context.Context, key string) (time.Time, error)
	MimeType(ctx context.Context, key string) (string, error)
	Checksum(ctx context.Context, key string, opts ...ChecksumOptions) (string, error)

	Files(ctx context.Context, dir string) ([]string, error)
	AllFiles(ctx context.Context, dir string) ([]string, error)
	Directories(ctx context.Context, dir string) ([]string, error)
	AllDirectories(ctx context.Context, dir string) ([]string, error)
	MakeDirectory(ctx context.Context, dir string) error
	DeleteDirectory(ctx context.Context, dir string) error

	GetVisibility(ctx context.Context, key string) (string, error)
	SetVisibility(ctx context.Context, key, visibility string) error
}

type Cloud interface {
	Filesystem
	URL(key string) (string, error)
}

type Repository interface {
	Cloud
	TemporaryURLGenerator
	Name() string
	OpenStream(ctx context.Context, key string) (io.ReadCloser, FileInfo, error)
	Download(ctx context.Context, key string, w io.Writer) error
	LastModifiedInfo(ctx context.Context, key string) (FileInfo, error)
	Close() error
}
