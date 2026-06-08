package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
)

// localDriver 基于 gocloud.dev/blob/fileblob 实现本地磁盘驱动。
//
// 说明：
// 1. local 与 public 本质上都走本地目录驱动；
// 2. 两者差别主要体现在默认 visibility 与 URL 生成能力；
// 3. 临时 URL 不由 fileblob 原生提供，而是交给 Manager 统一签名。
type localDriver struct {
	root       string
	baseURL    string
	visibility string
	serve      bool
	bucket     *blob.Bucket
	tempURL    func(key string, expiry time.Time) string
}

// newLocalDriver 创建本地磁盘驱动。
func newLocalDriver(cfg DiskConfig) (*localDriver, error) {
	root := strings.TrimSpace(cfg.Root)
	if root == "" {
		return nil, fmt.Errorf("filesystem: local root is empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	bucket, err := fileblob.OpenBucket(root, &fileblob.Options{NoTempDir: true})
	if err != nil {
		return nil, err
	}
	return &localDriver{
		root:       root,
		baseURL:    strings.TrimRight(strings.TrimSpace(cfg.URL), "/"),
		visibility: cfg.Visibility,
		serve:      cfg.Serve,
		bucket:     bucket,
	}, nil
}

// Close 关闭底层 bucket。
func (d *localDriver) Close() error {
	return d.bucket.Close()
}

// Write 把内容写入本地 bucket。
func (d *localDriver) Write(ctx context.Context, key string, reader io.Reader, opts PutOptions) error {
	if opts.Visibility != d.visibility {
		return ErrUnsupportedVisibility
	}
	return d.bucket.Upload(ctx, normalizeKey(key), reader, &blob.WriterOptions{ContentType: opts.ContentType})
}

// ReadAll 读取整个文件内容。
func (d *localDriver) ReadAll(ctx context.Context, key string) ([]byte, error) {
	return d.bucket.ReadAll(ctx, normalizeKey(key))
}

// Open 以流方式打开文件，并返回统一元信息。
func (d *localDriver) Open(ctx context.Context, key string) (io.ReadCloser, FileInfo, error) {
	key = normalizeKey(key)
	reader, err := d.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, FileInfo{}, err
	}
	info, err := d.Stat(ctx, key)
	if err != nil {
		reader.Close()
		return nil, FileInfo{}, err
	}
	info.ContentType = reader.ContentType()
	return reader, info, nil
}

// Exists 判断文件是否存在。
func (d *localDriver) Exists(ctx context.Context, key string) (bool, error) {
	return d.bucket.Exists(ctx, normalizeKey(key))
}

// DirectoryExists 判断本地目录是否存在。
func (d *localDriver) DirectoryExists(ctx context.Context, dir string) (bool, error) {
	_ = ctx
	info, err := os.Stat(d.absolutePath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

// Delete 删除文件。
func (d *localDriver) Delete(ctx context.Context, key string) error {
	return d.bucket.Delete(ctx, normalizeKey(key))
}

// Copy 复制文件。
func (d *localDriver) Copy(ctx context.Context, src, dst string) error {
	return d.bucket.Copy(ctx, normalizeKey(dst), normalizeKey(src), nil)
}

// Move 通过“先复制后删除”实现移动。
func (d *localDriver) Move(ctx context.Context, src, dst string) error {
	if err := d.bucket.Copy(ctx, normalizeKey(dst), normalizeKey(src), nil); err != nil {
		return err
	}
	return d.bucket.Delete(ctx, normalizeKey(src))
}

// Stat 读取文件基础属性。
func (d *localDriver) Stat(ctx context.Context, key string) (FileInfo, error) {
	key = normalizeKey(key)
	attrs, err := d.bucket.Attributes(ctx, key)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		Path:         key,
		Size:         attrs.Size,
		LastModified: attrs.ModTime,
		ContentType:  attrs.ContentType,
	}, nil
}

// List 列出指定目录下的文件或目录。
func (d *localDriver) List(ctx context.Context, prefix string, recursive bool) ([]FileInfo, error) {
	prefix = normalizeKey(prefix)
	opts := &blob.ListOptions{Prefix: prefix}
	if !recursive {
		opts.Delimiter = "/"
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			opts.Prefix = prefix + "/"
		}
	}
	iter := d.bucket.List(opts)
	items := make([]FileInfo, 0)
	for {
		item, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		items = append(items, FileInfo{
			Path:         strings.TrimSuffix(item.Key, "/"),
			Size:         item.Size,
			LastModified: item.ModTime,
			IsDir:        item.IsDir,
		})
	}
	return items, nil
}

// MakeDirectory 创建目录。
func (d *localDriver) MakeDirectory(ctx context.Context, dir string) error {
	_ = ctx
	return os.MkdirAll(d.absolutePath(dir), 0o755)
}

// DeleteDirectory 删除目录及其所有子项。
func (d *localDriver) DeleteDirectory(ctx context.Context, dir string) error {
	_ = ctx
	if err := rejectEmptyDirectory(dir); err != nil {
		return err
	}
	return os.RemoveAll(d.absolutePath(dir))
}

// Path 返回文件对应的本地绝对路径。
func (d *localDriver) Path(key string) string {
	return d.absolutePath(key)
}

// URL 为 public 本地盘生成公开访问地址。
func (d *localDriver) URL(key string) (string, error) {
	if d.visibility != VisibilityPublic {
		return "", ErrPublicURLUnavailable
	}
	if d.baseURL == "" {
		return "", ErrPublicURLUnavailable
	}
	return joinURL(d.baseURL, key), nil
}

// TemporaryURL 生成 Manager 签名后的本地临时访问地址。
func (d *localDriver) TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error) {
	_ = ctx
	if !d.serve || d.tempURL == nil {
		return "", ErrTemporaryURLDisabled
	}
	return d.tempURL(normalizeKey(key), expiry), nil
}

// ProvidesTemporaryURLs 判断本地盘是否开启临时访问链接。
func (d *localDriver) ProvidesTemporaryURLs() bool {
	return d.serve && d.tempURL != nil
}

// SetVisibility 对本地盘做“磁盘级可见性”约束校验。
func (d *localDriver) SetVisibility(ctx context.Context, key, visibility string) error {
	_ = ctx
	_ = key
	if ensureVisibility(visibility, d.visibility) != d.visibility {
		return ErrUnsupportedVisibility
	}
	return nil
}

// GetVisibility 返回当前磁盘固定可见性。
func (d *localDriver) GetVisibility(ctx context.Context, key string) (string, error) {
	_ = ctx
	_ = key
	return d.visibility, nil
}

// absolutePath 把相对路径转换为本地文件系统绝对路径。
func (d *localDriver) absolutePath(key string) string {
	key = normalizeKey(key)
	if key == "" {
		return d.root
	}
	return filepath.Join(d.root, filepath.FromSlash(key))
}
