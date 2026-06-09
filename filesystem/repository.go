package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
	"time"
)

// Repository 是业务代码直接使用的磁盘操作入口。
//
// 它负责：
// 1. 对外暴露 Laravel 风格的方法集合；
// 2. 处理默认选项、参数归一化和错误透传；
// 3. 把最终调用分派给底层 driver。
type Repository struct {
	name    string
	config  DiskConfig
	driver  driver
	manager *Manager
	err     error
}

// newErrorRepository 用于把初始化错误延迟到真正调用时再返回。
func newErrorRepository(name string, err error, m *Manager) *Repository {
	return &Repository{name: name, manager: m, err: err}
}

// Name 返回当前仓储对应的磁盘名称。
func (r *Repository) Name() string {
	return r.name
}

// Close 关闭当前仓储底层资源。
func (r *Repository) Close() error {
	if r.err != nil || r.driver == nil {
		return r.err
	}
	return r.driver.Close()
}

// Put 直接把字符串、字节切片或 Reader 写入目标路径。
func (r *Repository) Put(ctx context.Context, key string, value any, opts ...PutOptions) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	reader, contentType, err := toReader(value)
	if err != nil {
		return err
	}
	put := r.resolvePutOptions(opts...)
	if put.ContentType == "" {
		put.ContentType = contentType
	}
	return r.driver.Write(ctx, key, reader, put)
}

// PutReader 把已有 Reader 流写入目标路径。
func (r *Repository) PutReader(ctx context.Context, key string, reader io.Reader, opts ...PutOptions) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.driver.Write(ctx, key, reader, r.resolvePutOptions(opts...))
}

// PutFile 使用上传文件原名写入指定目录，并返回最终相对路径。
func (r *Repository) PutFile(ctx context.Context, dir string, file *multipart.FileHeader, opts ...PutOptions) (string, error) {
	if file == nil {
		return "", ErrInvalidUploadFile
	}
	name := strings.TrimSpace(file.Filename)
	if name == "" {
		name = fmt.Sprintf("upload-%d", time.Now().UnixNano())
	}
	return r.PutFileAs(ctx, dir, file, name, opts...)
}

// PutFileAs 使用指定文件名写入上传文件，并返回最终相对路径。
func (r *Repository) PutFileAs(ctx context.Context, dir string, file *multipart.FileHeader, name string, opts ...PutOptions) (string, error) {
	if err := r.ensureReady(); err != nil {
		return "", err
	}
	if file == nil {
		return "", ErrInvalidUploadFile
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("filesystem: empty filename")
	}
	key := joinKey(dir, path.Base(name))
	src, err := openMultipart(file)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := src.Close(); err != nil {
			reportCleanupError(ctx, err, "close_upload_source", map[string]any{"disk": r.name, "key": key})
		}
	}()

	put := r.resolvePutOptions(opts...)
	if put.ContentType == "" {
		put.ContentType = file.Header.Get("Content-Type")
	}
	if err := r.driver.Write(ctx, key, src, put); err != nil {
		return "", err
	}
	return normalizeKey(key), nil
}

// Get 读取整个文件内容到内存。
func (r *Repository) Get(ctx context.Context, key string) ([]byte, error) {
	if err := r.ensureReady(); err != nil {
		return nil, err
	}
	return r.driver.ReadAll(ctx, key)
}

// JSON 读取文件内容并反序列化为 JSON。
func (r *Repository) JSON(ctx context.Context, key string, out any) error {
	data, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// OpenStream 以流式方式打开文件，并返回元信息。
func (r *Repository) OpenStream(ctx context.Context, key string) (io.ReadCloser, FileInfo, error) {
	if err := r.ensureReady(); err != nil {
		return nil, FileInfo{}, err
	}
	return r.driver.Open(ctx, key)
}

// ReadStream 以流式方式打开文件。
func (r *Repository) ReadStream(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, _, err := r.OpenStream(ctx, key)
	return rc, err
}

// WriteStream 把已有 Reader 流写入目标路径。
func (r *Repository) WriteStream(ctx context.Context, key string, reader io.Reader, opts ...PutOptions) error {
	return r.PutReader(ctx, key, reader, opts...)
}

// Download 把文件内容复制到指定 Writer。
func (r *Repository) Download(ctx context.Context, key string, w io.Writer) error {
	rc, _, err := r.OpenStream(ctx, key)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			reportCleanupError(ctx, closeErr, "close_download_reader", map[string]any{"disk": r.name, "key": key})
		}
	}()
	_, err = io.Copy(w, rc)
	return err
}

// Exists 判断文件是否存在。
func (r *Repository) Exists(ctx context.Context, key string) (bool, error) {
	if err := r.ensureReady(); err != nil {
		return false, err
	}
	return r.driver.Exists(ctx, key)
}

// Missing 判断文件是否不存在。
func (r *Repository) Missing(ctx context.Context, key string) (bool, error) {
	exists, err := r.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// FileExists 判断文件是否存在。
func (r *Repository) FileExists(ctx context.Context, key string) (bool, error) {
	return r.Exists(ctx, key)
}

// DirectoryExists 判断目录是否存在。
func (r *Repository) DirectoryExists(ctx context.Context, dir string) (bool, error) {
	if err := r.ensureReady(); err != nil {
		return false, err
	}
	if checker, ok := r.driver.(directoryExistser); ok {
		return checker.DirectoryExists(ctx, dir)
	}
	items, err := r.driver.List(ctx, dir, false)
	if err != nil {
		return false, err
	}
	return len(items) > 0, nil
}

// Prepend 把文本写到文件开头，已有内容以换行分隔。
func (r *Repository) Prepend(ctx context.Context, key string, data string, opts ...PutOptions) error {
	exists, err := r.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return r.Put(ctx, key, data, opts...)
	}
	current, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	return r.Put(ctx, key, data+"\n"+string(current), opts...)
}

// Append 把文本追加到文件末尾，已有内容以换行分隔。
func (r *Repository) Append(ctx context.Context, key string, data string, opts ...PutOptions) error {
	exists, err := r.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return r.Put(ctx, key, data, opts...)
	}
	current, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	return r.Put(ctx, key, string(current)+"\n"+data, opts...)
}

// Delete 删除一个或多个文件。
func (r *Repository) Delete(ctx context.Context, keys ...string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	for _, key := range keys {
		if err := r.driver.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// Copy 在同一磁盘内复制文件。
func (r *Repository) Copy(ctx context.Context, src, dst string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.driver.Copy(ctx, src, dst)
}

// Move 在同一磁盘内移动或重命名文件。
func (r *Repository) Move(ctx context.Context, src, dst string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.driver.Move(ctx, src, dst)
}

// Path 返回文件在底层驱动上的物理路径或逻辑定位。
func (r *Repository) Path(key string) string {
	if r.err != nil || r.driver == nil {
		return ""
	}
	return r.driver.Path(key)
}

// Size 返回文件大小。
func (r *Repository) Size(ctx context.Context, key string) (int64, error) {
	info, err := r.LastModifiedInfo(ctx, key)
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

// LastModified 返回文件最后修改时间。
func (r *Repository) LastModified(ctx context.Context, key string) (time.Time, error) {
	info, err := r.LastModifiedInfo(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	return info.LastModified, nil
}

// LastModifiedInfo 返回完整文件元信息。
func (r *Repository) LastModifiedInfo(ctx context.Context, key string) (FileInfo, error) {
	if err := r.ensureReady(); err != nil {
		return FileInfo{}, err
	}
	return r.driver.Stat(ctx, key)
}

// MimeType 返回文件 MIME 类型。
func (r *Repository) MimeType(ctx context.Context, key string) (string, error) {
	info, err := r.LastModifiedInfo(ctx, key)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(info.ContentType) != "" {
		return info.ContentType, nil
	}
	rc, err := r.ReadStream(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			reportCleanupError(ctx, closeErr, "close_mime_type_reader", map[string]any{"disk": r.name, "key": key})
		}
	}()
	buffer := make([]byte, 512)
	n, err := io.ReadFull(rc, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	return http.DetectContentType(buffer[:n]), nil
}

// Checksum 以流式方式计算文件校验和，默认使用 SHA-256。
func (r *Repository) Checksum(ctx context.Context, key string, opts ...ChecksumOptions) (string, error) {
	algorithm := "sha256"
	if len(opts) > 0 && strings.TrimSpace(opts[0].Algorithm) != "" {
		algorithm = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(opts[0].Algorithm), "-", ""))
	}
	if algorithm != "sha256" {
		return "", fmt.Errorf("filesystem: unsupported checksum algorithm %q", algorithm)
	}
	rc, err := r.ReadStream(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			reportCleanupError(ctx, closeErr, "close_checksum_reader", map[string]any{"disk": r.name, "key": key})
		}
	}()
	hash := sha256.New()
	if _, err := io.Copy(hash, rc); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// MakeDirectory 创建目录。
func (r *Repository) MakeDirectory(ctx context.Context, dir string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.driver.MakeDirectory(ctx, dir)
}

// DeleteDirectory 删除目录及其下所有文件。
func (r *Repository) DeleteDirectory(ctx context.Context, dir string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	if err := rejectEmptyDirectory(dir); err != nil {
		return err
	}
	return r.driver.DeleteDirectory(ctx, dir)
}

// Files 返回目录下一层文件列表。
func (r *Repository) Files(ctx context.Context, dir string) ([]string, error) {
	items, err := r.list(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	return collectPaths(items, false), nil
}

// AllFiles 递归返回目录下所有文件列表。
func (r *Repository) AllFiles(ctx context.Context, dir string) ([]string, error) {
	items, err := r.list(ctx, dir, true)
	if err != nil {
		return nil, err
	}
	return collectPaths(items, false), nil
}

// Directories 返回目录下一层子目录列表。
func (r *Repository) Directories(ctx context.Context, dir string) ([]string, error) {
	items, err := r.list(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	return collectPaths(items, true), nil
}

// AllDirectories 递归返回目录下所有子目录列表。
func (r *Repository) AllDirectories(ctx context.Context, dir string) ([]string, error) {
	items, err := r.list(ctx, dir, true)
	if err != nil {
		return nil, err
	}
	return collectPaths(items, true), nil
}

// URL 生成公开访问地址。
func (r *Repository) URL(key string) (string, error) {
	if err := r.ensureReady(); err != nil {
		return "", err
	}
	return r.driver.URL(key)
}

// TemporaryURL 生成临时签名地址。
func (r *Repository) TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error) {
	if err := r.ensureReady(); err != nil {
		return "", err
	}
	return r.driver.TemporaryURL(ctx, key, expiry)
}

// ProvidesTemporaryURLs 判断当前磁盘是否能生成临时访问链接。
func (r *Repository) ProvidesTemporaryURLs() bool {
	if r.err != nil || r.driver == nil {
		return false
	}
	if checker, ok := r.driver.(temporaryURLProvider); ok {
		return checker.ProvidesTemporaryURLs()
	}
	_, err := r.driver.TemporaryURL(context.Background(), "__prismgo_probe__", time.Now().Add(time.Second))
	return err == nil
}

// ProvidesTemporaryUploadURLs 判断当前磁盘是否能生成临时上传链接。
func (r *Repository) ProvidesTemporaryUploadURLs() bool {
	if r.err != nil || r.driver == nil {
		return false
	}
	generator, ok := r.driver.(temporaryUploadURLDriver)
	return ok && generator.ProvidesTemporaryUploadURLs()
}

// TemporaryUploadURL 生成临时上传链接。
func (r *Repository) TemporaryUploadURL(ctx context.Context, key string, expiry time.Time, opts ...TemporaryUploadURLOptions) (TemporaryUploadURLResult, error) {
	if err := r.ensureReady(); err != nil {
		return TemporaryUploadURLResult{}, err
	}
	generator, ok := r.driver.(temporaryUploadURLDriver)
	if !ok || !generator.ProvidesTemporaryUploadURLs() {
		return TemporaryUploadURLResult{}, ErrTemporaryUploadURLUnavailable
	}
	return generator.TemporaryUploadURL(ctx, key, expiry, opts...)
}

// SetVisibility 调整目标文件可见性。
func (r *Repository) SetVisibility(ctx context.Context, key, visibility string) error {
	if err := r.ensureReady(); err != nil {
		return err
	}
	return r.driver.SetVisibility(ctx, key, visibility)
}

// GetVisibility 返回目标文件当前可见性。
func (r *Repository) GetVisibility(ctx context.Context, key string) (string, error) {
	if err := r.ensureReady(); err != nil {
		return "", err
	}
	return r.driver.GetVisibility(ctx, key)
}

// ensureReady 用于在仓储初始化失败时阻止后续调用继续执行。
func (r *Repository) ensureReady() error {
	return r.err
}

// resolvePutOptions 合并调用方选项与磁盘默认配置。
func (r *Repository) resolvePutOptions(opts ...PutOptions) PutOptions {
	put := PutOptions{
		Visibility: r.config.Visibility,
	}
	if len(opts) > 0 {
		put = opts[0]
		if strings.TrimSpace(put.Visibility) == "" {
			put.Visibility = r.config.Visibility
		}
	}
	put.Visibility = ensureVisibility(put.Visibility, r.config.Visibility)
	return put
}

// list 调用底层驱动列出文件或目录。
func (r *Repository) list(ctx context.Context, dir string, recursive bool) ([]FileInfo, error) {
	if err := r.ensureReady(); err != nil {
		return nil, err
	}
	return r.driver.List(ctx, dir, recursive)
}

// collectPaths 从元信息列表中提取目标类型的路径集合。
func collectPaths(items []FileInfo, wantDir bool) []string {
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.IsDir != wantDir {
			continue
		}
		p := normalizeKey(item.Path)
		if item.IsDir {
			p = normalizeDir(p)
			p = strings.TrimSuffix(p, "/")
		}
		if _, ok := seen[p]; ok || p == "" {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// toReader 把常见输入类型统一转换成 Reader。
func toReader(value any) (io.Reader, string, error) {
	switch data := value.(type) {
	case string:
		return strings.NewReader(data), "text/plain; charset=utf-8", nil
	case []byte:
		return bytes.NewReader(data), "", nil
	case io.Reader:
		return data, "", nil
	default:
		return nil, "", fmt.Errorf("filesystem: unsupported content type %T", value)
	}
}
