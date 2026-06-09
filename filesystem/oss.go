package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossDriver 基于 aliyun-oss-go-sdk 适配统一文件系统接口。
type ossDriver struct {
	bucket     *oss.Bucket
	cfg        OSSConfig
	baseURL    string
	visibility string
}

// newOSSDriver 创建 OSS 驱动实例。
func newOSSDriver(cfg OSSConfig) (*ossDriver, error) {
	if strings.TrimSpace(cfg.Bucket) == "" || strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, fmt.Errorf("filesystem: oss bucket or endpoint is empty")
	}
	clientOptions := []oss.ClientOption{}
	if cfg.Timeout > 0 {
		// OSS SDK 的超时配置以秒为单位，向上取整避免亚秒配置被截断成无限等待。
		seconds := int64((cfg.Timeout + time.Second - 1) / time.Second)
		clientOptions = append(clientOptions, oss.Timeout(seconds, seconds))
	}
	client, err := oss.New(strings.TrimSpace(cfg.Endpoint), strings.TrimSpace(cfg.AccessKey), strings.TrimSpace(cfg.SecretKey), clientOptions...)
	if err != nil {
		return nil, err
	}
	bucket, err := client.Bucket(strings.TrimSpace(cfg.Bucket))
	if err != nil {
		return nil, err
	}
	cfg.Prefix = normalizeDir(cfg.Prefix)
	cfg.Visibility = ensureVisibility(cfg.Visibility, VisibilityPrivate)
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if baseURL == "" {
		endpoint := strings.TrimSpace(cfg.Endpoint)
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			endpoint = "https://" + endpoint
		}
		baseURL = strings.TrimRight(endpoint, "/") + "/" + strings.TrimSpace(cfg.Bucket)
	}
	return &ossDriver{
		bucket:     bucket,
		cfg:        cfg,
		baseURL:    baseURL,
		visibility: cfg.Visibility,
	}, nil
}

// Close 预留关闭方法，当前 OSS SDK 无需显式关闭。
func (d *ossDriver) Close() error {
	return nil
}

// Write 把内容写入 OSS 对象。
func (d *ossDriver) Write(ctx context.Context, key string, reader io.Reader, opts PutOptions) error {
	key = d.objectKey(key)
	options := ossRequestOptions(ctx)
	if opts.ContentType != "" {
		options = append(options, oss.ContentType(opts.ContentType))
	}
	options = append(options, oss.ObjectACL(d.objectACL(opts.Visibility)))
	return d.bucket.PutObject(key, reader, options...)
}

// ReadAll 读取整个对象内容。
func (d *ossDriver) ReadAll(ctx context.Context, key string) ([]byte, error) {
	rc, _, err := d.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			reportCleanupError(ctx, err, "close_oss_reader", map[string]any{"key": key})
		}
	}()
	return io.ReadAll(rc)
}

// Open 打开对象读取流并附带元信息。
func (d *ossDriver) Open(ctx context.Context, key string) (io.ReadCloser, FileInfo, error) {
	key = d.objectKey(key)
	headers, err := d.bucket.GetObjectDetailedMeta(key, ossRequestOptions(ctx)...)
	if err != nil {
		return nil, FileInfo{}, err
	}
	reader, err := d.bucket.GetObject(key, ossRequestOptions(ctx)...)
	if err != nil {
		return nil, FileInfo{}, err
	}
	info := d.fileInfoFromHeaders(key, headers)
	return reader, info, nil
}

// Exists 判断对象是否存在。
func (d *ossDriver) Exists(ctx context.Context, key string) (bool, error) {
	return d.bucket.IsObjectExist(d.objectKey(key), ossRequestOptions(ctx)...)
}

// DirectoryExists 判断 OSS 目录前缀是否存在。
func (d *ossDriver) DirectoryExists(ctx context.Context, dir string) (bool, error) {
	marker := d.objectKey(normalizeDir(dir))
	if marker != "" {
		exists, err := d.bucket.IsObjectExist(marker, ossRequestOptions(ctx)...)
		if err != nil || exists {
			return exists, err
		}
	}
	items, err := d.List(ctx, dir, false)
	if err != nil {
		return false, err
	}
	return len(items) > 0, nil
}

// Delete 删除对象。
func (d *ossDriver) Delete(ctx context.Context, key string) error {
	return d.bucket.DeleteObject(d.objectKey(key), ossRequestOptions(ctx)...)
}

// Copy 在同一桶内复制对象。
func (d *ossDriver) Copy(ctx context.Context, src, dst string) error {
	_, err := d.bucket.CopyObject(d.objectKey(src), d.objectKey(dst), ossRequestOptions(ctx)...)
	return err
}

// Move 通过“先复制后删除”实现移动。
func (d *ossDriver) Move(ctx context.Context, src, dst string) error {
	if err := d.Copy(ctx, src, dst); err != nil {
		return err
	}
	return d.Delete(ctx, src)
}

// Stat 读取对象元信息。
func (d *ossDriver) Stat(ctx context.Context, key string) (FileInfo, error) {
	headers, err := d.bucket.GetObjectDetailedMeta(d.objectKey(key), ossRequestOptions(ctx)...)
	if err != nil {
		return FileInfo{}, err
	}
	return d.fileInfoFromHeaders(key, headers), nil
}

// List 列出对象或目录前缀。
func (d *ossDriver) List(ctx context.Context, prefix string, recursive bool) ([]FileInfo, error) {
	objectPrefix := d.objectKey(prefix)
	if objectPrefix != "" && !strings.HasSuffix(objectPrefix, "/") {
		objectPrefix += "/"
	}
	options := ossRequestOptions(ctx, oss.Prefix(objectPrefix), oss.MaxKeys(1000), oss.ListType(2))
	if !recursive {
		options = append(options, oss.Delimiter("/"))
	}
	items := make([]FileInfo, 0)
	for {
		res, err := d.bucket.ListObjectsV2(options...)
		if err != nil {
			return nil, err
		}
		for _, current := range res.Objects {
			items = append(items, FileInfo{
				Path:         d.stripPrefix(current.Key),
				Size:         current.Size,
				LastModified: current.LastModified,
				IsDir:        strings.HasSuffix(current.Key, "/"),
			})
		}
		if !recursive {
			for _, current := range res.CommonPrefixes {
				items = append(items, FileInfo{
					Path:  strings.TrimSuffix(d.stripPrefix(current), "/"),
					IsDir: true,
				})
			}
		}
		if !res.IsTruncated {
			break
		}
		options = appendOrReplace(options, oss.ContinuationToken(res.NextContinuationToken))
	}
	return items, nil
}

// MakeDirectory 通过创建零字节占位对象模拟目录。
func (d *ossDriver) MakeDirectory(ctx context.Context, dir string) error {
	return d.bucket.PutObject(d.objectKey(normalizeDir(dir)), bytes.NewReader(nil), ossRequestOptions(ctx)...)
}

// DeleteDirectory 递归删除指定前缀下的所有对象。
func (d *ossDriver) DeleteDirectory(ctx context.Context, dir string) error {
	if err := rejectEmptyDirectory(dir); err != nil {
		return err
	}
	items, err := d.List(ctx, dir, true)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := d.Delete(ctx, item.Path); err != nil {
			return err
		}
	}
	if dir = strings.TrimSpace(dir); dir != "" {
		_ = d.Delete(ctx, normalizeDir(dir))
	}
	return nil
}

// Path 返回 OSS 逻辑路径表示。
func (d *ossDriver) Path(key string) string {
	return "oss://" + strings.TrimSpace(d.cfg.Bucket) + "/" + d.objectKey(key)
}

// URL 为公开对象生成访问地址。
func (d *ossDriver) URL(key string) (string, error) {
	if d.visibility != VisibilityPublic {
		return "", ErrPublicURLUnavailable
	}
	return joinURL(d.baseURL, d.objectKey(key)), nil
}

// TemporaryURL 使用 OSS SDK 生成签名访问地址。
func (d *ossDriver) TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error) {
	_ = ctx
	seconds := int64(time.Until(expiry).Seconds())
	if seconds <= 0 {
		return "", ErrTemporaryURLInvalid
	}
	return d.bucket.SignURL(d.objectKey(key), oss.HTTPGet, seconds)
}

// ProvidesTemporaryURLs 判断 OSS 是否支持临时访问链接。
func (d *ossDriver) ProvidesTemporaryURLs() bool {
	return true
}

// ProvidesTemporaryUploadURLs 判断 OSS 是否支持临时上传链接。
func (d *ossDriver) ProvidesTemporaryUploadURLs() bool {
	return true
}

// TemporaryUploadURL 使用 OSS SDK 生成签名 PUT 上传地址。
func (d *ossDriver) TemporaryUploadURL(ctx context.Context, key string, expiry time.Time, opts ...TemporaryUploadURLOptions) (TemporaryUploadURLResult, error) {
	_ = ctx
	seconds := int64(time.Until(expiry).Seconds())
	if seconds <= 0 {
		return TemporaryUploadURLResult{}, ErrTemporaryURLInvalid
	}
	put := TemporaryUploadURLOptions{}
	if len(opts) > 0 {
		put = opts[0]
	}
	options := []oss.Option{}
	headers := make(map[string]string, len(put.Headers)+2)
	for k, v := range put.Headers {
		headers[k] = v
	}
	if strings.TrimSpace(put.ContentType) != "" {
		options = append(options, oss.ContentType(put.ContentType))
		headers[oss.HTTPHeaderContentType] = put.ContentType
	}
	if strings.TrimSpace(put.Visibility) != "" {
		acl := d.objectACL(put.Visibility)
		options = append(options, oss.ObjectACL(acl))
		headers["x-oss-object-acl"] = string(acl)
	}
	url, err := d.bucket.SignURL(d.objectKey(key), oss.HTTPPut, seconds, options...)
	if err != nil {
		return TemporaryUploadURLResult{}, err
	}
	return TemporaryUploadURLResult{
		URL:     url,
		Method:  http.MethodPut,
		Headers: headers,
		Fields:  map[string]string{},
		Expires: expiry,
	}, nil
}

// SetVisibility 把对象 ACL 映射为 public/private。
func (d *ossDriver) SetVisibility(ctx context.Context, key, visibility string) error {
	return d.bucket.SetObjectACL(d.objectKey(key), d.objectACL(visibility), ossRequestOptions(ctx)...)
}

// GetVisibility 读取对象 ACL 并归一化为统一可见性值。
func (d *ossDriver) GetVisibility(ctx context.Context, key string) (string, error) {
	result, err := d.bucket.GetObjectACL(d.objectKey(key), ossRequestOptions(ctx)...)
	if err != nil {
		return "", err
	}
	switch result.ACL {
	case string(oss.ACLPublicRead), string(oss.ACLPublicReadWrite):
		return VisibilityPublic, nil
	default:
		return VisibilityPrivate, nil
	}
}

// ossRequestOptions 为每个 OSS 网络请求注入 context，确保取消和超时能传递到底层 HTTP 请求。
func ossRequestOptions(ctx context.Context, options ...oss.Option) []oss.Option {
	if ctx == nil {
		ctx = context.Background()
	}
	return append([]oss.Option{oss.WithContext(ctx)}, options...)
}

// objectKey 把业务相对路径拼接为 OSS 对象 key。
func (d *ossDriver) objectKey(key string) string {
	return joinKey(d.cfg.Prefix, key)
}

// stripPrefix 把 OSS 返回的完整对象 key 转回业务相对路径。
func (d *ossDriver) stripPrefix(key string) string {
	key = normalizeKey(key)
	prefix := strings.TrimSuffix(d.cfg.Prefix, "/")
	if prefix != "" {
		key = strings.TrimPrefix(strings.TrimPrefix(key, prefix), "/")
	}
	return key
}

// objectACL 把统一 visibility 转换为 OSS ACL 类型。
func (d *ossDriver) objectACL(visibility string) oss.ACLType {
	if ensureVisibility(visibility, d.visibility) == VisibilityPublic {
		return oss.ACLPublicRead
	}
	return oss.ACLPrivate
}

// fileInfoFromHeaders 从 OSS 响应头中提取统一文件元信息。
func (d *ossDriver) fileInfoFromHeaders(key string, headers http.Header) FileInfo {
	size, _ := strconv.ParseInt(headers.Get("Content-Length"), 10, 64)
	lastModified, _ := time.Parse(http.TimeFormat, headers.Get("Last-Modified"))
	return FileInfo{
		Path:         normalizeKey(key),
		Size:         size,
		LastModified: lastModified,
		ContentType:  headers.Get("Content-Type"),
	}
}

// appendOrReplace 用于在分页列举中追加 continuation token 选项。
func appendOrReplace(options []oss.Option, next oss.Option) []oss.Option {
	if len(options) == 0 {
		return []oss.Option{next}
	}
	return append(options, next)
}
