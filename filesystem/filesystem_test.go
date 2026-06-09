package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	fscontract "github.com/prismgo/framework/contracts/filesystem"
)

// fakeDriver 用于隔离 Repository/Facade/Manager 的通用行为测试。
type fakeDriver struct {
	files          map[string]fakeFile
	listFlat       []FileInfo
	listRecursive  []FileInfo
	pathPrefix     string
	publicBase     string
	tempBase       string
	visibility     string
	writeErr       error
	closeErr       error
	deleteErr      error
	deleteDirCalls int
}

type fakeFile struct {
	data         []byte
	contentType  string
	lastModified time.Time
	visibility   string
}

// Close 模拟底层资源关闭。
func (d *fakeDriver) Close() error {
	return d.closeErr
}

// Write 把内容写入 fake driver。
func (d *fakeDriver) Write(ctx context.Context, key string, reader io.Reader, opts PutOptions) error {
	_ = ctx
	if d.writeErr != nil {
		return d.writeErr
	}
	if d.files == nil {
		d.files = make(map[string]fakeFile)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	key = normalizeKey(key)
	d.files[key] = fakeFile{
		data:         data,
		contentType:  opts.ContentType,
		lastModified: time.Now().UTC(),
		visibility:   ensureVisibility(opts.Visibility, d.visibility),
	}
	return nil
}

// ReadAll 返回整文件内容。
func (d *fakeDriver) ReadAll(ctx context.Context, key string) ([]byte, error) {
	_ = ctx
	file, ok := d.files[normalizeKey(key)]
	if !ok {
		return nil, osErrNotExist()
	}
	return append([]byte(nil), file.data...), nil
}

// Open 返回文件读取流和元信息。
func (d *fakeDriver) Open(ctx context.Context, key string) (io.ReadCloser, FileInfo, error) {
	_ = ctx
	file, ok := d.files[normalizeKey(key)]
	if !ok {
		return nil, FileInfo{}, osErrNotExist()
	}
	return io.NopCloser(bytes.NewReader(file.data)), FileInfo{
		Path:         normalizeKey(key),
		Size:         int64(len(file.data)),
		LastModified: file.lastModified,
		ContentType:  file.contentType,
	}, nil
}

// Exists 判断文件是否存在。
func (d *fakeDriver) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	_, ok := d.files[normalizeKey(key)]
	return ok, nil
}

// Delete 删除文件。
func (d *fakeDriver) Delete(ctx context.Context, key string) error {
	_ = ctx
	if d.deleteErr != nil {
		return d.deleteErr
	}
	delete(d.files, normalizeKey(key))
	return nil
}

// Copy 复制文件。
func (d *fakeDriver) Copy(ctx context.Context, src, dst string) error {
	_ = ctx
	file, ok := d.files[normalizeKey(src)]
	if !ok {
		return osErrNotExist()
	}
	d.files[normalizeKey(dst)] = file
	return nil
}

// Move 移动文件。
func (d *fakeDriver) Move(ctx context.Context, src, dst string) error {
	_ = ctx
	file, ok := d.files[normalizeKey(src)]
	if !ok {
		return osErrNotExist()
	}
	delete(d.files, normalizeKey(src))
	d.files[normalizeKey(dst)] = file
	return nil
}

// Stat 返回文件元信息。
func (d *fakeDriver) Stat(ctx context.Context, key string) (FileInfo, error) {
	_ = ctx
	file, ok := d.files[normalizeKey(key)]
	if !ok {
		return FileInfo{}, osErrNotExist()
	}
	return FileInfo{
		Path:         normalizeKey(key),
		Size:         int64(len(file.data)),
		LastModified: file.lastModified,
		ContentType:  file.contentType,
	}, nil
}

// List 返回预置的目录结果。
func (d *fakeDriver) List(ctx context.Context, prefix string, recursive bool) ([]FileInfo, error) {
	_ = ctx
	_ = prefix
	if recursive {
		return append([]FileInfo(nil), d.listRecursive...), nil
	}
	return append([]FileInfo(nil), d.listFlat...), nil
}

// MakeDirectory 模拟创建目录。
func (d *fakeDriver) MakeDirectory(ctx context.Context, dir string) error {
	_ = ctx
	key := strings.TrimSuffix(normalizeDir(dir), "/")
	if key == "" {
		return nil
	}
	if d.listFlat == nil {
		d.listFlat = []FileInfo{}
	}
	d.listFlat = append(d.listFlat, FileInfo{Path: key, IsDir: true})
	d.listRecursive = append(d.listRecursive, FileInfo{Path: key, IsDir: true})
	return nil
}

// DeleteDirectory 模拟删除目录。
func (d *fakeDriver) DeleteDirectory(ctx context.Context, dir string) error {
	_ = ctx
	d.deleteDirCalls++
	key := strings.TrimSuffix(normalizeDir(dir), "/")
	filteredFlat := d.listFlat[:0]
	for _, item := range d.listFlat {
		if item.Path != key {
			filteredFlat = append(filteredFlat, item)
		}
	}
	d.listFlat = filteredFlat

	filteredRecursive := d.listRecursive[:0]
	for _, item := range d.listRecursive {
		if item.Path != key {
			filteredRecursive = append(filteredRecursive, item)
		}
	}
	d.listRecursive = filteredRecursive
	return nil
}

// Path 返回 fake 物理路径。
func (d *fakeDriver) Path(key string) string {
	return filepath.Join(d.pathPrefix, filepath.FromSlash(normalizeKey(key)))
}

// URL 生成 fake 公开地址。
func (d *fakeDriver) URL(key string) (string, error) {
	if strings.TrimSpace(d.publicBase) == "" {
		return "", ErrPublicURLUnavailable
	}
	return joinURL(d.publicBase, key), nil
}

// TemporaryURL 生成 fake 临时地址。
func (d *fakeDriver) TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error) {
	_ = ctx
	if strings.TrimSpace(d.tempBase) == "" {
		return "", ErrTemporaryURLDisabled
	}
	return joinURL(d.tempBase, key) + "?expires=" + url.QueryEscape(expiry.UTC().Format(time.RFC3339)), nil
}

// SetVisibility 修改 fake 文件可见性。
func (d *fakeDriver) SetVisibility(ctx context.Context, key, visibility string) error {
	_ = ctx
	file, ok := d.files[normalizeKey(key)]
	if !ok {
		return osErrNotExist()
	}
	file.visibility = ensureVisibility(visibility, d.visibility)
	d.files[normalizeKey(key)] = file
	return nil
}

// GetVisibility 返回 fake 文件可见性。
func (d *fakeDriver) GetVisibility(ctx context.Context, key string) (string, error) {
	_ = ctx
	file, ok := d.files[normalizeKey(key)]
	if !ok {
		return "", osErrNotExist()
	}
	return file.visibility, nil
}

// osErrNotExist 返回一个可被 errors.Is 识别的不存在错误。
func osErrNotExist() error {
	return errors.New("file does not exist")
}

// newMultipartHeader 为 PutFile / PutFileAs 构造可复用的上传文件头。
func newMultipartHeader(t *testing.T, field, name string, data []byte) *multipart.FileHeader {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	var (
		part io.Writer
		err  error
	)
	if name == "" {
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", `form-data; name="`+field+`"; filename=""`)
		header.Set("Content-Type", http.DetectContentType(data))
		part, err = writer.CreatePart(header)
	} else {
		part, err = writer.CreateFormFile(field, name)
	}
	if err != nil {
		t.Fatalf("create multipart file failed: %v", err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatalf("write multipart body failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(data) + 4096)); err != nil {
		t.Fatalf("parse multipart form failed: %v", err)
	}
	return req.MultipartForm.File[field][0]
}

// newTestManager 创建一个同时包含 local/public 的测试管理器。
func newTestManager(t *testing.T, root string) *Manager {
	t.Helper()

	manager, err := NewManager(Config{
		Default: "local",
		Cloud:   "oss",
		Disks: map[string]DiskConfig{
			"local": {
				Driver:     "local",
				Root:       filepath.Join(root, "private"),
				Visibility: VisibilityPrivate,
				Serve:      true,
			},
			"public": {
				Driver:     "local",
				Root:       filepath.Join(root, "public"),
				URL:        "http://example.test/storage",
				Visibility: VisibilityPublic,
				Serve:      true,
			},
		},
		TemporaryURL: TemporaryURLConfig{
			SigningKey: "unit-test-secret",
		},
	})
	if err != nil {
		t.Fatalf("create manager failed: %v", err)
	}
	return manager
}

// sorted 用于稳定比较列表结果。
func sorted(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// TestUtilityHelpers 验证路径、URL、签名等纯函数工具。
func TestUtilityHelpers(t *testing.T) {
	if got := normalizeKey(`\foo\bar\..\demo.txt`); got != "foo/demo.txt" {
		t.Fatalf("unexpected normalizeKey result: %s", got)
	}
	if got := normalizeKey("."); got != "" {
		t.Fatalf("expected dot path to normalize to empty, got %s", got)
	}
	if got := normalizeKey("/"); got != "" {
		t.Fatalf("expected slash path to normalize to empty, got %s", got)
	}
	if got := normalizeKey(""); got != "" {
		t.Fatalf("expected empty normalizeKey, got %s", got)
	}
	if got := normalizeDir("foo/bar"); got != "foo/bar/" {
		t.Fatalf("unexpected normalizeDir result: %s", got)
	}
	if got := joinKey("foo", "bar.txt"); got != "foo/bar.txt" {
		t.Fatalf("unexpected joinKey result: %s", got)
	}
	if got := joinKey("", "bar.txt"); got != "bar.txt" {
		t.Fatalf("unexpected joinKey empty prefix result: %s", got)
	}
	if got := joinKey("foo", ""); got != "foo" {
		t.Fatalf("unexpected joinKey empty suffix result: %s", got)
	}
	if got := joinURL("http://example.test/storage", "dir/a b.txt"); got != "http://example.test/storage/dir/a%20b.txt" {
		t.Fatalf("unexpected joinURL result: %s", got)
	}
	if got := joinURL("", "dir/demo.txt"); got != "/dir/demo.txt" {
		t.Fatalf("unexpected joinURL empty base result: %s", got)
	}
	if got := joinURL("http://example.test/storage", ""); got != "http://example.test/storage" {
		t.Fatalf("unexpected joinURL empty key result: %s", got)
	}
	if got := ensureVisibility("PUBLIC", VisibilityPrivate); got != VisibilityPublic {
		t.Fatalf("unexpected visibility result: %s", got)
	}
	if got := ensureVisibility("bad", VisibilityPrivate); got != VisibilityPrivate {
		t.Fatalf("unexpected fallback visibility: %s", got)
	}
	if got := urlPathEscape("a/b"); got != "a%2Fb" {
		t.Fatalf("unexpected path escape: %s", got)
	}
	if got := urlQueryEscape("a b"); got != "a+b" {
		t.Fatalf("unexpected query escape: %s", got)
	}

	expires := time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC)
	sig1 := signToken("secret", "local", "a/b.txt", expires)
	sig2 := signToken("secret", "local", "a/b.txt", expires)
	if sig1 == "" || sig1 != sig2 {
		t.Fatalf("expected stable signature, got %s / %s", sig1, sig2)
	}

	reader, contentType, err := toReader("hello")
	if err != nil || contentType == "" {
		t.Fatalf("toReader string failed: %v", err)
	}
	data, _ := io.ReadAll(reader)
	if string(data) != "hello" {
		t.Fatalf("unexpected string reader payload: %s", string(data))
	}

	reader, contentType, err = toReader([]byte("bytes"))
	if err != nil || contentType != "" {
		t.Fatalf("toReader bytes failed: %v / %s", err, contentType)
	}
	data, _ = io.ReadAll(reader)
	if string(data) != "bytes" {
		t.Fatalf("unexpected bytes reader payload: %s", string(data))
	}

	srcReader := strings.NewReader("stream")
	reader, contentType, err = toReader(srcReader)
	if err != nil || contentType != "" || reader != srcReader {
		t.Fatalf("toReader reader failed: %v", err)
	}

	if _, _, err := toReader(123); err == nil {
		t.Fatal("expected unsupported content type error")
	}

	paths := collectPaths([]FileInfo{
		{Path: "foo/a.txt"},
		{Path: "foo/a.txt"},
		{Path: "foo/bar", IsDir: true},
		{Path: "foo/bar", IsDir: true},
	}, false)
	if len(paths) != 1 || paths[0] != "foo/a.txt" {
		t.Fatalf("unexpected collectPaths files result: %+v", paths)
	}
	dirs := collectPaths([]FileInfo{
		{Path: "foo/bar", IsDir: true},
		{Path: "foo/bar/", IsDir: true},
	}, true)
	if len(dirs) != 1 || dirs[0] != "foo/bar" {
		t.Fatalf("unexpected collectPaths dirs result: %+v", dirs)
	}
}

// TestManagerValidationAndErrorRepository 覆盖管理器初始化和错误仓储分支。
func TestManagerValidationAndErrorRepository(t *testing.T) {
	if _, err := NewManager(Config{}); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected ErrDiskNotFound for empty disks, got %v", err)
	}

	_, err := NewManager(Config{
		Default: "public",
		Disks: map[string]DiskConfig{
			"local": {Driver: "local", Root: t.TempDir()},
		},
	})
	if !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected missing default error, got %v", err)
	}

	manager, err := NewManager(Config{
		Disks: map[string]DiskConfig{
			"local": {Driver: "local", Root: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if manager.DefaultName() != "local" {
		t.Fatalf("expected default disk local, got %s", manager.DefaultName())
	}
	if manager.CloudName() != "" {
		t.Fatalf("expected empty cloud name, got %s", manager.CloudName())
	}

	errRepo := manager.Disk("missing")
	if err := errRepo.Put(context.Background(), "a.txt", "demo"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected disk not found from error repository, got %v", err)
	}
	if err := errRepo.Close(); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected close to return init error, got %v", err)
	}
	if errRepo.Path("a.txt") != "" {
		t.Fatalf("expected error repository path to be empty, got %s", errRepo.Path("a.txt"))
	}

	manager.specs["broken"] = DiskConfig{Driver: "memory"}
	if err := manager.Disk("broken").Put(context.Background(), "demo.txt", "demo"); !errors.Is(err, ErrUnsupportedDriver) {
		t.Fatalf("expected unsupported driver error, got %v", err)
	}
}

// TestManagerDefaultAndCacheBranches 覆盖默认磁盘、空名称回退和缓存分支。
func TestManagerDefaultAndCacheBranches(t *testing.T) {
	root := t.TempDir()
	manager, err := NewManager(Config{
		Default: "local",
		Cloud:   "oss",
		Disks: map[string]DiskConfig{
			"local": {
				Root:       filepath.Join(root, "local"),
				Visibility: VisibilityPrivate,
				Serve:      true,
			},
			"public": {
				Driver:     "public",
				Root:       filepath.Join(root, "public"),
				URL:        "http://example.test/storage",
				Visibility: VisibilityPublic,
				Serve:      true,
			},
		},
		TemporaryURL: TemporaryURLConfig{SigningKey: "cache-secret"},
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer func() { _ = manager.Close() }()

	if manager.CloudName() != "oss" {
		t.Fatalf("expected cloud name oss, got %s", manager.CloudName())
	}
	if manager.Default() == nil {
		t.Fatal("expected default repository")
	}
	repoFromEmpty := manager.Disk("")
	repoFromDefault := manager.Disk("local")
	if repoFromEmpty != repoFromDefault {
		t.Fatal("expected empty disk name to fall back to default repository")
	}
	publicA := manager.Disk("public")
	publicB := manager.Disk("public")
	if publicA != publicB {
		t.Fatal("expected disk cache to reuse the same repository instance")
	}
}

// TestManagerTemporaryURLVerification 覆盖签名 URL 的校验逻辑。
func TestManagerTemporaryURLVerification(t *testing.T) {
	manager := &Manager{
		tempURL: TemporaryURLConfig{SigningKey: "verify-secret"},
	}
	expires := time.Now().Add(2 * time.Minute).UTC().Truncate(time.Second)
	signature := signToken("verify-secret", "local", "notes/test.txt", expires)
	if err := manager.VerifyTemporaryURL("local", "notes/test.txt", expires, signature); err != nil {
		t.Fatalf("expected valid signature, got %v", err)
	}
	if err := manager.VerifyTemporaryURL("", "notes/test.txt", expires, signature); !errors.Is(err, ErrTemporaryURLInvalid) {
		t.Fatalf("expected empty disk invalid error, got %v", err)
	}
	if err := manager.VerifyTemporaryURL("local", "notes/test.txt", time.Now().Add(-time.Minute), signature); !errors.Is(err, ErrTemporaryURLInvalid) {
		t.Fatalf("expected expired url error, got %v", err)
	}
	if err := manager.VerifyTemporaryURL("local", "notes/test.txt", expires, "bad-signature"); !errors.Is(err, ErrTemporaryURLInvalid) {
		t.Fatalf("expected bad signature error, got %v", err)
	}
}

// TestRepositoryWithFakeDriver 覆盖 Repository 的 Laravel 风格常用方法。
func TestRepositoryWithFakeDriver(t *testing.T) {
	driver := &fakeDriver{
		files:      make(map[string]fakeFile),
		pathPrefix: t.TempDir(),
		publicBase: "http://example.test/files",
		tempBase:   "http://example.test/temp",
		visibility: VisibilityPrivate,
	}
	repo := &Repository{
		name:   "fake",
		config: DiskConfig{Visibility: VisibilityPrivate},
		driver: driver,
	}
	if repo.Name() != "fake" {
		t.Fatalf("unexpected repository name: %s", repo.Name())
	}

	if err := repo.Put(context.Background(), "docs/readme.txt", "hello world"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := repo.Put(context.Background(), "docs/bad.txt", 123); err == nil {
		t.Fatal("expected Put unsupported type error")
	}
	if err := repo.PutReader(context.Background(), "docs/stream.txt", strings.NewReader("stream")); err != nil {
		t.Fatalf("PutReader failed: %v", err)
	}
	if err := repo.WriteStream(context.Background(), "docs/write-stream.txt", strings.NewReader("write stream"), PutOptions{ContentType: "text/custom"}); err != nil {
		t.Fatalf("WriteStream failed: %v", err)
	}
	readStream, err := repo.ReadStream(context.Background(), "docs/write-stream.txt")
	if err != nil {
		t.Fatalf("ReadStream failed: %v", err)
	}
	readStreamData, err := io.ReadAll(readStream)
	if closeErr := readStream.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(readStreamData) != "write stream" {
		t.Fatalf("ReadStream content = %q err=%v", string(readStreamData), err)
	}

	header := newMultipartHeader(t, "file", "avatar.txt", []byte("avatar"))
	if path, err := repo.PutFile(context.Background(), "uploads", header); err != nil || path != "uploads/avatar.txt" {
		t.Fatalf("PutFile failed: %v / %s", err, path)
	}
	if path, err := repo.PutFileAs(context.Background(), "uploads", header, "custom.txt", PutOptions{ContentType: "text/custom"}); err != nil || path != "uploads/custom.txt" {
		t.Fatalf("PutFileAs failed: %v / %s", err, path)
	}
	emptyNameHeader := newMultipartHeader(t, "file", "   ", []byte("noname"))
	if path, err := repo.PutFile(context.Background(), "uploads", emptyNameHeader); err != nil || !strings.HasPrefix(path, "uploads/upload-") {
		t.Fatalf("PutFile empty name fallback failed: %v / %s", err, path)
	}
	if _, err := repo.PutFileAs(context.Background(), "uploads", header, "   "); err == nil {
		t.Fatal("expected PutFileAs empty filename error")
	}
	if _, err := repo.PutFileAs(context.Background(), "uploads", &multipart.FileHeader{Filename: "broken.txt"}, "broken.txt"); !errors.Is(err, ErrInvalidUploadFile) {
		t.Fatalf("expected PutFileAs open multipart upload error, got %v", err)
	}
	if _, err := repo.PutFile(context.Background(), "uploads", nil); !errors.Is(err, ErrInvalidUploadFile) {
		t.Fatalf("expected PutFile nil upload error, got %v", err)
	}
	if _, err := repo.PutFileAs(context.Background(), "uploads", nil, "demo.txt"); !errors.Is(err, ErrInvalidUploadFile) {
		t.Fatalf("expected PutFileAs nil upload error, got %v", err)
	}
	driver.writeErr = errors.New("write failed")
	if _, err := repo.PutFileAs(context.Background(), "uploads", header, "write-error.txt"); err == nil {
		t.Fatal("expected PutFileAs write error")
	}
	driver.writeErr = nil

	content, err := repo.Get(context.Background(), "docs/readme.txt")
	if err != nil || string(content) != "hello world" {
		t.Fatalf("Get failed: %v / %s", err, string(content))
	}

	stream, info, err := repo.OpenStream(context.Background(), "docs/readme.txt")
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Errorf("close stream: %v", err)
		}
	}()
	streamData, _ := io.ReadAll(stream)
	if string(streamData) != "hello world" || info.Size == 0 {
		t.Fatalf("unexpected stream content/info: %s / %+v", string(streamData), info)
	}

	var download bytes.Buffer
	if err := repo.Download(context.Background(), "docs/readme.txt", &download); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	if download.String() != "hello world" {
		t.Fatalf("unexpected download content: %s", download.String())
	}

	exists, err := repo.Exists(context.Background(), "docs/readme.txt")
	if err != nil || !exists {
		t.Fatalf("Exists failed: %v / %v", err, exists)
	}
	if missing, err := repo.Missing(context.Background(), "docs/missing.txt"); err != nil || !missing {
		t.Fatalf("Missing failed: %v / %v", err, missing)
	}
	if fileExists, err := repo.FileExists(context.Background(), "docs/readme.txt"); err != nil || !fileExists {
		t.Fatalf("FileExists failed: %v / %v", err, fileExists)
	}
	driver.listFlat = []FileInfo{{Path: "docs", IsDir: true}}
	if dirExists, err := repo.DirectoryExists(context.Background(), "docs"); err != nil || !dirExists {
		t.Fatalf("DirectoryExists failed: %v / %v", err, dirExists)
	}

	if err := repo.Prepend(context.Background(), "docs/order.txt", "middle"); err != nil {
		t.Fatalf("Prepend missing file failed: %v", err)
	}
	if err := repo.Prepend(context.Background(), "docs/order.txt", "first"); err != nil {
		t.Fatalf("Prepend existing file failed: %v", err)
	}
	if err := repo.Append(context.Background(), "docs/order.txt", "last"); err != nil {
		t.Fatalf("Append existing file failed: %v", err)
	}
	ordered, err := repo.Get(context.Background(), "docs/order.txt")
	if err != nil || string(ordered) != "first\nmiddle\nlast" {
		t.Fatalf("unexpected prepend/append content: %q err=%v", string(ordered), err)
	}

	if err := repo.Put(context.Background(), "docs/payload.json", `{"name":"prism"}`, PutOptions{ContentType: "application/json"}); err != nil {
		t.Fatalf("Put JSON failed: %v", err)
	}
	var payload map[string]string
	if err := repo.JSON(context.Background(), "docs/payload.json", &payload); err != nil || payload["name"] != "prism" {
		t.Fatalf("JSON failed: %#v err=%v", payload, err)
	}
	if err := repo.Put(context.Background(), "docs/invalid.json", `{`); err != nil {
		t.Fatalf("Put invalid JSON failed: %v", err)
	}
	if err := repo.JSON(context.Background(), "docs/invalid.json", &payload); err == nil {
		t.Fatal("expected invalid JSON decode error")
	}
	if mimeType, err := repo.MimeType(context.Background(), "docs/payload.json"); err != nil || mimeType != "application/json" {
		t.Fatalf("MimeType metadata failed: %q err=%v", mimeType, err)
	}
	driver.files["docs/sniff.bin"] = fakeFile{data: []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, lastModified: time.Now().UTC()}
	if mimeType, err := repo.MimeType(context.Background(), "docs/sniff.bin"); err != nil || mimeType != "image/png" {
		t.Fatalf("MimeType sniff failed: %q err=%v", mimeType, err)
	}
	checksum, err := repo.Checksum(context.Background(), "docs/readme.txt")
	expectedHash := sha256.Sum256([]byte("hello world"))
	if err != nil || checksum != hex.EncodeToString(expectedHash[:]) {
		t.Fatalf("Checksum failed: %q err=%v", checksum, err)
	}
	if _, err := repo.Checksum(context.Background(), "docs/readme.txt", ChecksumOptions{Algorithm: "md5"}); err == nil {
		t.Fatal("expected unsupported checksum algorithm error")
	}
	if err := repo.Copy(context.Background(), "docs/readme.txt", "docs/readme-copy.txt"); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	if err := repo.Move(context.Background(), "docs/readme-copy.txt", "archive/readme.txt"); err != nil {
		t.Fatalf("Move failed: %v", err)
	}
	if path := repo.Path("archive/readme.txt"); !strings.Contains(path, filepath.FromSlash("archive/readme.txt")) {
		t.Fatalf("unexpected path: %s", path)
	}
	if size, err := repo.Size(context.Background(), "archive/readme.txt"); err != nil || size <= 0 {
		t.Fatalf("Size failed: %v / %d", err, size)
	}
	if modified, err := repo.LastModified(context.Background(), "archive/readme.txt"); err != nil || modified.IsZero() {
		t.Fatalf("LastModified failed: %v / %v", err, modified)
	}
	if _, err := repo.LastModifiedInfo(context.Background(), "archive/readme.txt"); err != nil {
		t.Fatalf("LastModifiedInfo failed: %v", err)
	}

	driver.listFlat = []FileInfo{
		{Path: "uploads/images", IsDir: true},
		{Path: "uploads/a.txt"},
	}
	driver.listRecursive = []FileInfo{
		{Path: "uploads/images", IsDir: true},
		{Path: "uploads/images/nested", IsDir: true},
		{Path: "uploads/a.txt"},
		{Path: "uploads/images/b.txt"},
	}
	if err := repo.MakeDirectory(context.Background(), "uploads/new"); err != nil {
		t.Fatalf("MakeDirectory failed: %v", err)
	}
	if files, err := repo.Files(context.Background(), "uploads"); err != nil || len(files) == 0 {
		t.Fatalf("Files failed: %v / %+v", err, files)
	}
	if allFiles, err := repo.AllFiles(context.Background(), "uploads"); err != nil || len(allFiles) < 2 {
		t.Fatalf("AllFiles failed: %v / %+v", err, allFiles)
	}
	if dirs, err := repo.Directories(context.Background(), "uploads"); err != nil || len(dirs) == 0 {
		t.Fatalf("Directories failed: %v / %+v", err, dirs)
	}
	if allDirs, err := repo.AllDirectories(context.Background(), "uploads"); err != nil || len(allDirs) < 2 {
		t.Fatalf("AllDirectories failed: %v / %+v", err, allDirs)
	}
	if err := repo.DeleteDirectory(context.Background(), "uploads/new"); err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	calls := driver.deleteDirCalls
	if err := repo.DeleteDirectory(context.Background(), ""); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected empty DeleteDirectory error, got %v", err)
	}
	if err := repo.DeleteDirectory(context.Background(), "   "); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected whitespace DeleteDirectory error, got %v", err)
	}
	if driver.deleteDirCalls != calls {
		t.Fatalf("empty DeleteDirectory should not call driver, calls=%d want %d", driver.deleteDirCalls, calls)
	}

	if got, err := repo.URL("archive/readme.txt"); err != nil || !strings.Contains(got, "/archive/readme.txt") {
		t.Fatalf("URL failed: %v / %s", err, got)
	}
	if got, err := repo.TemporaryURL(context.Background(), "archive/readme.txt", time.Now().Add(time.Minute)); err != nil || !strings.Contains(got, "/archive/readme.txt") {
		t.Fatalf("TemporaryURL failed: %v / %s", err, got)
	}
	if !repo.ProvidesTemporaryURLs() {
		t.Fatal("expected fake temp URL disk to provide temporary URLs")
	}
	if repo.ProvidesTemporaryUploadURLs() {
		t.Fatal("fake driver should not provide temporary upload URLs")
	}
	var upload fscontract.TemporaryUploadURLGenerator = repo
	if upload.ProvidesTemporaryUploadURLs() {
		t.Fatal("repository should report unsupported temporary upload URLs")
	}
	if _, err := upload.TemporaryUploadURL(context.Background(), "archive/readme.txt", time.Now().Add(time.Minute)); !errors.Is(err, ErrTemporaryUploadURLUnavailable) {
		t.Fatalf("TemporaryUploadURL error = %v, want unavailable", err)
	}
	if err := repo.SetVisibility(context.Background(), "archive/readme.txt", VisibilityPublic); err != nil {
		t.Fatalf("SetVisibility failed: %v", err)
	}
	if visibility, err := repo.GetVisibility(context.Background(), "archive/readme.txt"); err != nil || visibility != VisibilityPublic {
		t.Fatalf("GetVisibility failed: %v / %s", err, visibility)
	}

	if err := repo.Delete(context.Background(), "archive/readme.txt", "docs/readme.txt"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	exists, _ = repo.Exists(context.Background(), "archive/readme.txt")
	if exists {
		t.Fatal("expected file to be deleted")
	}

	driver.deleteErr = errors.New("delete failed")
	if err := repo.Delete(context.Background(), "docs/stream.txt"); err == nil {
		t.Fatal("expected delete error")
	}
	driver.deleteErr = nil

	driver.closeErr = errors.New("close failed")
	if err := repo.Close(); err == nil {
		t.Fatal("expected close error")
	}
}

// TestRepositoryErrorBranches 统一覆盖错误仓储上的提前返回路径。
func TestRepositoryErrorBranches(t *testing.T) {
	repo := &Repository{
		name:   "error",
		config: DiskConfig{Visibility: VisibilityPrivate},
		err:    ErrDiskNotFound,
	}
	header := newMultipartHeader(t, "file", "demo.txt", []byte("demo"))

	if _, err := repo.Get(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Get to return init error, got %v", err)
	}
	if _, _, err := repo.OpenStream(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected OpenStream to return init error, got %v", err)
	}
	if err := repo.Download(context.Background(), "a.txt", &bytes.Buffer{}); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Download to return init error, got %v", err)
	}
	if _, err := repo.Exists(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Exists to return init error, got %v", err)
	}
	if err := repo.Delete(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Delete to return init error, got %v", err)
	}
	if err := repo.Copy(context.Background(), "a.txt", "b.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Copy to return init error, got %v", err)
	}
	if err := repo.Move(context.Background(), "a.txt", "b.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Move to return init error, got %v", err)
	}
	if _, err := repo.Size(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Size to return init error, got %v", err)
	}
	if _, err := repo.LastModified(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected LastModified to return init error, got %v", err)
	}
	if _, err := repo.LastModifiedInfo(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected LastModifiedInfo to return init error, got %v", err)
	}
	if err := repo.MakeDirectory(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected MakeDirectory to return init error, got %v", err)
	}
	if err := repo.DeleteDirectory(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected DeleteDirectory to return init error, got %v", err)
	}
	if _, err := repo.Files(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Files to return init error, got %v", err)
	}
	if _, err := repo.AllFiles(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected AllFiles to return init error, got %v", err)
	}
	if _, err := repo.Directories(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected Directories to return init error, got %v", err)
	}
	if _, err := repo.AllDirectories(context.Background(), "a"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected AllDirectories to return init error, got %v", err)
	}
	if _, err := repo.URL("a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected URL to return init error, got %v", err)
	}
	if _, err := repo.TemporaryURL(context.Background(), "a.txt", time.Now().Add(time.Minute)); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected TemporaryURL to return init error, got %v", err)
	}
	if err := repo.SetVisibility(context.Background(), "a.txt", VisibilityPublic); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected SetVisibility to return init error, got %v", err)
	}
	if _, err := repo.GetVisibility(context.Background(), "a.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected GetVisibility to return init error, got %v", err)
	}
	if _, err := repo.PutFile(context.Background(), "uploads", header); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected PutFile to return init error, got %v", err)
	}
	if _, err := repo.PutFileAs(context.Background(), "uploads", header, "demo.txt"); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected PutFileAs to return init error, got %v", err)
	}
	if err := repo.PutReader(context.Background(), "a.txt", strings.NewReader("x")); !errors.Is(err, ErrDiskNotFound) {
		t.Fatalf("expected PutReader to return init error, got %v", err)
	}
}

// TestFacadeAndManagerClose 覆盖 facade 全局入口和 Close 返回首个错误的行为。
func TestFacadeAndManagerClose(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	assertPanics(t, func() { _ = Resolve() })
	driver := &fakeDriver{
		files:      make(map[string]fakeFile),
		pathPrefix: t.TempDir(),
		publicBase: "http://example.test/files",
		tempBase:   "http://example.test/temp",
		visibility: VisibilityPrivate,
	}
	manager := &Manager{
		defaultName: "fake",
		cloudName:   "fake-cloud",
		tempURL:     TemporaryURLConfig{SigningKey: "facade-secret"},
		disks: map[string]*Repository{
			"fake": {
				name:   "fake",
				config: DiskConfig{Visibility: VisibilityPrivate},
				driver: driver,
			},
			"broken-close": {
				name:   "broken-close",
				config: DiskConfig{Visibility: VisibilityPrivate},
				driver: &fakeDriver{closeErr: errors.New("first close error")},
			},
		},
	}
	registry := useFilesystemTestContainer(t)
	if err := registry.Instance(serviceKey, manager); err != nil {
		t.Fatalf("bind filesystem manager: %v", err)
	}
	if Disk("fake") != manager.Disk("fake") {
		t.Fatal("expected facade Disk to return active manager repository")
	}
	if DefaultName() != "fake" {
		t.Fatalf("facade DefaultName = %s, want fake", DefaultName())
	}
	if CloudName() != "fake-cloud" {
		t.Fatalf("facade CloudName = %s, want fake-cloud", CloudName())
	}
	if Name() != "fake" {
		t.Fatalf("facade Name = %s, want fake", Name())
	}

	if err := Put(context.Background(), "demo.txt", "demo"); err != nil {
		t.Fatalf("facade Put failed: %v", err)
	}
	if exists, err := Exists(context.Background(), "demo.txt"); err != nil || !exists {
		t.Fatalf("facade Exists failed: %v / %v", err, exists)
	}
	if _, err := Get(context.Background(), "demo.txt"); err != nil {
		t.Fatalf("facade Get failed: %v", err)
	}
	if _, _, err := OpenStream(context.Background(), "demo.txt"); err != nil {
		t.Fatalf("facade OpenStream failed: %v", err)
	}
	var downloaded bytes.Buffer
	if err := Download(context.Background(), "demo.txt", &downloaded); err != nil {
		t.Fatalf("facade Download failed: %v", err)
	}
	if downloaded.String() != "demo" {
		t.Fatalf("facade Download content = %q, want demo", downloaded.String())
	}
	if err := Copy(context.Background(), "demo.txt", "copy.txt"); err != nil {
		t.Fatalf("facade Copy failed: %v", err)
	}
	if err := Move(context.Background(), "copy.txt", "moved.txt"); err != nil {
		t.Fatalf("facade Move failed: %v", err)
	}
	if Path("moved.txt") == "" {
		t.Fatal("facade Path should not be empty")
	}
	if _, err := Size(context.Background(), "moved.txt"); err != nil {
		t.Fatalf("facade Size failed: %v", err)
	}
	if _, err := LastModified(context.Background(), "moved.txt"); err != nil {
		t.Fatalf("facade LastModified failed: %v", err)
	}
	if _, err := LastModifiedInfo(context.Background(), "moved.txt"); err != nil {
		t.Fatalf("facade LastModifiedInfo failed: %v", err)
	}
	if err := MakeDirectory(context.Background(), "docs"); err != nil {
		t.Fatalf("facade MakeDirectory failed: %v", err)
	}
	if _, err := Files(context.Background(), "docs"); err != nil {
		t.Fatalf("facade Files failed: %v", err)
	}
	if _, err := AllFiles(context.Background(), "docs"); err != nil {
		t.Fatalf("facade AllFiles failed: %v", err)
	}
	if _, err := Directories(context.Background(), "docs"); err != nil {
		t.Fatalf("facade Directories failed: %v", err)
	}
	if _, err := AllDirectories(context.Background(), "docs"); err != nil {
		t.Fatalf("facade AllDirectories failed: %v", err)
	}
	if _, err := URL("moved.txt"); err != nil {
		t.Fatalf("facade URL failed: %v", err)
	}
	if _, err := TemporaryURL(context.Background(), "moved.txt", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("facade TemporaryURL failed: %v", err)
	}
	expires := time.Now().Add(time.Minute)
	signature := signToken("facade-secret", "fake", "moved.txt", expires)
	if err := VerifyTemporaryURL("fake", "moved.txt", expires, signature); err != nil {
		t.Fatalf("facade VerifyTemporaryURL failed: %v", err)
	}
	if err := SetVisibility(context.Background(), "moved.txt", VisibilityPublic); err != nil {
		t.Fatalf("facade SetVisibility failed: %v", err)
	}
	if _, err := GetVisibility(context.Background(), "moved.txt"); err != nil {
		t.Fatalf("facade GetVisibility failed: %v", err)
	}
	if err := Delete(context.Background(), "moved.txt"); err != nil {
		t.Fatalf("facade Delete failed: %v", err)
	}
	if err := DeleteDirectory(context.Background(), "docs"); err != nil {
		t.Fatalf("facade DeleteDirectory failed: %v", err)
	}

	if err := Close(); err == nil || err.Error() != "first close error" {
		t.Fatalf("expected first close error, got %v", err)
	}
}

// TestLocalDriverIntegration 使用真实 fileblob 覆盖本地磁盘行为。
func TestLocalDriverIntegration(t *testing.T) {
	root := t.TempDir()
	manager := newTestManager(t, root)
	defer func() { _ = manager.Close() }()

	privateDisk := manager.Disk("local")
	publicDisk := manager.Disk("public")

	if err := privateDisk.Put(context.Background(), "notes/test.txt", "hello"); err != nil {
		t.Fatalf("private Put failed: %v", err)
	}
	if err := publicDisk.Put(context.Background(), "avatars/me.txt", "public", PutOptions{Visibility: VisibilityPublic}); err != nil {
		t.Fatalf("public Put failed: %v", err)
	}

	if content, err := privateDisk.Get(context.Background(), "notes/test.txt"); err != nil || string(content) != "hello" {
		t.Fatalf("private Get failed: %v / %s", err, string(content))
	}
	if exists, err := privateDisk.Exists(context.Background(), "notes/test.txt"); err != nil || !exists {
		t.Fatalf("private Exists failed: %v / %v", err, exists)
	}
	if err := privateDisk.Copy(context.Background(), "notes/test.txt", "notes/test-copy.txt"); err != nil {
		t.Fatalf("private Copy failed: %v", err)
	}
	if err := privateDisk.Move(context.Background(), "notes/test-copy.txt", "archive/test.txt"); err != nil {
		t.Fatalf("private Move failed: %v", err)
	}
	if path := privateDisk.Path("archive/test.txt"); !strings.Contains(path, filepath.FromSlash("archive/test.txt")) {
		t.Fatalf("unexpected local Path: %s", path)
	}
	if size, err := privateDisk.Size(context.Background(), "archive/test.txt"); err != nil || size != int64(len("hello")) {
		t.Fatalf("private Size failed: %v / %d", err, size)
	}
	if modified, err := privateDisk.LastModified(context.Background(), "archive/test.txt"); err != nil || modified.IsZero() {
		t.Fatalf("private LastModified failed: %v / %v", err, modified)
	}

	if err := privateDisk.MakeDirectory(context.Background(), "uploads/images"); err != nil {
		t.Fatalf("MakeDirectory failed: %v", err)
	}
	if err := privateDisk.Put(context.Background(), "uploads/images/a.txt", "A"); err != nil {
		t.Fatalf("nested Put failed: %v", err)
	}
	if err := privateDisk.Put(context.Background(), "uploads/b.txt", "B"); err != nil {
		t.Fatalf("flat Put failed: %v", err)
	}

	files, err := privateDisk.Files(context.Background(), "uploads")
	if err != nil {
		t.Fatalf("Files failed: %v", err)
	}
	if got := sorted(files); len(got) == 0 || got[0] != "uploads/b.txt" {
		t.Fatalf("unexpected Files result: %+v", got)
	}
	allFiles, err := privateDisk.AllFiles(context.Background(), "uploads")
	if err != nil {
		t.Fatalf("AllFiles failed: %v", err)
	}
	if got := sorted(allFiles); len(got) != 2 {
		t.Fatalf("unexpected AllFiles result: %+v", got)
	}
	dirs, err := privateDisk.Directories(context.Background(), "uploads")
	if err != nil {
		t.Fatalf("Directories failed: %v", err)
	}
	if len(dirs) != 1 || dirs[0] != "uploads/images" {
		t.Fatalf("unexpected Directories result: %+v", dirs)
	}
	allDirs, err := privateDisk.AllDirectories(context.Background(), "uploads")
	if err != nil {
		t.Fatalf("AllDirectories failed: %v", err)
	}
	// fileblob 递归列举时不会像对象存储那样返回目录占位项，这里只验证调用成功。
	_ = allDirs
	if err := privateDisk.DeleteDirectory(context.Background(), "uploads/images"); err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	if err := privateDisk.Put(context.Background(), "root-guard.txt", "keep"); err != nil {
		t.Fatalf("root guard Put failed: %v", err)
	}
	if err := privateDisk.DeleteDirectory(context.Background(), ""); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected repository empty DeleteDirectory error, got %v", err)
	}
	if exists, err := privateDisk.Exists(context.Background(), "root-guard.txt"); err != nil || !exists {
		t.Fatalf("empty DeleteDirectory should preserve root file, got %v / %v", err, exists)
	}

	if _, err := privateDisk.URL("notes/test.txt"); !errors.Is(err, ErrPublicURLUnavailable) {
		t.Fatalf("expected private disk URL unavailable, got %v", err)
	}
	publicURL, err := publicDisk.URL("avatars/me.txt")
	if err != nil || publicURL != "http://example.test/storage/avatars/me.txt" {
		t.Fatalf("unexpected public URL: %v / %s", err, publicURL)
	}

	expires := time.Now().Add(2 * time.Minute).UTC().Truncate(time.Second)
	tempURL, err := privateDisk.TemporaryURL(context.Background(), "notes/test.txt", expires)
	if err != nil {
		t.Fatalf("TemporaryURL failed: %v", err)
	}
	parsed, err := url.Parse(tempURL)
	if err != nil {
		t.Fatalf("parse temporary url failed: %v", err)
	}
	if !strings.Contains(parsed.Path, "/storage-temp/local/notes/test.txt") {
		t.Fatalf("unexpected temporary url path: %s", parsed.Path)
	}
	if err := manager.VerifyTemporaryURL("local", "notes/test.txt", expires, parsed.Query().Get("signature")); err != nil {
		t.Fatalf("VerifyTemporaryURL failed: %v", err)
	}
	if signed := manager.signedLocalURL("public", "/avatars/me.txt", expires); !strings.Contains(signed, "/storage-temp/public/avatars/me.txt") {
		t.Fatalf("unexpected signedLocalURL public result: %s", signed)
	}
	privateRepo, ok := privateDisk.(*Repository)
	if !ok {
		t.Fatal("expected private disk to use Repository implementation")
	}
	localDriverRef, ok := privateRepo.driver.(*localDriver)
	if !ok {
		t.Fatal("expected local driver type assertion to succeed")
	}
	if rc, info, err := localDriverRef.Open(context.Background(), "notes/test.txt"); err != nil {
		t.Fatalf("direct local Open failed: %v", err)
	} else {
		_ = rc.Close()
		if info.Size == 0 {
			t.Fatalf("expected direct local Open info size > 0, got %+v", info)
		}
	}

	secondManager, err := NewManager(Config{
		Default: "local",
		Disks: map[string]DiskConfig{
			"local": {
				Driver:     "local",
				Root:       filepath.Join(root, "private-blank-url"),
				Visibility: VisibilityPrivate,
				Serve:      true,
			},
		},
		TemporaryURL: TemporaryURLConfig{SigningKey: "blank-url-secret"},
	})
	if err != nil {
		t.Fatalf("create second manager failed: %v", err)
	}
	defer func() { _ = secondManager.Close() }()
	if signed := secondManager.signedLocalURL("local", "demo.txt", expires); !strings.HasPrefix(signed, "/storage-temp/local/demo.txt") {
		t.Fatalf("unexpected signedLocalURL fallback: %s", signed)
	}

	if err := privateDisk.SetVisibility(context.Background(), "notes/test.txt", VisibilityPublic); !errors.Is(err, ErrUnsupportedVisibility) {
		t.Fatalf("expected private visibility error, got %v", err)
	}
	if err := publicDisk.SetVisibility(context.Background(), "avatars/me.txt", VisibilityPrivate); !errors.Is(err, ErrUnsupportedVisibility) {
		t.Fatalf("expected public visibility error, got %v", err)
	}
	if visibility, err := privateDisk.GetVisibility(context.Background(), "notes/test.txt"); err != nil || visibility != VisibilityPrivate {
		t.Fatalf("unexpected private visibility: %v / %s", err, visibility)
	}
	if visibility, err := publicDisk.GetVisibility(context.Background(), "avatars/me.txt"); err != nil || visibility != VisibilityPublic {
		t.Fatalf("unexpected public visibility: %v / %s", err, visibility)
	}
	if cached := manager.Disk("public"); cached != publicDisk {
		t.Fatal("expected local manager to reuse cached public disk")
	}

	if _, err := newLocalDriver(DiskConfig{}); err == nil {
		t.Fatal("expected empty local root error")
	}
	closedLocal, err := newLocalDriver(DiskConfig{
		Root:       filepath.Join(root, "closed-local"),
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("create closed local driver failed: %v", err)
	}
	_ = closedLocal.Close()
	if _, err := closedLocal.List(context.Background(), "", true); err == nil {
		t.Fatal("expected List on closed local driver to fail")
	}
	localWithNoURL, err := newLocalDriver(DiskConfig{
		Root:       filepath.Join(root, "no-url"),
		Visibility: VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("create local driver without url failed: %v", err)
	}
	defer func() { _ = localWithNoURL.Close() }()
	if _, err := localWithNoURL.URL("a.txt"); !errors.Is(err, ErrPublicURLUnavailable) {
		t.Fatalf("expected public url unavailable, got %v", err)
	}
	if _, err := localWithNoURL.TemporaryURL(context.Background(), "a.txt", expires); !errors.Is(err, ErrTemporaryURLDisabled) {
		t.Fatalf("expected temporary url disabled, got %v", err)
	}
	if err := localWithNoURL.Write(context.Background(), "a.txt", strings.NewReader("x"), PutOptions{Visibility: VisibilityPrivate}); !errors.Is(err, ErrUnsupportedVisibility) {
		t.Fatalf("expected unsupported visibility, got %v", err)
	}
	if err := localWithNoURL.DeleteDirectory(context.Background(), ""); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected direct local empty DeleteDirectory error, got %v", err)
	}
	if err := privateDisk.SetVisibility(context.Background(), "notes/test.txt", VisibilityPrivate); err != nil {
		t.Fatalf("expected local private visibility no-op, got %v", err)
	}
	if err := publicDisk.SetVisibility(context.Background(), "avatars/me.txt", VisibilityPublic); err != nil {
		t.Fatalf("expected public visibility no-op, got %v", err)
	}
	if err := privateDisk.Delete(context.Background(), "archive/test.txt"); err != nil {
		t.Fatalf("expected local Delete success, got %v", err)
	}
	if exists, err := privateDisk.Exists(context.Background(), "archive/test.txt"); err != nil || exists {
		t.Fatalf("expected deleted local file to disappear, got %v / %v", err, exists)
	}
	if absoluteRoot := localWithNoURL.absolutePath(""); absoluteRoot != localWithNoURL.root {
		t.Fatalf("expected absolutePath(\"\") to return root, got %s", absoluteRoot)
	}
	if items, err := localWithNoURL.List(context.Background(), "", false); err != nil || len(items) != 0 {
		t.Fatalf("expected empty local list at root, got %v / %+v", err, items)
	}
	if items, err := localWithNoURL.List(context.Background(), "nested", false); err != nil || len(items) != 0 {
		t.Fatalf("expected empty local list for nested prefix, got %v / %+v", err, items)
	}
	if _, _, err := localWithNoURL.Open(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected Open missing file error")
	}
	if err := localWithNoURL.Copy(context.Background(), "missing.txt", "other.txt"); err == nil {
		t.Fatal("expected Copy missing file error")
	}
	if err := localWithNoURL.Move(context.Background(), "missing.txt", "other.txt"); err == nil {
		t.Fatal("expected Move missing file error")
	}
	if _, err := localWithNoURL.Stat(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected Stat missing file error")
	}

	badRootFile := filepath.Join(root, "blocked-root")
	if err := os.WriteFile(badRootFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocked-root file failed: %v", err)
	}
	if _, err := newLocalDriver(DiskConfig{Root: filepath.Join(badRootFile, "child")}); err == nil {
		t.Fatal("expected MkdirAll failure for file parent path")
	}
}
