package filesystem

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// fakeOSSObject 保存 fake OSS 服务端中的对象状态。
type fakeOSSObject struct {
	data         []byte
	contentType  string
	lastModified time.Time
	acl          string
}

// fakeOSSServer 用内存结构模拟一个最小可用的 OSS HTTP 服务。
type fakeOSSServer struct {
	mu      sync.Mutex
	objects map[string]fakeOSSObject
	delay   time.Duration
}

// newFakeOSSServer 创建测试专用的 fake OSS 服务。
func newFakeOSSServer() *fakeOSSServer {
	return &fakeOSSServer{
		objects: make(map[string]fakeOSSObject),
	}
}

// ServeHTTP 响应 ossDriver 依赖的最小 REST 语义。
func (s *fakeOSSServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.delay > 0 {
		// 测试 context/client timeout 时故意延迟响应，让底层 HTTP 请求走取消路径。
		time.Sleep(s.delay)
	}
	bucket, key := splitOSSPath(r.URL.Path)
	if bucket == "" {
		http.Error(w, "missing bucket", http.StatusBadRequest)
		return
	}

	switch {
	case r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2":
		s.handleList(w, r)
	case r.Method == http.MethodPut && r.URL.Query().Has("acl"):
		s.handleSetACL(w, key, r)
	case r.Method == http.MethodGet && r.URL.Query().Has("acl"):
		s.handleGetACL(w, key)
	case r.Method == http.MethodPut && key != "":
		s.handlePutObject(w, bucket, key, r)
	case r.Method == http.MethodHead && key != "":
		s.handleHeadObject(w, key)
	case r.Method == http.MethodGet && key != "":
		s.handleGetObject(w, key)
	case r.Method == http.MethodDelete && key != "":
		s.handleDeleteObject(w, key)
	default:
		http.Error(w, "unsupported request", http.StatusBadRequest)
	}
}

// handlePutObject 处理普通上传和服务端复制。
func (s *fakeOSSServer) handlePutObject(w http.ResponseWriter, bucket, key string, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if copySource := r.Header.Get("x-oss-copy-source"); copySource != "" {
		srcBucket, srcKey := splitCopySource(copySource)
		if srcBucket == "" || srcBucket != bucket {
			http.Error(w, "copy source bucket mismatch", http.StatusBadRequest)
			return
		}
		src, ok := s.objects[srcKey]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if acl := strings.TrimSpace(r.Header.Get("x-oss-object-acl")); acl != "" {
			src.acl = acl
		}
		src.lastModified = time.Now().UTC()
		s.objects[key] = src
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<CopyObjectResult><LastModified>`+src.lastModified.Format(time.RFC3339)+`</LastModified><ETag>"copied"</ETag></CopyObjectResult>`)
		return
	}

	data, _ := io.ReadAll(r.Body)
	acl := strings.TrimSpace(r.Header.Get("x-oss-object-acl"))
	if acl == "" {
		acl = string(oss.ACLPrivate)
	}
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	s.objects[key] = fakeOSSObject{
		data:         append([]byte(nil), data...),
		contentType:  contentType,
		lastModified: time.Now().UTC(),
		acl:          acl,
	}
	w.WriteHeader(http.StatusOK)
}

// handleHeadObject 返回对象元信息。
func (s *fakeOSSServer) handleHeadObject(w http.ResponseWriter, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	object, ok := s.objects[key]
	if !ok {
		http.NotFound(w, nil)
		return
	}
	writeObjectHeaders(w, object)
	w.WriteHeader(http.StatusOK)
}

// handleGetObject 返回对象内容。
func (s *fakeOSSServer) handleGetObject(w http.ResponseWriter, key string) {
	if key == "tenant-a/head-only.txt" {
		http.NotFound(w, nil)
		return
	}
	s.mu.Lock()
	object, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, nil)
		return
	}
	writeObjectHeaders(w, object)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(object.data)
}

// handleDeleteObject 删除对象。
func (s *fakeOSSServer) handleDeleteObject(w http.ResponseWriter, key string) {
	if key == "tenant-a/errdir/error-delete.txt" {
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// handleSetACL 设置对象 ACL。
func (s *fakeOSSServer) handleSetACL(w http.ResponseWriter, key string, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	object, ok := s.objects[key]
	if !ok {
		http.NotFound(w, r)
		return
	}
	object.acl = strings.TrimSpace(r.Header.Get("x-oss-object-acl"))
	object.lastModified = time.Now().UTC()
	s.objects[key] = object
	w.WriteHeader(http.StatusOK)
}

// handleGetACL 返回对象 ACL XML。
func (s *fakeOSSServer) handleGetACL(w http.ResponseWriter, key string) {
	s.mu.Lock()
	object, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<AccessControlPolicy><Owner><ID>1</ID><DisplayName>tester</DisplayName></Owner><AccessControlList><Grant>`+object.acl+`</Grant></AccessControlList></AccessControlPolicy>`)
}

// handleList 按 prefix / delimiter / continuation-token 返回 ListObjectsV2 XML。
func (s *fakeOSSServer) handleList(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix, _ := url.QueryUnescape(r.URL.Query().Get("prefix"))
	delimiter := r.URL.Query().Get("delimiter")
	token := r.URL.Query().Get("continuation-token")

	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	start := 0
	if token != "" {
		start, _ = strconv.Atoi(token)
	}
	pageSize := len(keys)
	if pageSize > 1 {
		pageSize = 1
	}
	end := start + pageSize
	if end > len(keys) {
		end = len(keys)
	}
	pageKeys := keys[start:end]

	type listObject struct {
		Key          string
		Size         int64
		LastModified string
	}
	type listResult struct {
		XMLName               xml.Name     `xml:"ListBucketResult"`
		Prefix                string       `xml:"Prefix"`
		Delimiter             string       `xml:"Delimiter,omitempty"`
		MaxKeys               int          `xml:"MaxKeys"`
		IsTruncated           bool         `xml:"IsTruncated"`
		NextContinuationToken string       `xml:"NextContinuationToken,omitempty"`
		Objects               []listObject `xml:"Contents"`
		CommonPrefixes        []string     `xml:"CommonPrefixes>Prefix,omitempty"`
	}

	result := listResult{
		Prefix:      prefix,
		Delimiter:   delimiter,
		MaxKeys:     pageSize,
		IsTruncated: end < len(keys),
	}
	if result.IsTruncated {
		result.NextContinuationToken = strconv.Itoa(end)
	}

	commonPrefixSeen := map[string]struct{}{}
	for _, key := range pageKeys {
		object := s.objects[key]
		if delimiter != "" {
			trimmed := strings.TrimPrefix(key, prefix)
			if idx := strings.Index(trimmed, delimiter); idx >= 0 {
				dir := prefix + trimmed[:idx+1]
				if _, ok := commonPrefixSeen[dir]; !ok {
					commonPrefixSeen[dir] = struct{}{}
					result.CommonPrefixes = append(result.CommonPrefixes, dir)
				}
				continue
			}
		}
		result.Objects = append(result.Objects, listObject{
			Key:          key,
			Size:         int64(len(object.data)),
			LastModified: object.lastModified.Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

// splitOSSPath 解析 path-style OSS 请求路径。
func splitOSSPath(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	if len(parts) == 1 {
		return bucket, ""
	}
	key, _ := url.PathUnescape(parts[1])
	return bucket, key
}

// splitCopySource 解析 x-oss-copy-source 头。
func splitCopySource(value string) (string, string) {
	value = strings.TrimPrefix(value, "/")
	parts := strings.SplitN(value, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	key, _ := url.QueryUnescape(parts[1])
	return parts[0], key
}

// writeObjectHeaders 把 fake 对象头写到响应。
func writeObjectHeaders(w http.ResponseWriter, object fakeOSSObject) {
	w.Header().Set("Content-Length", strconv.Itoa(len(object.data)))
	w.Header().Set("Content-Type", object.contentType)
	w.Header().Set("Last-Modified", object.lastModified.UTC().Format(http.TimeFormat))
}

// TestOSSDriverEndToEnd 覆盖 ossDriver 的主要行为和辅助函数。
func TestOSSDriverEndToEnd(t *testing.T) {
	serverState := newFakeOSSServer()
	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   server.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Prefix:     "tenant-a",
		Visibility: VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("newOSSDriver failed: %v", err)
	}

	if err := driver.Write(context.Background(), "docs/readme.txt", strings.NewReader("hello oss"), PutOptions{
		Visibility:  VisibilityPublic,
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	serverState.mu.Lock()
	serverState.objects["tenant-a/head-only.txt"] = fakeOSSObject{
		data:         []byte("head-only"),
		contentType:  "text/plain",
		lastModified: time.Now().UTC(),
		acl:          string(oss.ACLPrivate),
	}
	serverState.mu.Unlock()

	exists, err := driver.Exists(context.Background(), "docs/readme.txt")
	if err != nil || !exists {
		t.Fatalf("Exists failed: %v / %v", err, exists)
	}

	content, err := driver.ReadAll(context.Background(), "docs/readme.txt")
	if err != nil || string(content) != "hello oss" {
		t.Fatalf("ReadAll failed: %v / %s", err, string(content))
	}

	stream, info, err := driver.Open(context.Background(), "docs/readme.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer stream.Close()
	data, _ := io.ReadAll(stream)
	if string(data) != "hello oss" || info.ContentType != "text/plain" {
		t.Fatalf("unexpected Open payload/info: %s / %+v", string(data), info)
	}

	stat, err := driver.Stat(context.Background(), "docs/readme.txt")
	if err != nil || stat.Size != int64(len("hello oss")) {
		t.Fatalf("Stat failed: %v / %+v", err, stat)
	}

	publicURL, err := driver.URL("docs/readme.txt")
	if err != nil || !strings.Contains(publicURL, "/demo-bucket/tenant-a/docs/readme.txt") {
		t.Fatalf("URL failed: %v / %s", err, publicURL)
	}

	signedURL, err := driver.TemporaryURL(context.Background(), "docs/readme.txt", time.Now().Add(2*time.Minute))
	if err != nil || !strings.Contains(signedURL, "tenant-a%2Fdocs%2Freadme.txt") {
		t.Fatalf("TemporaryURL failed: %v / %s", err, signedURL)
	}
	if _, err := driver.TemporaryURL(context.Background(), "docs/readme.txt", time.Now().Add(-time.Minute)); !errors.Is(err, ErrTemporaryURLInvalid) {
		t.Fatalf("expected invalid temporary url error, got %v", err)
	}
	if _, err := driver.ReadAll(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected ReadAll missing object error")
	}
	if _, _, err := driver.Open(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected Open missing object error")
	}
	if _, _, err := driver.Open(context.Background(), "head-only.txt"); err == nil {
		t.Fatal("expected Open get-object error after successful HEAD")
	}
	if _, err := driver.Stat(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected Stat missing object error")
	}
	if err := driver.Copy(context.Background(), "missing.txt", "other.txt"); err == nil {
		t.Fatal("expected Copy missing object error")
	}
	if err := driver.Move(context.Background(), "missing.txt", "other.txt"); err == nil {
		t.Fatal("expected Move missing object error")
	}
	if _, err := driver.GetVisibility(context.Background(), "missing.txt"); err == nil {
		t.Fatal("expected GetVisibility missing object error")
	}

	if err := driver.SetVisibility(context.Background(), "docs/readme.txt", VisibilityPrivate); err != nil {
		t.Fatalf("SetVisibility private failed: %v", err)
	}
	if visibility, err := driver.GetVisibility(context.Background(), "docs/readme.txt"); err != nil || visibility != VisibilityPrivate {
		t.Fatalf("GetVisibility private failed: %v / %s", err, visibility)
	}
	if err := driver.SetVisibility(context.Background(), "docs/readme.txt", VisibilityPublic); err != nil {
		t.Fatalf("SetVisibility public failed: %v", err)
	}
	serverState.mu.Lock()
	object := serverState.objects["tenant-a/docs/readme.txt"]
	object.acl = string(oss.ACLPublicReadWrite)
	serverState.objects["tenant-a/docs/readme.txt"] = object
	serverState.mu.Unlock()
	if visibility, err := driver.GetVisibility(context.Background(), "docs/readme.txt"); err != nil || visibility != VisibilityPublic {
		t.Fatalf("GetVisibility public failed: %v / %s", err, visibility)
	}

	if err := driver.Copy(context.Background(), "docs/readme.txt", "docs/copied.txt"); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	if err := driver.Move(context.Background(), "docs/copied.txt", "archive/moved.txt"); err != nil {
		t.Fatalf("Move failed: %v", err)
	}
	if exists, _ := driver.Exists(context.Background(), "docs/copied.txt"); exists {
		t.Fatal("expected copied.txt to be moved away")
	}

	if err := driver.MakeDirectory(context.Background(), "photos/raw"); err != nil {
		t.Fatalf("MakeDirectory failed: %v", err)
	}
	if err := driver.Write(context.Background(), "photos/raw/one.txt", strings.NewReader("1"), PutOptions{Visibility: VisibilityPublic}); err != nil {
		t.Fatalf("write photos/raw/one.txt failed: %v", err)
	}
	if err := driver.Write(context.Background(), "photos/two.txt", strings.NewReader("2"), PutOptions{Visibility: VisibilityPublic}); err != nil {
		t.Fatalf("write photos/two.txt failed: %v", err)
	}

	flat, err := driver.List(context.Background(), "photos", false)
	if err != nil {
		t.Fatalf("List flat failed: %v", err)
	}
	if len(flat) == 0 {
		t.Fatal("expected flat list result")
	}

	recursive, err := driver.List(context.Background(), "photos", true)
	if err != nil {
		t.Fatalf("List recursive failed: %v", err)
	}
	if len(recursive) < 2 {
		t.Fatalf("expected recursive list items, got %+v", recursive)
	}

	if err := driver.DeleteDirectory(context.Background(), "photos"); err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	if exists, _ := driver.Exists(context.Background(), "photos/two.txt"); exists {
		t.Fatal("expected DeleteDirectory to remove nested object")
	}
	if err := driver.DeleteDirectory(context.Background(), ""); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected DeleteDirectory root guard error, got %v", err)
	}
	if err := driver.DeleteDirectory(context.Background(), "   "); !errors.Is(err, ErrEmptyDirectory) {
		t.Fatalf("expected DeleteDirectory whitespace guard error, got %v", err)
	}

	if err := driver.Write(context.Background(), "cleanup.txt", strings.NewReader("bye"), PutOptions{Visibility: VisibilityPublic}); err != nil {
		t.Fatalf("write cleanup.txt failed: %v", err)
	}
	if err := driver.Delete(context.Background(), "cleanup.txt"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if exists, _ := driver.Exists(context.Background(), "cleanup.txt"); exists {
		t.Fatal("expected cleanup.txt to be deleted")
	}
	if err := driver.Write(context.Background(), "errdir/error-delete.txt", strings.NewReader("err"), PutOptions{Visibility: VisibilityPublic}); err != nil {
		t.Fatalf("write errdir/error-delete.txt failed: %v", err)
	}
	if err := driver.DeleteDirectory(context.Background(), "errdir"); err == nil {
		t.Fatal("expected DeleteDirectory delete error")
	}

	if driver.Path("docs/readme.txt") != "oss://demo-bucket/tenant-a/docs/readme.txt" {
		t.Fatalf("unexpected Path result: %s", driver.Path("docs/readme.txt"))
	}
	if got := driver.objectKey("foo/bar.txt"); got != "tenant-a/foo/bar.txt" {
		t.Fatalf("unexpected objectKey: %s", got)
	}
	if got := driver.stripPrefix("tenant-a/foo/bar.txt"); got != "foo/bar.txt" {
		t.Fatalf("unexpected stripPrefix: %s", got)
	}
	if acl := driver.objectACL(VisibilityPublic); acl != oss.ACLPublicRead {
		t.Fatalf("unexpected public ACL: %s", acl)
	}
	if acl := driver.objectACL(VisibilityPrivate); acl != oss.ACLPrivate {
		t.Fatalf("unexpected private ACL: %s", acl)
	}

	headers := http.Header{}
	headers.Set("Content-Length", "9")
	headers.Set("Content-Type", "text/plain")
	headers.Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	if got := driver.fileInfoFromHeaders("demo.txt", headers); got.Path != "demo.txt" || got.Size != 9 {
		t.Fatalf("unexpected fileInfoFromHeaders result: %+v", got)
	}
	if out := appendOrReplace(nil, oss.Delimiter("/")); len(out) != 1 {
		t.Fatalf("expected appendOrReplace to create slice, got %+v", out)
	}
	if out := appendOrReplace([]oss.Option{oss.Prefix("demo")}, oss.Delimiter("/")); len(out) != 2 {
		t.Fatalf("expected appendOrReplace to append option, got %+v", out)
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("Close should be nil, got %v", err)
	}

	privateDriver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   server.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		URL:        "http://cdn.example.test",
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("new private oss driver failed: %v", err)
	}
	if privateDriver.baseURL != "http://cdn.example.test" {
		t.Fatalf("expected custom baseURL to be preserved, got %s", privateDriver.baseURL)
	}
	if _, err := privateDriver.URL("docs/readme.txt"); !errors.Is(err, ErrPublicURLUnavailable) {
		t.Fatalf("expected private URL unavailable, got %v", err)
	}
	if got := privateDriver.stripPrefix("docs/readme.txt"); got != "docs/readme.txt" {
		t.Fatalf("unexpected stripPrefix without prefix: %s", got)
	}

	endpointWithoutScheme := strings.TrimPrefix(server.URL, "http://")
	schemeLessDriver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   endpointWithoutScheme,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Visibility: VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("new scheme-less oss driver failed: %v", err)
	}
	if !strings.HasPrefix(schemeLessDriver.baseURL, "https://") {
		t.Fatalf("expected scheme-less endpoint to default to https, got %s", schemeLessDriver.baseURL)
	}

	brokenServer := httptest.NewServer(newFakeOSSServer())
	brokenDriver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   brokenServer.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Visibility: VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("new broken oss driver failed: %v", err)
	}
	brokenServer.Close()
	if err := brokenDriver.DeleteDirectory(context.Background(), "broken"); err == nil {
		t.Fatal("expected DeleteDirectory list error after server close")
	}
}

func TestOSSDriverContextCancellation(t *testing.T) {
	serverState := newFakeOSSServer()
	serverState.delay = 200 * time.Millisecond
	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   server.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Visibility: VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("newOSSDriver failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := driver.Exists(ctx, "slow.txt"); err == nil {
		t.Fatal("expected Exists to fail when context deadline is exceeded")
	}
}

func TestOSSDriverTemporaryUploadURLSignsVisibilityHeader(t *testing.T) {
	serverState := newFakeOSSServer()
	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   server.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Prefix:     "tenant-a",
		Visibility: VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("newOSSDriver failed: %v", err)
	}

	expiry := time.Now().Add(time.Hour)
	result, err := driver.TemporaryUploadURL(context.Background(), "uploads/avatar.txt", expiry, TemporaryUploadURLOptions{
		ContentType: "text/plain",
		Visibility:  VisibilityPublic,
	})
	if err != nil {
		t.Fatalf("TemporaryUploadURL failed: %v", err)
	}
	if result.Method != http.MethodPut {
		t.Fatalf("TemporaryUploadURL method = %s, want PUT", result.Method)
	}
	if got := result.Headers["x-oss-object-acl"]; got != string(oss.ACLPublicRead) {
		t.Fatalf("temporary upload ACL header = %q, want %q", got, oss.ACLPublicRead)
	}
	if got := result.Headers[oss.HTTPHeaderContentType]; got != "text/plain" {
		t.Fatalf("temporary upload content type header = %q, want text/plain", got)
	}

	expectedURL := signURLWithMatchingExpires(t, driver, result.URL, "uploads/avatar.txt",
		oss.ContentType("text/plain"),
		oss.ObjectACL(oss.ACLPublicRead),
	)
	if result.URL != expectedURL {
		t.Fatalf("TemporaryUploadURL did not sign expected options\n got: %s\nwant: %s", result.URL, expectedURL)
	}

	req, err := http.NewRequest(http.MethodPut, result.URL, strings.NewReader("avatar"))
	if err != nil {
		t.Fatalf("build PUT request: %v", err)
	}
	for k, v := range result.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT signed URL failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT signed URL status = %d, want 200", resp.StatusCode)
	}

	serverState.mu.Lock()
	object := serverState.objects["tenant-a/uploads/avatar.txt"]
	serverState.mu.Unlock()
	if object.acl != string(oss.ACLPublicRead) {
		t.Fatalf("uploaded object ACL = %q, want %q", object.acl, oss.ACLPublicRead)
	}
}

func signURLWithMatchingExpires(t *testing.T, driver *ossDriver, signedURL, key string, options ...oss.Option) string {
	t.Helper()
	parsed, err := url.Parse(signedURL)
	if err != nil {
		t.Fatalf("parse signed URL: %v", err)
	}
	expires, err := strconv.ParseInt(parsed.Query().Get(oss.HTTPParamExpires), 10, 64)
	if err != nil {
		t.Fatalf("parse signed URL Expires: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		seconds := expires - time.Now().Unix()
		if seconds <= 0 {
			break
		}
		last, err = driver.bucket.SignURL(driver.objectKey(key), oss.HTTPPut, seconds, options...)
		if err != nil {
			t.Fatalf("sign expected URL: %v", err)
		}
		expected, err := url.Parse(last)
		if err != nil {
			t.Fatalf("parse expected URL: %v", err)
		}
		if expected.Query().Get(oss.HTTPParamExpires) == parsed.Query().Get(oss.HTTPParamExpires) {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("could not generate expected URL with matching Expires; last=%s", last)
	return ""
}

func TestOSSDriverClientTimeout(t *testing.T) {
	serverState := newFakeOSSServer()
	serverState.delay = 1500 * time.Millisecond
	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:     "demo-bucket",
		Endpoint:   server.URL,
		AccessKey:  "test-key",
		SecretKey:  "test-secret",
		Visibility: VisibilityPublic,
		Timeout:    time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newOSSDriver failed: %v", err)
	}

	start := time.Now()
	if _, err := driver.Exists(context.Background(), "slow.txt"); err == nil {
		t.Fatal("expected Exists to fail when OSS client timeout is exceeded")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("OSS client timeout took too long: %s", elapsed)
	}
}

// TestNewOSSDriverValidation 覆盖 OSS 驱动创建时的错误分支。
func TestNewOSSDriverValidation(t *testing.T) {
	if _, err := newOSSDriver(OSSConfig{}); err == nil {
		t.Fatal("expected empty bucket/endpoint error")
	}
	if _, err := newOSSDriver(OSSConfig{
		Bucket:    "BadBucket",
		Endpoint:  "://bad-endpoint",
		AccessKey: "key",
		SecretKey: "secret",
	}); err == nil {
		t.Fatal("expected invalid oss config error")
	}
	if _, err := newOSSDriver(OSSConfig{
		Bucket:    "demo-bucket",
		Endpoint:  "http://127.0.0.1:bad-port",
		AccessKey: "key",
		SecretKey: "secret",
	}); err == nil {
		t.Fatal("expected invalid endpoint parse error")
	}
}

// TestManagerBuildRepositoryWithOSS 覆盖 Manager 构建 oss 仓储的分支。
func TestManagerBuildRepositoryWithOSS(t *testing.T) {
	server := httptest.NewServer(newFakeOSSServer())
	defer server.Close()

	manager, err := NewManager(Config{
		Default: "oss",
		Cloud:   "oss",
		Disks: map[string]DiskConfig{
			"oss": {
				Driver:     "oss",
				Visibility: VisibilityPublic,
				OSS: OSSConfig{
					Bucket:     "demo-bucket",
					Endpoint:   server.URL,
					AccessKey:  "test-key",
					SecretKey:  "test-secret",
					Visibility: VisibilityPublic,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager with oss failed: %v", err)
	}
	defer func() { _ = manager.Close() }()

	if manager.Default().Name() != "oss" {
		t.Fatalf("expected default oss repository, got %s", manager.Default().Name())
	}
}
