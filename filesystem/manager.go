package filesystem

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	configpkg "github.com/prismgo/framework/config"
	fscontract "github.com/prismgo/framework/contracts/filesystem"
	"github.com/prismgo/framework/support"
)

// Manager 负责管理整个应用内的磁盘配置和磁盘实例缓存。
//
// 设计说明：
// 1. 只在第一次访问某个磁盘时创建底层驱动，降低启动成本；
// 2. 同一磁盘实例会被缓存复用，避免重复打开 bucket/client；
// 3. 本地临时 URL 的签名校验也统一放在这里管理。
type Manager struct {
	mu          sync.Mutex
	defaultName string
	cloudName   string
	specs       map[string]DiskConfig
	disks       map[string]*Repository
	tempURL     TemporaryURLConfig
}

// NewManager 根据配置构建一个新的文件系统管理器。
func NewManager(cfg Config) (*Manager, error) {
	defaultName := strings.TrimSpace(cfg.Default)
	if defaultName == "" {
		defaultName = "local"
	}
	if len(cfg.Disks) == 0 {
		return nil, fmt.Errorf("%w: no disks configured", ErrDiskNotFound)
	}
	if _, ok := cfg.Disks[defaultName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrDiskNotFound, defaultName)
	}
	specs := make(map[string]DiskConfig, len(cfg.Disks))
	for name, disk := range cfg.Disks {
		specs[strings.TrimSpace(name)] = cloneDiskConfig(disk)
	}
	return &Manager{
		defaultName: defaultName,
		cloudName:   strings.TrimSpace(cfg.Cloud),
		specs:       specs,
		disks:       make(map[string]*Repository),
		tempURL:     cfg.TemporaryURL,
	}, nil
}

// DefaultName 返回默认磁盘名称。
func (m *Manager) DefaultName() string {
	return m.defaultName
}

// CloudName 返回预留的云磁盘名称。
func (m *Manager) CloudName() string {
	return m.cloudName
}

// Default 返回默认磁盘仓储实例。
func (m *Manager) Default() fscontract.Repository {
	return m.defaultRepository()
}

// Cloud 返回配置的云磁盘仓储实例。
func (m *Manager) Cloud() fscontract.Cloud {
	return m.Disk(m.cloudName)
}

// Disk 按名称获取磁盘仓储实例。
//
// 若磁盘尚未创建，会在这里延迟初始化并加入缓存。
func (m *Manager) Disk(name string) fscontract.Repository {
	return m.diskRepository(name)
}

func (m *Manager) defaultRepository() *Repository {
	return m.diskRepository(m.defaultName)
}

func (m *Manager) diskRepository(name string) *Repository {
	name = strings.TrimSpace(name)
	if name == "" {
		name = m.defaultName
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if repo, ok := m.disks[name]; ok {
		return repo
	}
	cfg, ok := m.specs[name]
	if !ok {
		return newErrorRepository(name, fmt.Errorf("%w: %s", ErrDiskNotFound, name), m)
	}
	repo, err := m.buildRepository(name, cfg)
	if err != nil {
		return newErrorRepository(name, err, m)
	}
	m.disks[name] = repo
	return repo
}

// Close 关闭所有已经创建过的磁盘实例。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, disk := range m.disks {
		if err := disk.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.disks = make(map[string]*Repository)
	return firstErr
}

// VerifyTemporaryURL 校验本地临时链接的签名和过期时间。
func (m *Manager) VerifyTemporaryURL(disk, key string, expires time.Time, signature string) error {
	disk = strings.TrimSpace(disk)
	if disk == "" {
		return ErrTemporaryURLInvalid
	}
	signature = strings.TrimSpace(signature)
	if signature == "" || expires.Before(time.Now()) {
		return ErrTemporaryURLInvalid
	}
	expected := signToken(m.tempURL.SigningKey, disk, key, expires)
	if !strings.EqualFold(expected, signature) {
		return ErrTemporaryURLInvalid
	}
	return nil
}

// buildRepository 根据磁盘配置构造统一仓储实例。
func (m *Manager) buildRepository(name string, cfg DiskConfig) (*Repository, error) {
	driverName := normalizeDriverName(cfg.Driver)
	if driverName == "" {
		driverName = "local"
	}
	cfg.Visibility = ensureVisibility(cfg.Visibility, VisibilityPrivate)

	var drv Driver
	var err error
	switch driverName {
	case "local", "public":
		drv, err = newLocalDriver(cfg)
	case "oss":
		drv, err = newOSSDriver(cfg.OSS)
	default:
		factory, ok := lookupDriverFactory(driverName)
		if !ok {
			err = fmt.Errorf("%w: %s", ErrUnsupportedDriver, driverName)
			break
		}
		drv, err = buildCustomDriver(name, driverName, cfg, factory)
	}
	if err != nil {
		return nil, err
	}
	if local, ok := drv.(*localDriver); ok {
		local.tempURL = func(key string, expiry time.Time) string {
			return m.signedLocalURL(name, key, expiry)
		}
	}
	return &Repository{
		name:    name,
		config:  cfg,
		driver:  drv,
		manager: m,
	}, nil
}

// buildCustomDriver 调用用户通过 Extend 注册的 driver 工厂。
func buildCustomDriver(name, driverName string, cfg DiskConfig, factory DriverFactory) (Driver, error) {
	drv, err := factory(DriverFactoryContext{
		Name:   name,
		Driver: driverName,
		Config: cloneDiskConfig(cfg),
	})
	if err != nil {
		return nil, fmt.Errorf("filesystem: build custom driver %q for disk %q: %w", driverName, name, err)
	}
	drv, err = normalizeCustomDriver(driverName, drv)
	if err != nil {
		return nil, fmt.Errorf("filesystem: build custom driver %q for disk %q: %w", driverName, name, err)
	}
	return drv, nil
}

// signedLocalURL 为本地驱动生成统一格式的签名访问地址。
func (m *Manager) signedLocalURL(disk, key string, expires time.Time) string {
	base := strings.TrimRight(m.diskRepository(disk).config.URL, "/")
	if base == "" {
		base = "/storage-temp/" + url.PathEscape(disk)
	} else if strings.HasSuffix(base, "/storage") {
		// 公开文件统一走 /storage/*path，签名文件改走 /storage-temp，避免 Gin 通配符路由冲突。
		base = strings.TrimSuffix(base, "/storage") + "/storage-temp/" + url.PathEscape(disk)
	} else {
		base = base + "/temp/" + url.PathEscape(disk)
	}
	key = normalizeKey(key)
	signature := signToken(m.tempURL.SigningKey, disk, key, expires)
	return joinURL(base, key) + "?expires=" + url.QueryEscape(expires.UTC().Format(time.RFC3339)) + "&signature=" + url.QueryEscape(signature)
}

func NewManagerFromConfig(...any) (func() error, *Manager, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, nil, err
	}
	m, err := NewManager(cfg)
	if err != nil {
		return nil, nil, err
	}
	return m.Close, m, nil
}

func buildConfig() (Config, error) {
	rawDisks := configpkg.GetStringMap("filesystem.disks")
	if len(rawDisks) == 0 {
		return Config{}, fmt.Errorf("filesystem.disks is empty")
	}

	disks := make(map[string]DiskConfig, len(rawDisks))
	tempKey := strings.TrimSpace(configpkg.GetString("filesystem.temporary_url.signing_key", ""))
	if tempKey == "" {
		tempKey = configpkg.GetString("app.key", "")
	}
	appURL := strings.TrimRight(strings.TrimSpace(configpkg.GetString("app.url", "")), "/")
	for name, raw := range rawDisks {
		spec, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		driver := normalizeDriverName(castString(spec["driver"]))
		root := strings.TrimSpace(castString(spec["root"]))
		url := strings.TrimRight(strings.TrimSpace(castString(spec["url"])), "/")
		if url == "" && driver == "local" {
			switch name {
			case "public", "local":
				if appURL != "" {
					url = appURL + "/storage"
				} else {
					url = "/storage"
				}
			}
		}
		if root != "" && (driver == "" || driver == "local" || driver == "public") {
			root = support.StoragePath(root)
		} else if root != "" {
			root = filepath.Clean(root)
		}
		disks[name] = DiskConfig{
			Driver:     driver,
			Root:       root,
			URL:        url,
			Prefix:     strings.TrimSpace(castString(spec["prefix"])),
			Visibility: strings.TrimSpace(castString(spec["visibility"])),
			Serve:      castBool(spec["serve"]),
			OSS: OSSConfig{
				Bucket:     strings.TrimSpace(castString(spec["bucket"])),
				Endpoint:   strings.TrimSpace(castString(spec["endpoint"])),
				AccessKey:  strings.TrimSpace(castString(spec["access_key"])),
				SecretKey:  strings.TrimSpace(castString(spec["secret_key"])),
				Prefix:     strings.TrimSpace(castString(spec["prefix"])),
				URL:        strings.TrimSpace(castString(spec["url"])),
				Visibility: strings.TrimSpace(castString(spec["visibility"])),
				Timeout:    time.Duration(castInt(spec["timeout"])) * time.Second,
			},
			Options: cloneAnyMap(spec),
		}
	}

	cfg := Config{
		Default: configpkg.GetString("filesystem.default", "local"),
		Cloud:   configpkg.GetString("filesystem.cloud", "oss"),
		Disks:   disks,
		Links:   buildLinksConfig(configpkg.GetStringMap("filesystem.links")),
		TemporaryURL: TemporaryURLConfig{
			SigningKey: tempKey,
		},
	}

	if publicDisk, ok := disks["public"]; ok && publicDisk.Driver == "local" && strings.TrimSpace(publicDisk.URL) == "" {
		return Config{}, fmt.Errorf("filesystem public disk url is empty")
	}

	// 验证签名密钥：如果有任何磁盘启用了 Serve，则必须配置签名密钥
	if tempKey == "" {
		for _, disk := range disks {
			if disk.Serve {
				return Config{}, fmt.Errorf("filesystem: temporary URL signing key is required when serve is enabled")
			}
		}
	}

	return cfg, nil
}

func buildLinksConfig(rawLinks map[string]any) map[string]string {
	if len(rawLinks) == 0 {
		return nil
	}
	links := make(map[string]string, len(rawLinks))
	for rawLink, rawTarget := range rawLinks {
		link := strings.TrimSpace(rawLink)
		target, ok := rawTarget.(string)
		if !ok {
			continue
		}
		target = strings.TrimSpace(target)
		if link == "" || target == "" {
			continue
		}
		links[resolveLinkPath(link)] = resolveLinkPath(target)
	}
	if len(links) == 0 {
		return nil
	}
	return links
}

func resolveLinkPath(path string) string {
	path = strings.TrimSpace(path)
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Clean(support.BasePath(path))
}

func castString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func castBool(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(strings.TrimSpace(value), "true")
	default:
		return false
	}
}

func castInt(v any) int {
	switch value := v.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		return parseInt(value, 0)
	default:
		return parseInt(castString(value), 0)
	}
}

func parseInt(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func cloneDiskConfig(spec DiskConfig) DiskConfig {
	spec.Options = cloneAnyMap(spec.Options)
	return spec
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
