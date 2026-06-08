package filesystem

import (
	"time"

	fscontract "github.com/prismgo/framework/contracts/filesystem"
)

const (
	// VisibilityPublic 表示文件可以直接生成公开访问地址。
	VisibilityPublic = fscontract.VisibilityPublic
	// VisibilityPrivate 表示文件只能通过内部读取或临时签名地址访问。
	VisibilityPrivate = fscontract.VisibilityPrivate
)

// Config 描述整个文件系统管理器的初始化参数。
type Config struct {
	// Default 是默认磁盘名称，对应 Laravel 中的 default disk。
	Default string
	// Cloud 是预留的云盘名称，便于业务代码表达“当前云存储”。
	Cloud string
	// Disks 存放所有已注册磁盘配置。
	Disks map[string]DiskConfig
	// TemporaryURL 控制本地临时签名地址的签名行为。
	TemporaryURL TemporaryURLConfig
}

// DiskConfig 描述单个磁盘的通用配置。
type DiskConfig struct {
	// Driver 决定底层驱动类型，支持 local / oss 以及 Extend 注册的自定义 driver。
	Driver string
	// Root 仅 local 驱动使用，表示本地根目录。
	Root string
	// URL 用于生成公开访问地址或本地临时 URL 前缀。
	URL string
	// Prefix 用于给磁盘统一追加对象前缀。
	Prefix string
	// Visibility 表示该磁盘默认可见性。
	Visibility string
	// Serve 控制本地磁盘是否允许生成临时访问地址。
	Serve bool
	// OSS 保存对象存储驱动的专属配置。
	OSS OSSConfig
	// Options 保存自定义 driver 的原始扩展参数，内置 driver 会安全忽略。
	Options map[string]any
}

// OSSConfig 描述阿里云 OSS 驱动所需的连接参数。
type OSSConfig struct {
	Bucket     string
	Endpoint   string
	AccessKey  string
	SecretKey  string
	URL        string
	Prefix     string
	Timeout    time.Duration
	Visibility string
}

// TemporaryURLConfig 控制临时签名 URL 的签名密钥。
type TemporaryURLConfig struct {
	SigningKey string
}

// FileInfo 是跨驱动统一后的文件元信息。
type FileInfo = fscontract.FileInfo

// PutOptions 描述写入文件时的附加参数。
type PutOptions = fscontract.PutOptions

// ChecksumOptions 描述校验和计算选项。
type ChecksumOptions = fscontract.ChecksumOptions

// TemporaryUploadURLOptions 描述临时上传 URL 生成选项。
type TemporaryUploadURLOptions = fscontract.TemporaryUploadURLOptions

// TemporaryUploadURLResult 描述临时上传 URL 生成结果。
type TemporaryUploadURLResult = fscontract.TemporaryUploadURLResult
