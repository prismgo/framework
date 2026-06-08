package filesystem

import "errors"

var (
	// ErrDiskNotFound 表示请求的磁盘未注册或管理器未初始化。
	ErrDiskNotFound = errors.New("filesystem: disk not found")
	// ErrUnsupportedDriver 表示当前驱动类型尚未实现。
	ErrUnsupportedDriver = errors.New("filesystem: unsupported driver")
	// ErrUnsupportedVisibility 表示当前磁盘不支持目标可见性。
	ErrUnsupportedVisibility = errors.New("filesystem: unsupported visibility")
	// ErrPublicURLUnavailable 表示目标文件无法生成公开地址。
	ErrPublicURLUnavailable = errors.New("filesystem: public url unavailable")
	// ErrTemporaryURLDisabled 表示当前磁盘未开启临时链接能力。
	ErrTemporaryURLDisabled = errors.New("filesystem: temporary url disabled")
	// ErrTemporaryURLInvalid 表示临时链接参数不合法或已过期。
	ErrTemporaryURLInvalid = errors.New("filesystem: temporary url invalid")
	// ErrTemporaryUploadURLUnavailable 表示当前磁盘不能生成临时上传链接。
	ErrTemporaryUploadURLUnavailable = errors.New("filesystem: temporary upload url unavailable")
	// ErrCrossDiskOperation 预留给跨磁盘复制/移动场景。
	ErrCrossDiskOperation = errors.New("filesystem: cross disk operation is not supported")
	// ErrEmptyDirectory 表示破坏性目录删除收到了空目录，避免误删本地根目录或 OSS 前缀根。
	ErrEmptyDirectory = errors.New("filesystem: empty directory")
	// ErrInvalidUploadFile 表示上传文件参数为空，或无法作为 multipart 文件打开。
	ErrInvalidUploadFile = errors.New("filesystem: invalid upload file")
)
