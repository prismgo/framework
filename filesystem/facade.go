package filesystem

import (
	"context"
	"io"
	"mime/multipart"
	"time"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	fscontract "github.com/prismgo/framework/contracts/filesystem"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "filesystem.manager"

// Resolve 从当前 Application 容器解析文件系统 Manager。
func Resolve() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// DefaultName 返回全局管理器的默认磁盘名称。
func DefaultName() string {
	if m := Resolve(); m != nil {
		return m.DefaultName()
	}
	return ""
}

// CloudName 返回全局管理器配置的云磁盘名称。
func CloudName() string {
	if m := Resolve(); m != nil {
		return m.CloudName()
	}
	return ""
}

// Close 关闭全局管理器中已经创建过的磁盘实例。
func Close() error {
	if m := Resolve(); m != nil {
		return m.Close()
	}
	return nil
}

// VerifyTemporaryURL 校验本地临时链接的签名和过期时间。
func VerifyTemporaryURL(disk, key string, expires time.Time, signature string) error {
	if m := Resolve(); m != nil {
		return m.VerifyTemporaryURL(disk, key, expires, signature)
	}
	return ErrTemporaryURLInvalid
}

// Default 返回默认 disk 对应的 Repository。
//
// 说明：filesystem.Default 不是“当前 facade 实例”别名，而是文件系统 manager 下的
// default disk 选择器，因此保留为文件系统包的业务 API。
func Default() fscontract.Repository {
	return Resolve().Default()
}

// Disk 返回指定名称的全局磁盘。
func Disk(name string) fscontract.Repository {
	return Resolve().Disk(name)
}

// Name 返回默认磁盘对应的仓储名称。
func Name() string {
	return Resolve().defaultRepository().Name()
}

// Put 使用默认磁盘写入内容。
func Put(ctx context.Context, key string, value any, opts ...PutOptions) error {
	return Resolve().defaultRepository().Put(ctx, key, value, opts...)
}

// PutReader 使用默认磁盘写入 Reader 内容。
func PutReader(ctx context.Context, key string, reader io.Reader, opts ...PutOptions) error {
	return Resolve().defaultRepository().PutReader(ctx, key, reader, opts...)
}

// PutFile 使用默认磁盘按原文件名保存上传文件。
func PutFile(ctx context.Context, dir string, file *multipart.FileHeader, opts ...PutOptions) (string, error) {
	return Resolve().defaultRepository().PutFile(ctx, dir, file, opts...)
}

// PutFileAs 使用默认磁盘按指定文件名保存上传文件。
func PutFileAs(ctx context.Context, dir string, file *multipart.FileHeader, name string, opts ...PutOptions) (string, error) {
	return Resolve().defaultRepository().PutFileAs(ctx, dir, file, name, opts...)
}

// Get 使用默认磁盘读取文件内容。
func Get(ctx context.Context, key string) ([]byte, error) {
	return Resolve().defaultRepository().Get(ctx, key)
}

// OpenStream 使用默认磁盘以流式方式打开文件。
func OpenStream(ctx context.Context, key string) (io.ReadCloser, FileInfo, error) {
	return Resolve().defaultRepository().OpenStream(ctx, key)
}

// Download 使用默认磁盘把文件内容复制到指定 Writer。
func Download(ctx context.Context, key string, w io.Writer) error {
	return Resolve().defaultRepository().Download(ctx, key, w)
}

// Exists 使用默认磁盘判断文件是否存在。
func Exists(ctx context.Context, key string) (bool, error) {
	return Resolve().defaultRepository().Exists(ctx, key)
}

// Delete 使用默认磁盘删除文件。
func Delete(ctx context.Context, keys ...string) error {
	return Resolve().defaultRepository().Delete(ctx, keys...)
}

// Copy 使用默认磁盘复制文件。
func Copy(ctx context.Context, src, dst string) error {
	return Resolve().defaultRepository().Copy(ctx, src, dst)
}

// Move 使用默认磁盘移动文件。
func Move(ctx context.Context, src, dst string) error {
	return Resolve().defaultRepository().Move(ctx, src, dst)
}

// Path 使用默认磁盘返回物理路径或逻辑路径。
func Path(key string) string {
	return Resolve().defaultRepository().Path(key)
}

// Size 使用默认磁盘返回文件大小。
func Size(ctx context.Context, key string) (int64, error) {
	return Resolve().defaultRepository().Size(ctx, key)
}

// LastModified 使用默认磁盘返回最后修改时间。
func LastModified(ctx context.Context, key string) (time.Time, error) {
	return Resolve().defaultRepository().LastModified(ctx, key)
}

// LastModifiedInfo 使用默认磁盘返回完整文件元信息。
func LastModifiedInfo(ctx context.Context, key string) (FileInfo, error) {
	return Resolve().defaultRepository().LastModifiedInfo(ctx, key)
}

// MakeDirectory 使用默认磁盘创建目录。
func MakeDirectory(ctx context.Context, dir string) error {
	return Resolve().defaultRepository().MakeDirectory(ctx, dir)
}

// DeleteDirectory 使用默认磁盘删除目录。
func DeleteDirectory(ctx context.Context, dir string) error {
	return Resolve().defaultRepository().DeleteDirectory(ctx, dir)
}

// Files 使用默认磁盘列出当前层文件。
func Files(ctx context.Context, dir string) ([]string, error) {
	return Resolve().defaultRepository().Files(ctx, dir)
}

// AllFiles 使用默认磁盘递归列出文件。
func AllFiles(ctx context.Context, dir string) ([]string, error) {
	return Resolve().defaultRepository().AllFiles(ctx, dir)
}

// Directories 使用默认磁盘列出当前层目录。
func Directories(ctx context.Context, dir string) ([]string, error) {
	return Resolve().defaultRepository().Directories(ctx, dir)
}

// AllDirectories 使用默认磁盘递归列出目录。
func AllDirectories(ctx context.Context, dir string) ([]string, error) {
	return Resolve().defaultRepository().AllDirectories(ctx, dir)
}

// URL 使用默认磁盘生成公开访问地址。
func URL(key string) (string, error) {
	return Resolve().defaultRepository().URL(key)
}

// TemporaryURL 使用默认磁盘生成临时签名地址。
func TemporaryURL(ctx context.Context, key string, expiry time.Time) (string, error) {
	return Resolve().defaultRepository().TemporaryURL(ctx, key, expiry)
}

// SetVisibility 使用默认磁盘设置文件可见性。
func SetVisibility(ctx context.Context, key, visibility string) error {
	return Resolve().defaultRepository().SetVisibility(ctx, key, visibility)
}

// GetVisibility 使用默认磁盘获取文件可见性。
func GetVisibility(ctx context.Context, key string) (string, error) {
	return Resolve().defaultRepository().GetVisibility(ctx, key)
}

// ManagerCloseOption 返回文件系统 Manager 的关闭选项，供 bootstrap 注册时使用。
func ManagerCloseOption() containercontract.BindingOption {
	return container.WithCloser(func(m *Manager) error {
		return m.Close()
	})
}
