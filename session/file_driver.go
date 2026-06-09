package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/support"
)

const sessionFilePerm os.FileMode = 0o600

// FileDriver 使用本地文件系统持久化 session payload。
//
// 需求背景：本项目要求未显式选择 driver 时默认使用 file-backed session，并支持过期、损坏恢复、
// SESSION_ENCRYPT 和同 session ID 独占锁。
// 设计思路：每个 session ID 对应一个文件，文件内容使用 session Payload Encoding；写入时先写
// 临时文件再 rename，避免半写入记录被后续请求读取。加密开启时对已编码 payload 做字节级包装。
type FileDriver struct {
	root      string
	locks     *fileLockManager
	encrypt   bool
	encryptor Encryptor
	codec     encodingcontract.Codec
}

// NewFileDriver 根据 Config 创建 file driver，并确保 session 目录存在。
//
// 参数 cfg.Files 是 session 文件根目录；cfg.Encoding 为空时最终使用 msgpack，显式 json 时保留
// 旧 JSON 字节形态；cfg.Encrypt/cfg.Encryptor 控制服务端 payload 加密。
func NewFileDriver(cfg Config) (*FileDriver, error) {
	cfg = normalizeConfig(cfg)
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, err
	}
	root := support.StoragePath(cfg.Files)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: session files path is not directory", ErrInvalidConfig)
	}
	locks, err := newFileLockManager(filepath.Join(root, ".locks"))
	if err != nil {
		return nil, err
	}
	return &FileDriver{
		root:      root,
		locks:     locks,
		encrypt:   cfg.Encrypt,
		encryptor: cfg.Encryptor,
		codec:     codec,
	}, nil
}

// Read 读取并反序列化指定 session ID 的 payload。
//
// 参数 id 必须是安全 session ID；文件缺失、过期、损坏或解密失败都会返回可识别错误，
// Manager 会据此恢复为新 session，避免敏感 payload 泄露到业务层。
func (d *FileDriver) Read(ctx context.Context, id string) (Payload, error) {
	if !validSessionID(id) {
		return Payload{}, ErrInvalidSessionID
	}
	raw, err := os.ReadFile(d.pathForID(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Payload{}, ErrSessionNotFound
		}
		return Payload{}, fmt.Errorf("%w: read session file", ErrPayloadDeserialize)
	}
	payload, err := d.decode(ctx, raw)
	if err != nil {
		return Payload{}, err
	}
	if payload.ID != id {
		return Payload{}, ErrPayloadMalformed
	}
	if payload.ExpiresAt != nil && !payload.ExpiresAt.After(time.Now()) {
		_ = d.Destroy(ctx, id)
		return Payload{}, ErrSessionExpired
	}
	return payload, nil
}

// Write 序列化并原子写入 session payload。
//
// 参数 expiresAt 会同步写入 payload.ExpiresAt；payload.Values 必须可由当前 Payload Encoding 序列化。
func (d *FileDriver) Write(ctx context.Context, id string, payload Payload, expiresAt *time.Time) error {
	if !validSessionID(id) || payload.ID != id {
		return ErrInvalidSessionID
	}
	payload.ExpiresAt = expiresAt
	data, err := d.encode(ctx, payload)
	if err != nil {
		return err
	}
	return d.atomicWrite(id, data)
}

// Destroy 删除指定 session 文件；文件不存在视为成功，方便恢复路径重复调用。
func (d *FileDriver) Destroy(_ context.Context, id string) error {
	if !validSessionID(id) {
		return ErrInvalidSessionID
	}
	err := os.Remove(d.pathForID(id))
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// GC 清理 ExpiresAt 不晚于 before 的 session 文件。
//
// 参数 before 是清理阈值；损坏或无法解密的文件也会被清理，避免无效记录长期堆积。
func (d *FileDriver) GC(ctx context.Context, before time.Time) error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id := entry.Name()
		if !validSessionID(id) {
			continue
		}
		payload, err := d.Read(ctx, id)
		if err != nil {
			if recoverableReadError(err) {
				_ = d.Destroy(ctx, id)
				continue
			}
			return err
		}
		if payload.ExpiresAt != nil && !payload.ExpiresAt.After(before) {
			if err := d.Destroy(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// Lock 获取同 session ID 的跨进程文件锁。
func (d *FileDriver) Lock(ctx context.Context, id string, ttl time.Duration, wait time.Duration) (Lock, error) {
	if !validSessionID(id) {
		return nil, ErrInvalidSessionID
	}
	return d.locks.acquire(ctx, id, ttl, wait)
}

func (d *FileDriver) encode(ctx context.Context, payload Payload) ([]byte, error) {
	raw, err := d.codec.Marshal(payload)
	if err != nil {
		return nil, safeError("serialize payload", ErrPayloadSerialize)
	}
	if !d.encrypt {
		return raw, nil
	}
	return encryptPayload(ctx, d.encryptor, raw)
}

func (d *FileDriver) decode(ctx context.Context, raw []byte) (Payload, error) {
	if d.encrypt {
		decrypted, err := decryptPayload(ctx, d.encryptor, raw)
		if err != nil {
			return Payload{}, err
		}
		raw = decrypted
	}
	var payload Payload
	if err := d.codec.Unmarshal(raw, &payload); err != nil {
		return Payload{}, safeError("deserialize payload", ErrPayloadDeserialize)
	}
	if payload.Values == nil {
		payload.Values = make(map[string]any)
	}
	return payload, nil
}

func (d *FileDriver) atomicWrite(id string, data []byte) error {
	temp, err := os.CreateTemp(d.root, ".tmp-"+id+"-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer func() {
		if err := os.Remove(tempName); err != nil && !errors.Is(err, os.ErrNotExist) {
			reportCleanupError(context.Background(), err, "remove_temp_session_file", map[string]any{"path": tempName})
		}
	}()
	if _, err := temp.Write(data); err != nil {
		if closeErr := temp.Close(); closeErr != nil {
			reportCleanupError(context.Background(), closeErr, "close_temp_session_file_after_write_error", map[string]any{"path": tempName})
		}
		return err
	}
	if err := temp.Chmod(sessionFilePerm); err != nil {
		if closeErr := temp.Close(); closeErr != nil {
			reportCleanupError(context.Background(), closeErr, "close_temp_session_file_after_chmod_error", map[string]any{"path": tempName})
		}
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, d.pathForID(id))
}

func (d *FileDriver) pathForID(id string) string {
	return filepath.Join(d.root, id)
}

func init() {
	Extend(DefaultDriver, func(cfg Config) (Driver, error) {
		return NewFileDriver(cfg)
	})
}
