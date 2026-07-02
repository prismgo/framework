package session

import "errors"

var (
	// ErrInvalidConfig 表示 session 配置缺失或取值无效，例如 driver 名称为空。
	ErrInvalidConfig = errors.New("session: invalid config")
	// ErrDriverNotFound 表示指定 driver 尚未注册，调用方可据此回退或终止启动。
	ErrDriverNotFound = errors.New("session: driver not found")
	// ErrInvalidSessionID 表示请求携带的 session ID 不符合安全格式要求。
	ErrInvalidSessionID = errors.New("session: invalid id")
	// ErrSessionNotFound 表示持久化层没有找到指定 session 记录。
	ErrSessionNotFound = errors.New("session: not found")
	// ErrSessionExpired 表示 session 已超过服务端有效期。
	ErrSessionExpired = errors.New("session: expired")
	// ErrPayloadMalformed 表示持久化 payload 结构损坏或字段不符合契约。
	ErrPayloadMalformed = errors.New("session: payload malformed")
	// ErrPayloadSerialize 表示写入前序列化失败。
	ErrPayloadSerialize = errors.New("session: payload serialize failed")
	// ErrPayloadDeserialize 表示读取后反序列化失败。
	ErrPayloadDeserialize = errors.New("session: payload deserialize failed")
	// ErrEncryptionFailed 表示服务端 payload 加密失败。
	ErrEncryptionFailed = errors.New("session: encryption failed")
	// ErrDecryptionFailed 表示服务端 payload 解密失败。
	ErrDecryptionFailed = errors.New("session: decryption failed")
	// ErrLockTimeout 表示同 session ID 独占锁在等待时间内未获取成功。
	ErrLockTimeout = errors.New("session: lock wait timeout")
	// ErrLockNotHeld 表示释放或续期锁时当前调用方并未持有该锁。
	ErrLockNotHeld = errors.New("session: lock not held")
	// ErrInvalidValueType 表示 session 值类型不符合操作要求，例如 Increment/Decrement 遇到非整数类型。
	ErrInvalidValueType = errors.New("session: invalid value type")
	// ErrInvalidExpiresAt 表示写入时 expiresAt 为 nil 或过去时间，无法设置有效的过期时间。
	ErrInvalidExpiresAt = errors.New("session: invalid expires at")
)

// SensitiveError 表示涉及敏感 session 内容的失败。
//
// 需求背景：session payload 可能保存登录态、权限或业务临时数据，错误信息不允许把
// payload、文件原文或解密后的内容返回给调用方。
// 设计思路：Error 只输出操作名称，真实底层错误通过 Unwrap 保留给 errors.Is/As 判断，
// 既满足可诊断性，也避免日志和 HTTP 错误响应泄露敏感内容。
type SensitiveError struct {
	// Op 是失败的逻辑操作名称，例如 encrypt payload 或 decrypt payload。
	Op string
	// Err 是可被 errors.Is/As 识别的底层错误哨兵。
	Err error
}

// Error 返回脱敏后的错误文本。
func (e SensitiveError) Error() string {
	if e.Op == "" {
		return "session: sensitive operation failed"
	}
	return "session: " + e.Op + " failed"
}

// Unwrap 返回底层错误，便于调用方用 errors.Is/As 做显式错误处理。
func (e SensitiveError) Unwrap() error {
	return e.Err
}

// safeError 把敏感操作错误统一包装为 SensitiveError。
//
// 参数 op 表示当前失败的逻辑操作；参数 err 表示可识别的错误原因。函数只在 err 非空时
// 包装，便于调用方直接 return safeError(...) 而不额外分支。
func safeError(op string, err error) error {
	if err == nil {
		return nil
	}
	return SensitiveError{Op: op, Err: err}
}
