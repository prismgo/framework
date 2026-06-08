package encryption

import "errors"

var (
	// ErrInvalidKey 表示 APP_KEY 或 APP_PREVIOUS_KEYS 为空、格式错误或长度不匹配。
	ErrInvalidKey = errors.New("encryption: invalid application key")

	// ErrUnsupportedCipher 表示配置的 app.cipher 不是当前支持的 AES-256-GCM。
	ErrUnsupportedCipher = errors.New("encryption: unsupported cipher")

	// ErrInvalidPayload 表示密文 envelope 或 base64 payload 格式非法，尚未进入认证解密。
	ErrInvalidPayload = errors.New("encryption: invalid payload")

	// ErrDecrypt 表示密文通过格式检查，但所有 key 都无法完成 AEAD 认证解密。
	ErrDecrypt = errors.New("encryption: decrypt failed")
)
