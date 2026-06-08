package encryption

import (
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	// CipherAES256GCM 是 Prismgo 当前唯一支持的 Go-native 认证加密算法。
	CipherAES256GCM = "AES-256-GCM"

	keyPrefix = "base64:"
	keySize   = 32
)

// Config 描述应用级加密器的显式配置。
//
// Key 是当前 APP_KEY，用于产生新密文；PreviousKeys 是 APP_PREVIOUS_KEYS，
// 仅用于解密旧密文；Cipher 为空时由 New 默认成 AES-256-GCM。
type Config struct {
	Key          string
	PreviousKeys []string
	Cipher       string
}

// parseKey 解析单个 Laravel 风格 base64 应用 key。
//
// value 必须是 base64: 前缀加 32 字节随机 key 的标准 base64 文本。返回错误会包裹
// ErrInvalidKey，且不包含原始 key 文本，避免配置或日志泄漏敏感材料。
func parseKey(value string) ([]byte, error) {
	if value == "" {
		return nil, ErrInvalidKey
	}
	if value != strings.TrimSpace(value) {
		return nil, fmt.Errorf("%w", ErrInvalidKey)
	}

	encoded, ok := strings.CutPrefix(value, keyPrefix)
	if !ok || encoded == "" {
		return nil, fmt.Errorf("%w", ErrInvalidKey)
	}
	if strings.ContainsAny(encoded, " \t\r\n") {
		return nil, fmt.Errorf("%w", ErrInvalidKey)
	}

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w", ErrInvalidKey)
	}
	if len(raw) != keySize {
		return nil, fmt.Errorf("%w", ErrInvalidKey)
	}
	return raw, nil
}

// parseKeys 解析 previous key 列表。
//
// values 来自 APP_PREVIOUS_KEYS 拆分后的多个条目。空白条目会被忽略，非空条目必须
// 与当前 key 使用相同的 base64:32字节格式。
func parseKeys(values []string) ([][]byte, error) {
	keys := make([][]byte, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key, err := parseKey(trimmed)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
