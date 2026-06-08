package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	contractsencryption "github.com/prismgo/framework/contracts/encryption"
)

const envelopePrefix = "prismgo:v1:"

var (
	_ contractsencryption.Encrypter       = (*Encrypter)(nil)
	_ contractsencryption.StringEncrypter = (*Encrypter)(nil)
)

// Encrypter 是基于 APP_KEY 的 AES-256-GCM 应用级加密器。
//
// current 只用于新密文加密和优先解密；previous 保存轮换前 key，仅用于旧密文解密。
type Encrypter struct {
	current  []byte
	previous [][]byte
}

// New 根据显式 Config 构造应用级加密器。
//
// cfg.Key 是当前应用 key；cfg.PreviousKeys 是解密旧密文的历史 key；cfg.Cipher 为空时
// 使用 AES-256-GCM。构造阶段即校验 key 和算法，避免运行期静默使用弱配置。
func New(cfg Config) (*Encrypter, error) {
	cipherName := strings.TrimSpace(cfg.Cipher)
	if cipherName == "" {
		cipherName = CipherAES256GCM
	}
	if cipherName != CipherAES256GCM {
		return nil, ErrUnsupportedCipher
	}

	current, err := parseKey(cfg.Key)
	if err != nil {
		return nil, err
	}
	previous, err := parseKeys(cfg.PreviousKeys)
	if err != nil {
		return nil, err
	}
	return &Encrypter{current: current, previous: previous}, nil
}

// Encrypt 使用当前 key 加密 plaintext，并返回带版本前缀的 envelope。
//
// ctx 目前仅用于接口一致性和调用方取消语义预留；plaintext 不会被原地修改。
func (e *Encrypter) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	aead, err := newGCM(e.current)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("encryption: generate nonce: %w", err)
	}

	sealed := aead.Seal(nil, nonce, plaintext, nil)
	payload := make([]byte, 0, len(nonce)+len(sealed))
	payload = append(payload, nonce...)
	payload = append(payload, sealed...)

	token := envelopePrefix + base64.StdEncoding.EncodeToString(payload)
	return []byte(token), nil
}

// Decrypt 解密 Prismgo v1 envelope。
//
// ciphertext 必须带 prismgo:v1: 前缀。解密会先尝试 current，再依次尝试 previous keys；
// 所有 key 认证失败时返回 ErrDecrypt，不泄漏密文或明文内容。
func (e *Encrypter) Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	token := string(ciphertext)
	if !strings.HasPrefix(token, envelopePrefix) {
		return nil, ErrInvalidPayload
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, envelopePrefix))
	if err != nil {
		return nil, ErrInvalidPayload
	}

	plain, ok, err := e.openWithKey(e.current, payload)
	if err != nil {
		return nil, err
	}
	if ok {
		return plain, nil
	}
	for _, key := range e.previous {
		plain, ok, err := e.openWithKey(key, payload)
		if err != nil {
			return nil, err
		}
		if ok {
			return plain, nil
		}
	}
	return nil, ErrDecrypt
}

// openWithKey 使用单个 key 尝试打开 payload。
//
// key 是当前或历史 32 字节 AES key；payload 是 nonce||ciphertext||tag。
func (e *Encrypter) openWithKey(key []byte, payload []byte) ([]byte, bool, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, false, err
	}
	nonceSize := aead.NonceSize()
	if len(payload) < nonceSize+aead.Overhead() {
		return nil, false, ErrInvalidPayload
	}

	nonce := payload[:nonceSize]
	sealed := payload[nonceSize:]
	plain, err := aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, false, nil
	}
	return plain, true, nil
}

// newGCM 创建 AES-256-GCM AEAD 实例。
//
// key 必须是 parseKey 校验过的 32 字节 key；保留该 helper 便于后续调用方复用同一
// 算法边界，而不暴露额外配置能力。
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
