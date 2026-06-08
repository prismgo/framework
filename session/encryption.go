package session

import (
	"context"

	encryptioncontract "github.com/prismgo/framework/contracts/encryption"
)

// Encryptor 是 SESSION_ENCRYPT=true 时使用的 payload 加密扩展点。
//
// 需求背景：session payload 加密统一复用 contracts/encryption.Encrypter，避免 session
// 自己保留一套和 cookie、queue 不一致的加密契约。
type Encryptor = encryptioncontract.Encrypter

// NopEncryptor 是默认无加密实现。
//
// 设计思路：为了满足 SESSION_ENCRYPT=false 的默认语义，NopEncryptor 只复制并返回输入，
// 不共享底层切片，避免后续调用方修改返回值时意外影响原始 payload。
type NopEncryptor struct{}

// Encrypt 在无加密模式下复制 plaintext 并返回。
func (NopEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	return cloneBytes(plaintext), nil
}

// Decrypt 在无加密模式下复制 ciphertext 并返回。
func (NopEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	return cloneBytes(ciphertext), nil
}

// encryptPayload 统一执行 payload 加密和错误脱敏。
//
// 参数 ctx 用于控制加密操作生命周期；参数 encryptor 为空时使用 NopEncryptor；参数
// plaintext 是待持久化的序列化 payload。复杂逻辑集中在这里，保证后续 file driver 不会
// 直接暴露底层加密错误文本。
func encryptPayload(ctx context.Context, encryptor Encryptor, plaintext []byte) ([]byte, error) {
	if encryptor == nil {
		encryptor = NopEncryptor{}
	}
	out, err := encryptor.Encrypt(ctx, plaintext)
	if err != nil {
		return nil, safeError("encrypt payload", ErrEncryptionFailed)
	}
	return out, nil
}

// decryptPayload 统一执行 payload 解密和错误脱敏。
//
// 参数 ctx 用于控制解密操作生命周期；参数 encryptor 为空时使用 NopEncryptor；参数
// ciphertext 是从持久化层读取的原始字节。解密失败统一归类为 ErrDecryptionFailed，
// 不把文件内容、密文或解密后的部分内容放入错误文本。
func decryptPayload(ctx context.Context, encryptor Encryptor, ciphertext []byte) ([]byte, error) {
	if encryptor == nil {
		encryptor = NopEncryptor{}
	}
	out, err := encryptor.Decrypt(ctx, ciphertext)
	if err != nil {
		return nil, safeError("decrypt payload", ErrDecryptionFailed)
	}
	return out, nil
}

// cloneBytes 复制字节切片。
//
// 设计原因：加密扩展点返回的数据会继续传递给序列化或持久化流程，复制可以避免调用方
// 复用输入切片导致的隐式共享和并发修改风险。
func cloneBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}
