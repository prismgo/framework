// Package encryption 定义 Prismgo 应用级加密能力的公共契约。
//
// 需求背景：Laravel 通过 encryption contract 统一表达框架加密依赖。Prismgo
// 需要在不让 cookie、session、queue 直接依赖具体实现包的前提下，共享同一
// APP_KEY / APP_PREVIOUS_KEYS 加密边界。
package encryption

import "context"

// Encrypter 是字节 Payload 的加密契约。
//
// 用途：session、queue 等已经拥有序列化字节的组件通过该接口加密和解密 payload。
type Encrypter interface {
	// Encrypt 加密 plaintext。
	//
	// ctx 用于承载调用链取消信号；plaintext 是待加密的原始字节；返回值是框架
	// 加密后的密文字节。错误不能包含明文内容。
	Encrypt(ctx context.Context, plaintext []byte) ([]byte, error)

	// Decrypt 解密 ciphertext。
	//
	// ctx 用于承载调用链取消信号；ciphertext 是 Encrypt 产出的密文字节；返回值
	// 是解密后的原始字节。认证失败、格式错误或 key 不匹配时返回错误。
	Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error)
}

// StringEncrypter 是字符串 Payload 的加密契约。
//
// 用途：cookie 或业务字符串值通过该接口加密和解密文本。
type StringEncrypter interface {
	// EncryptString 加密 plaintext。
	//
	// ctx 用于承载调用链取消信号；plaintext 是待加密文本；返回值是可安全存储或
	// 传输的密文文本。错误不能包含明文内容。
	EncryptString(ctx context.Context, plaintext string) (string, error)

	// DecryptString 解密 ciphertext。
	//
	// ctx 用于承载调用链取消信号；ciphertext 是 EncryptString 产出的密文文本；
	// 返回值是解密后的原始文本。认证失败、格式错误或 key 不匹配时返回错误。
	DecryptString(ctx context.Context, ciphertext string) (string, error)
}
