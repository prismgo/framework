package cookie

import (
	"context"

	cookiecontract "github.com/prismgo/framework/contracts/cookie"
	encryptioncontract "github.com/prismgo/framework/contracts/encryption"
)

// Signer 负责 cookie 值的签名和验签。
type Signer = cookiecontract.Signer

// Encryptor 负责 cookie 值传输前后的加密和解密。
//
// 需求背景：cookie 加密现在统一复用 prismgo/contracts/encryption.StringEncrypter，
// 避免 cookie、session、queue 各自维护不同的加密契约。
type Encryptor = encryptioncontract.StringEncrypter

// PassthroughSecurity 是默认透传安全实现。
//
// 设计原因：Phase 2 只建立签名/加密扩展点，具体安全算法可在后续配置阶段接入。默认
// 透传可以让普通 cookie 创建和测试夹具先稳定编译，同时保留替换为真实实现的边界。
// WARNING: do not use in production; replace with real Signer/Encryptor before deployment.
type PassthroughSecurity struct{}

// Sign 在默认模式下直接返回 value。
func (PassthroughSecurity) Sign(_ context.Context, _ string, value string) (string, error) {
	return value, nil
}

// Unsign 在默认模式下直接返回 value。
func (PassthroughSecurity) Unsign(_ context.Context, _ string, value string) (string, error) {
	return value, nil
}

// EncryptString 在默认模式下直接返回 plaintext。
func (PassthroughSecurity) EncryptString(_ context.Context, plaintext string) (string, error) {
	return plaintext, nil
}

// DecryptString 在默认模式下直接返回 ciphertext。
func (PassthroughSecurity) DecryptString(_ context.Context, ciphertext string) (string, error) {
	return ciphertext, nil
}

// secureOutgoing 按“先加密、后签名”的顺序处理即将写出的 cookie。
//
// 参数 ctx 用于安全操作生命周期控制；参数 c 是待写出的 cookie 值对象；signer 和
// encryptor 为空时回退到 PassthroughSecurity。该顺序保证签名覆盖最终传输值，后续
// RequestCookie 可以先验签再解密。
func secureOutgoing(ctx context.Context, c Cookie, signer Signer, encryptor Encryptor) (Cookie, error) {
	if signer == nil {
		signer = PassthroughSecurity{}
	}
	if encryptor == nil {
		encryptor = PassthroughSecurity{}
	}
	value, err := encryptor.EncryptString(ctx, c.Value)
	if err != nil {
		return c, safeError("encrypt value", ErrCookieEncryption)
	}
	value, err = signer.Sign(ctx, c.Name, value)
	if err != nil {
		return c, safeError("sign value", ErrCookieSignature)
	}
	c.Value = value
	return c, nil
}

// secureIncoming 按“先验签、后解密”的顺序读取客户端 cookie 值。
//
// 参数 ctx 用于安全操作生命周期控制；name 和 value 来自请求 cookie；signer 和 encryptor
// 为空时回退到 PassthroughSecurity。错误统一脱敏，避免把篡改后的 cookie 原始值写入日志。
func secureIncoming(ctx context.Context, name string, value string, signer Signer, encryptor Encryptor) (string, error) {
	if signer == nil {
		signer = PassthroughSecurity{}
	}
	if encryptor == nil {
		encryptor = PassthroughSecurity{}
	}
	unsigned, err := signer.Unsign(ctx, name, value)
	if err != nil {
		return "", safeError("verify value", ErrCookieSignature)
	}
	plaintext, err := encryptor.DecryptString(ctx, unsigned)
	if err != nil {
		return "", safeError("decrypt value", ErrCookieDecryption)
	}
	return plaintext, nil
}
