package encryption

import (
	"context"

	contractsencryption "github.com/prismgo/framework/contracts/encryption"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "encryption.default"

// Resolve 从当前 Application 容器解析默认字节加密契约。
//
// 需求背景：facade 只依赖 contracts/encryption，避免调用方绑定自定义实现时被具体
// *Encrypter 类型限制。
func Resolve() contractsencryption.Encrypter {
	return facade.Resolve[contractsencryption.Encrypter](serviceKey)
}

// ResolveString 从当前 Application 容器解析默认字符串加密契约。
//
// 设计思路：字符串和字节加密共用同一个 service key，具体实现只要满足对应契约即可。
func ResolveString() contractsencryption.StringEncrypter {
	return facade.Resolve[contractsencryption.StringEncrypter](serviceKey)
}

// Encrypt 通过默认加密契约加密字节 payload。
func Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	return Resolve().Encrypt(ctx, plaintext)
}

// Decrypt 通过默认加密契约解密字节 payload。
func Decrypt(ctx context.Context, ciphertext []byte) ([]byte, error) {
	return Resolve().Decrypt(ctx, ciphertext)
}

// EncryptString 通过默认字符串加密契约加密文本。
func EncryptString(ctx context.Context, plaintext string) (string, error) {
	return ResolveString().EncryptString(ctx, plaintext)
}

// DecryptString 通过默认字符串加密契约解密文本。
func DecryptString(ctx context.Context, ciphertext string) (string, error) {
	return ResolveString().DecryptString(ctx, ciphertext)
}
