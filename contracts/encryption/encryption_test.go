package encryption

import (
	"context"
	"testing"
)

// fakeByteEncrypter 用于编译期验证字节加密契约的最小方法集。
type fakeByteEncrypter struct{}

func (fakeByteEncrypter) Encrypt(context.Context, []byte) ([]byte, error) { return nil, nil }
func (fakeByteEncrypter) Decrypt(context.Context, []byte) ([]byte, error) { return nil, nil }

// fakeStringEncrypter 用于编译期验证字符串加密契约的最小方法集。
type fakeStringEncrypter struct{}

func (fakeStringEncrypter) EncryptString(context.Context, string) (string, error) { return "", nil }
func (fakeStringEncrypter) DecryptString(context.Context, string) (string, error) { return "", nil }

// TestContractsCompile 确认公开契约只要求调用方实现预期的加密与解密方法。
func TestContractsCompile(t *testing.T) {
	var _ Encrypter = fakeByteEncrypter{}
	var _ StringEncrypter = fakeStringEncrypter{}
}
