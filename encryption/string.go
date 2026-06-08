package encryption

import "context"

// EncryptString 加密字符串明文。
//
// ctx 透传给字节级 Encrypt；plaintext 是调用方的原始文本，错误路径不得包含该文本。
func (e *Encrypter) EncryptString(ctx context.Context, plaintext string) (string, error) {
	token, err := e.Encrypt(ctx, []byte(plaintext))
	if err != nil {
		return "", err
	}
	return string(token), nil
}

// DecryptString 解密字符串密文。
//
// ctx 透传给字节级 Decrypt；ciphertext 必须是 EncryptString 返回的 Prismgo envelope。
func (e *Encrypter) DecryptString(ctx context.Context, ciphertext string) (string, error) {
	plain, err := e.Decrypt(ctx, []byte(ciphertext))
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
