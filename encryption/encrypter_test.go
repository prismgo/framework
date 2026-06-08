package encryption

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// testKey 生成稳定的 32 字节 base64 APP_KEY，方便测试不同 key 的轮换行为。
func testKey(seed byte) string {
	raw := bytes.Repeat([]byte{seed}, 32)
	return "base64:" + base64.StdEncoding.EncodeToString(raw)
}

// TestEncrypterEncryptsDecryptsAndUsesEnvelope 验证新密文必须带 Prismgo 版本 envelope。
func TestEncrypterEncryptsDecryptsAndUsesEnvelope(t *testing.T) {
	enc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new encrypter: %v", err)
	}

	token, err := enc.Encrypt(context.Background(), []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if !strings.HasPrefix(string(token), "prismgo:v1:") {
		t.Fatalf("missing envelope prefix: %q", token)
	}

	plain, err := enc.Decrypt(context.Background(), token)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(plain) != "secret" {
		t.Fatalf("plain = %q", plain)
	}

	secondToken, err := enc.Encrypt(context.Background(), []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt second token: %v", err)
	}
	if string(token) == string(secondToken) {
		t.Fatal("expected random nonce to produce different envelopes")
	}
}

// TestPreviousKeysDecryptOnly 验证 previous keys 只用于旧密文解密，不参与新密文加密。
func TestPreviousKeysDecryptOnly(t *testing.T) {
	oldEnc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("old encrypter: %v", err)
	}
	oldToken, err := oldEnc.Encrypt(context.Background(), []byte("rotated"))
	if err != nil {
		t.Fatalf("encrypt old: %v", err)
	}

	newEnc, err := New(Config{Key: testKey(2), PreviousKeys: []string{testKey(1)}, Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new encrypter: %v", err)
	}
	plain, err := newEnc.Decrypt(context.Background(), oldToken)
	if err != nil {
		t.Fatalf("decrypt old token: %v", err)
	}
	if string(plain) != "rotated" {
		t.Fatalf("plain = %q", plain)
	}

	newToken, err := newEnc.Encrypt(context.Background(), []byte("fresh"))
	if err != nil {
		t.Fatalf("encrypt fresh: %v", err)
	}
	if _, err := oldEnc.Decrypt(context.Background(), newToken); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("old key should not decrypt fresh token, got %v", err)
	}
}

// TestInvalidKeyCipherAndPayloadErrors 覆盖配置错误、未知格式以及篡改密文的脱敏错误类型。
func TestInvalidKeyCipherAndPayloadErrors(t *testing.T) {
	if _, err := New(Config{Key: "", Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := New(Config{Key: " " + testKey(1), Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("leading whitespace key error = %v", err)
	}
	wrappedRaw := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	wrappedKey := "base64:" + wrappedRaw[:8] + "\n" + wrappedRaw[8:]
	if _, err := New(Config{Key: wrappedKey, Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("wrapped base64 key error = %v", err)
	}
	if _, err := New(Config{Key: "plain-key", Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("missing base64 prefix error = %v", err)
	}
	if _, err := New(Config{Key: "base64:not-valid", Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("invalid base64 error = %v", err)
	}
	if _, err := New(Config{Key: "base64:" + base64.StdEncoding.EncodeToString([]byte("short")), Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("short key error = %v", err)
	}
	if _, err := New(Config{Key: testKey(1), PreviousKeys: []string{" ", "plain-key"}, Cipher: CipherAES256GCM}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("invalid previous key error = %v", err)
	}
	if _, err := New(Config{Key: testKey(1), Cipher: "AES-128-CBC"}); !errors.Is(err, ErrUnsupportedCipher) {
		t.Fatalf("unsupported cipher error = %v", err)
	}

	enc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new encrypter: %v", err)
	}
	if _, err := enc.Decrypt(context.Background(), []byte("legacy-token")); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("legacy token error = %v", err)
	}
	if _, err := enc.Decrypt(context.Background(), []byte("prismgo:v1:not-valid")); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("invalid base64 payload error = %v", err)
	}
	shortPayload := "prismgo:v1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0}, 12))
	if _, err := enc.Decrypt(context.Background(), []byte(shortPayload)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("short payload error = %v", err)
	}
	noTagPayload := "prismgo:v1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0}, 13))
	if _, err := enc.Decrypt(context.Background(), []byte(noTagPayload)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("payload without authentication tag error = %v", err)
	}

	token, err := enc.Encrypt(context.Background(), []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	tamperedPayload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(string(token), "prismgo:v1:"))
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	tamperedPayload[len(tamperedPayload)-1] ^= 1
	tamperedToken := []byte("prismgo:v1:" + base64.StdEncoding.EncodeToString(tamperedPayload))
	if _, err := enc.Decrypt(context.Background(), tamperedToken); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("tampered token error = %v", err)
	}
}

// TestErrorsDoNotLeakSensitiveInputs 锁定错误文本脱敏要求，避免把 key、明文或密文写入日志。
func TestErrorsDoNotLeakSensitiveInputs(t *testing.T) {
	secretKey := "plain-secret-key"
	if _, err := New(Config{Key: secretKey, Cipher: CipherAES256GCM}); err == nil || strings.Contains(err.Error(), secretKey) {
		t.Fatalf("invalid key leaked sensitive input: %v", err)
	}

	enc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new encrypter: %v", err)
	}
	legacyToken := "legacy-secret-token"
	if _, err := enc.Decrypt(context.Background(), []byte(legacyToken)); err == nil || strings.Contains(err.Error(), legacyToken) {
		t.Fatalf("invalid payload leaked ciphertext: %v", err)
	}

	token, err := enc.Encrypt(context.Background(), []byte("plaintext-secret"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	tamperedPayload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(string(token), "prismgo:v1:"))
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	tamperedPayload[len(tamperedPayload)-1] ^= 1
	if _, err := enc.Decrypt(context.Background(), []byte("prismgo:v1:"+base64.StdEncoding.EncodeToString(tamperedPayload))); err == nil || strings.Contains(err.Error(), "plaintext-secret") {
		t.Fatalf("decrypt error leaked plaintext: %v", err)
	}
}

// TestDefaultCipherAndCanceledContexts 覆盖默认算法、空 previous key 跳过和 ctx 取消路径。
func TestDefaultCipherAndCanceledContexts(t *testing.T) {
	enc, err := New(Config{Key: testKey(1), PreviousKeys: []string{" ", ""}})
	if err != nil {
		t.Fatalf("new default encrypter: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := enc.Encrypt(ctx, []byte("secret")); !errors.Is(err, context.Canceled) {
		t.Fatalf("encrypt canceled error = %v", err)
	}
	if _, err := enc.Decrypt(ctx, []byte("prismgo:v1:any")); !errors.Is(err, context.Canceled) {
		t.Fatalf("decrypt canceled error = %v", err)
	}
	if _, err := enc.EncryptString(ctx, "secret"); !errors.Is(err, context.Canceled) {
		t.Fatalf("encrypt string canceled error = %v", err)
	}
	if _, err := enc.DecryptString(context.Background(), "legacy-token"); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("decrypt string invalid payload error = %v", err)
	}
}

// TestNewGCMRejectsInvalidKeyLength 验证底层 AEAD helper 不接受非 32 字节 key。
func TestNewGCMRejectsInvalidKeyLength(t *testing.T) {
	if _, err := newGCM([]byte("short")); err == nil {
		t.Fatal("expected invalid key length error")
	}
}

// TestStringEncrypter 验证字符串接口只是字节接口的安全薄封装，便于 cookie 等调用方复用。
func TestStringEncrypter(t *testing.T) {
	enc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new encrypter: %v", err)
	}

	token, err := enc.EncryptString(context.Background(), "hello")
	if err != nil {
		t.Fatalf("encrypt string: %v", err)
	}
	plain, err := enc.DecryptString(context.Background(), token)
	if err != nil {
		t.Fatalf("decrypt string: %v", err)
	}
	if plain != "hello" {
		t.Fatalf("plain = %q", plain)
	}
}
