package encryption

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	configpkg "github.com/prismgo/framework/config"
)

func init() {
	configpkg.Add("app", func() map[string]any {
		return map[string]any{
			"key":           configpkg.Env("APP_KEY", ""),
			"cipher":        configpkg.Env("APP_CIPHER", CipherAES256GCM),
			"previous_keys": configpkg.Env("APP_PREVIOUS_KEYS", ""),
			"debug":         configpkg.Env("APP_DEBUG", false),
		}
	})
}

// TestNewFromConfigReadsAppEncryptionSettings 验证加密器从 app 配置读取当前 key、算法和历史 key。
// 需求背景：Laravel 风格的默认加密器必须由 APP_KEY / APP_CIPHER / APP_PREVIOUS_KEYS 统一驱动。
func TestNewFromConfigReadsAppEncryptionSettings(t *testing.T) {
	oldEnc, err := New(Config{Key: testKey(1), Cipher: CipherAES256GCM})
	if err != nil {
		t.Fatalf("new old encrypter: %v", err)
	}
	oldToken, err := oldEnc.Encrypt(context.Background(), []byte("rotated-from-env"))
	if err != nil {
		t.Fatalf("encrypt old token: %v", err)
	}

	repo := newAppConfigFromEnv(t, map[string]string{
		"APP_KEY":           testKey(2),
		"APP_CIPHER":        CipherAES256GCM,
		"APP_PREVIOUS_KEYS": "  " + testKey(1) + " , , " + testKey(3) + " ",
	})
	enc, err := NewFromConfig(repo)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	plain, err := enc.Decrypt(context.Background(), oldToken)
	if err != nil {
		t.Fatalf("decrypt old token with previous key: %v", err)
	}
	if string(plain) != "rotated-from-env" {
		t.Fatalf("plain = %q", plain)
	}

	fresh, err := enc.Encrypt(context.Background(), []byte("fresh-from-env"))
	if err != nil {
		t.Fatalf("encrypt fresh token: %v", err)
	}
	if _, err := oldEnc.Decrypt(context.Background(), fresh); err == nil {
		t.Fatal("fresh token should use current APP_KEY, not previous key")
	}
}

// TestSplitPreviousKeysTrimsAndSkipsBlanks 锁定 APP_PREVIOUS_KEYS 的逗号分隔语义。
func TestSplitPreviousKeysTrimsAndSkipsBlanks(t *testing.T) {
	got := splitPreviousKeys("  first , , second,,\tthird  ")
	want := []string{"first", "second", "third"}
	if len(got) != len(want) {
		t.Fatalf("split len = %d, want %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("split[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if got := splitPreviousKeys(" , \t, "); len(got) != 0 {
		t.Fatalf("blank split = %#v, want empty", got)
	}
}

// TestAppConfigDefaults 验证 app 默认配置不会在缺失 APP_KEY 时静默给出可用加密 key。
func TestAppConfigDefaults(t *testing.T) {
	repo := newAppConfigFromEnv(t, nil)
	if got := repo.GetString("app.key"); got != "" {
		t.Fatalf("app.key default = %q, want empty", got)
	}
	if got := repo.GetString("app.cipher"); got != CipherAES256GCM {
		t.Fatalf("app.cipher default = %q, want %q", got, CipherAES256GCM)
	}
	if got := repo.GetString("app.previous_keys"); got != "" {
		t.Fatalf("app.previous_keys default = %q, want empty", got)
	}
}

func newAppConfigFromEnv(t *testing.T, values map[string]string) *configpkg.Config {
	t.Helper()

	envPath := filepath.Join(t.TempDir(), ".env")
	var content string
	for key, value := range values {
		content += key + "=" + value + "\n"
	}
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	repo, err := configpkg.NewFromFile(envPath)
	if err != nil {
		t.Fatalf("load config from env: %v", err)
	}
	return repo
}
