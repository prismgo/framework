package cmd

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/support"
)

const appKeyPrefix = "base64:"

// KeyGenerateCommand 生成 Laravel 风格 APP_KEY。
//
// 需求背景：prismgo/encryption 要求 APP_KEY 使用 `base64:` 加 32 字节随机密钥的
// 标准 base64 表达。该命令把原本需要手工执行 OpenSSL 的步骤收口到 Artisan 风格
// 命令中，并保持 APP_PREVIOUS_KEYS 由运维在轮换密钥时手动维护。
type KeyGenerateCommand struct {
	basePath string
}

// NewKeyGenerateCommand 创建 `key:generate` 命令。
//
// basePath 可选；为空时使用当前应用根目录，用于定位 `.env` 文件。
func NewKeyGenerateCommand(basePath ...string) *KeyGenerateCommand {
	root := ""
	if len(basePath) > 0 {
		root = strings.TrimSpace(basePath[0])
	}
	return &KeyGenerateCommand{basePath: root}
}

func (c *KeyGenerateCommand) Definition() *console.Definition {
	definition := console.MustDefinition("key:generate {--show : Display the key instead of modifying files} {--force : Force overwriting an existing APP_KEY}", "Set the application key")
	definition.Examples = []string{
		"go run ./ key:generate",
		"go run ./ key:generate --show",
		"go run ./ key:generate --force",
	}
	return definition
}

func (c *KeyGenerateCommand) Handle(ctx console.CommandContext) error {
	key, err := generateApplicationKey()
	if err != nil {
		return err
	}
	if ctx.Input().OptionBool("show") {
		ctx.IO().Line(key)
		return nil
	}

	envPath := c.envPath()
	if err := writeApplicationKey(envPath, key, ctx.Input().OptionBool("force")); err != nil {
		return err
	}
	ctx.IO().Success("Application key set successfully.")
	return nil
}

func (c *KeyGenerateCommand) envPath() string {
	if c.basePath != "" {
		return filepath.Join(c.basePath, ".env")
	}
	return support.BasePath(".env")
}

// generateApplicationKey 生成 prismgo/encryption 可接受的 APP_KEY。
//
// 设计说明：AES-256-GCM 需要 32 字节 key；这里直接生成 32 个随机字节，再使用
// 标准 base64 编码并加 `base64:` 前缀，与 prismgo/encryption.parseKey 的格式约束一致。
func generateApplicationKey() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("key generate: random bytes: %w", err)
	}
	return appKeyPrefix + base64.StdEncoding.EncodeToString(raw), nil
}

func writeApplicationKey(envPath string, key string, force bool) error {
	contents, err := os.ReadFile(envPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("key generate: .env file not found at %s", envPath)
		}
		return fmt.Errorf("key generate: read .env: %w", err)
	}

	updated, found, err := replaceApplicationKey(string(contents), key, force)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("key generate: APP_KEY entry not found in %s", envPath)
	}
	if err := os.WriteFile(envPath, []byte(updated), 0o644); err != nil {
		return fmt.Errorf("key generate: write .env: %w", err)
	}
	return nil
}

func replaceApplicationKey(contents string, key string, force bool) (string, bool, error) {
	lines := strings.SplitAfter(contents, "\n")
	for i, line := range lines {
		body := strings.TrimSuffix(line, "\n")
		body = strings.TrimSuffix(body, "\r")
		if !strings.HasPrefix(body, "APP_KEY=") {
			continue
		}

		current := strings.TrimSpace(strings.TrimPrefix(body, "APP_KEY="))
		if current != "" && !force {
			return "", true, fmt.Errorf("key generate: APP_KEY already exists; use --force to overwrite")
		}
		lines[i] = "APP_KEY=" + key + lineEnding(line)
		return strings.Join(lines, ""), true, nil
	}
	return contents, false, nil
}

func lineEnding(line string) string {
	if strings.HasSuffix(line, "\r\n") {
		return "\r\n"
	}
	if strings.HasSuffix(line, "\n") {
		return "\n"
	}
	return ""
}
