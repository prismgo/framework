package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/encryption"

	"github.com/spf13/cobra"
)

func TestKeyGenerateShowOutputsPrismgoCompatibleKey(t *testing.T) {
	cmd := NewKeyGenerateCommand(t.TempDir())
	stdout := &bytes.Buffer{}

	if err := cmd.Handle(keyGenerateContext(cmd, fakeInput{bools: map[string]bool{"show": true}}, stdout)); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	key := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(key, "base64:") {
		t.Fatalf("generated key = %q, want base64 prefix", key)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(key, "base64:"))
	if err != nil {
		t.Fatalf("generated key is not standard base64: %v", err)
	}
	if len(raw) != 32 {
		t.Fatalf("generated key raw length = %d, want 32", len(raw))
	}
	if _, err := encryption.New(encryption.Config{Key: key}); err != nil {
		t.Fatalf("generated key should be accepted by prismgo/encryption: %v", err)
	}
}

func TestKeyGenerateWritesEnvAppKeyWhenEmpty(t *testing.T) {
	basePath := t.TempDir()
	envPath := filepath.Join(basePath, ".env")
	if err := os.WriteFile(envPath, []byte("APP_NAME=demo\nAPP_KEY=\nAPP_ENV=local\n"), 0o644); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cmd := NewKeyGenerateCommand(basePath)
	stdout := &bytes.Buffer{}

	if err := cmd.Handle(keyGenerateContext(cmd, fakeInput{}, stdout)); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	contents, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	if !strings.Contains(string(contents), "APP_NAME=demo\nAPP_KEY=base64:") || !strings.Contains(string(contents), "\nAPP_ENV=local\n") {
		t.Fatalf("env was not updated in place: %q", string(contents))
	}
	if !strings.Contains(stdout.String(), "Application key set successfully") {
		t.Fatalf("stdout missing success message: %q", stdout.String())
	}
}

func TestKeyGenerateRefusesToOverwriteExistingKeyWithoutForce(t *testing.T) {
	basePath := t.TempDir()
	envPath := filepath.Join(basePath, ".env")
	if err := os.WriteFile(envPath, []byte("APP_KEY=base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"), 0o644); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cmd := NewKeyGenerateCommand(basePath)
	err := cmd.Handle(keyGenerateContext(cmd, fakeInput{}, &bytes.Buffer{}))
	if err == nil || !strings.Contains(err.Error(), "APP_KEY already exists") {
		t.Fatalf("Handle error = %v, want existing APP_KEY error", err)
	}

	contents, readErr := os.ReadFile(envPath)
	if readErr != nil {
		t.Fatalf("read env: %v", readErr)
	}
	if string(contents) != "APP_KEY=base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n" {
		t.Fatalf("env changed without force: %q", string(contents))
	}
}

func TestKeyGenerateForceOverwritesExistingKey(t *testing.T) {
	basePath := t.TempDir()
	envPath := filepath.Join(basePath, ".env")
	oldKey := "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	if err := os.WriteFile(envPath, []byte("APP_KEY="+oldKey+"\n"), 0o644); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cmd := NewKeyGenerateCommand(basePath)
	if err := cmd.Handle(keyGenerateContext(cmd, fakeInput{bools: map[string]bool{"force": true}}, &bytes.Buffer{})); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	contents, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	updated := strings.TrimPrefix(strings.TrimSpace(string(contents)), "APP_KEY=")
	if updated == oldKey || !strings.HasPrefix(updated, "base64:") {
		t.Fatalf("forced key was not replaced with new base64 key: %q", string(contents))
	}
}

func TestKeyGenerateReturnsClearErrorsForMissingEnvOrAppKeyLine(t *testing.T) {
	t.Run("missing env", func(t *testing.T) {
		cmd := NewKeyGenerateCommand(t.TempDir())
		err := cmd.Handle(keyGenerateContext(cmd, fakeInput{}, &bytes.Buffer{}))
		if err == nil || !strings.Contains(err.Error(), ".env file not found") {
			t.Fatalf("Handle error = %v, want missing env error", err)
		}
	})

	t.Run("missing app key line", func(t *testing.T) {
		basePath := t.TempDir()
		if err := os.WriteFile(filepath.Join(basePath, ".env"), []byte("APP_NAME=demo\n"), 0o644); err != nil {
			t.Fatalf("write env: %v", err)
		}

		cmd := NewKeyGenerateCommand(basePath)
		err := cmd.Handle(keyGenerateContext(cmd, fakeInput{}, &bytes.Buffer{}))
		if err == nil || !strings.Contains(err.Error(), "APP_KEY entry not found") {
			t.Fatalf("Handle error = %v, want missing APP_KEY error", err)
		}
	})
}

func TestReplaceApplicationKeyPreservesLineEndings(t *testing.T) {
	updated, found, err := replaceApplicationKey("APP_NAME=demo\r\nAPP_KEY=\r\n", "base64:test", false)
	if err != nil {
		t.Fatalf("replaceApplicationKey returned error: %v", err)
	}
	if !found {
		t.Fatal("expected APP_KEY to be found")
	}
	if updated != "APP_NAME=demo\r\nAPP_KEY=base64:test\r\n" {
		t.Fatalf("updated env = %q", updated)
	}

	updated, found, err = replaceApplicationKey("APP_KEY=", "base64:test", false)
	if err != nil {
		t.Fatalf("replaceApplicationKey no newline returned error: %v", err)
	}
	if !found || updated != "APP_KEY=base64:test" {
		t.Fatalf("updated no-newline env = %q, found=%v", updated, found)
	}
}

func keyGenerateContext(cmd console.Command, input fakeInput, stdout *bytes.Buffer) console.CommandContext {
	return console.NewCommandContext(
		context.Background(),
		cmd,
		*cmd.Definition(),
		input,
		console.NewIO(strings.NewReader(""), stdout, io.Discard),
		nil,
		&cobra.Command{Use: "key:generate"},
	)
}
