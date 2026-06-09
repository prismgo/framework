package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/provider/publish"
)

func TestVendorPublishCommandDefinition(t *testing.T) {
	cmd := NewVendorPublishCommand()
	def := cmd.Definition()
	if def.Name != "vendor:publish" {
		t.Fatalf("command name = %q, want vendor:publish", def.Name)
	}
	if len(def.Options) != 6 {
		t.Fatalf("command options = %d, want 6 (provider, tag, force, all, existing, dry-run)", len(def.Options))
	}
}

func TestVendorPublishCommandHiddenInProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	cmd := NewVendorPublishCommand()
	def := cmd.Definition()
	if !def.Hidden {
		t.Fatal("Definition.Hidden should be true in production")
	}
}

func TestVendorPublishCommandVisibleInNonProduction(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	cmd := NewVendorPublishCommand()
	def := cmd.Definition()
	if def.Hidden {
		t.Fatal("Definition.Hidden should be false in non-production")
	}
}

func TestVendorPublishCommandHandleProductionRejected(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	publish.Clear()
	defer publish.Clear()

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err == nil || !strings.Contains(err.Error(), "production") {
		t.Fatalf("Handle() in production error = %v, want production rejection", err)
	}
}

func TestVendorPublishCommandHandleWithProvider(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWriteFile(t, srcFile, "package config")

	_ = publish.Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"provider": "acme"},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("dst file not created: %v", err)
	}
}

func TestVendorPublishCommandHandleWithForce(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "en.json")
	dstFile := filepath.Join(dst, "en.json")
	mustWriteFile(t, srcFile, "version 2")
	mustWriteFile(t, dstFile, "version 1")

	_ = publish.Register("acme", map[string]string{
		srcFile: dstFile,
	}, "lang")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"provider": "acme"},
		bools:   map[string]bool{"force": true},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() with force error = %v", err)
	}

	data, _ := os.ReadFile(dstFile)
	if string(data) != "version 2" {
		t.Fatalf("force should overwrite, got %q", string(data))
	}
}

func TestVendorPublishCommandHandleWithExisting(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "en.json")
	dstFile := filepath.Join(dst, "en.json")
	mustWriteFile(t, srcFile, "version 2")
	mustWriteFile(t, dstFile, "version 1")

	_ = publish.Register("acme", map[string]string{
		srcFile: dstFile,
	}, "lang")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"provider": "acme"},
		bools:   map[string]bool{"existing": true},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() with existing error = %v", err)
	}

	data, _ := os.ReadFile(dstFile)
	if string(data) != "version 2" {
		t.Fatalf("--existing should overwrite already-existing file, got %q", string(data))
	}
}

func TestVendorPublishCommandHandleWithTag(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "en.json")
	dstFile := filepath.Join(dst, "en.json")
	mustWriteFile(t, srcFile, `{"hello":"world"}`)

	_ = publish.Register("acme", map[string]string{
		srcFile: dstFile,
	}, "lang")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"tag": "lang"},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() with tag error = %v", err)
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("file should be published by tag, not found: %v", err)
	}
}

func TestVendorPublishCommandHandleAll(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile1 := filepath.Join(src, "config.go")
	srcFile2 := filepath.Join(src, "en.json")
	dstFile1 := filepath.Join(dst, "config.go")
	dstFile2 := filepath.Join(dst, "en.json")
	mustWriteFile(t, srcFile1, "package config")
	mustWriteFile(t, srcFile2, "{}")

	_ = publish.Register("acme", map[string]string{
		srcFile1: dstFile1,
	}, "config")
	_ = publish.Register("acme", map[string]string{
		srcFile2: dstFile2,
	}, "lang")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		bools: map[string]bool{"all": true},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() --all error = %v", err)
	}

	if _, err := os.Stat(dstFile1); err != nil {
		t.Fatalf("config.go not published: %v", err)
	}
	if _, err := os.Stat(dstFile2); err != nil {
		t.Fatalf("en.json not published: %v", err)
	}
}

func TestVendorPublishCommandHandleDryRun(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	mustWriteFile(t, srcFile, "package config")

	_ = publish.Register("acme", map[string]string{
		srcFile: filepath.Join(dst, "config.go"),
	}, "config")

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"provider": "acme"},
		bools:   map[string]bool{"dry-run": true},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() --dry-run error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(dst, "config.go")); err == nil {
		t.Fatal("--dry-run should not create files")
	}
}

func TestVendorPublishCommandHandleNothingRegistered(t *testing.T) {
	setupCommandConfigContainer(t)
	t.Setenv("APP_ENV", "local")
	publish.Clear()
	defer publish.Clear()

	cmd := NewVendorPublishCommand()
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "vendor:publish"})
	err := cmd.Handle(commandCtx)
	if err != nil {
		t.Fatalf("Handle() with no registrations error = %v", err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir parent of %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
