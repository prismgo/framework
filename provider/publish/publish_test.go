package publish

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegisterBasic(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcDir := filepath.Join(tmp, "src")
	mustMkdir(t, srcDir)
	srcFile := filepath.Join(srcDir, "en.json")
	mustWrite(t, srcFile, `{"hello":"world"}`)

	err := Register("acme", map[string]string{
		srcFile: filepath.Join(tmp, "dst", "en.json"),
	}, "lang")
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	entries := Entries("", nil)
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d, want 1", len(entries))
	}

	e := entries[0]
	if e.Provider != "acme" {
		t.Fatalf("entry.Provider = %q, want %q", e.Provider, "acme")
	}
	if len(e.Tags) != 1 || e.Tags[0] != "lang" {
		t.Fatalf("entry.Tags = %v, want [lang]", e.Tags)
	}
}

func TestRegisterRelativePathResolution(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()

	err := Register("test_provider", map[string]string{
		"testdata": filepath.Join(tmp, "dst", "testdata"),
	}, "config")
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	entries := Entries("", nil)
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d, want 1", len(entries))
	}
	if !filepath.IsAbs(entries[0].Source) {
		t.Fatalf("Source should be absolute path, got %q", entries[0].Source)
	}
}

func TestRegisterAbsolutePath(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "config.go")
	mustWrite(t, srcFile, "package config")

	err := Register("acme", map[string]string{
		srcFile: filepath.Join(tmp, "dst", "config.go"),
	}, "config")
	if err != nil {
		t.Fatalf("Register() with absolute path error = %v", err)
	}

	entries := Entries("", nil)
	if entries[0].Source != srcFile {
		t.Fatalf("Source = %q, want %q", entries[0].Source, srcFile)
	}
}

func TestRegisterEmptyProviderName(t *testing.T) {
	setupTest(t)

	err := Register("", map[string]string{
		"/tmp/source": "/tmp/target",
	}, "lang")
	if err == nil || !strings.Contains(err.Error(), "provider name is required") {
		t.Fatalf("Register with empty name error = %v, want 'provider name is required'", err)
	}
}

func TestRegisterEmptyPaths(t *testing.T) {
	setupTest(t)

	err := Register("acme", map[string]string{}, "lang")
	if err != nil {
		t.Fatalf("Register() with empty paths error = %v", err)
	}

	if len(Entries("", nil)) != 0 {
		t.Fatal("Register with empty paths should register nothing")
	}
}

func TestRegisterProductionSkips(t *testing.T) {
	Clear()
	t.Setenv("APP_ENV", "production")

	err := Register("acme", map[string]string{
		"/tmp/source": "/tmp/target",
	}, "lang")
	if err != nil {
		t.Fatalf("Register() should not error in production, got %v", err)
	}

	if len(Entries("", nil)) != 0 {
		t.Fatal("Register should skip in production, but entries were registered")
	}
}

func TestRegisterMigrationTag(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "2025_01_01_000000_create_users.go")
	mustWrite(t, srcFile, "package migrations")

	err := Register("acme", map[string]string{
		srcFile: filepath.Join(tmp, "dst", "2025_01_01_000000_create_users.go"),
	}, "migrations")
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	entries := Entries("", nil)
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d, want 1", len(entries))
	}
	if !entries[0].IsMigration {
		t.Fatal("entry should have IsMigration=true when tags contain 'migrations'")
	}
}

func TestEntriesFilterByProvider(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "a.txt")
	mustWrite(t, srcFile, "a")

	_ = Register("alpha", map[string]string{srcFile: "/tmp/a.txt"}, "lang")
	_ = Register("beta", map[string]string{srcFile: "/tmp/b.txt"}, "lang")

	alpha := Entries("alpha", nil)
	if len(alpha) != 1 || alpha[0].Provider != "alpha" {
		t.Fatalf("Entries(alpha) len = %d, want 1 alpha entry", len(alpha))
	}

	beta := Entries("beta", nil)
	if len(beta) != 1 || beta[0].Provider != "beta" {
		t.Fatalf("Entries(beta) len = %d, want 1 beta entry", len(beta))
	}
}

func TestEntriesFilterByTag(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("acme", map[string]string{srcFile: "/tmp/x.txt"}, "lang")
	_ = Register("acme", map[string]string{srcFile: "/tmp/y.txt"}, "config")

	langEntries := Entries("", []string{"lang"})
	if len(langEntries) != 1 || langEntries[0].Tags[0] != "lang" {
		t.Fatalf("tag filter lang len = %d, want 1", len(langEntries))
	}

	configEntries := Entries("", []string{"config"})
	if len(configEntries) != 1 || configEntries[0].Tags[0] != "config" {
		t.Fatalf("tag filter config len = %d, want 1", len(configEntries))
	}
}

func TestEntriesFilterByProviderAndTag(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("alpha", map[string]string{srcFile: "/tmp/x.txt"}, "lang")
	_ = Register("beta", map[string]string{srcFile: "/tmp/x.txt"}, "lang")

	result := Entries("alpha", []string{"lang"})
	if len(result) != 1 || result[0].Provider != "alpha" {
		t.Fatalf("provider+tag filter len = %d, want 1 alpha+lang entry", len(result))
	}
}

func TestEntriesAllWithoutFilter(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("alpha", map[string]string{srcFile: "/tmp/x.txt"}, "lang")
	_ = Register("beta", map[string]string{srcFile: "/tmp/y.txt"}, "config")

	if len(Entries("", nil)) != 2 {
		t.Fatalf("unfiltered Entries len = %d, want 2", len(Entries("", nil)))
	}
}

func TestProviders(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("alpha", map[string]string{srcFile: "/tmp/x.txt"}, "lang")
	_ = Register("beta", map[string]string{srcFile: "/tmp/y.txt"}, "config")

	providers := Providers()
	if len(providers) != 2 {
		t.Fatalf("Providers() len = %d, want 2", len(providers))
	}
}

func TestTags(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("alpha", map[string]string{srcFile: "/tmp/x.txt"}, "lang", "config")

	tags := Tags()
	if len(tags) != 2 {
		t.Fatalf("Tags() len = %d, want 2", len(tags))
	}
}

func TestCopySingleFile(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "package config")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 1 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (1, 0)", published, skipped)
	}

	data, _ := os.ReadFile(dstFile)
	if string(data) != "package config" {
		t.Fatalf("copied content = %q, want 'package config'", string(data))
	}
}

func TestCopySkipExisting(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "new content")
	mustWrite(t, dstFile, "old content")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 0 || skipped != 1 {
		t.Fatalf("published = %d, skipped = %d, want (0, 1)", published, skipped)
	}

	data, _ := os.ReadFile(dstFile)
	if string(data) != "old content" {
		t.Fatal("existing file should not be overwritten without force")
	}
}

func TestCopyForceOverwrite(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "new content")
	mustWrite(t, dstFile, "old content")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	published, skipped, err := Copy("", nil, true, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 1 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (1, 0)", published, skipped)
	}

	data, _ := os.ReadFile(dstFile)
	if string(data) != "new content" {
		t.Fatal("existing file should be overwritten with --force")
	}
}

func TestCopyExistingFlag(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()

	srcFile1 := filepath.Join(src, "en.json")
	srcFile2 := filepath.Join(src, "zh_CN.json")
	dstFile1 := filepath.Join(dst, "en.json")
	dstFile2 := filepath.Join(dst, "zh_CN.json")
	mustWrite(t, srcFile1, "version 2")
	mustWrite(t, srcFile2, "版本 2")
	mustWrite(t, dstFile1, "version 1")

	_ = Register("acme", map[string]string{
		srcFile1: dstFile1,
		srcFile2: dstFile2,
	}, "lang")

	published, skipped, err := Copy("", nil, false, true)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 1 || skipped != 1 {
		t.Fatalf("published = %d, skipped = %d, want (1, 1): only existing file copied, missing file skipped", published, skipped)
	}
}

func TestCopyDirectory(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcSub := filepath.Join(src, "lang")
	mustMkdir(t, srcSub)
	mustWrite(t, filepath.Join(srcSub, "en.json"), `{"hello":"world"}`)
	mustWrite(t, filepath.Join(srcSub, "zh_CN.json"), `{"hello":"世界"}`)

	_ = Register("acme", map[string]string{
		srcSub: dst,
	}, "lang")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 2 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (2, 0)", published, skipped)
	}

	if _, err := os.Stat(filepath.Join(dst, "en.json")); err != nil {
		t.Fatalf("en.json not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "zh_CN.json")); err != nil {
		t.Fatalf("zh_CN.json not found: %v", err)
	}
}

func TestCopyDirectorySkippedCount(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcSub := filepath.Join(src, "lang")
	mustMkdir(t, srcSub)
	mustWrite(t, filepath.Join(srcSub, "en.json"), `{"hello":"world"}`)
	mustWrite(t, filepath.Join(srcSub, "already_exists.json"), `{}`)
	mustWrite(t, filepath.Join(dst, "already_exists.json"), `{}`)

	_ = Register("acme", map[string]string{
		srcSub: dst,
	}, "lang")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 1 || skipped != 1 {
		t.Fatalf("published = %d, skipped = %d, want (1, 1): en.json copied, already_exists.json skipped", published, skipped)
	}
}

func TestCopyMigrationDateUpdate(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "2025_01_01_000000_create_users.go")
	dstFile := filepath.Join(dst, "2025_01_01_000000_create_users.go")
	mustWrite(t, srcFile, "package migrations")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "migrations")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if published != 1 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (1, 0)", published, skipped)
	}

	entries, _ := os.ReadDir(dst)
	if len(entries) != 1 {
		t.Fatalf("dst dir has %d files, want 1", len(entries))
	}

	name := entries[0].Name()
	if name == "2025_01_01_000000_create_users.go" {
		t.Fatal("migration file name should have been updated with current timestamp")
	}
	if !strings.Contains(name, "_create_users.go") {
		t.Fatalf("migration file should preserve suffix, got %q", name)
	}
}

func TestCopyNonMigrationFilePreservesName(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "package config")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	_, _, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("non-migration file not found: %v", err)
	}
}

func TestCopyProductionRejected(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	Clear()

	_, _, err := Copy("", nil, false, false)
	if err == nil || !strings.Contains(err.Error(), "production") {
		t.Fatalf("Copy() in production error = %v, want production error", err)
	}
}

func TestDryRunReturnsPlan(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	mustWrite(t, srcFile, "package config")

	_ = Register("acme", map[string]string{
		srcFile: filepath.Join(dst, "config.go"),
	}, "config")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("DryRun() len = %d, want 1", len(items))
	}
	if items[0].Source != srcFile {
		t.Fatalf("DryRun source = %q, want %q", items[0].Source, srcFile)
	}
}

func TestDryRunDoesNotCopy(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "package config")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	_, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() error = %v", err)
	}

	if _, err := os.Stat(dstFile); err == nil {
		t.Fatal("DryRun should not actually copy files")
	}
}

func TestDryRunMarksExisting(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "config.go")
	dstFile := filepath.Join(dst, "config.go")
	mustWrite(t, srcFile, "package config")
	mustWrite(t, dstFile, "old package config")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "config")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() error = %v", err)
	}
	if !items[0].Exists {
		t.Fatal("DryRun item should mark existing target file as Exists=true")
	}
}

func TestDryRunMigrationDatePreview(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcFile := filepath.Join(src, "2025_01_01_000000_create_users.go")
	dstFile := filepath.Join(dst, "2025_01_01_000000_create_users.go")
	mustWrite(t, srcFile, "package migrations")

	_ = Register("acme", map[string]string{
		srcFile: dstFile,
	}, "migrations")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("DryRun() len = %d, want 1", len(items))
	}
	if items[0].Target == dstFile {
		t.Fatal("DryRun should show updated migration date in target path")
	}
	if !items[0].IsMigration {
		t.Fatal("DryRun item should have IsMigration=true")
	}
}

func TestDryRunProductionRejected(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	Clear()

	_, err := DryRun("", nil)
	if err == nil || !strings.Contains(err.Error(), "production") {
		t.Fatalf("DryRun() in production error = %v, want production error", err)
	}
}

func TestIsAvailable(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	if !IsAvailable() {
		t.Fatal("IsAvailable() should be true in local env")
	}

	t.Setenv("APP_ENV", "production")
	if IsAvailable() {
		t.Fatal("IsAvailable() should be false in production")
	}
}

func TestClear(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("acme", map[string]string{srcFile: "/tmp/x.txt"}, "lang")
	if len(Entries("", nil)) != 1 {
		t.Fatal("expected 1 entry before clear")
	}

	Clear()
	if len(Entries("", nil)) != 0 {
		t.Fatal("expected 0 entries after clear")
	}
}

func TestDryRunDirectory(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcSub := filepath.Join(src, "lang")
	mustMkdir(t, srcSub)
	mustWrite(t, filepath.Join(srcSub, "en.json"), `{"hello":"world"}`)
	mustWrite(t, filepath.Join(srcSub, "zh_CN.json"), `{"hello":"世界"}`)
	mustWrite(t, filepath.Join(dst, "en.json"), `{}`)

	_ = Register("acme", map[string]string{
		srcSub: dst,
	}, "lang")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() directory error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("DryRun() directory len = %d, want 2", len(items))
	}

	existingCount := 0
	for _, item := range items {
		if item.Exists {
			existingCount++
		}
	}
	if existingCount != 1 {
		t.Fatalf("DryRun existing count = %d, want 1", existingCount)
	}
}

func TestCopySourceNotExist(t *testing.T) {
	setupTest(t)
	dst := t.TempDir()

	_ = Register("acme", map[string]string{
		"/nonexistent/path/file.go": filepath.Join(dst, "file.go"),
	}, "config")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() with missing source error = %v", err)
	}
	if published != 0 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (0, 0)", published, skipped)
	}
}

func TestCopyDirectorySourceNotExist(t *testing.T) {
	setupTest(t)
	dst := t.TempDir()

	_ = Register("acme", map[string]string{
		"/nonexistent/dir": dst,
	}, "config")

	published, skipped, err := Copy("", nil, false, false)
	if err != nil {
		t.Fatalf("Copy() with missing directory source error = %v", err)
	}
	if published != 0 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (0, 0)", published, skipped)
	}
}

func TestRegisterDuplicateTags(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "a.txt")
	mustWrite(t, srcFile, "a")

	err := Register("acme", map[string]string{
		srcFile: "/tmp/a.txt",
	}, "lang", "lang", "LANG")
	if err != nil {
		t.Fatalf("Register() with duplicate tags error = %v", err)
	}

	entries := Entries("", nil)
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d, want 1", len(entries))
	}
	if len(entries[0].Tags) != 1 {
		t.Fatalf("Tags len = %d, want 1 (duplicates removed)", len(entries[0].Tags))
	}
}

func TestRegisterEmptyTag(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "a.txt")
	mustWrite(t, srcFile, "a")

	err := Register("acme", map[string]string{
		srcFile: "/tmp/a.txt",
	}, "lang", "", "  ")
	if err != nil {
		t.Fatalf("Register() with empty tags error = %v", err)
	}

	entries := Entries("", nil)
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d, want 1", len(entries))
	}
	if len(entries[0].Tags) != 1 {
		t.Fatalf("Tags len = %d, want 1 (empty tags removed)", len(entries[0].Tags))
	}
}

func TestRegisterSkipEmptyPaths(t *testing.T) {
	setupTest(t)

	err := Register("acme", map[string]string{
		"":  "/tmp/target",
		"k": "",
	}, "lang")
	if err != nil {
		t.Fatalf("Register() with empty/zero paths error = %v", err)
	}

	if len(Entries("", nil)) != 0 {
		t.Fatal("entries with empty source or target should be skipped")
	}
}

func TestCopyForceOverwriteDirectory(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcSub := filepath.Join(src, "lang")
	mustMkdir(t, srcSub)
	mustWrite(t, filepath.Join(srcSub, "en.json"), "new content")
	mustWrite(t, filepath.Join(srcSub, "zh_CN.json"), "new 中文")
	mustWrite(t, filepath.Join(dst, "en.json"), "old content")

	_ = Register("acme", map[string]string{
		srcSub: dst,
	}, "lang")

	published, skipped, err := Copy("", nil, true, false)
	if err != nil {
		t.Fatalf("Copy() directory force error = %v", err)
	}
	if published != 2 || skipped != 0 {
		t.Fatalf("published = %d, skipped = %d, want (2, 0)", published, skipped)
	}

	data, _ := os.ReadFile(filepath.Join(dst, "en.json"))
	if string(data) != "new content" {
		t.Fatalf("force should overwrite directory file, got %q", string(data))
	}
}

func TestCopyExistingFlagDirectory(t *testing.T) {
	setupTest(t)
	src := t.TempDir()
	dst := t.TempDir()
	srcSub := filepath.Join(src, "lang")
	mustMkdir(t, srcSub)
	mustWrite(t, filepath.Join(srcSub, "en.json"), "new content")
	mustWrite(t, filepath.Join(srcSub, "zh_CN.json"), "new 中文")
	mustWrite(t, filepath.Join(dst, "en.json"), "old content")

	_ = Register("acme", map[string]string{
		srcSub: dst,
	}, "lang")

	published, skipped, err := Copy("", nil, false, true)
	if err != nil {
		t.Fatalf("Copy() existing flag error = %v", err)
	}
	if published != 1 || skipped != 1 {
		t.Fatalf("published = %d, skipped = %d, want (1, 1)", published, skipped)
	}
}

func TestEntriesHasAnyTagEmptySet(t *testing.T) {
	setupTest(t)
	tmp := t.TempDir()
	srcFile := filepath.Join(tmp, "x.txt")
	mustWrite(t, srcFile, "x")

	_ = Register("acme", map[string]string{srcFile: "/tmp/x.txt"}, "lang")

	result := Entries("", []string{})
	if len(result) != 1 {
		t.Fatalf("empty tag filter should return all, got %d", len(result))
	}
}

func TestDryRunDirectorySourceNotExist(t *testing.T) {
	setupTest(t)
	dst := t.TempDir()

	_ = Register("acme", map[string]string{
		"/nonexistent/dir": dst,
	}, "config")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() with missing directory source error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("DryRun items = %d, want 0", len(items))
	}
}

func TestDryRunFileSourceNotExist(t *testing.T) {
	setupTest(t)
	dst := t.TempDir()

	_ = Register("acme", map[string]string{
		"/nonexistent/file.go": filepath.Join(dst, "file.go"),
	}, "config")

	items, err := DryRun("", nil)
	if err != nil {
		t.Fatalf("DryRun() with missing file source error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("DryRun items = %d, want 0", len(items))
	}
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir parent of %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// setupTest 初始化测试环境：清空注册表并设置为非生产环境。
func setupTest(t *testing.T) {
	t.Helper()
	Clear()
	t.Setenv("APP_ENV", "local")
}
