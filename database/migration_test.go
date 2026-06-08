package database

import (
	"path/filepath"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func registryAutoMigrationUp(*gorm.DB) error { return nil }

func registryAutoMigrationDown(*gorm.DB) error { return nil }

func registryAutoSeed(*gorm.DB) error { return nil }

func resetMigrationRegistriesForTest() {
	migrationRegistryMu.Lock()
	migrationRegistry = map[string]MigrationEntry{}
	migrationRegistryMu.Unlock()

	seederRegistryMu.Lock()
	seederRegistry = map[string]SeedFunc{}
	seederRegistryMu.Unlock()
}

func TestRegisterMigrationInfersNameFromUpFunctionFile(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	RegisterMigration(registryAutoMigrationUp, registryAutoMigrationDown)

	if _, ok := MigrationByName("migration_test"); !ok {
		t.Fatalf("expected migration migration_test to be registered")
	}
}

func TestRegisterSeederInfersShortAndNamespaceClasses(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	RegisterSeeder(registryAutoSeed)

	if _, ok := SeederByClass("MigrationTest"); !ok {
		t.Fatalf("expected short seeder class MigrationTest to be registered")
	}
	names := SeederClassNames()
	hasNamespaceAlias := false
	for _, name := range names {
		if name != "MigrationTest" && strings.HasSuffix(name, "\\MigrationTest") {
			hasNamespaceAlias = true
			break
		}
	}
	if !hasNamespaceAlias {
		t.Fatalf("expected namespace seeder alias, got %#v", names)
	}
	if err := EnsureSeederRegistered("MigrationTest"); err != nil {
		t.Fatalf("EnsureSeederRegistered registered class failed: %v", err)
	}
	if err := EnsureSeederRegistered("MissingSeeder"); err == nil || !strings.Contains(err.Error(), "MissingSeeder") {
		t.Fatalf("EnsureSeederRegistered missing class error = %v", err)
	}
}

func TestExplicitMigrationAndSeederRegistrationValidation(t *testing.T) {
	resetMigrationRegistriesForTest()
	t.Cleanup(resetMigrationRegistriesForTest)

	up := func(*gorm.DB) error { return nil }
	down := func(*gorm.DB) error { return nil }
	seed := func(*gorm.DB) error { return nil }

	RegisterMigrationAs("  explicit_name  ", up, down)
	if entry, ok := MigrationByName("explicit_name"); !ok || entry.Up == nil || entry.Down == nil {
		t.Fatalf("expected explicit migration to be registered, got %#v ok=%v", entry, ok)
	}
	RegisterSeederAs("  ExplicitSeeder  ", seed)
	if fn, ok := SeederByClass("ExplicitSeeder"); !ok || fn == nil {
		t.Fatalf("expected explicit seeder to be registered, got ok=%v", ok)
	}

	assertStringPanicContains(t, "migration name is empty", func() {
		RegisterMigrationAs(" ", nil, nil)
	})
	assertStringPanicContains(t, "seeder class name is empty", func() {
		RegisterSeederAs(" ", nil)
	})
}

func TestSeederClassNamesFromTimestampedSeederSource(t *testing.T) {
	source := functionSource{
		FilePath:    filepath.Join("repo", "database", "seeders", "202604280001_database_seeder.go"),
		PackagePath: "prismgo/database/seeders",
	}

	names := seederClassNamesFromSource(source)
	want := []string{"DatabaseSeeder", "Prismgo\\Database\\Seeders\\DatabaseSeeder"}
	if strings.Join(names, "|") != strings.Join(want, "|") {
		t.Fatalf("seeder class names = %#v, want %#v", names, want)
	}
}

func TestMigrationNamingHelpersSkipUnusableNames(t *testing.T) {
	if got := migrationNameFromSourceFile("no_extension"); got != "no_extension" {
		t.Fatalf("migrationNameFromSourceFile = %q, want no_extension", got)
	}
	if got := seederClassNamesFromSource(functionSource{FilePath: "202604280001_---.go"}); got != nil {
		t.Fatalf("unusable seeder source names = %#v, want nil", got)
	}
	if got := trimLeadingTimestamp("20260428_bad"); got != "20260428_bad" {
		t.Fatalf("short timestamp trim = %q", got)
	}
	if got := trimLeadingTimestamp("202604280001_database_seeder"); got != "database_seeder" {
		t.Fatalf("timestamp trim = %q", got)
	}
	if got := packagePathFromRuntimeName("plain"); got != "" {
		t.Fatalf("plain runtime package path = %q, want empty", got)
	}
	if got := namespaceFromPackagePath("github.com/example/prismgo/database/seeders"); !strings.Contains(got, "Database\\Seeders") {
		t.Fatalf("namespace = %q, want Database\\Seeders suffix", got)
	}
	if got := uniqueStrings([]string{" A ", "A", "", "B"}); strings.Join(got, "|") != "A|B" {
		t.Fatalf("uniqueStrings = %#v, want A/B", got)
	}
}

func TestAutoRegisterPanicsForNilHandlers(t *testing.T) {
	var up MigrationFunc
	assertPanicContains(t, "handler must be a non-nil function", func() {
		RegisterMigration(up, nil)
	})

	var seed SeedFunc
	assertPanicContains(t, "handler must be a non-nil function", func() {
		RegisterSeeder(seed)
	})
}

func assertPanicContains(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if !strings.Contains(recovered.(error).Error(), want) {
			t.Fatalf("panic = %v, want substring %q", recovered, want)
		}
	}()
	fn()
}

func assertStringPanicContains(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if !strings.Contains(recovered.(string), want) {
			t.Fatalf("panic = %v, want substring %q", recovered, want)
		}
	}()
	fn()
}
