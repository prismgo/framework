package schema

import (
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestDefaultOptionBoundaryBranches(t *testing.T) {
	oldStringLength, oldTimePrec, oldMorph := defaultStringLength, defaultTimePrec, defaultMorphKeyType
	t.Cleanup(func() {
		defaultStringLength = oldStringLength
		defaultTimePrec = oldTimePrec
		defaultMorphKeyType = oldMorph
	})

	// Invalid string lengths are ignored so existing application defaults remain stable.
	DefaultStringLength(128)
	DefaultStringLength(0)
	if got := defaultStringLengthValue(); got != 128 {
		t.Fatalf("default string length = %d", got)
	}

	// Nil and negative time precision both reset the optional precision override.
	precision := 6
	DefaultTimePrecision(&precision)
	if got := defaultTimePrecisionValue(); got == nil || *got != 6 {
		t.Fatalf("default time precision = %#v", got)
	}
	DefaultTimePrecision(nil)
	if got := defaultTimePrecisionValue(); got != nil {
		t.Fatalf("nil precision should clear override, got %#v", got)
	}
	negative := -1
	DefaultTimePrecision(&negative)
	if got := defaultTimePrecisionValue(); got != nil {
		t.Fatalf("negative precision should clear override, got %#v", got)
	}

	// Unknown morph key types intentionally fall back to integer keys.
	DefaultMorphKeyType("unexpected")
	if got := defaultMorphKeyTypeValue(); got != "int" {
		t.Fatalf("default morph key type = %q", got)
	}
}

func TestBlueprintCoversMorphKeyAndColumnIndexBranches(t *testing.T) {
	oldMorph := defaultMorphKeyType
	t.Cleanup(func() { defaultMorphKeyType = oldMorph })

	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

	for name, tt := range map[string]struct {
		setup func()
		want  string
	}{
		"integer": {setup: func() { DefaultMorphKeyType("int") }, want: "`owner_id` bigint unsigned"},
		"uuid":    {setup: MorphUsingUuids, want: "`owner_id` char(36)"},
		"ulid":    {setup: MorphUsingUlids, want: "`owner_id` char(26)"},
	} {
		t.Run(name, func(t *testing.T) {
			tt.setup()
			blueprint := NewBlueprint("schema_morph_"+name, createTable)
			blueprint.Id()
			blueprint.Morphs("owner")

			sqls, err := blueprint.Compile(mysqlDB)
			if err != nil {
				t.Fatalf("compile morph blueprint: %v", err)
			}
			if joined := strings.Join(sqls, "\n"); !strings.Contains(joined, tt.want) {
				t.Fatalf("compiled morph SQL missing %q in %s", tt.want, joined)
			}
		})
	}

	// Passing false to Index removes the default column index during change flows.
	if err := New(db).Create("schema_index_toggle", func(table *Blueprint) {
		table.Id()
		table.String("name").Index()
	}); err != nil {
		t.Fatalf("create indexed table: %v", err)
	}
	blueprint := NewBlueprint("schema_index_toggle", alterTable)
	blueprint.String("name").Index(false)
	sqls, err := blueprint.Compile(mysqlDB)
	if err != nil {
		t.Fatalf("compile index toggle: %v", err)
	}
	if joined := strings.Join(sqls, "\n"); !strings.Contains(joined, "DROP INDEX `schema_index_toggle_name_index`") {
		t.Fatalf("expected index drop SQL, got %s", joined)
	}
}

func TestBlueprintCompilesAdditionalIndexAndForeignBranches(t *testing.T) {
	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

	// Composite primary keys are emitted inline for SQLite create statements.
	sqliteBlueprint := NewBlueprint("schema_inline_indexes", createTable)
	sqliteBlueprint.Integer("account_id")
	sqliteBlueprint.Integer("role_id")
	sqliteBlueprint.Primary("account_id", "role_id")
	sqls, err := sqliteBlueprint.Compile(db)
	if err != nil {
		t.Fatalf("compile sqlite composite primary key: %v", err)
	}
	if joined := strings.Join(sqls, "\n"); !strings.Contains(joined, "PRIMARY KEY (`account_id`, `role_id`)") {
		t.Fatalf("expected sqlite inline primary key, got %s", joined)
	}

	// Named foreign keys with explicit actions cover optional SQL branches used by migrations.
	mysqlBlueprint := NewBlueprint("schema_foreign_branches", alterTable)
	mysqlBlueprint.Foreign("owner_id").
		Name("fk_schema_foreign_owner").
		References("uuid").
		On("owners").
		NoActionOnDelete().
		NullOnUpdate()
	mysqlBlueprint.DropForeign("fk_schema_foreign_old")
	sqls, err = mysqlBlueprint.Compile(mysqlDB)
	if err != nil {
		t.Fatalf("compile mysql foreign branches: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	for _, want := range []string{
		"ADD CONSTRAINT `fk_schema_foreign_owner`",
		"REFERENCES `owners` (`uuid`)",
		"ON DELETE NO ACTION",
		"ON UPDATE SET NULL",
		"DROP FOREIGN KEY `fk_schema_foreign_old`",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compiled foreign SQL missing %q in %s", want, joined)
		}
	}
}
