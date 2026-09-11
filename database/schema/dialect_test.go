package schema

import (
	"errors"
	"reflect"
	"testing"

	"gorm.io/gorm"
)

type blueprintCompilerDialector struct {
	gorm.Dialector
	compiled *Blueprint
}

func (d *blueprintCompilerDialector) Name() string { return "custom" }

func (d *blueprintCompilerDialector) CompileBlueprint(_ *gorm.DB, blueprint *Blueprint) ([]string, error) {
	d.compiled = blueprint
	return []string{"CUSTOM BLUEPRINT SQL"}, nil
}

func TestBlueprintCompileUsesDialectorCompiler(t *testing.T) {
	dialector := &blueprintCompilerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}
	blueprint := NewBlueprint("widgets", createTable)

	actual, err := blueprint.Compile(db)
	if err != nil {
		t.Fatalf("Compile() error = %v, want nil", err)
	}
	if dialector.compiled != blueprint {
		t.Fatalf("CompileBlueprint() blueprint = %p, want %p", dialector.compiled, blueprint)
	}
	if len(actual) != 1 || actual[0] != "CUSTOM BLUEPRINT SQL" {
		t.Fatalf("Compile() SQL = %#v, want %#v", actual, []string{"CUSTOM BLUEPRINT SQL"})
	}
}

type namedOnlyDialector struct {
	gorm.Dialector
	name string
}

func (d namedOnlyDialector) Name() string { return d.name }

func TestBlueprintCompileRejectsSQLiteNameWithoutCompiler(t *testing.T) {
	db := &gorm.DB{Config: &gorm.Config{Dialector: namedOnlyDialector{name: "sqlite"}}}
	blueprint := NewBlueprint("widgets", createTable)
	blueprint.Id()

	actual, err := blueprint.Compile(db)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("Compile() = (%#v, %v), want nil SQL and ErrUnsupportedFeature", actual, err)
	}
}

type recordingMigrator struct {
	gorm.Migrator
	tables  []string
	dropped []string
}

func (m *recordingMigrator) GetTables() ([]string, error) {
	return append([]string(nil), m.tables...), nil
}

func (m *recordingMigrator) DropTable(values ...any) error {
	for _, value := range values {
		m.dropped = append(m.dropped, value.(string))
	}
	return nil
}

type migratorOnlyDialector struct {
	gorm.Dialector
	migrator *recordingMigrator
}

func (d *migratorOnlyDialector) Name() string { return "postgres" }

func (d *migratorOnlyDialector) Migrator(*gorm.DB) gorm.Migrator { return d.migrator }

func TestDropAllTablesFallsBackToStandardMigrator(t *testing.T) {
	migrator := &recordingMigrator{tables: []string{"widgets", "users"}}
	dialector := &migratorOnlyDialector{migrator: migrator}
	db := openSchemaFakeMySQL(t)
	db.Dialector = dialector

	err := New(db).DropAllTables()
	if err != nil {
		t.Fatalf("DropAllTables() error = %v, want nil", err)
	}
	want := []string{"widgets", "users"}
	if !reflect.DeepEqual(migrator.dropped, want) {
		t.Fatalf("DropAllTables() dropped = %#v, want %#v", migrator.dropped, want)
	}
}

type schemaListerDialector struct {
	gorm.Dialector
	called bool
}

func (d *schemaListerDialector) Name() string { return "custom" }

func (d *schemaListerDialector) GetSchemas(*gorm.DB) ([]SchemaInfo, error) {
	d.called = true
	return []SchemaInfo{{Name: "adapter_schema"}}, nil
}

func TestBuilderGetSchemasUsesDialectorLister(t *testing.T) {
	dialector := &schemaListerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	actual, err := New(db).GetSchemas()
	if err != nil {
		t.Fatalf("GetSchemas() error = %v, want nil", err)
	}
	if !dialector.called {
		t.Fatal("GetSchemas() did not call the Dialector schema lister")
	}
	want := []SchemaInfo{{Name: "adapter_schema"}}
	if len(actual) != 1 || actual[0] != want[0] {
		t.Fatalf("GetSchemas() = %#v, want %#v", actual, want)
	}
}

type tableListerDialector struct {
	gorm.Dialector
	schemas []string
}

func (d *tableListerDialector) Name() string { return "custom" }

func (d *tableListerDialector) GetTables(_ *gorm.DB, schemas []string) ([]TableInfo, error) {
	d.schemas = schemas
	return []TableInfo{{Name: "widgets", Schema: "tenant", Type: "table"}}, nil
}

func TestBuilderGetTablesUsesDialectorLister(t *testing.T) {
	dialector := &tableListerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	actual, err := New(db).GetTables("tenant")
	if err != nil {
		t.Fatalf("GetTables() error = %v, want nil", err)
	}
	if len(dialector.schemas) != 1 || dialector.schemas[0] != "tenant" {
		t.Fatalf("GetTables() schemas = %#v, want %#v", dialector.schemas, []string{"tenant"})
	}
	want := []TableInfo{{Name: "widgets", Schema: "tenant", Type: "table"}}
	if len(actual) != 1 || actual[0] != want[0] {
		t.Fatalf("GetTables() = %#v, want %#v", actual, want)
	}
}

type viewListerDialector struct {
	gorm.Dialector
	schemas []string
}

func (d *viewListerDialector) Name() string { return "custom" }

func (d *viewListerDialector) GetViews(_ *gorm.DB, schemas []string) ([]ViewInfo, error) {
	d.schemas = schemas
	return []ViewInfo{{Name: "active_widgets", Schema: "tenant", Definition: "SELECT 1"}}, nil
}

func TestBuilderGetViewsUsesDialectorLister(t *testing.T) {
	dialector := &viewListerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	actual, err := New(db).GetViews([]string{"tenant"})
	if err != nil {
		t.Fatalf("GetViews() error = %v, want nil", err)
	}
	if len(dialector.schemas) != 1 || dialector.schemas[0] != "tenant" {
		t.Fatalf("GetViews() schemas = %#v, want %#v", dialector.schemas, []string{"tenant"})
	}
	want := []ViewInfo{{Name: "active_widgets", Schema: "tenant", Definition: "SELECT 1"}}
	if len(actual) != 1 || actual[0] != want[0] {
		t.Fatalf("GetViews() = %#v, want %#v", actual, want)
	}
}

type typeListerDialector struct {
	gorm.Dialector
	filter any
}

func (d *typeListerDialector) Name() string { return "custom" }

func (d *typeListerDialector) GetTypes(_ *gorm.DB, filter any) ([]TypeInfo, error) {
	d.filter = filter
	return []TypeInfo{{Name: "status", Schema: "tenant", Type: "enum"}}, nil
}

func TestBuilderGetTypesUsesDialectorLister(t *testing.T) {
	dialector := &typeListerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	actual, err := New(db).GetTypes("tenant")
	if err != nil {
		t.Fatalf("GetTypes() error = %v, want nil", err)
	}
	if dialector.filter != "tenant" {
		t.Fatalf("GetTypes() filter = %#v, want %q", dialector.filter, "tenant")
	}
	want := []TypeInfo{{Name: "status", Schema: "tenant", Type: "enum"}}
	if len(actual) != 1 || actual[0] != want[0] {
		t.Fatalf("GetTypes() = %#v, want %#v", actual, want)
	}
}

type foreignKeyListerDialector struct {
	gorm.Dialector
	table string
}

func (d *foreignKeyListerDialector) Name() string { return "custom" }

func (d *foreignKeyListerDialector) GetForeignKeys(_ *gorm.DB, table string) ([]ForeignKeyInfo, error) {
	d.table = table
	return []ForeignKeyInfo{{Name: "fk_widgets_owner", Columns: []string{"owner_id"}}}, nil
}

func TestBuilderGetForeignKeysUsesDialectorLister(t *testing.T) {
	dialector := &foreignKeyListerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	actual, err := New(db).GetForeignKeys("widgets")
	if err != nil {
		t.Fatalf("GetForeignKeys() error = %v, want nil", err)
	}
	if dialector.table != "widgets" {
		t.Fatalf("GetForeignKeys() table = %q, want %q", dialector.table, "widgets")
	}
	if len(actual) != 1 || actual[0].Name != "fk_widgets_owner" {
		t.Fatalf("GetForeignKeys() = %#v, want one fk_widgets_owner entry", actual)
	}
}

type foreignKeyControllerDialector struct {
	gorm.Dialector
	events []string
}

func (d *foreignKeyControllerDialector) Name() string { return "custom" }

func (d *foreignKeyControllerDialector) EnableForeignKeyConstraints(*gorm.DB) error {
	d.events = append(d.events, "enable")
	return nil
}

func (d *foreignKeyControllerDialector) DisableForeignKeyConstraints(*gorm.DB) error {
	d.events = append(d.events, "disable")
	return nil
}

func TestBuilderWithoutForeignKeyConstraintsUsesDialectorController(t *testing.T) {
	dialector := &foreignKeyControllerDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}

	err := New(db).WithoutForeignKeyConstraints(func() error {
		dialector.events = append(dialector.events, "callback")
		return nil
	})
	if err != nil {
		t.Fatalf("WithoutForeignKeyConstraints() error = %v, want nil", err)
	}
	want := []string{"disable", "callback", "enable"}
	if !reflect.DeepEqual(dialector.events, want) {
		t.Fatalf("WithoutForeignKeyConstraints() events = %#v, want %#v", dialector.events, want)
	}
}
