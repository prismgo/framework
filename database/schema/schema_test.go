package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type namedDialector struct {
	gorm.Dialector
	name string
}

func (d namedDialector) Name() string { return d.name }

type syncDefaultWidget struct {
	ID        uint
	Name      string
	EventAt   time.Time
	ImageID   string
	ImageType string
	Explicit  string `gorm:"size:77"`
	Typed     string `gorm:"type:text"`
}

func (syncDefaultWidget) TableName() string { return "schema_sync_defaults" }

const schemaFakeMySQLDriverName = "schema_fake_mysql"

var (
	registerSchemaFakeMySQLOnce sync.Once
	schemaFakeMySQLMu           sync.Mutex
	schemaFakeMySQLExecs        []string
)

type schemaFakeMySQLDriver struct{}

func (schemaFakeMySQLDriver) Open(string) (driver.Conn, error) { return schemaFakeMySQLConn{}, nil }

type schemaFakeMySQLConn struct{}

func (schemaFakeMySQLConn) Prepare(string) (driver.Stmt, error) { return nil, nil }
func (schemaFakeMySQLConn) Close() error                        { return nil }
func (schemaFakeMySQLConn) Begin() (driver.Tx, error)           { return schemaFakeMySQLTx{}, nil }

func (schemaFakeMySQLConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	schemaFakeMySQLMu.Lock()
	defer schemaFakeMySQLMu.Unlock()
	schemaFakeMySQLExecs = append(schemaFakeMySQLExecs, query)
	return driver.RowsAffected(1), nil
}

func (schemaFakeMySQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.ToLower(query)
	switch {
	case strings.Contains(normalized, "select database()"):
		return &schemaFakeRows{columns: []string{"database"}, rows: [][]driver.Value{{"prismgo_test"}}}, nil
	case strings.Contains(normalized, "information_schema.schemata"):
		return &schemaFakeRows{columns: []string{"name"}, rows: [][]driver.Value{{"prismgo_test"}}}, nil
	case strings.Contains(normalized, "information_schema.views"):
		return &schemaFakeRows{columns: []string{"name", "schema", "definition"}, rows: nil}, nil
	case strings.Contains(normalized, "information_schema.key_column_usage"):
		return &schemaFakeRows{columns: []string{"name", "column_name", "foreign_table", "foreign_column", "on_update", "on_delete"}, rows: nil}, nil
	case strings.Contains(normalized, "information_schema.statistics"):
		count := int64(0)
		if len(args) > 0 && args[len(args)-1].Value == "schema_index_toggle_name_index" {
			count = 1
		}
		return &schemaFakeRows{columns: []string{"count"}, rows: [][]driver.Value{{count}}}, nil
	case strings.Contains(normalized, "select table_name as name"):
		return &schemaFakeRows{columns: []string{"name", "schema", "type"}, rows: nil}, nil
	case strings.Contains(normalized, "information_schema.tables"):
		return &schemaFakeRows{columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}}, nil
	case strings.Contains(normalized, "information_schema.columns"):
		count := int64(0)
		if len(args) > 0 && (args[len(args)-1].Value == "age" || args[len(args)-1].Value == "username") {
			count = 1
		}
		return &schemaFakeRows{columns: []string{"count"}, rows: [][]driver.Value{{count}}}, nil
	default:
		return &schemaFakeRows{columns: []string{"ok"}, rows: [][]driver.Value{{1}}}, nil
	}
}

type schemaFakeMySQLTx struct{}

func (schemaFakeMySQLTx) Commit() error   { return nil }
func (schemaFakeMySQLTx) Rollback() error { return nil }

type schemaFakeRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r schemaFakeRows) Columns() []string { return r.columns }
func (r schemaFakeRows) Close() error      { return nil }

func (r *schemaFakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func openSchemaFakeMySQL(t *testing.T) *gorm.DB {
	t.Helper()
	registerSchemaFakeMySQLOnce.Do(func() {
		sql.Register(schemaFakeMySQLDriverName, schemaFakeMySQLDriver{})
	})
	schemaFakeMySQLMu.Lock()
	schemaFakeMySQLExecs = nil
	schemaFakeMySQLMu.Unlock()
	sqlDB, err := sql.Open(schemaFakeMySQLDriverName, "")
	if err != nil {
		t.Fatalf("open fake mysql sql db: %v", err)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		DriverName:                schemaFakeMySQLDriverName,
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open fake mysql gorm db: %v", err)
	}
	return db
}

func TestChangeColumnMySQLCompile(t *testing.T) {
	mysqlDB := openSchemaFakeMySQL(t)
	blueprint := NewBlueprint("schema_change_columns", alterTable)
	blueprint.String("age", 16).Nullable().Default("18").Change()
	blueprint.String("username", 64).Unique(false).Change()
	sqls, err := blueprint.Compile(mysqlDB)
	if err != nil {
		t.Fatalf("compile mysql change: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	if !strings.Contains(joined, "ALTER TABLE `schema_change_columns` MODIFY COLUMN `age` varchar(16) NULL DEFAULT '18'") {
		t.Fatalf("expected modify column SQL, got %s", joined)
	}

}

func TestBlueprintCompilesLaravelColumnSurfaceForMySQL(t *testing.T) {
	db := openSchemaFakeMySQL(t)

	blueprint := NewBlueprint("schema_all_columns", createTable)
	blueprint.Id()
	blueprint.TinyIncrements("tiny_id")
	blueprint.SmallIncrements("small_id")
	blueprint.MediumIncrements("medium_id")
	blueprint.Increments("int_id")
	blueprint.BigIncrements("big_id")
	blueprint.Char("code", 16).Charset("utf8mb4").Collation("utf8mb4_unicode_ci")
	blueprint.String("name")
	blueprint.Text("description")
	blueprint.TinyText("tiny_note")
	blueprint.MediumText("medium_note")
	blueprint.LongText("long_note")
	blueprint.Boolean("enabled")
	blueprint.TinyInteger("tiny_count")
	blueprint.SmallInteger("small_count")
	blueprint.MediumInteger("medium_count")
	blueprint.Integer("count")
	blueprint.BigInteger("big_count")
	blueprint.UnsignedDecimal("price", 10, 2)
	blueprint.Float("ratio")
	blueprint.Double("score")
	blueprint.Date("birthday")
	blueprint.DateTime("published_at")
	blueprint.DateTimeTz("published_tz_at")
	blueprint.Time("starts_at")
	blueprint.TimeTz("starts_tz_at")
	blueprint.Timestamp("created_signal").UseCurrent().UseCurrentOnUpdate()
	blueprint.TimestampTz("updated_signal")
	blueprint.Year("year_value")
	blueprint.Binary("blob_value")
	blueprint.Json("metadata")
	blueprint.Jsonb("metadata_b")
	blueprint.Uuid("uuid")
	blueprint.Ulid("ulid")
	blueprint.IpAddress("ip")
	blueprint.MacAddress("mac")
	blueprint.RememberToken()
	blueprint.Enum("status", []string{"draft", "published"}).Default("draft")
	blueprint.Set("flags", []string{"a", "b"}).Nullable()
	blueprint.Geometry("shape")
	blueprint.Geography("geo")
	blueprint.Point("point")
	blueprint.LineString("line")
	blueprint.Polygon("polygon")
	blueprint.Vector("embedding", 3)
	blueprint.ForeignId("user_id").Constrained("users").CascadeOnDelete().CascadeOnUpdate()
	blueprint.Morphs("resource")
	blueprint.NullableMorphs("owner")
	blueprint.Primary("code")
	blueprint.Unique("uuid")
	blueprint.Index("name")
	blueprint.FullText("description")
	blueprint.SpatialIndex("shape")

	sqls, err := blueprint.Compile(db)
	if err != nil {
		t.Fatalf("compile mysql: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	for _, want := range []string{
		"CREATE TABLE `schema_all_columns`",
		"`name` varchar(255)",
		"`price` decimal(10,2) unsigned",
		"DEFAULT CURRENT_TIMESTAMP",
		"enum('draft','published')",
		"CONSTRAINT `schema_all_columns_user_id_foreign`",
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compiled SQL missing %q in %s", want, joined)
		}
	}
}

func TestDefaultOptionsAffectBlueprintAndSyncModelParsing(t *testing.T) {
	oldStringLength, oldTimePrec, oldMorph := defaultStringLength, defaultTimePrec, defaultMorphKeyType
	t.Cleanup(func() {
		defaultStringLength = oldStringLength
		defaultTimePrec = oldTimePrec
		defaultMorphKeyType = oldMorph
	})

	precision := 3
	DefaultStringLength(191)
	DefaultTimePrecision(&precision)
	MorphUsingUuids()

	db := openSchemaFakeMySQL(t)
	mysqlDB := db
	blueprint := NewBlueprint("schema_defaults", createTable)
	blueprint.Id()
	blueprint.String("name")
	blueprint.Char("code")
	blueprint.DateTime("seen_at")
	blueprint.Time("starts_at")
	blueprint.Timestamp("published_at")
	blueprint.Morphs("owner")
	sqls, err := blueprint.Compile(mysqlDB)
	if err != nil {
		t.Fatalf("compile defaults: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	for _, want := range []string{
		"`name` varchar(191)",
		"`code` char(191)",
		"`seen_at` datetime(3)",
		"`starts_at` time(3)",
		"`published_at` timestamp(3)",
		"`owner_id` char(36)",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compiled default SQL missing %q in %s", want, joined)
		}
	}

	MorphUsingUlids()
	stmt, err := parseModel(db, &syncDefaultWidget{})
	if err != nil {
		t.Fatalf("parse sync model: %v", err)
	}
	applyModelDefaults(stmt)
	if got := stmt.Schema.FieldsByDBName["name"].Size; got != 191 {
		t.Fatalf("default string size = %d", got)
	}
	if got := stmt.Schema.FieldsByDBName["event_at"].Precision; got != 3 {
		t.Fatalf("default time precision = %d", got)
	}
	if got := stmt.Schema.FieldsByDBName["image_id"].Size; got != 26 {
		t.Fatalf("default morph id size = %d", got)
	}
	if got := stmt.Schema.FieldsByDBName["explicit"].Size; got != 77 {
		t.Fatalf("explicit size should be preserved, got %d", got)
	}
	if got := stmt.Schema.FieldsByDBName["typed"].Size; got != 0 {
		t.Fatalf("typed string should not receive default size, got %d", got)
	}

	if err := New(db).SyncModels(&syncDefaultWidget{}); err != nil {
		t.Fatalf("sync model with defaults: %v", err)
	}
}

func TestDefaultStringLengthAffectsSyncModelsCreateSQL(t *testing.T) {
	oldStringLength := defaultStringLength
	t.Cleanup(func() { defaultStringLength = oldStringLength })
	DefaultStringLength(191)

	if err := New(openSchemaFakeMySQL(t)).SyncModels(&syncDefaultWidget{}); err != nil {
		t.Fatalf("sync model with fake mysql: %v", err)
	}

	schemaFakeMySQLMu.Lock()
	joined := strings.Join(schemaFakeMySQLExecs, "\n")
	schemaFakeMySQLMu.Unlock()
	if !strings.Contains(joined, "`name` varchar(191)") {
		t.Fatalf("sync model create SQL should use default string length, got %s", joined)
	}
	if !strings.Contains(joined, "`explicit` varchar(77)") {
		t.Fatalf("sync model create SQL should keep explicit size, got %s", joined)
	}
	if !strings.Contains(joined, "`typed` text") {
		t.Fatalf("sync model create SQL should keep explicit type, got %s", joined)
	}
}

func TestUnsupportedDialectAndCallbackError(t *testing.T) {
	db := openSchemaFakeMySQL(t)
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "postgres"}
	builder := New(db)
	if err := builder.Create("schema_unsupported", func(table *Blueprint) { table.Id() }); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}

	controller := &foreignKeyControllerDialector{Dialector: db.Dialector}
	builder = New(&gorm.DB{Config: &gorm.Config{Dialector: controller}})
	wantErr := errors.New("callback failed")
	err := builder.WithoutForeignKeyConstraints(func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func TestSyncModelsDropAndNilBuilderBranches(t *testing.T) {
	if err := (*Builder)(nil).Drop("x"); err == nil || !strings.Contains(err.Error(), "nil builder") {
		t.Fatalf("expected nil builder error, got %v", err)
	}
}

func TestCompileAlterMySQLIndexAndForeignBranches(t *testing.T) {
	db := openSchemaFakeMySQL(t)

	blueprint := NewBlueprint("schema_alter_mysql", alterTable)
	if blueprint.TableName() != "schema_alter_mysql" {
		t.Fatalf("table name mismatch")
	}
	blueprint.String("name", 32).First()
	blueprint.String("email", 64).After("name").Invisible()
	blueprint.IndexNamed("idx_schema_alter_mysql_name", "name")
	blueprint.UniqueNamed("uix_schema_alter_mysql_email", "email")
	blueprint.FullText("name").Name("ft_schema_alter_mysql_name")
	blueprint.SpatialIndex("shape").Name("sp_schema_alter_mysql_shape")
	blueprint.Foreign("user_id").References("id").On("users").NullOnDelete().RestrictOnUpdate()
	blueprint.DropIndex("idx_old")
	blueprint.DropPrimary()
	blueprint.DropForeign("fk_old")
	blueprint.RenameIndex("idx_from", "idx_to")
	blueprint.Raw("ALTER TABLE `schema_alter_mysql` COMMENT = 'patched'")

	sqls, err := blueprint.Compile(db)
	if err != nil {
		t.Fatalf("compile mysql alter: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	for _, want := range []string{
		"ADD COLUMN `name` varchar(32)",
		"ADD INDEX `idx_schema_alter_mysql_name`",
		"ADD UNIQUE INDEX `uix_schema_alter_mysql_email`",
		"ADD FULLTEXT INDEX `ft_schema_alter_mysql_name`",
		"ADD SPATIAL INDEX `sp_schema_alter_mysql_shape`",
		"ADD CONSTRAINT `schema_alter_mysql_user_id_foreign`",
		"DROP PRIMARY KEY",
		"DROP FOREIGN KEY `fk_old`",
		"RENAME INDEX `idx_from` TO `idx_to`",
		"COMMENT = 'patched'",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compiled alter SQL missing %q in %s", want, joined)
		}
	}
}

func TestConnectionAndUnsupportedResolveBranches(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	err := New(nil).Connection("missing").Create("x", func(table *Blueprint) { table.Id() })
	if err == nil {
		t.Fatal("expected missing connection error")
	}
	if got := sqlLiteral(nil); got != "NULL" {
		t.Fatalf("nil literal = %s", got)
	}
	if got := sqlLiteral(false); got != "0" {
		t.Fatalf("false literal = %s", got)
	}
}

func TestUnsupportedMetadata(t *testing.T) {
	db := openSchemaFakeMySQL(t)
	unsupportedDB := db.Session(&gorm.Session{})
	unsupportedDB.Dialector = namedDialector{Dialector: db.Dialector, name: "postgres"}
	unsupported := New(unsupportedDB)
	for name, fn := range map[string]func() error{
		"get schemas": func() error { _, err := unsupported.GetSchemas(); return err },
		"get tables":  func() error { _, err := unsupported.GetTables(nil); return err },
		"get views":   func() error { _, err := unsupported.GetViews(nil); return err },
		"get types":   func() error { _, err := unsupported.GetTypes(nil); return err },
		"foreign":     func() error { _, err := unsupported.GetForeignKeys("x"); return err },
		"extension":   func() error { return unsupported.EnsureExtensionExists("vector") },
	} {
		if err := fn(); !errors.Is(err, ErrUnsupportedFeature) {
			t.Fatalf("%s should be unsupported, got %v", name, err)
		}
	}
	if _, err := unsupported.CreateDatabase("demo"); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("create database should be unsupported, got %v", err)
	}
	if _, err := unsupported.DropDatabaseIfExists("demo"); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("drop database should be unsupported, got %v", err)
	}
	if err := unsupported.EnsureExtensionExists(""); err == nil || !strings.Contains(err.Error(), "extension name") {
		t.Fatalf("expected extension name validation error, got %v", err)
	}
}

func TestMySQLMetadataBranchesCompileAgainstNamedDialect(t *testing.T) {
	builder := New(openSchemaFakeMySQL(t))

	for name, fn := range map[string]func() error{
		"create database": func() error { _, err := builder.CreateDatabase("schema_meta_demo"); return err },
		"drop database":   func() error { _, err := builder.DropDatabaseIfExists("schema_meta_demo"); return err },
		"get schemas":     func() error { _, err := builder.GetSchemas(); return err },
		"get tables":      func() error { _, err := builder.GetTables([]string{"main"}); return err },
		"get views":       func() error { _, err := builder.GetViews("main"); return err },
		"foreign keys":    func() error { _, err := builder.GetForeignKeys("schema_meta_widgets"); return err },
	} {
		if err := fn(); err != nil {
			t.Fatalf("%s error = %v, want nil from built-in MySQL adapter", name, err)
		}
	}
}

func TestNoopAndHelperBranches(t *testing.T) {
	if got := normalizeSchemas(123); got != nil {
		t.Fatalf("unexpected schemas for unsupported filter: %#v", got)
	}
	if got := normalizeSchemas([]string{"a", "b"}); len(got) != 2 {
		t.Fatalf("expected two schemas, got %#v", got)
	}
	if stringSlicesEqual([]string{"a"}, []string{"b"}) {
		t.Fatal("different slices should not match")
	}
	if stringSlicesEqual([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("different length slices should not match")
	}
	if schemaQualifiedName("", "plain") != "plain" {
		t.Fatal("empty schema should not qualify name")
	}
}

func TestValidationResolveAndConstraintBranches(t *testing.T) {
	builder := New()
	if _, err := builder.CreateDatabase(""); err == nil || !strings.Contains(err.Error(), "database name") {
		t.Fatalf("expected create database validation error, got %v", err)
	}
	if _, err := builder.DropDatabaseIfExists(""); err == nil || !strings.Contains(err.Error(), "database name") {
		t.Fatalf("expected drop database validation error, got %v", err)
	}
	errBuilder := New(errorDB(errors.New("boom")))
	for name, fn := range map[string]func() error{
		"schemas":        func() error { _, err := errBuilder.GetSchemas(); return err },
		"tables":         func() error { _, err := errBuilder.GetTables(nil); return err },
		"views":          func() error { _, err := errBuilder.GetViews(nil); return err },
		"types":          func() error { _, err := errBuilder.GetTypes(nil); return err },
		"indexes":        func() error { _, err := errBuilder.GetIndexes("x"); return err },
		"index listing":  func() error { _, err := errBuilder.GetIndexListing("x"); return err },
		"foreign keys":   func() error { _, err := errBuilder.GetForeignKeys("x"); return err },
		"drop all views": func() error { return errBuilder.DropAllViews() },
		"extension":      func() error { return errBuilder.EnsureExtensionExists("vector") },
	} {
		if err := fn(); err == nil {
			t.Fatalf("%s should return resolve error", name)
		}
	}
}

func TestBuilderCloseMethod(t *testing.T) {
	db := openSchemaFakeMySQL(t)
	builder := New(db)

	// Close 方法应该存在且可以调用
	err := builder.Close()
	if err != nil {
		t.Fatalf("Close should not return error for normal builder: %v", err)
	}

	// 对 nil builder 调用 Close 应该返回错误
	var nilBuilder *Builder
	err = nilBuilder.Close()
	if err == nil {
		t.Fatal("Close on nil builder should return error")
	}
}

func TestBlueprintAliasAndModifierCoverage(t *testing.T) {
	mysqlDB := openSchemaFakeMySQL(t)

	blueprint := NewBlueprint("schema_aliases", createTable)
	blueprint.Id("custom_id")
	blueprint.ForeignIdFor("owner_id").Constrained("users", "id").Name("fk_alias_owner").RestrictOnDelete().NullOnUpdate()
	blueprint.NullableTimestamps()
	blueprint.TimestampsTz()
	blueprint.SoftDeletesTz()
	blueprint.String("name").NotNull().StoredAs("LOWER(name)").VirtualAs("LOWER(name)").From(10).Instant().Lock("none")

	sqls, err := blueprint.Compile(mysqlDB)
	if err != nil {
		t.Fatalf("compile aliases: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	for _, want := range []string{"`custom_id` bigint unsigned", "CONSTRAINT `fk_alias_owner`", "ON DELETE RESTRICT", "ON UPDATE SET NULL"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compiled alias SQL missing %q in %s", want, joined)
		}
	}

	drop := NewBlueprint("schema_aliases", alterTable)
	drop.DropColumns([]string{"legacy_a", "legacy_b"})
	drop.DropSoftDeletesTz()
	drop.DropTimestampsTz()
	drop.DropConstrainedForeignId("owner_id")
	drop.DropUnique("uix_old")
	drop.DropFullText("ft_old")
	drop.DropSpatialIndex("sp_old")
	if _, err := drop.Compile(mysqlDB); err != nil {
		t.Fatalf("compile drop aliases: %v", err)
	}
}

func TestCompilerErrorAndHelperBranches(t *testing.T) {
	mysqlDB := openSchemaFakeMySQL(t)

	empty := NewBlueprint("schema_empty", createTable)
	if _, err := empty.Compile(mysqlDB); err == nil || !strings.Contains(err.Error(), "no columns") {
		t.Fatalf("expected no columns error, got %v", err)
	}
	changeMissing := NewBlueprint("schema_missing_change", alterTable)
	changeMissing.String("missing").Change()
	if _, err := changeMissing.Compile(mysqlDB); err == nil || !strings.Contains(err.Error(), "cannot change missing column") {
		t.Fatalf("expected missing change error, got %v", err)
	}
	if got := (&IndexDefinition{kind: "plain", name: "idx_plain", columns: []string{"name"}}).inlineSQL(); !strings.Contains(got, "KEY `idx_plain`") {
		t.Fatalf("expected plain inline index, got %s", got)
	}
	if got := (&IndexDefinition{kind: "index", name: "idx", columns: []string{"name"}}).alterMySQL("schema_indexes"); !strings.Contains(got, "ADD INDEX") {
		t.Fatalf("expected mysql index alter, got %s", got)
	}
	if optionalName("fallback", "") != "fallback" || optionalName("fallback", "value") != "value" {
		t.Fatal("optionalName did not return expected values")
	}
	if sqlLiteral(true) != "1" || sqlLiteral("CURRENT_TIMESTAMP") != "CURRENT_TIMESTAMP" || sqlLiteral("(JSON_OBJECT())") != "(JSON_OBJECT())" || sqlLiteral(7) != "7" {
		t.Fatal("sqlLiteral branch mismatch")
	}
	longName := defaultIndexName(strings.Repeat("a", 80), "index", []string{"column"})
	if len(longName) != 64 {
		t.Fatalf("expected hashed index name length 64, got %d", len(longName))
	}
	if !strings.Contains(longName, "_") {
		t.Fatalf("expected hashed index name suffix, got %s", longName)
	}
	first := defaultIndexName(strings.Repeat("a", 70), "index", []string{"same_prefix_column_alpha"})
	second := defaultIndexName(strings.Repeat("a", 70), "index", []string{"same_prefix_column_bravo"})
	if len(first) > 64 || len(second) > 64 {
		t.Fatalf("hashed index names must fit MySQL limit: %q %q", first, second)
	}
	if first == second {
		t.Fatalf("long index names should keep distinct hash suffixes, got %q", first)
	}
}

func TestUnsupportedConstraintBranches(t *testing.T) {
	db := openSchemaFakeMySQL(t)
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "postgres"}
	builder := New(db)
	if err := builder.EnableForeignKeyConstraints(); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported enable constraints, got %v", err)
	}
	if err := builder.DisableForeignKeyConstraints(); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported disable constraints, got %v", err)
	}
	if got := dialect(nil); got != "" {
		t.Fatalf("nil dialect = %q", got)
	}
	if got := New(errorDB(errors.New("boom"))).HasTable("x"); got {
		t.Fatal("HasTable on error DB should be false")
	}
	if _, err := New(errorDB(errors.New("boom"))).GetColumns("x"); err == nil {
		t.Fatal("expected GetColumns to return resolve error")
	}
}
