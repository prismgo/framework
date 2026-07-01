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

	"github.com/glebarez/sqlite"
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

type syncBaseWidget struct {
	ID uint
}

func (syncBaseWidget) TableName() string { return "schema_sync_widgets" }

type syncExpandedWidget struct {
	ID   uint
	Code string
}

func (syncExpandedWidget) TableName() string { return "schema_sync_widgets" }

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

func (schemaFakeMySQLConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.ToLower(query)
	switch {
	case strings.Contains(normalized, "select database()"):
		return &schemaFakeRows{columns: []string{"database"}, rows: [][]driver.Value{{"prismgo_test"}}}, nil
	case strings.Contains(normalized, "information_schema.tables"):
		return &schemaFakeRows{columns: []string{"count"}, rows: [][]driver.Value{{int64(0)}}}, nil
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

func openSQLite(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
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

func TestCreateTableAndInspectSQLite(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)

	err := builder.Create("schema_widgets", func(table *Blueprint) {
		table.Id()
		table.String("name", 64).Default("guest").Comment("display name")
		table.Boolean("enabled").Default(true)
		table.Decimal("amount", 10, 2).Unsigned().Nullable()
		table.Json("payload").Nullable()
		table.Timestamps()
		table.SoftDeletes()
		table.UniqueNamed("uix_schema_widgets_name", "name")
		table.IndexNamed("idx_schema_widgets_enabled", "enabled")
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if !builder.HasTable("schema_widgets") {
		t.Fatal("expected schema_widgets table")
	}
	if !builder.HasColumn("schema_widgets", "name") {
		t.Fatal("expected name column")
	}
	if !builder.HasIndex("schema_widgets", "uix_schema_widgets_name") {
		t.Fatal("expected unique index")
	}

	if err := builder.Create("schema_widgets", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("second create should be idempotent: %v", err)
	}
}

func TestAlterTableSQLiteIsIdempotent(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_alters", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("create table: %v", err)
	}
	addColumn := func() error {
		return builder.Table("schema_alters", func(table *Blueprint) {
			table.String("code", 32).Nullable()
			table.IndexNamed("idx_schema_alters_code", "code")
		})
	}
	if err := addColumn(); err != nil {
		t.Fatalf("alter add column: %v", err)
	}
	if err := addColumn(); err != nil {
		t.Fatalf("alter add column twice: %v", err)
	}
	if !builder.HasColumn("schema_alters", "code") {
		t.Fatal("expected code column")
	}
}

func TestRenameDropAndColumnIndexSQLite(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_column_changes", func(table *Blueprint) {
		table.Id()
		table.String("username").Index()
		table.String("age", 8).Nullable()
		table.String("remember_token").Nullable()
		table.Timestamps()
		table.SoftDeletes()
		table.Morphs("owner")
		table.ForeignId("user_id")
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if !builder.HasIndex("schema_column_changes", "schema_column_changes_username_index") {
		t.Fatal("expected column-level index to be created")
	}
	if err := builder.Table("schema_column_changes", func(table *Blueprint) {
		table.RenameColumn("username", "name")
		table.DropColumn("age")
		table.DropRememberToken()
		table.DropTimestamps()
		table.DropSoftDeletes()
		table.DropMorphs("owner")
		table.DropForeignIdFor("user_id")
	}); err != nil {
		t.Fatalf("rename/drop columns: %v", err)
	}
	if !builder.HasColumn("schema_column_changes", "name") {
		t.Fatal("expected renamed name column")
	}
	for _, column := range []string{"username", "age", "remember_token", "created_at", "updated_at", "deleted_at", "owner_id", "owner_type", "user_id"} {
		if builder.HasColumn("schema_column_changes", column) {
			t.Fatalf("expected %s to be dropped", column)
		}
	}
}

func TestChangeColumnMySQLCompileAndSQLiteUnsupported(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_change_columns", func(table *Blueprint) {
		table.Id()
		table.String("age", 8)
		table.String("username", 32)
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}
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

	err = builder.Table("schema_change_columns", func(table *Blueprint) {
		table.String("age", 16).Change()
	})
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("sqlite change should be unsupported, got %v", err)
	}
}

func TestRenameDropAndForeignKeyTogglesSQLite(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_renames", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := builder.Rename("schema_renames", "schema_renamed"); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	if !builder.HasTable("schema_renamed") {
		t.Fatal("expected renamed table")
	}
	if err := builder.DisableForeignKeyConstraints(); err != nil {
		t.Fatalf("disable constraints: %v", err)
	}
	if err := builder.EnableForeignKeyConstraints(); err != nil {
		t.Fatalf("enable constraints: %v", err)
	}
	if err := builder.WithoutForeignKeyConstraints(func() error { return nil }); err != nil {
		t.Fatalf("without constraints: %v", err)
	}
	if err := builder.DropIfExists("schema_renamed"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if builder.HasTable("schema_renamed") {
		t.Fatal("expected table dropped")
	}
}

func TestBlueprintCompilesLaravelColumnSurfaceForMySQL(t *testing.T) {
	db := openSQLite(t)
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

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

	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}
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
	db := openSQLite(t)
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "postgres"}
	builder := New(db)
	if err := builder.Create("schema_unsupported", func(table *Blueprint) { table.Id() }); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}

	db = openSQLite(t)
	builder = New(db)
	wantErr := errors.New("callback failed")
	err := builder.WithoutForeignKeyConstraints(func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func TestSyncModelsDropAndNilBuilderBranches(t *testing.T) {
	type syncWidget struct {
		ID   uint
		Code string
	}
	db := openSQLite(t)
	builder := New(db)
	if err := builder.SyncModels(&syncWidget{}); err != nil {
		t.Fatalf("sync model: %v", err)
	}
	if !builder.HasTable("sync_widgets") {
		t.Fatal("expected sync_widgets table")
	}
	if err := builder.Drop("sync_widgets"); err != nil {
		t.Fatalf("drop synced table: %v", err)
	}
	if err := builder.Drop("sync_widgets"); err != nil {
		t.Fatalf("drop missing table should be no-op: %v", err)
	}
	if err := (*Builder)(nil).Drop("x"); err == nil || !strings.Contains(err.Error(), "nil builder") {
		t.Fatalf("expected nil builder error, got %v", err)
	}
}

func TestCompileAlterMySQLIndexAndForeignBranches(t *testing.T) {
	db := openSQLite(t)
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

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

func TestCompileSQLiteAlterDropAndRenameIndexBranches(t *testing.T) {
	db := openSQLite(t)
	blueprint := NewBlueprint("schema_alter_sqlite", alterTable)
	blueprint.String("name").Nullable()
	blueprint.IndexNamed("idx_schema_alter_sqlite_name", "name")
	blueprint.DropIndex("idx_missing")
	blueprint.RenameIndex("idx_from", "idx_to")
	sqls, err := blueprint.Compile(db)
	if err != nil {
		t.Fatalf("compile sqlite alter: %v", err)
	}
	joined := strings.Join(sqls, "\n")
	if !strings.Contains(joined, "ADD COLUMN `name` text") {
		t.Fatalf("expected add column SQL, got %s", joined)
	}
	if !strings.Contains(joined, "CREATE INDEX IF NOT EXISTS `idx_schema_alter_sqlite_name`") {
		t.Fatalf("expected create index SQL, got %s", joined)
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

func TestFacadeFunctionsDelegateToDefaultBuilder(t *testing.T) {
	db := openSQLite(t)
	registry := useIsolatedFacadeRegistry(t)

	builder := New(db)
	if err := registry.Instance(serviceKey, builder); err != nil {
		t.Fatalf("bind facade builder: %v", err)
	}
	if Resolve() != builder {
		t.Fatal("expected Resolve to return the configured builder")
	}
	if err := Create("schema_facades", func(table *Blueprint) {
		table.Id()
		table.String("name").Nullable()
	}); err != nil {
		t.Fatalf("facade create: %v", err)
	}
	if !HasTable("schema_facades") || !HasColumn("schema_facades", "name") {
		t.Fatal("expected facade-created table and column")
	}
	if err := Table("schema_facades", func(table *Blueprint) {
		table.String("code").Nullable()
		table.IndexNamed("idx_schema_facades_code", "code")
	}); err != nil {
		t.Fatalf("facade table: %v", err)
	}
	if !HasIndex("schema_facades", "idx_schema_facades_code") {
		t.Fatal("expected facade-created index")
	}
	if err := Rename("schema_facades", "schema_facades_renamed"); err != nil {
		t.Fatalf("facade rename: %v", err)
	}
	if err := DisableForeignKeyConstraints(); err != nil {
		t.Fatalf("facade disable constraints: %v", err)
	}
	if err := EnableForeignKeyConstraints(); err != nil {
		t.Fatalf("facade enable constraints: %v", err)
	}
	if err := WithoutForeignKeyConstraints(func() error { return nil }); err != nil {
		t.Fatalf("facade without constraints: %v", err)
	}
	if err := DropIfExists("schema_facades_renamed"); err != nil {
		t.Fatalf("facade drop if exists: %v", err)
	}
	if err := Drop("schema_facades_renamed"); err != nil {
		t.Fatalf("facade drop missing: %v", err)
	}
}

func TestFacadeInspectionAndConditionalHelpers(t *testing.T) {
	db := openSQLite(t)
	registry := useIsolatedFacadeRegistry(t)
	if err := registry.Instance(serviceKey, New(db)); err != nil {
		t.Fatalf("bind facade builder: %v", err)
	}

	if err := Create("schema_facade_inspections", func(table *Blueprint) {
		table.Id()
		table.String("name")
	}); err != nil {
		t.Fatalf("create facade inspection table: %v", err)
	}
	ran := false
	if err := WhenTableHasColumn("schema_facade_inspections", "name", func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("expected WhenTableHasColumn to run, ran=%v err=%v", ran, err)
	}
	ran = false
	if err := WhenTableDoesntHaveColumn("schema_facade_inspections", "missing", func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("expected WhenTableDoesntHaveColumn to run, ran=%v err=%v", ran, err)
	}
	columns, err := GetColumns("schema_facade_inspections")
	if err != nil || len(columns) == 0 {
		t.Fatalf("expected columns, got %#v err=%v", columns, err)
	}
	listing, err := GetColumnListing("schema_facade_inspections")
	if err != nil || len(listing) != len(columns) {
		t.Fatalf("expected column listing, got %#v err=%v", listing, err)
	}
	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("Connection without config facade did not panic")
			}
			if got := strings.TrimSpace(recovered.(error).Error()); got != `container "config.default": container factory is not registered` {
				t.Fatalf("panic = %q, want config.default not registered", got)
			}
		}()
		_ = Connection("mysql")
	}()
}

func TestPackageFacadeFullSurfaceAndUseRegistersBuilder(t *testing.T) {
	db := openSQLite(t)
	registry := useIsolatedFacadeRegistry(t)

	builder := New(db)
	if err := registry.Instance(serviceKey, builder); err != nil {
		t.Fatalf("bind builder: %v", err)
	}
	if Resolve() != builder {
		t.Fatal("Resolve should return the provided builder")
	}
	bound := Bind(db)
	if bound == builder || bound.db != db {
		t.Fatal("Bind should return a local builder bound to the provided connection")
	}
	if Resolve() != builder {
		t.Fatal("Bind should not replace the current facade builder")
	}
	if Resolve() == nil {
		t.Fatal("resolve facade builder returned nil")
	}
	if err := Create("schema_facade_surface", func(table *Blueprint) {
		table.Id()
		table.String("name")
		table.String("code").Nullable()
		table.IndexNamed("idx_schema_facade_surface_code", "code")
	}); err != nil {
		t.Fatalf("facade create surface table: %v", err)
	}
	if err := db.Exec("CREATE VIEW schema_facade_surface_view AS SELECT id FROM schema_facade_surface").Error; err != nil {
		t.Fatalf("create facade surface view: %v", err)
	}
	if _, err := GetSchemas(); err != nil {
		t.Fatalf("facade get schemas: %v", err)
	}
	if _, err := GetTables(nil); err != nil {
		t.Fatalf("facade get tables: %v", err)
	}
	if _, err := GetTableListing(nil, false); err != nil {
		t.Fatalf("facade get table listing: %v", err)
	}
	if !HasTable("schema_facade_surface") || !HasView("schema_facade_surface_view") {
		t.Fatal("facade table/view lookup failed")
	}
	if _, err := GetViews(nil); err != nil {
		t.Fatalf("facade get views: %v", err)
	}
	if _, err := GetTypes(nil); err != nil {
		t.Fatalf("facade get types: %v", err)
	}
	if !HasColumns("schema_facade_surface", []string{"id", "name"}) {
		t.Fatal("facade has columns failed")
	}
	if _, err := GetColumnType("schema_facade_surface", "name", true); err != nil {
		t.Fatalf("facade get column type: %v", err)
	}
	if _, err := GetIndexes("schema_facade_surface"); err != nil {
		t.Fatalf("facade get indexes: %v", err)
	}
	if _, err := GetIndexListing("schema_facade_surface"); err != nil {
		t.Fatalf("facade get index listing: %v", err)
	}
	if !HasIndex("schema_facade_surface", []any{"code"}) {
		t.Fatal("facade has index by []any failed")
	}
	ran := false
	if err := WhenTableHasIndex("schema_facade_surface", "idx_schema_facade_surface_code", func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("facade WhenTableHasIndex failed, ran=%v err=%v", ran, err)
	}
	ran = false
	if err := WhenTableDoesntHaveIndex("schema_facade_surface", "idx_missing_surface", func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("facade WhenTableDoesntHaveIndex failed, ran=%v err=%v", ran, err)
	}
	if _, err := GetForeignKeys("schema_facade_surface"); err != nil {
		t.Fatalf("facade get foreign keys: %v", err)
	}
	if err := EnsureVectorExtensionExists(); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("facade vector extension should be unsupported, got %v", err)
	}
	if err := EnsureExtensionExists("vector"); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("facade extension should be unsupported, got %v", err)
	}
	if err := Table("schema_facade_surface", func(table *Blueprint) {
		table.DropIndex("idx_schema_facade_surface_code")
	}); err != nil {
		t.Fatalf("facade drop code index: %v", err)
	}
	if err := DropColumns("schema_facade_surface", "code"); err != nil {
		t.Fatalf("facade drop columns: %v", err)
	}
	if HasColumn("schema_facade_surface", "code") {
		t.Fatal("expected facade drop columns to remove code")
	}
	if err := DropAllViews(); err != nil {
		t.Fatalf("facade drop all views: %v", err)
	}
	if err := DropAllTypes(); err != nil {
		t.Fatalf("facade drop all types: %v", err)
	}
	if err := DropAllTables(); err != nil {
		t.Fatalf("facade drop all tables: %v", err)
	}
}

func TestSQLiteMetadataInspectionHelpers(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_meta_widgets", func(table *Blueprint) {
		table.Id()
		table.String("name", 64)
		table.String("code", 32).Nullable()
		table.UniqueNamed("uix_schema_meta_widgets_name", "name")
		table.IndexNamed("idx_schema_meta_widgets_code", "code")
	}); err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	if err := db.Exec("CREATE VIEW schema_meta_widget_view AS SELECT id, name FROM schema_meta_widgets").Error; err != nil {
		t.Fatalf("create view: %v", err)
	}
	if err := db.Exec("CREATE TABLE schema_meta_parents (id integer primary key)").Error; err != nil {
		t.Fatalf("create parent table: %v", err)
	}
	if err := db.Exec("CREATE TABLE schema_meta_children (id integer primary key, parent_id integer, CONSTRAINT fk_schema_meta_parent FOREIGN KEY(parent_id) REFERENCES schema_meta_parents(id) ON DELETE CASCADE ON UPDATE NO ACTION)").Error; err != nil {
		t.Fatalf("create child table: %v", err)
	}

	schemas, err := builder.GetSchemas()
	if err != nil || len(schemas) == 0 {
		t.Fatalf("expected sqlite schemas, got %#v err=%v", schemas, err)
	}
	tables, err := builder.GetTables(nil)
	if err != nil || !tableInfoContains(tables, "schema_meta_widgets") {
		t.Fatalf("expected metadata table, got %#v err=%v", tables, err)
	}
	listing, err := builder.GetTableListing(nil, false)
	if err != nil || !stringContains(listing, "schema_meta_widgets") {
		t.Fatalf("expected table listing, got %#v err=%v", listing, err)
	}
	qualified, err := builder.GetTableListing(nil)
	if err != nil || !stringContains(qualified, "main.schema_meta_widgets") {
		t.Fatalf("expected qualified table listing, got %#v err=%v", qualified, err)
	}
	if !builder.HasView("schema_meta_widget_view") || !builder.HasView("main.schema_meta_widget_view") {
		t.Fatal("expected view lookup to support plain and schema-qualified names")
	}
	views, err := builder.GetViews(nil)
	if err != nil || !viewInfoContains(views, "schema_meta_widget_view") {
		t.Fatalf("expected view listing, got %#v err=%v", views, err)
	}
	types, err := builder.GetTypes(nil)
	if err != nil || len(types) != 0 {
		t.Fatalf("expected empty sqlite types, got %#v err=%v", types, err)
	}
	columns, err := builder.GetColumns("schema_meta_widgets")
	if err != nil || !columnInfoContains(columns, "name") {
		t.Fatalf("expected column details, got %#v err=%v", columns, err)
	}
	columnType, err := builder.GetColumnType("schema_meta_widgets", "name")
	if err != nil || columnType == "" {
		t.Fatalf("expected column type, got %q err=%v", columnType, err)
	}
	if _, err := builder.GetColumnType("schema_meta_widgets", "missing"); err == nil {
		t.Fatal("expected missing column type error")
	}
	if !builder.HasColumns("schema_meta_widgets", []string{"id", "name"}) {
		t.Fatal("expected HasColumns to pass")
	}
	if builder.HasColumns("schema_meta_widgets", []string{"id", "missing"}) {
		t.Fatal("expected HasColumns to fail for missing column")
	}
	indexes, err := builder.GetIndexes("schema_meta_widgets")
	if err != nil || !indexInfoContains(indexes, "uix_schema_meta_widgets_name") {
		t.Fatalf("expected indexes, got %#v err=%v", indexes, err)
	}
	indexListing, err := builder.GetIndexListing("schema_meta_widgets")
	if err != nil || !stringContains(indexListing, "idx_schema_meta_widgets_code") {
		t.Fatalf("expected index listing, got %#v err=%v", indexListing, err)
	}
	if !builder.HasIndex("schema_meta_widgets", "uix_schema_meta_widgets_name", "unique") {
		t.Fatal("expected unique index by name")
	}
	if !builder.HasIndex("schema_meta_widgets", []string{"code"}) {
		t.Fatal("expected index by column list")
	}
	ran := false
	if err := builder.WhenTableHasIndex("schema_meta_widgets", []string{"code"}, func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("expected WhenTableHasIndex to run, ran=%v err=%v", ran, err)
	}
	ran = false
	if err := builder.WhenTableDoesntHaveIndex("schema_meta_widgets", "idx_missing", func() error {
		ran = true
		return nil
	}); err != nil || !ran {
		t.Fatalf("expected WhenTableDoesntHaveIndex to run, ran=%v err=%v", ran, err)
	}
	foreignKeys, err := builder.GetForeignKeys("schema_meta_children")
	if err != nil || len(foreignKeys) == 0 || foreignKeys[0].ForeignTable != "schema_meta_parents" {
		t.Fatalf("expected sqlite foreign keys, got %#v err=%v", foreignKeys, err)
	}
}

func TestDropAllSQLiteObjectsAndUnsupportedMetadata(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_drop_all_widgets", func(table *Blueprint) {
		table.Id()
		table.String("name")
	}); err != nil {
		t.Fatalf("create drop-all table: %v", err)
	}
	if err := db.Exec("CREATE VIEW schema_drop_all_view AS SELECT id FROM schema_drop_all_widgets").Error; err != nil {
		t.Fatalf("create drop-all view: %v", err)
	}
	if err := builder.DropAllViews(); err != nil {
		t.Fatalf("drop all views: %v", err)
	}
	if builder.HasView("schema_drop_all_view") {
		t.Fatal("expected view to be dropped")
	}
	if err := builder.DropAllTables(); err != nil {
		t.Fatalf("drop all tables: %v", err)
	}
	if builder.HasTable("schema_drop_all_widgets") {
		t.Fatal("expected table to be dropped")
	}
	if err := builder.DropAllTypes(); err != nil {
		t.Fatalf("drop all types should be a no-op on sqlite: %v", err)
	}

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
	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}
	builder := New(mysqlDB)

	for name, fn := range map[string]func() error{
		"create database": func() error { _, err := builder.CreateDatabase("schema_meta_demo"); return err },
		"drop database":   func() error { _, err := builder.DropDatabaseIfExists("schema_meta_demo"); return err },
		"get schemas":     func() error { _, err := builder.GetSchemas(); return err },
		"get tables":      func() error { _, err := builder.GetTables([]string{"main"}); return err },
		"get views":       func() error { _, err := builder.GetViews("main"); return err },
		"foreign keys":    func() error { _, err := builder.GetForeignKeys("schema_meta_widgets"); return err },
	} {
		if err := fn(); err == nil {
			t.Fatalf("%s should return sqlite execution error on named mysql dialect", name)
		}
	}
}

func TestNoopAndHelperBranches(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.Create("schema_noop_a", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("create noop source: %v", err)
	}
	if err := builder.Create("schema_noop_b", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("create noop target: %v", err)
	}
	if err := builder.Rename("schema_missing_source", "schema_noop_c"); err != nil {
		t.Fatalf("rename missing should no-op: %v", err)
	}
	if err := builder.Rename("schema_noop_a", "schema_noop_b"); err != nil {
		t.Fatalf("rename to existing should no-op: %v", err)
	}
	ran := false
	if err := builder.WhenTableHasColumn("schema_noop_a", "missing", func() error {
		ran = true
		return nil
	}); err != nil || ran {
		t.Fatalf("WhenTableHasColumn should skip missing column, ran=%v err=%v", ran, err)
	}
	if err := builder.WhenTableDoesntHaveColumn("schema_noop_a", "id", func() error {
		ran = true
		return nil
	}); err != nil || ran {
		t.Fatalf("WhenTableDoesntHaveColumn should skip existing column, ran=%v err=%v", ran, err)
	}
	if builder.HasColumn("missing_table", "missing") {
		t.Fatal("HasColumn should return false for missing table")
	}
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
	db := openSQLite(t)
	builder := New(db)
	if _, err := builder.CreateDatabase(""); err == nil || !strings.Contains(err.Error(), "database name") {
		t.Fatalf("expected create database validation error, got %v", err)
	}
	if _, err := builder.DropDatabaseIfExists(""); err == nil || !strings.Contains(err.Error(), "database name") {
		t.Fatalf("expected drop database validation error, got %v", err)
	}
	if err := builder.Create("schema_existing_branch", func(table *Blueprint) { table.Id() }); err != nil {
		t.Fatalf("create existing branch table: %v", err)
	}
	if err := builder.Create("schema_existing_branch", func(table *Blueprint) { table.String("ignored") }); err != nil {
		t.Fatalf("create existing table should no-op: %v", err)
	}

	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}
	mysqlBuilder := New(mysqlDB)
	if err := mysqlBuilder.EnableForeignKeyConstraints(); err == nil {
		t.Fatal("named mysql enable constraints should hit sqlite execution error")
	}
	if err := mysqlBuilder.DisableForeignKeyConstraints(); err == nil {
		t.Fatal("named mysql disable constraints should hit sqlite execution error")
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

func TestSyncModelsAddsMissingColumns(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	if err := builder.SyncModels(&syncBaseWidget{}); err != nil {
		t.Fatalf("sync base model: %v", err)
	}
	if builder.HasColumn("schema_sync_widgets", "code") {
		t.Fatal("code should not exist before expanded sync")
	}
	if err := builder.SyncModels(&syncExpandedWidget{}); err != nil {
		t.Fatalf("sync expanded model: %v", err)
	}
	if !builder.HasColumn("schema_sync_widgets", "code") {
		t.Fatal("expected SyncModels to add missing code column")
	}
	useIsolatedFacadeRegistry(t)
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("package SyncModels without facade binding did not panic")
		}
		if got := strings.TrimSpace(recovered.(error).Error()); got != `container "database.schema": container factory is not registered` {
			t.Fatalf("panic = %q, want database.schema not registered", got)
		}
	}()
	_ = SyncModels(&syncExpandedWidget{})
}

func TestBuilderCloseMethod(t *testing.T) {
	db := openSQLite(t)
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
	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

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
	if _, err := drop.Compile(db); err != nil {
		t.Fatalf("compile drop aliases: %v", err)
	}
}

func TestCompilerErrorAndHelperBranches(t *testing.T) {
	db := openSQLite(t)
	mysqlDB := db.Session(&gorm.Session{})
	mysqlDB.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

	empty := NewBlueprint("schema_empty", createTable)
	if _, err := empty.Compile(mysqlDB); err == nil || !strings.Contains(err.Error(), "no columns") {
		t.Fatalf("expected no columns error, got %v", err)
	}
	changeMissing := NewBlueprint("schema_missing_change", alterTable)
	changeMissing.String("missing").Change()
	if _, err := changeMissing.Compile(mysqlDB); err == nil || !strings.Contains(err.Error(), "cannot change missing column") {
		t.Fatalf("expected missing change error, got %v", err)
	}
	badRename := NewBlueprint("schema_bad_rename", alterTable)
	badRename.RenameColumn("", "name")
	if _, err := badRename.Compile(db); err == nil || !strings.Contains(err.Error(), "rename column requires") {
		t.Fatalf("expected rename validation error, got %v", err)
	}

	if got := (&IndexDefinition{kind: "plain", name: "idx_plain", columns: []string{"name"}}).inlineSQL(); !strings.Contains(got, "KEY `idx_plain`") {
		t.Fatalf("expected plain inline index, got %s", got)
	}
	if got := (&IndexDefinition{kind: "plain"}).inlineSQLiteSQL(); got != "" {
		t.Fatalf("expected empty sqlite inline index, got %s", got)
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

func TestSQLiteTypeBranchesAndForeignActions(t *testing.T) {
	db := openSQLite(t)
	builder := New(db)
	err := builder.Create("schema_sqlite_types", func(table *Blueprint) {
		table.Id()
		table.Boolean("enabled")
		table.Float("ratio")
		table.Decimal("amount")
		table.Binary("blob_value")
		table.Date("business_date")
		table.String("name")
		table.ForeignId("user_id").Constrained("users").NoActionOnDelete().NoActionOnUpdate()
	})
	if err != nil {
		t.Fatalf("create sqlite type table: %v", err)
	}
	if !builder.HasTable("schema_sqlite_types") {
		t.Fatal("expected schema_sqlite_types")
	}
}

func TestUnsupportedConstraintBranches(t *testing.T) {
	db := openSQLite(t)
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

func tableInfoContains(items []TableInfo, name string) bool {
	for _, item := range items {
		if item.Name == name {
			return true
		}
	}
	return false
}

func viewInfoContains(items []ViewInfo, name string) bool {
	for _, item := range items {
		if item.Name == name {
			return true
		}
	}
	return false
}

func columnInfoContains(items []ColumnInfo, name string) bool {
	for _, item := range items {
		if item.Name == name {
			return true
		}
	}
	return false
}

func indexInfoContains(items []IndexInfo, name string) bool {
	for _, item := range items {
		if item.Name == name {
			return true
		}
	}
	return false
}

func stringContains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}
