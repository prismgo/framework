package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type migratorWidget struct {
	ID       uint
	TenantID uint
	Code     string
	Status   string
}

type namedDialector struct {
	gorm.Dialector
	name string
}

func (d namedDialector) Name() string { return d.name }

const fakeMySQLDriverName = "prismgo_migrator_fake_mysql"

var registerFakeMySQLOnce sync.Once

type fakeMySQLDriver struct{}

func (fakeMySQLDriver) Open(string) (driver.Conn, error) { return fakeMySQLConn{}, nil }

type fakeMySQLConn struct{}

func (fakeMySQLConn) Prepare(string) (driver.Stmt, error) { return nil, nil }
func (fakeMySQLConn) Close() error                        { return nil }
func (fakeMySQLConn) Begin() (driver.Tx, error)           { return fakeMySQLTx{}, nil }

func (fakeMySQLConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (fakeMySQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.ToLower(query)
	switch {
	case strings.Contains(normalized, "information_schema.statistics"):
		count := int64(0)
		if len(args) >= 2 && (args[1].Value == "idx_existing" || args[1].Value == "idx_to_drop") {
			count = 1
		}
		return &fakeRows{columns: []string{"count"}, rows: [][]driver.Value{{count}}}, nil
	case strings.Contains(normalized, "information_schema.key_column_usage"):
		return &fakeRows{columns: []string{"column_name"}, rows: [][]driver.Value{{"tenant_id"}, {"code"}}}, nil
	case strings.Contains(normalized, "information_schema.tables"):
		return &fakeRows{columns: []string{"engine"}, rows: [][]driver.Value{{"MyISAM"}}}, nil
	default:
		return &fakeRows{columns: []string{"ok"}, rows: [][]driver.Value{{1}}}, nil
	}
}

type fakeMySQLTx struct{}

func (fakeMySQLTx) Commit() error   { return nil }
func (fakeMySQLTx) Rollback() error { return nil }

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r fakeRows) Columns() []string { return r.columns }
func (r fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func openFakeMySQL(t *testing.T) *gorm.DB {
	t.Helper()
	registerFakeMySQLOnce.Do(func() {
		sql.Register(fakeMySQLDriverName, fakeMySQLDriver{})
	})
	sqlDB, err := sql.Open(fakeMySQLDriverName, "")
	if err != nil {
		t.Fatalf("open fake mysql sql db: %v", err)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		DriverName:                fakeMySQLDriverName,
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open fake mysql gorm db: %v", err)
	}
	return db
}

func TestMigratorSQLiteBranches(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&migratorWidget{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	names, err := ManagedTableNames(db, []any{&migratorWidget{}})
	if err != nil {
		t.Fatalf("managed table names: %v", err)
	}
	if len(names) != 1 || names[0] != "migrator_widgets" {
		t.Fatalf("unexpected table names: %#v", names)
	}

	if err := EnsureInnoDB(db, []any{&migratorWidget{}}); err != nil {
		t.Fatalf("sqlite should skip innodb enforcement: %v", err)
	}
	if err := EnsureCompositeIndexes(db, []CompositeIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_migrator_widgets_tenant_status",
		Columns: "`tenant_id`, `status`",
	}}); err != nil {
		t.Fatalf("ensure sqlite composite index: %v", err)
	}
	if err := EnsureCompositeUniqueIndexes(db, []CompositeUniqueIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_migrator_widgets_tenant_code",
		Columns: "`tenant_id`, `code`",
	}}); err != nil {
		t.Fatalf("ensure sqlite composite unique index: %v", err)
	}
	if err := DropObsoleteIndexes(db, []DropIndex{{
		Table: "migrator_widgets",
		Name:  "idx_legacy",
	}}); err != nil {
		t.Fatalf("sqlite should skip drop obsolete indexes: %v", err)
	}
}

func TestMigratorInformationSchemaQueriesReturnErrorsOnSQLite(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := IndexExists(db, "missing", "idx_missing"); err == nil {
		t.Fatal("expected sqlite information_schema error from IndexExists")
	}
	if _, err := PrimaryKeyColumns(db, "missing"); err == nil {
		t.Fatal("expected sqlite information_schema error from PrimaryKeyColumns")
	}
	if _, err := currentMySQLTableEngine(db, "missing"); err == nil {
		t.Fatal("expected sqlite information_schema error from currentMySQLTableEngine")
	}
}

func TestManagedTableNamesRejectsInvalidModel(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := ManagedTableNames(db, []any{42}); err == nil {
		t.Fatal("expected invalid model parse error")
	}
}

func TestMigratorMySQLBranchesReturnContextualErrors(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&migratorWidget{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	db.Dialector = namedDialector{Dialector: db.Dialector, name: "mysql"}

	if err := EnsureInnoDB(db, []any{&migratorWidget{}}); err == nil || !strings.Contains(err.Error(), "query table migrator_widgets engine failed") {
		t.Fatalf("expected contextual innodb error, got %v", err)
	}
	if err := EnsureCompositeIndexes(db, []CompositeIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_missing",
		Columns: "`tenant_id`",
	}}); err == nil || !strings.Contains(err.Error(), "check index migrator_widgets.idx_missing failed") {
		t.Fatalf("expected contextual composite index error, got %v", err)
	}
	if err := EnsureCompositeUniqueIndexes(db, []CompositeUniqueIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_unique_missing",
		Columns: "`tenant_id`, `code`",
	}}); err == nil || !strings.Contains(err.Error(), "check index migrator_widgets.idx_unique_missing failed") {
		t.Fatalf("expected contextual composite unique index error, got %v", err)
	}
	if err := DropObsoleteIndexes(db, []DropIndex{{
		Table: "migrator_widgets",
		Name:  "idx_legacy",
	}}); err == nil || !strings.Contains(err.Error(), "check index migrator_widgets.idx_legacy failed") {
		t.Fatalf("expected contextual drop index error, got %v", err)
	}
}

func TestMigratorMySQLBranchesWithFakeDriver(t *testing.T) {
	db := openFakeMySQL(t)

	if err := EnsureInnoDB(db, []any{&migratorWidget{}}); err != nil {
		t.Fatalf("ensure innodb with fake mysql: %v", err)
	}
	if err := EnsureCompositeIndexes(db, []CompositeIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_to_create",
		Columns: "`tenant_id`, `status`",
	}, {
		Table:   "migrator_widgets",
		Name:    "idx_existing",
		Columns: "`tenant_id`",
	}}); err != nil {
		t.Fatalf("ensure composite indexes with fake mysql: %v", err)
	}
	if err := EnsureCompositeUniqueIndexes(db, []CompositeUniqueIndex{{
		Table:   "migrator_widgets",
		Name:    "idx_unique_to_create",
		Columns: "`tenant_id`, `code`",
	}, {
		Table:   "migrator_widgets",
		Name:    "idx_existing",
		Columns: "`tenant_id`",
	}}); err != nil {
		t.Fatalf("ensure composite unique indexes with fake mysql: %v", err)
	}
	if err := DropObsoleteIndexes(db, []DropIndex{{
		Table: "migrator_widgets",
		Name:  "idx_to_drop",
	}, {
		Table: "migrator_widgets",
		Name:  "idx_missing",
	}}); err != nil {
		t.Fatalf("drop obsolete indexes with fake mysql: %v", err)
	}

	exists, err := IndexExists(db, "migrator_widgets", "idx_existing")
	if err != nil || !exists {
		t.Fatalf("expected fake index, exists=%v err=%v", exists, err)
	}
	columns, err := PrimaryKeyColumns(db, "migrator_widgets")
	if err != nil {
		t.Fatalf("fake primary key columns: %v", err)
	}
	if !SameColumns(columns, []string{"tenant_id", "code"}) {
		t.Fatalf("columns = %#v, want tenant_id/code", columns)
	}
	engine, err := currentMySQLTableEngine(db, "migrator_widgets")
	if err != nil || engine != "MyISAM" {
		t.Fatalf("engine = %q err=%v, want MyISAM", engine, err)
	}
}
