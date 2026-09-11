package database

import (
	"context"
	"fmt"
	"strings"
	"sync"

	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/exception"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// DriverContext describes the connection request passed to an extension dialector factory.
type DriverContext struct {
	Driver      string
	DSN         string
	TablePrefix string
	Options     map[string]any
}

// DialectorFactory builds a GORM dialector without opening a database connection.
type DialectorFactory func(DriverContext) (gorm.Dialector, error)

// Manager owns the dialector factories installed for one Application.
type Manager struct {
	mu        sync.RWMutex
	factories map[string]DialectorFactory
	debug     bool
}

// NewManager creates an independent database manager.
func NewManager() *Manager {
	return newManager(false)
}

func newManager(debug bool) *Manager {
	manager := &Manager{
		factories: make(map[string]DialectorFactory),
		debug:     debug,
	}
	manager.Extend("mysql", func(ctx DriverContext) (gorm.Dialector, error) {
		return mysql.New(mysql.Config{
			DSN:                       ctx.DSN,
			SkipInitializeWithVersion: true,
		}), nil
	})
	return manager
}

// ManagerFrom resolves the database manager owned by resolver's Application.
func ManagerFrom(resolver containercontract.Resolver) (*Manager, error) {
	if resolver == nil {
		return nil, fmt.Errorf("database: resolver is nil")
	}
	raw, err := resolver.Make("database.manager")
	if err != nil {
		return nil, fmt.Errorf("database: resolve manager: %w", err)
	}
	manager, ok := raw.(*Manager)
	if !ok || manager == nil {
		return nil, fmt.Errorf("database: manager resolved %T, want *database.Manager", raw)
	}
	return manager, nil
}

// Extend installs or replaces a dialector factory for this manager.
func (m *Manager) Extend(name string, factory DialectorFactory) {
	if m == nil || factory == nil {
		return
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return
	}
	m.mu.Lock()
	m.factories[name] = factory
	m.mu.Unlock()
}

// Open opens a GORM connection through a dialector registered on this manager.
func (m *Manager) Open(driver, dsn string, cfg MySQLConfig) (*gorm.DB, error) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	if driver == "" {
		driver = "mysql"
	}
	m.mu.RLock()
	factory := m.factories[driver]
	m.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("database: driver %q is not registered", driver)
	}
	dialector, err := factory(DriverContext{
		Driver:      driver,
		DSN:         dsn,
		TablePrefix: cfg.Schema.TablePrefix,
		Options:     cloneDriverOptions(cfg.Driver.Options),
	})
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               gormLoggerFromDebug(m.debug),
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: cfg.Schema.TablePrefix,
		},
	})
	if err != nil {
		return nil, err
	}
	if driver != "mysql" {
		return db, nil
	}
	if err := configureConnection(db, cfg); err != nil {
		if sqlDB, closeErr := db.DB(); closeErr == nil {
			if closeErr = sqlDB.Close(); closeErr != nil {
				exception.Report(context.Background(), closeErr, map[string]any{"component": "database", "operation": "close_on_config_failure"})
			}
		}
		return nil, err
	}
	return db, nil
}

func cloneDriverOptions(options map[string]string) map[string]any {
	if len(options) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(options))
	for key, value := range options {
		cloned[key] = value
	}
	return cloned
}
