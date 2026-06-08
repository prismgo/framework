package migration

import (
	"fmt"
	"os"
	"strings"

	"gorm.io/gorm"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/database"
)

type dbSession struct {
	DB      *gorm.DB
	cleanup func()
}

func (s dbSession) Close() {
	if s.cleanup != nil {
		s.cleanup()
	}
}

func openDatabaseSession(connection string) (dbSession, error) {
	connection = strings.TrimSpace(connection)
	if connection == "" {
		db := database.Resolve()
		if db == nil {
			return dbSession{}, fmt.Errorf("database: connection not initialized")
		}
		return dbSession{DB: db}, nil
	}

	db, err := database.OpenConnection(connection)
	if err != nil {
		return dbSession{}, err
	}
	cleanup := func() {
		sqlDB, sqlErr := db.DB()
		if sqlErr == nil {
			_ = sqlDB.Close()
		}
	}
	return dbSession{DB: db, cleanup: cleanup}, nil
}

func commandEnvironment() string {
	env := strings.TrimSpace(config.GetString("app.env", ""))
	if env == "" {
		env = strings.TrimSpace(os.Getenv("APP_ENV"))
	}
	if env == "" {
		env = "production"
	}
	return strings.ToLower(env)
}

func requireForceInProduction(force bool, commandName string) error {
	if force {
		return nil
	}
	if commandEnvironment() != "production" {
		return nil
	}
	return fmt.Errorf("%s is blocked in production, rerun with --force", commandName)
}
