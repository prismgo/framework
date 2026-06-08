package database

import (
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/facade"

	"gorm.io/gorm"
)

const serviceKey = "database.default"

// Resolve 从当前 Application 容器解析数据库连接。
func Resolve() *gorm.DB {
	return facade.Resolve[*gorm.DB](serviceKey)
}

// DBCloseOption 返回数据库连接的关闭选项，供 bootstrap 注册时使用。
func DBCloseOption() containercontract.BindingOption {
	return container.WithCloser(func(db *gorm.DB) error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	})
}
