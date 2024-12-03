package dbClientUtils

import (
	"database/sql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

func InitializeDb(dsn string) *gorm.DB {
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Error connecting to DB: %v", err)
	}
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{})
	if err != nil {
		log.Printf("Error initializing DB: %v", err)
	}
	return gormDB
}
