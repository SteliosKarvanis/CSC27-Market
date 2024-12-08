package dbUtils

import (
	"database/sql"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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
		log.Printf("DB: Error initializing: %v", err)
		log.Printf("DB: Retrying in 5s")
		time.Sleep(5 * time.Second)
		gormDB = InitializeDb(dsn)
	}
	return gormDB
}
