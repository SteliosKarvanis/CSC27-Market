package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"csc27/utils/dtypes"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"reflect"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatalf("unable to load env variables")
	}
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:%s)/kafka_stock", user, password, port)
	sqlDB, _ := sql.Open("mysql", dsn)
	gormDB, _ := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{})
	gormDB.AutoMigrate(dtypes.Tables...)

	log.Println("Database migrated successfully")
	for _, table := range dtypes.Tables {
		log.Printf("Migrating Table: %v\n", reflect.TypeOf(table))
	}
}