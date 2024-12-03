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
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatalf("unable to load env variables")
	}
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	db := os.Getenv("MYSQL_DATABASE")
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:%s)/%s", user, password, port, db)
	sqlDB, _ := sql.Open("mysql", dsn)
	gormDB, _ := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{})

	// Custom Query
	product:=dtypes.Product{
		ProductID: "PROD-001",
		Price: 100.0,
		Quantity: 100000,
	}
	_ = gormDB.Create(product)
}
