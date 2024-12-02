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
	var tscs []dtypes.Transaction
	gormDB.Find(&tscs)
	for _, tsc := range tscs {
		fmt.Printf("TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
			tsc.TransactionID, tsc.ProductID, tsc.Price, tsc.Quantity)
	}
	var prods []dtypes.Product
	gormDB.Find(&prods)
	for _, prod := range prods {
		fmt.Printf("ProductID: %s, Price: %.2f, Quantity: %d\n",
			prod.ProductID, prod.Price, prod.Quantity)
	}
}
