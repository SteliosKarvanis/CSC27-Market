package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"log"
	"os"
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
	println(dsn)

	// Open a connection to the database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %v\n", err)
	}
	defer db.Close()

	// Ping the database to ensure the connection is established
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v\n", err)
	}

	fmt.Println("Connected to MySQL database!")

	// Create a table
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		email VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`

	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("Error creating table: %v\n", err)
	}

	fmt.Println("Table created successfully!")

	// Insert data into the table
	insertQuery := `INSERT INTO users (name, email) VALUES (?, ?)`
	result, err := db.Exec(insertQuery, "John Doe", "john.doe@example.com")
	if err != nil {
		log.Fatalf("Error inserting data: %v\n", err)
	}

	lastInsertID, _ := result.LastInsertId()
	fmt.Printf("Inserted data with ID: %d\n", lastInsertID)

	// Query data from the table
	rows, err := db.Query("SELECT id, name, email, created_at FROM users")
	if err != nil {
		log.Fatalf("Error querying data: %v\n", err)
	}
	defer rows.Close()

	fmt.Println("Users in the database:")
	for rows.Next() {
		var id int
		var name, email string
		var createdAt string

		err = rows.Scan(&id, &name, &email, &createdAt)
		if err != nil {
			log.Fatalf("Error scanning row: %v\n", err)
		}

		fmt.Printf("ID: %d, Name: %s, Email: %s, Created At: %s\n", id, name, email, createdAt)
	}
}
