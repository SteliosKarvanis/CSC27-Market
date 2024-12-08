package main

import (
	"fmt"
	"os"

	"csc27/utils/dbUtils"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Retry.Max = 10

	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	db := os.Getenv("MYSQL_DATABASE")
	dbService := os.Getenv("MYSQL_DATABASE_SERVICE")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, dbService, port, db)

	dbClient := dbUtils.InitializeDbClient(config, dsn)
	dbClient.Start()
}
