package main

import (
	"fmt"
	"log"
	"os"

	"csc27/utils/dbUtils"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Retry.Max = 10

	//TODO: Fix this
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	db := os.Getenv("MYSQL_DATABASE")
	dsn := fmt.Sprintf("%s:%s@tcp(db:%s)/%s", user, password, port, db)
	log.Printf("Connector: connected on db at %s", dsn)

	dbClient := dbUtils.InitializeDbClient(config, dsn)
	dbClient.Start()
}
