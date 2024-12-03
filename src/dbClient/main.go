package main

import (
	"fmt"
	"os"

	"csc27/utils/dbClientUtils"
	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

const (
	onHost = true
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.Retry.Max = 10

	//TODO: Fix this
	if onHost {
		godotenv.Load("../.env")
	}
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	db := os.Getenv("MYSQL_DATABASE")
	dsn := fmt.Sprintf("%s:%s@tcp(:%s)/%s", user, password, port, db)
	println(dsn)

	dbClient := dbClientUtils.InitializeDbClient(config, dsn, onHost)
	dbClient.Start()
}
