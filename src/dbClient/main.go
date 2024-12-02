package main

import (
	"csc27/utils/consumer"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

func main() {
	group := "transactions-group2"
	brokers := []string{"localhost:29092", "localhost:39092", "localhost:49092", "localhost:59092"}
	//brokers := []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	topics := []string{"transactions"}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	//TODO: Fix this
	godotenv.Load("../.env")
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	db := os.Getenv("MYSQL_DATABASE")
	dsn := fmt.Sprintf("%s:%s@tcp(:%s)/%s", user, password, port, db)
	println(dsn)
	gc := consumer.InitializeGroupConsumer(config, group, topics, brokers, dsn)
	gc.StartConsuming()
}
