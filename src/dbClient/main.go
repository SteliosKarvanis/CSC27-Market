package main

import (
	"csc27/utils/consumer"
	"github.com/IBM/sarama"
)

func main() {
	group := "fgada"
	// brokers := []string{"localhost:29092"}
	brokers := []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	topics := []string{"transactions"}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	gc := &consumer.GroupConsumer{ConsumerConfig: config, Group: group, Topics: topics}
	gc.StartConsuming(brokers)
}
