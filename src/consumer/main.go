package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	// Kafka broker address
	brokers := []string{"localhost:29092"}
	topic := "transactions"

	// Create a new consumer
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Error closing consumer: %v", err)
		}
	}()

	// Get the list of partitions for the topic
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Error getting partitions: %v", err)
	}

	// Consume messages from each partition
	for _, partition := range partitions {
		go func(partition int32) {
			// Create a partition consumer
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("Error consuming partition %d: %v", partition, err)
			}
			defer pc.Close()

			// Read messages
			for message := range pc.Messages() {
				fmt.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					message.Partition, message.Offset, string(message.Key), string(message.Value))
			}
		}(partition)
	}

	// Wait indefinitely
	select {}
}
