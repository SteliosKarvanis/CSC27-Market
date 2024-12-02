package consumer

import (
	"github.com/IBM/sarama"
	"log"
	"time"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	messages chan *sarama.ConsumerMessage
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			consumer.messages <- message
		case <-session.Context().Done():
			return nil
		}
	}
}

func InitializeClient(brokers []string, group string, config *sarama.Config) sarama.ConsumerGroup {
	timeout := 5
	for {
		client, err := sarama.NewConsumerGroup(brokers, group, config)
		if err != nil {
			log.Printf("Error creating consumer group client. Retrying in %d seconds", timeout)
			time.Sleep(time.Duration(timeout) * time.Second)
		} else {
			return client
		}
	}
}
