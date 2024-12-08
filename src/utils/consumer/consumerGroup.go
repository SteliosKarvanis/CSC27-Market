package consumer

import (
	"github.com/IBM/sarama"
	"log"
)

// Consumer represents a Sarama consumer group consumer
type ConsumerGroup struct {
	ready    chan bool
	messages chan *sarama.ConsumerMessage
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (cg *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(cg.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (cg *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (cg *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("Consumer: Message channel was closed")
				return nil
			}
			log.Printf("Consumer: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			cg.messages <- message
		case <-session.Context().Done():
			return nil
		}
	}
}
