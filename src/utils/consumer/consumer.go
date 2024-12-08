package consumer

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	Client   sarama.ConsumerGroup
	Topics   []string
	Messages chan *sarama.ConsumerMessage
}

func InitializeConsumer(config *sarama.Config, group string, topics []string, brokers []string) *Consumer {
	client := initializeClient(brokers, group, config)
	return &Consumer{
		Client:   client,
		Topics:   topics,
		Messages: make(chan *sarama.ConsumerMessage),
	}
}

func (consumer *Consumer) StartConsuming() {
	cg := ConsumerGroup{
		ready:    make(chan bool),
		messages: consumer.Messages,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumer.Client.Consume(ctx, consumer.Topics, &cg); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Consumer: error in initialization. %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			cg.ready = make(chan bool)
		}
	}()
	<-cg.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		}
	}
	cancel()
	wg.Wait()
	if err := consumer.Client.Close(); err != nil {
		log.Panicf("Consumer: Error closing client: %v", err)
	}
}

func initializeClient(brokers []string, group string, config *sarama.Config) sarama.ConsumerGroup {
	timeout := 5
	for {
		client, err := sarama.NewConsumerGroup(brokers, group, config)
		if err != nil {
			log.Printf("Consumer: Error creating group client. Retrying in %d seconds", timeout)
			time.Sleep(time.Duration(timeout) * time.Second)
		} else {
			return client
		}
	}
}
