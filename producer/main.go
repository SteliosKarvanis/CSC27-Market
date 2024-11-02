package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
)

var (
	brokers    = "localhost:29092"
	test_topic = "test-topic"
)

func main() {
	keepRunning := true
	reader := bufio.NewScanner(os.Stdin)
	log.Println("Starting a new Sarama producer")

	producerProvider := newProducerProvider(strings.Split(brokers, ","))

	ctx, cancel := context.WithCancel(context.Background())
	for {
		var text string
		if reader.Scan() { // Waits here until input is given and Enter is pressed
			text = reader.Text()
		}
		if text == "exit" {
			keepRunning = false
			break
		}
		go func() {
			select {
			case <-ctx.Done():
				return
			default:
				msg := sarama.ProducerMessage{Topic: test_topic, Key: nil, Value: sarama.StringEncoder(text)}
				producerProvider.send(msg)
			}
		}()
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		<-sigterm
		log.Println("terminating: via signal")
		keepRunning = false
	}
	cancel()

	producerProvider.clear()
}
