package main

import (
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
)

var (
	brokers      = []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	addr         = ":8080"
	requestTopic = "transactions"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	server := &Server{
		provider: newProducerProvider(brokers),
	}
	server.registerEndpoints()

	defer func() {
		if err := server.provider.clear(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Printf("Listening for requests on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
