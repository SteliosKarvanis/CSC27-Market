package main

import (
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
	"csc27/utils/producer"
	"csc27/utils/server"
)

var (
	brokers      = []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	addr         = ":8080"
	requestTopic = "transactions"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	server := &server.Server{
		Provider: producer.NewProducerProvider(brokers),
	}
	server.RegisterEndpoints()

	defer func() {
		if err := server.Provider.Clear(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Printf("Listening for requests on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
