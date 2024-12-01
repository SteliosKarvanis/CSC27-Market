package main

import (
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
)

var (
	brokers = []string{"localhost:29092", "localhost:39092", "localhost:49092", "localhost:59092"}
	addr    = "localhost:8080"
	requestTopic = "transactions"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	server := &Server{
		syncProducer: newSyncProducer(brokers),
	}
	server.registerEndpoints()

	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()
	
	log.Printf("Listening for requests on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
