package main

import (
	"csc27/utils/server"
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
)

var (
	addr = ":8080"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	server := server.InitializeServer()
	server.RegisterEndpoints()

	defer func() {
		if err := server.Provider.Clear(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Printf("Listening for requests on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
