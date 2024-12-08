package main

import (
	"csc27/utils/constants"
	"csc27/utils/server"
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	server := server.InitializeServer()
	server.StartServer()

	defer func() {
		if err := server.Provider.Clear(); err != nil {
			log.Println("Server: Failed to close", err)
		}
	}()

	log.Printf("Server: Listening for requests on %s...\n", constants.ServerAddr)
	log.Fatal(http.ListenAndServe(constants.ServerAddr, nil))
}
