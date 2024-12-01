package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"net/http"
)

type Server struct {
	syncProducer sarama.SyncProducer
}

func (s *Server) Close() error {
	if err := s.syncProducer.Close(); err != nil {
		log.Println("Failed to shut down data collector cleanly", err)
	}

	return nil
}

func newSyncProducer(brokerList []string) sarama.SyncProducer {
	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := getSamaraConfig()

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

////////////////////////////////////////
///////////// Endpoints ////////////////
////////////////////////////////////////

func (s *Server) registerEndpoints() {
	http.HandleFunc("/transactions", s.receiveRequest)
}

func (s *Server) receiveRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request from %s\n", r.RemoteAddr)
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	var txn Request
	if err := json.NewDecoder(r.Body).Decode(&txn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(txn)
	if err != nil {
		http.Error(w, "Failed to serialize transaction", http.StatusInternalServerError)
		return
	}

	s.syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: requestTopic,
		Value: sarama.StringEncoder(data),
	})

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Transaction sent successfully")
}
