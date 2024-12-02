package server

import (
	"csc27/utils/dtypes"
	"csc27/utils/producer"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type Server struct {
	Provider *producer.ProducerProvider
}

////////////////////////////////////////
///////////// Endpoints ////////////////
////////////////////////////////////////

func (s *Server) RegisterEndpoints() {
	http.HandleFunc("/transactions", s.ReceiveRequest)
}

func (s *Server) ReceiveRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request from %s\n", r.RemoteAddr)
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	var txn dtypes.Transaction
	if err := json.NewDecoder(r.Body).Decode(&txn); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(txn)
	if err != nil {
		http.Error(w, "Failed to serialize transaction", http.StatusInternalServerError)
		return
	}
	s.Provider.Send("transactions", data)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Transaction sent successfully")
}
