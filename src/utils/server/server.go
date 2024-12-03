package server

import (
	"csc27/utils/constants"
	"csc27/utils/consumer"
	"csc27/utils/dtypes"
	"csc27/utils/producer"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Server struct {
	Provider             *producer.ProducerProvider
	TransactionConsumer  *consumer.Consumer
	ConsultConsumer      *consumer.Consumer
	TransactionResponses map[string]chan *sarama.ConsumerMessage
	ConsultResponses     map[string]chan *sarama.ConsumerMessage
}

func InitializeServer() *Server {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	provider := producer.NewProducerProvider(constants.BROKERS_CONTAINER)
	transactionConsumer := consumer.InitializeConsumer(config, constants.TransactionResponseConsumerGroup, []string{constants.TransactionResponseTopic}, constants.BROKERS_CONTAINER)
	consultConsumer := consumer.InitializeConsumer(config, constants.ConsultResponseConsumerGroup, []string{constants.ConsultAvailabilityResponseTopic}, constants.BROKERS_CONTAINER)

	server := &Server{
		Provider:             provider,
		TransactionConsumer:  transactionConsumer,
		ConsultConsumer:      consultConsumer,
		TransactionResponses: make(map[string]chan *sarama.ConsumerMessage),
		ConsultResponses:     make(map[string]chan *sarama.ConsumerMessage),
	}

	return server
}

////////////////////////////////////////
///////////// Endpoints ////////////////
////////////////////////////////////////

func (s *Server) RegisterEndpoints() {
	http.HandleFunc("/transactions", s.ReceiveRequest)
	http.HandleFunc("/consult-product", s.ConsultProduct)
}

func (s *Server) StartServer() {
	s.RegisterEndpoints()
	go s.TransactionConsumer.StartConsuming()
	go s.ConsultConsumer.StartConsuming()
	go func() {
		for {
			select {
			case msg := <-s.TransactionConsumer.Messages:
				log.Printf("Received Response %s\n", msg.Value)
				var response dtypes.TransactionResponse
				json.Unmarshal(msg.Value, &response)
				s.TransactionResponses[response.TransactionID] <- msg
			case msg := <-s.ConsultConsumer.Messages:
				log.Printf("Received Response %s\n", msg.Value)
				var response dtypes.ConsultProductResponse
				json.Unmarshal(msg.Value, &response)
				s.TransactionResponses[response.ProductID] <- msg
			}
		}
	}()
}

func (s *Server) ReceiveRequest(w http.ResponseWriter, r *http.Request) {
	// Check HTTP Type
	log.Printf("Received request from %s\n", r.RemoteAddr)
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode Request
	var req dtypes.TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Serialize Request
	txn := dtypes.TransactionRequestToTransaction(req)
	data, err := json.Marshal(txn)
	if err != nil {
		http.Error(w, "Failed to serialize transaction", http.StatusInternalServerError)
		return
	}

	// Send Request
	s.TransactionResponses[txn.TransactionID] = make(chan *sarama.ConsumerMessage)
	producer.Send(s.Provider, constants.TransactionRequestTopic, data)
	log.Printf("Sent transaction request %s\n", txn.TransactionID)

	// Receive Response
	w.WriteHeader(http.StatusOK)

	log.Printf("Waiting %s\n", txn.TransactionID)
	msg := <-s.TransactionResponses[txn.TransactionID]
	// Handle response
	log.Printf("Received response %s\n", txn.TransactionID)
	response := dtypes.TransactionResponse{}
	json.Unmarshal(msg.Value, &response)
	fmt.Fprintln(w, response.TransactionStatus)
}

func (s *Server) ConsultProduct(w http.ResponseWriter, r *http.Request) {
	// Check HTTP Type
	log.Printf("Received consult from %s\n", r.RemoteAddr)
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode Request
	var req dtypes.ConsultProductRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Serialize Request
	data, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "Failed to serialize consult", http.StatusInternalServerError)
		return
	}

	// Send Request
	s.ConsultResponses[req.ProductID] = make(chan *sarama.ConsumerMessage)
	producer.Send(s.Provider, constants.ConsultAvailabilityRequestTopic, data)
	log.Printf("Sent consult request %s\n", req.ProductID)

	// Receive Response
	w.WriteHeader(http.StatusOK)

	log.Printf("Waiting %s\n", req.ProductID)
	msg := <-s.ConsultResponses[req.ProductID]
	// Handle response
	log.Printf("Received response %s\n", req.ProductID)
	response := dtypes.ConsultProductResponse{}
	json.Unmarshal(msg.Value, &response)
	fmt.Fprintln(w, response)
}
