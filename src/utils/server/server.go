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
	Provider  *producer.ProducerProvider
	Consumer  *consumer.Consumer
	Responses map[string]chan *sarama.ConsumerMessage
}

func InitializeServer() *Server {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	provider := producer.NewProducerProvider(constants.BROKERS_CONTAINER)
	consumer := consumer.InitializeConsumer(config, constants.TransactionResponseConsumerGroup, []string{constants.TransactionResponseTopic}, constants.BROKERS_CONTAINER)

	server := &Server{
		Provider:  provider,
		Consumer:  consumer,
		Responses: make(map[string]chan *sarama.ConsumerMessage),
	}

	return server
}

////////////////////////////////////////
///////////// Endpoints ////////////////
////////////////////////////////////////

func (s *Server) RegisterEndpoints() {
	http.HandleFunc("/transactions", s.ReceiveRequest)
}

func (s *Server) StartServer() {
	s.RegisterEndpoints()
	go s.Consumer.StartConsuming()
	go func() {
		for {
			select {
			case msg := <-s.Consumer.Messages:
				var response dtypes.TransactionResponse
				json.Unmarshal(msg.Value, &response)
				log.Printf("Received Response %s\n", response.TransactionID)
				s.Responses[response.TransactionID] <- msg
			}
		}
	}()
}

func (s *Server) ReceiveRequest(w http.ResponseWriter, r *http.Request) {
	// Check HTTP Type
	log.Printf("Endpoint received %s\n", r.RemoteAddr)
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
	s.Responses[txn.TransactionID] = make(chan *sarama.ConsumerMessage)
	producer.Send(s.Provider, constants.TransactionRequestTopic, data)
	log.Printf("Sent transaction request %s\n", txn.TransactionID)

	// Receive Response
	w.WriteHeader(http.StatusOK)

	log.Printf("Waiting %s\n", txn.TransactionID)
	msg := <-s.Responses[txn.TransactionID]
	// Handle response
	log.Printf("Received response %s\n", txn.TransactionID)
	response := dtypes.TransactionResponse{}
	json.Unmarshal(msg.Value, &response)
	fmt.Fprintln(w, response.TransactionStatus)
}
