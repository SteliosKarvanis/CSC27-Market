package dbUtils

import (
	"csc27/utils/constants"
	"csc27/utils/consumer"
	"csc27/utils/dtypes"
	"csc27/utils/producer"
	"encoding/json"
	"log"
	"sync"

	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

type DbConnector struct {
	Db                        *gorm.DB
	TransationRequestConsumer *consumer.Consumer
	Producer                  *producer.ProducerProvider
	ProducerMux               *sync.Mutex
}

func InitializeDbClient(config *sarama.Config, dsn string, onHost bool) DbConnector {
	var brokers []string
	if onHost {
		brokers = constants.BROKERS_HOST
	} else {
		brokers = constants.BROKERS_CONTAINER
	}
	db := InitializeDb(dsn)
	return DbConnector{
		Db:                        db,
		TransationRequestConsumer: consumer.InitializeConsumer(config, constants.TransactionRequestConsumerGroup, []string{constants.TransactionRequestTopic}, brokers),
		Producer:                  producer.NewProducerProvider(brokers),
		ProducerMux:               &sync.Mutex{},
	}
}

func (dbConnector DbConnector) Start() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go dbConnector.TransationRequestConsumer.StartConsuming()

	for {
		select {
		case message := <-dbConnector.TransationRequestConsumer.Messages:
			log.Printf("Received message")
			err := dbConnector.ExecuteTransaction(message)
			if err != nil {
				log.Printf("Error executing transaction: %v", err)
			}
		case <-sigterm:
			log.Println("terminating: via signal")
			return
		}
	}
}

func (dbConnector DbConnector) ExecuteTransaction(message *sarama.ConsumerMessage) error {
	// Decode message
	var transaction dtypes.Transaction
	json.Unmarshal(message.Value, &transaction)

	dbConnector.ProducerMux.Lock()
	// Consult Product
	var product dtypes.Product
	err := dbConnector.Db.Where("product_id = ?", transaction.ProductID).First(&product).Error
	if err != nil {
		log.Fatalf("Error fetching product on db: %s", transaction.ProductID)
		return err
	}
	transaction.Price = product.Price
	// Check if there is enough quantity
	newQuantity := product.Quantity - transaction.Quantity
	if newQuantity < 0 {
		log.Printf("Transaction Denied. Product not available: %s", transaction.TransactionID)
		transaction.TransactionStatus = constants.TransactionStatusFailed
	} else {
		transaction.TransactionStatus = constants.TransactionStatusSuccess
		log.Printf("Transaction Successfull. Removing %d Updating existing ProductID: %s, Quantity:%d\n", transaction.Quantity, product.ProductID, newQuantity)
		dbConnector.Db.Model(&product).Updates(dtypes.Product{Quantity: newQuantity})
	}
	dbConnector.ProducerMux.Unlock()
	// Save transaction on DB
	result := dbConnector.Db.Create(&transaction)
	if result.Error != nil {
		log.Printf("Error saving on DB: %v", result.Error)
		return result.Error
	} else {
		log.Printf("Message saved on DB")
	}
	// Produce response
	response := dtypes.TransactionResponse{
		TransactionID:     transaction.TransactionID,
		TransactionStatus: transaction.TransactionStatus,
	}
	data, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return err
	}
	log.Printf("Sending response to topic: %s", constants.TransactionResponseTopic)
	err = producer.Send(dbConnector.Producer, constants.TransactionResponseTopic, data)
	if err != nil {
		log.Printf("Error sending message: %v", err)
		return err
	}
	return nil
}
