package dbClientUtils

import (
	"csc27/utils/constants"
	"csc27/utils/consumer"
	"csc27/utils/dtypes"
	"csc27/utils/producer"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
)

type DbClient struct {
	Db                        *gorm.DB
	TransationRequestConsumer *consumer.Consumer
	Producer                  *producer.ProducerProvider
}

func InitializeDbClient(config *sarama.Config, dsn string, onHost bool) DbClient {
	var brokers []string
	if onHost {
		brokers = constants.BROKERS_HOST
	} else {
		brokers = constants.BROKERS_CONTAINER
	}
	db := InitializeDb(dsn)
	return DbClient{
		Db:                        db,
		TransationRequestConsumer: consumer.InitializeConsumer(config, constants.TransactionRequestConsumerGroup, []string{constants.TransactionRequestTopic}, brokers),
		Producer:                  producer.NewProducerProvider(brokers),
	}
}

func (dbClient DbClient) Start() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go dbClient.TransationRequestConsumer.StartConsuming()

	for {
		select {
		case message := <-dbClient.TransationRequestConsumer.Messages:
			log.Printf("Received message")
			err := dbClient.ExecuteTransaction(message)
			if err != nil {
				log.Fatalf("Error executing transaction: %v", err)
			}
		case <-sigterm:
			log.Println("terminating: via signal")
			return
		}
	}
}

func (dbClient DbClient) ExecuteTransaction(message *sarama.ConsumerMessage) error {
	// Decode message
	var transaction dtypes.Transaction
	json.Unmarshal(message.Value, &transaction)

	fmt.Printf("Saving on DB TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
		transaction.TransactionID, transaction.ProductID, transaction.Price, transaction.Quantity)

	// Get Product
	var product dtypes.Product
	err := dbClient.Db.Where("product_id = ?", transaction.ProductID).First(&product).Error
	if err != nil {
		log.Fatalf("Error fetching product on db: %s", transaction.ProductID)
	} else {
		transaction.Price = product.Price
		// Check if there is enough quantity
		newQuantity := product.Quantity - transaction.Quantity
		if newQuantity <= 0 {
			log.Printf("Invalid Transaction: %s", transaction.TransactionID)
			transaction.TransactionStatus = constants.TransactionStatusFailed
		} else {
			transaction.TransactionStatus = constants.TransactionStatusSuccess
			log.Printf("Updating existing ProductID: %s, Quantity:%d\n", product.ProductID, newQuantity)
			dbClient.Db.Model(&product).Updates(dtypes.Product{Quantity: newQuantity})
		}
		// Save transaction on DB
		result := dbClient.Db.Create(&transaction)
		if result.Error != nil {
			log.Printf("Error saving on DB: %v", result.Error)
			return result.Error
		} else {
			log.Printf("Message saved on DB")
		}
	}
	return nil
}
