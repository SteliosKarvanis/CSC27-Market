package dbClientUtils

import (
	"csc27/utils/constants"
	"csc27/utils/consumer"
	"csc27/utils/dbUtils"
	"csc27/utils/dtypes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

type DbClient struct {
	Db                        *gorm.DB
	TransationRequestConsumer *consumer.Consumer
}

func InitializeDbClient(config *sarama.Config, dsn string, onHost bool) DbClient {
	var brokers []string
	if onHost {
		brokers = constants.BROKERS_HOST
	} else {
		brokers = constants.BROKERS_CONTAINER
	}
	db := dbUtils.InitializeDb(dsn)
	return DbClient{
		TransationRequestConsumer: consumer.InitializeConsumer(config, constants.TransactionsConsumerGroup, []string{constants.TransactionRequestTopic}, brokers, dsn),
		Db:                        db,
	}
}

func (dbClient DbClient) Start() {
	go dbClient.TransationRequestConsumer.StartConsuming()

	for {
		select {
		case message := <-dbClient.TransationRequestConsumer.Messages:
			err := dbClient.ExecuteTransaction(message)
			if err != nil {
				log.Fatalf("Error executing transaction: %v", err)
			}
		}
	}
}

func (dbClient DbClient) ExecuteTransaction(message *sarama.ConsumerMessage) error {
	// Decode message
	var transaction dtypes.Transaction
	json.Unmarshal(message.Value, &transaction)

	fmt.Printf("Saving on DB TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
		transaction.TransactionID, transaction.ProductID, transaction.Price, transaction.Quantity)

	// Check if product exists
	var product dtypes.Product
	err := dbClient.Db.Where("product_id = ?", transaction.ProductID).First(&product).Error
	if err != nil {
		log.Fatalf("Error fetching product on db: %s", transaction.ProductID)
	} else {
		transaction.Price = product.Price
		// Check if there is enough quantity
		newQuantity := product.Quantity - transaction.Quantity
		if newQuantity < 0 {
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
