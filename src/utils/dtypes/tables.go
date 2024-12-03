package dtypes

import (
	"time"

	"csc27/utils/constants"
	"github.com/google/uuid"
)

var Tables = []interface{}{&Transaction{}, &Product{}}

type Product struct {
	ProductID string  `json:"product_id" gorm:"primaryKey"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
}

type Transaction struct {
	TransactionID        string    `json:"transaction_id" gorm:"primaryKey"`
	ConsumerID           string    `json:"consumer_id"`
	ProductID            string    `json:"product_id"`
	TransactionTimeStamp time.Time `json:"transaction_timestamp" gorm:"type:timestamp;default:CURRENT_TIMESTAMP"`
	TransactionStatus    string    `json:"transaction_status"`
	Price                float64   `json:"price"`
	Quantity             int       `json:"quantity"`
}

func TransactionRequestToTransaction(transactionRequest TransactionRequest) Transaction {
	tsc := Transaction{
		TransactionID:        uuid.New().String(),
		ConsumerID:           transactionRequest.ConsumerID,
		ProductID:            transactionRequest.ProductID,
		TransactionTimeStamp: time.Now(),
		TransactionStatus:    constants.TransactionStatusPending,
		Quantity:             transactionRequest.Quantity,
	}
	return tsc
}
