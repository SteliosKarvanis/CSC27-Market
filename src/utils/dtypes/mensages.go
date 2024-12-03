package dtypes

type TransactionRequest struct {
	ConsumerID string `json:"consumer_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
}

type ConsultAvailabilityRequest struct {
	ProductID string `json:"product_id"`
}

type ConsultAvailabilityResponse struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type TransactionResponse struct {
	TransactionID     string `json:"transaction_id"`
	TransactionStatus string `json:"transaction_status"`
}
