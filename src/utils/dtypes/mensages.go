package dtypes

type TransactionRequest struct {
	ConsumerID string `json:"consumer_id"`
	ProductID  string `json:"product_id"`
	Quantity   int    `json:"quantity"`
}

type TransactionResponse struct {
	TransactionID     string `json:"transaction_id"`
	TransactionStatus string `json:"transaction_status"`
}

type ConsultProductRequest struct {
	ProductID string `json:"product_id"`
}

type ConsultProductResponse struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}
