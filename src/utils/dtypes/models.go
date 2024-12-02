package dtypes


var Tables = []interface{}{&Transaction{}, &Product{}}

type Product struct {
	ProductID string  `json:"product_id" gorm:"primaryKey"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
}

type Transaction struct {
	TransactionID string  `json:"transaction_id" gorm:"primaryKey"`
	ProductID     string  `json:"product_id"`
	Price         float64 `json:"price"`
	Quantity      int     `json:"quantity"`
}
