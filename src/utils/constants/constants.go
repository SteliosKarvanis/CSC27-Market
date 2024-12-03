package constants

const (
	TransactionRequestTopic          string = "transaction_request"
	TransactionResponseTopic         string = "transaction_response"
	ConsultAvailabilityRequestTopic  string = "consult_availability_request"
	ConsultAvailabilityResponseTopic string = "consult_availability_response"
)

const (
	TransactionStatusPending string = "Pending"
	TransactionStatusSuccess string = "Success"
	TransactionStatusFailed  string = "Failed"
)

var (
	BROKERS_CONTAINER = []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	BROKERS_HOST      = []string{"localhost:29092", "localhost:39092", "localhost:49092", "localhost:59092"}
)

const (
	TransactionRequestConsumerGroup string = "transactions-request-group"
	TransactionResponseConsumerGroup string = "transaction-response-group"
)
