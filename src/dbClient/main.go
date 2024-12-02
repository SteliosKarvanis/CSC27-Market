package main

import (
	"csc27/utils/consumer"
	"csc27/utils/dtypes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"os"
)

func main() {
	group := "transactions-group2"
	// brokers := []string{"localhost:29092"}
	brokers := []string{"broker-1:19092", "broker-2:19092", "broker-3:19092", "broker-4:19092"}
	topics := []string{"transactions"}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	//TODO: Fix this
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	port := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(:%s)/kafka_stock", user, password, port)
	println(dsn)
	sqlDB, _ := sql.Open("mysql", dsn)
	gormDB, _ := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{})

	consumptionFun := func(message *sarama.ConsumerMessage) error {
		var transaction dtypes.Transaction

		json.Unmarshal(message.Value, &transaction)
		fmt.Printf("Saving on DB TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
			transaction.TransactionID, transaction.ProductID, transaction.Price, transaction.Quantity)

		tx := gormDB.Create(&transaction)
		if tx.Error != nil {
			log.Printf("Error saving on DB: %v", tx.Error)
			return tx.Error
		} else {
			log.Printf("Message saved on DB")
		}
		tx.Commit()
		return nil
	}

	gc := &consumer.GroupConsumer{ConsumeFun: consumptionFun, ConsumerConfig: config, Group: group, Topics: topics}
	gc.StartConsuming(brokers)
}
