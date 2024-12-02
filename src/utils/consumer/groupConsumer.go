package consumer

import (
	"context"
	"csc27/utils/dtypes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type GroupConsumer struct {
	Client   sarama.ConsumerGroup
	Topics   []string
	Messages chan *sarama.ConsumerMessage
	Db       *gorm.DB
}

func InitializeGroupConsumer(config *sarama.Config, group string, topics []string, brokers []string, dsn string) *GroupConsumer {
	db := InitializeDb(dsn)
	client := InitializeClient(brokers, group, config)
	return &GroupConsumer{
		Client:   client,
		Topics:   topics,
		Messages: make(chan *sarama.ConsumerMessage),
		Db:       db,
	}
}

func (gc *GroupConsumer) StartConsuming() {
	consumer := Consumer{
		ready:    make(chan bool),
		messages: gc.Messages,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := gc.Client.Consume(ctx, gc.Topics, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	go gc.ConsumeMessages()
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	//sigusr1 := make(chan os.Signal, 1)
	//signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		}
	}
	cancel()
	wg.Wait()
	if err := gc.Client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func (gc *GroupConsumer) ConsumeMessages() {
	for {
		select {
		case message := <-gc.Messages:
			log.Printf("Consuming message: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			err := gc.Save(message)
			if err != nil {
				log.Printf("Error consuming message: %v", err)
			}
		}
	}
}

func (gc *GroupConsumer) Save(message *sarama.ConsumerMessage) error {
	var transaction dtypes.Transaction

	json.Unmarshal(message.Value, &transaction)
	fmt.Printf("Saving on DB TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
		transaction.TransactionID, transaction.ProductID, transaction.Price, transaction.Quantity)
	var product dtypes.Product
	err := gc.Db.Where("product_id = ?", transaction.ProductID).First(&product).Error
	if err != nil {
		if transaction.Quantity > 0 {
			log.Printf("Creating new ProductID: %s, Quantity: %d, Price: %.2f", transaction.ProductID, transaction.Quantity, transaction.Price)
			prod := dtypes.Product{ProductID: transaction.ProductID, Price: transaction.Price, Quantity: transaction.Quantity}
			_ = gc.Db.Create(prod)
		} else {
			log.Printf("Product inexistent: %s", transaction.ProductID)
		}
	} else {
		newQuantity := product.Quantity + transaction.Quantity
		if newQuantity < 0 || math.Abs(product.Price-transaction.Price) > 0.09 {
			log.Printf("Invalid Transaction: %s", transaction.TransactionID)
		} else {
			log.Printf("Updating existing ProductID: %s, Quantity:%d\n", product.ProductID, newQuantity)
			gc.Db.Model(&product).Updates(dtypes.Product{Quantity: newQuantity})
		}
	}
	result := gc.Db.Create(&transaction)
	if result.Error != nil {
		log.Printf("Error saving on DB: %v", result.Error)
		return result.Error
	} else {
		log.Printf("Message saved on DB")
	}

	var tscs []dtypes.Transaction
	gc.Db.Find(&tscs)
	for _, tsc := range tscs {
		fmt.Printf("Fetch: TransactionID: %s, ProductID: %s, Price: %.2f, Quantity: %d\n",
			tsc.TransactionID, tsc.ProductID, tsc.Price, tsc.Quantity)
	}
	return nil
}

func InitializeDb(dsn string) *gorm.DB {
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Error connecting to DB: %v", err)
	}
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{})
	if err != nil {
		log.Printf("Error initializing DB: %v", err)
	}
	return gormDB
}
