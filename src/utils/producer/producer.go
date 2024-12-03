package producer

import (
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type ProducerProvider struct {
	transactionIdGenerator int32
	producersLock          sync.Mutex
	producers              []sarama.AsyncProducer
	brokers                []string
	producerProviderFunc   func() sarama.AsyncProducer
}

func NewProducerProvider(brokers []string) *ProducerProvider {
	provider := &ProducerProvider{}
	provider.brokers = brokers
	provider.producerProviderFunc = func() sarama.AsyncProducer {
		config := GetSamaraConfig()
		suffix := provider.transactionIdGenerator
		// Append transactionIdGenerator to current config.Producer.Transaction.ID to ensure transaction-id uniqueness.
		if config.Producer.Transaction.ID != "" {
			provider.transactionIdGenerator++
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
		}
		producer, err := sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			return nil
		}
		return producer
	}
	return provider
}

func GetSamaraConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Idempotent = true
	config.Producer.Return.Errors = false
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Transaction.Retry.Backoff = 10
	config.Producer.Transaction.ID = "txn_producer"
	config.Net.MaxOpenRequests = 1
	// Retry
	return config
}

func Send(producerProvider *ProducerProvider, topic string, data []byte) error {
	log.Printf("Provider: sending message to topic %s\n", topic)
	producer := producerProvider.Borrow()
	log.Printf("Producer selected")
	defer producerProvider.Release(producer)

	// Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
		producer.AbortTxn()
		log.Printf("unable to start txn %s\n", err)
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	// Produce some records in transaction
	producer.Input() <- msg
	// commit transaction
	err = producer.CommitTxn()
	if err == nil {
		producer.AbortTxn()
		log.Printf("Producer: committed txn\n")
	}
	return err
}

func (p *ProducerProvider) GenerateProducerInstance() sarama.AsyncProducer {
	log.Printf("Generating new producer instance")
	config := GetSamaraConfig()
	suffix := p.transactionIdGenerator
	p.transactionIdGenerator++
	config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
	producer, err := sarama.NewAsyncProducer(p.brokers, config)
	if err != nil {
		log.Printf("Failed to create producer: %v\n", err)
		panic(err)
	}
	log.Printf("Generated new producer instance")
	return producer
}

func (p *ProducerProvider) Borrow() (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if len(p.producers) == 0 {
		for {
			producer = p.producerProviderFunc()
			if producer != nil {
				return
			}
		}
	}
	index := len(p.producers) - 1
	producer = p.producers[index]
	p.producers = p.producers[:index]
	return
}

func (p *ProducerProvider) Release(producer sarama.AsyncProducer) error {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		err := producer.Close()
		if err != nil {
			log.Printf("Failed to close producer: %v\n", err)
		}
		return err
	}
	p.producers = append(p.producers, producer)
	return nil
}

func (p *ProducerProvider) Clear() error {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for idx, producer := range p.producers {
		err := producer.Close()
		if err != nil {
			log.Printf("Failed to close producer %d: %v", idx, err)
			return err
		}
	}
	p.producers = p.producers[:0]
	return nil
}
