package main

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
}

func newProducerProvider(brokers []string) *ProducerProvider {
	provider := &ProducerProvider{}
	provider.brokers = brokers
	return provider
}

func getSamaraConfig() *sarama.Config {
	config := sarama.NewConfig()
	// config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Return.Successes = true
	// Retry
	config.Producer.Retry.Max = 10
	config.Producer.Transaction.Retry.Backoff = 1000
	return config
}

func (producerProvider *ProducerProvider) send(topic string, data []byte) error {
	producer := producerProvider.borrow()
	defer producerProvider.release(producer)

	// Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
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
		log.Printf("Producer: committed txn\n")
	} else {
		log.Printf("Producer: unable to commit txn %s\n", err)
		for {
			if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
				// fatal error. need to recreate producer.
				log.Printf("Producer: producer is in a fatal state, need to recreate it")
				break
			}
			// If producer is in abortable state, try to abort current transaction.
			if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
				err = producer.AbortTxn()
				if err != nil {
					// If an error occured just retry it.
					log.Printf("Producer: unable to abort transaction: %+v", err)
					continue
				}
				break
			}
			// if not you can retry
			err = producer.CommitTxn()
			if err != nil {
				log.Printf("Producer: unable to commit txn %s\n", err)
				continue
			}
		}
	}
	return err
}

func (p *ProducerProvider) generateProducerInstance() sarama.AsyncProducer {
	config := getSamaraConfig()
	suffix := p.transactionIdGenerator
	if config.Producer.Transaction.ID != "" {
		p.transactionIdGenerator++
		config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
	}
	producer, err := sarama.NewAsyncProducer(p.brokers, config)
	if err != nil {
		return nil
	}
	return producer
}

func (p *ProducerProvider) borrow() (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if len(p.producers) == 0 {
		for {
			producer = p.generateProducerInstance()
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

func (p *ProducerProvider) release(producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	p.producers = append(p.producers, producer)
}

func (p *ProducerProvider) clear() error {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for idx, producer := range p.producers {
		err := producer.Close()
		log.Printf("Producer %d closed\n", err)
		if err != nil {
			log.Printf("Failed to close producer %d: %v", idx, err)
			return err
		}
	}
	p.producers = p.producers[:0]
	return nil
}