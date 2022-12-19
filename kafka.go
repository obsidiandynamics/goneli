package goneli

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

/*
Interfaces.
*/

// KafkaConsumer specifies the methods of a minimal consumer.
type KafkaConsumer interface {
	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	Close() error
}

// KafkaConsumerProvider is a factory for creating KafkaConsumer instances.
type KafkaConsumerProvider func(conf *KafkaConfigMap) (KafkaConsumer, error)

// KafkaProducer specifies the methods of a minimal producer.
type KafkaProducer interface {
	Events() chan kafka.Event
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

// KafkaProducerProvider is a factory for creating KafkaProducer instances.
type KafkaProducerProvider func(conf *KafkaConfigMap) (KafkaProducer, error)

// StandardKafkaProducerProvider returns a factory for creating a conventional KafkaProducer, backed by the real client API.
func StandardKafkaProducerProvider() KafkaProducerProvider {
	return func(conf *KafkaConfigMap) (KafkaProducer, error) {
		return kafka.NewProducer(toKafkaNativeConfig(conf))
	}
}

/*
Standard provider implementations.
*/

// StandardKafkaConsumerProvider returns a factory for creating a conventional KafkaConsumer, backed by the real client API.
func StandardKafkaConsumerProvider() KafkaConsumerProvider {
	return func(conf *KafkaConfigMap) (KafkaConsumer, error) {
		return kafka.NewConsumer(toKafkaNativeConfig(conf))
	}
}

/*
Various helpers.
*/

func toKafkaNativeConfig(conf *KafkaConfigMap) *kafka.ConfigMap {
	result := kafka.ConfigMap{}
	for k, v := range *conf {
		result[k] = v
	}
	return &result
}

func copyKafkaConfig(configMap KafkaConfigMap) KafkaConfigMap {
	copy := KafkaConfigMap{}
	putAllKafkaConfig(configMap, copy)
	return copy
}

func putAllKafkaConfig(source, target KafkaConfigMap) {
	for k, v := range source {
		target[k] = v
	}
}

func isTimedOutError(err error) bool {
	kafkaError, ok := err.(kafka.Error)
	return ok && kafkaError.Code() == kafka.ErrTimedOut
}

func isFatalError(err error) bool {
	kafkaError, ok := err.(kafka.Error)
	return ok && kafkaError.IsFatal()
}

func setKafkaConfig(configMap KafkaConfigMap, key string, value interface{}) error {
	_, containsKey := configMap[key]
	if containsKey {
		return fmt.Errorf("cannot override configuration '%s'", key)
	}

	configMap[key] = value
	return nil
}

func setKafkaConfigs(configMap, toSet KafkaConfigMap) error {
	for k, v := range toSet {
		err := setKafkaConfig(configMap, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func containsPartition(partitions []kafka.TopicPartition, partition int32) bool {
	for _, p := range partitions {
		if p.Partition == partition {
			return true
		}
	}
	return false
}
