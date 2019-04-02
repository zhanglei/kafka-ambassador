package kafka

import (
	k "github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerI interface {
	String() string
	Produce(msg *k.Message, deliveryChan chan k.Event) error
	Events() chan k.Event
	ProduceChannel() chan *k.Message
	Len() int
	Flush(timeoutMs int) int
	Close()
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*k.Metadata, error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	OffsetsForTimes(times []k.TopicPartition, timeoutMs int) (offsets []k.TopicPartition, err error)
}
