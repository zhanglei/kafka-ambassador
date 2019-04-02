package kafka_mock

import (
	"github.com/anchorfree/kafka-ambassador/pkg/kafka"
	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

type MockedProducer struct {
	mock.Mock
}

func (m *MockedProducer) Send(topic string, message []byte) {
	m.Called(topic, message)
}
func (m *MockedProducer) ListTopics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockedProducer) GetProducersCount() int {
	args := m.Called()
	return args.Int(0)
}
func (m *MockedProducer) GetActiveProducerID() uint {
	args := m.Called()
	return args.Get(0).(uint)
}
func (m *MockedProducer) GetProducer() *kafka.ProducerWrapper {
	args := m.Called()
	return args.Get(0).(*kafka.ProducerWrapper)
}
func (m *MockedProducer) GenerateProducerID() uint {
	args := m.Called()
	return args.Get(0).(uint)
}
func (m *MockedProducer) AddActiveProducer(kafka.ProducerI, *k.ConfigMap) error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockedProducer) Init(*k.ConfigMap, *prometheus.Registry) error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockedProducer) ReSend() {
	m.Called()
}
func (m *MockedProducer) QueueIsEmpty() bool {
	args := m.Called()
	return args.Bool(0)
}
func (m *MockedProducer) Shutdown() {
	m.Called()
}
