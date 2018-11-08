package kafka

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Shopify/sarama"
	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	defaults map[string]interface{} = map[string]interface{}{
		"producer.wal.path":                           "",
		"kafka.socket.timeout.ms":                     1000, // mark connection as stalled
		"kafka.message.timeout.ms":                    6000, // try to deliver message with retries
		"kafka.max.in.flight.requests.per.connection": 20,
	}
)

func newBroker(t *testing.T, address string) *sarama.MockBroker {
	seedBroker := sarama.NewMockBrokerAddr(t, 1, address)
	return seedBroker
}

type N struct {
	w int
	r int
}

func (n *N) notify(bytesRead, bytesWritten int) {
	n.w = bytesWritten
	n.r = bytesRead
}

func TestIterator(t *testing.T) {
	assert := assert.New(t)
	address := "localhost:12345"
	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()
	broker := newBroker(t, address)

	flatten := config.Flatten(configMap)
	flatten["bootstrap.servers"] = address
	flatten["kafka.socket.timeout.ms"] = 1
	flatten["kafka.message.timeout.ms"] = 2

	p := &T{
		Logger: logger.Sugar(),
		Config: &Config{
			WalDirectory:    "",
			ResendRateLimit: 10,
			ResendPeriod:    0,
		},
	}
	p.Init(&configMap, prometheus.NewRegistry())
	p.Send("test", []byte("my message"))
	for i := 0; i < 10; i++ {
		err := p.wal.SetRecord("test", []byte("my message"+string(i)))
		assert.Nil(err)

	}
	time.Sleep(time.Second * 1)
	assert.Equal(int64(10), p.wal.Messages())
	// var n N
	// broker.SetNotifier(n.notify)

	p.iterateLimit(1)
	// logger.Sugar().Debugf("R: %v, W: %v", n.r, n.w)
	p.iterateLimit(0)

	broker.Close()
}
