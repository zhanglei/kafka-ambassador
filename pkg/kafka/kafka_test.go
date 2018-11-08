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
		"producer.wal.path":                     "",
		"socket.timeout.ms":                     1000, // mark connection as stalled
		"message.timeout.ms":                    6000, // try to deliver message with retries
		"max.in.flight.requests.per.connection": 20,
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
	flatten["socket.timeout.ms"] = 10
	flatten["message.timeout.ms"] = 2

	// TODO: I believe it is possible to simplify this part, but so far it is 2 loops.
	for k, v := range flatten {
		configMap[k] = v
	}

	p := &T{
		Logger: logger.Sugar(),
		Config: &Config{
			WalDirectory:    "",
			ResendRateLimit: 10,
			ResendPeriod:    0,
		},
	}
	p.Init(&configMap, prometheus.NewRegistry())
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ProduceRequest": sarama.NewMockProduceResponse(t).
			SetVersion(1).
			SetError("test", 0, sarama.ErrNetworkException),
	})
	time.Sleep(time.Second * 1)
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
