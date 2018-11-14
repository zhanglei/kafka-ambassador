package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/optiopay/kafka/kafkatest"
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

// func newBroker(t *testing.T, address string) *sarama.MockBroker {
// 	seedBroker := sarama.NewMockBrokerAddr(t, 1, address)
// 	return seedBroker
// }

// func TestIterator(t *testing.T) {
// 	assert := assert.New(t)
// 	address := "localhost:12345"
// 	configMap := make(kafka.ConfigMap)
// 	logger := zap.NewExample()
// 	broker := newBroker(t, address)

// 	flatten := config.Flatten(configMap)
// 	flatten["bootstrap.servers"] = address
// 	flatten["socket.timeout.ms"] = 10
// 	flatten["message.timeout.ms"] = 2

// 	// TODO: I believe it is possible to simplify this part, but so far it is 2 loops.
// 	for k, v := range flatten {
// 		configMap[k] = v
// 	}

// 	p := &T{
// 		Logger: logger.Sugar(),
// 		Config: &Config{
// 			WalDirectory:    "",
// 			ResendRateLimit: 10,
// 			ResendPeriod:    0,
// 		},
// 	}
// 	p.Init(&configMap, prometheus.NewRegistry())
// 	broker.SetHandlerByMap(map[string]sarama.MockResponse{
// 		"ProduceRequest": sarama.NewMockProduceResponse(t).
// 			SetVersion(1).
// 			SetError("test", 0, sarama.ErrNetworkException),
// 	})
// 	for i := 0; i < 10; i++ {
// 		err := p.wal.SetRecord("test", []byte(fmt.Sprintf("my message %d", i)))
// 		assert.Nil(err)
// 	}
// 	time.Sleep(time.Second * 1)
// 	assert.Equal(int64(10), p.wal.Messages())
// 	// var n N
// 	// broker.SetNotifier(n.notify)

// 	p.iterateLimit(1)
// 	// logger.Sugar().Debugf("R: %v, W: %v", n.r, n.w)
// 	p.iterateLimit(0)

// 	broker.Close()
// }

func TestLifeCycle(t *testing.T) {
	assert := assert.New(t)
	address := "localhost:12345"
	server := kafkatest.NewServer()
	go server.Run(address)
	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()

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
			CBTimeout:       2 * time.Second,
			CBMaxRequests:   5,
			CBMaxFailures:   3,
		},
	}
	time.Sleep(time.Second * 1)
	p.Init(&configMap, prometheus.NewRegistry())

	assert.Equal(int64(0), p.wal.Messages())

	p.cbOpen()
	assert.Equal(gobreaker.StateOpen, p.cb.State())

	p.Send("test", []byte("message for WAL 1"))
	p.Send("test", []byte("message for WAL 2"))
	assert.Equal(int64(2), p.wal.Messages())

	// we should pass MaxRequests in CB state and then start to
	p.cbHalf()
	assert.Equal(gobreaker.StateHalfOpen, p.cb.State())
	for i := 0; uint32(i) <= p.Config.CBMaxRequests; i++ {
		p.Send("test", []byte(fmt.Sprintf("message for WAL: %d", i)))
	}
	p.Send("test", []byte("message for WAL 3"))
	assert.Equal(int64(3), p.wal.Messages())

	p.cbClose()
	assert.Equal(gobreaker.StateClosed, p.cb.State())
	p.Send("test", []byte("message for WAL 4"))
	assert.Equal(int64(3), p.wal.Messages())

	p.Producer.Close()
	server.Close()
}
