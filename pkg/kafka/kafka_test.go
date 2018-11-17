package kafka

// import (
// 	"fmt"
// 	"testing"
// 	"time"

// 	"github.com/confluentinc/confluent-kafka-go/kafka"
// 	"github.com/prometheus/client_golang/prometheus"
// 	"github.com/sony/gobreaker"

// 	"github.com/anchorfree/kafka-ambassador/pkg/config"
// 	"github.com/optiopay/kafka/kafkatest"
// 	"github.com/stretchr/testify/assert"
// 	"go.uber.org/zap"
// )

// func TestLifeCycle(t *testing.T) {
// 	assert := assert.New(t)
// 	address := "127.0.0.1:12345"
// 	server := kafkatest.NewServer()
// 	go server.Run(address)
// 	configMap := make(kafka.ConfigMap)
// 	logger := zap.NewExample()

// 	flatten := config.Flatten(configMap)
// 	flatten["bootstrap.servers"] = address
// 	flatten["socket.timeout.ms"] = 10
// 	flatten["message.timeout.ms"] = 2

// 	for k, v := range flatten {
// 		configMap[k] = v
// 	}

// 	p := &T{
// 		Logger: logger.Sugar(),
// 		Config: &Config{
// 			WalDirectory:    "",
// 			ResendRateLimit: 10,
// 			ResendPeriod:    0,
// 			CBTimeout:       2 * time.Second,
// 			CBMaxRequests:   5,
// 			CBMaxFailures:   3,
// 		},
// 	}
// 	time.Sleep(time.Second * 5)
// 	p.Init(&configMap, prometheus.NewRegistry())

// 	assert.Equal(int64(0), p.wal.Messages())
// 	assert.Equal(gobreaker.StateClosed, p.cb.State())

// 	p.Send("test", []byte("message for WAL 1"))
// 	p.Send("test", []byte("message for WAL 2"))
// 	assert.Equal(int64(0), p.wal.Messages())

// 	server.Close()
// 	time.Sleep(time.Second * 5)
// 	// We should immediatelly set to Open after all brokers are down
// 	assert.Equal(gobreaker.StateOpen, p.cb.State())
// 	for i := 0; uint32(i) <= p.Config.CBMaxRequests; i++ {
// 		p.Send("test", []byte(fmt.Sprintf("message for WAL: %d", i)))
// 	}
// 	p.Send("test", []byte("message for WAL 3"))
// 	assert.Equal(int64(3), p.wal.Messages())

// 	p.cbClose()
// 	assert.Equal(gobreaker.StateClosed, p.cb.State())
// 	p.Send("test", []byte("message for WAL 4"))
// 	assert.Equal(int64(3), p.wal.Messages())

// 	p.Producer.Close()
// 	server.Close()
// }
