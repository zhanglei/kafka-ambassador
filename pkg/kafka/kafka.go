package kafka

import (
	"sync"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"
	"go.uber.org/ratelimit"
)

type Mode int
type Source int

const (
	Fallback Mode = iota
	Always
	Disable
)

const (
	FromWAL Source = iota
	Direct
)

type T struct {
	Producer    *kafka.Producer
	Logger      logger.Logger
	Config      *Config
	wal         wal.I
	mutex       *sync.RWMutex
	resendMutex *sync.Mutex
	cb          *gobreaker.TwoStepCircuitBreaker
	rl          ratelimit.Limiter
}

type Config struct {
	ResendPeriod     time.Duration
	WalMode          Mode
	AlwaysWalTopics  []string
	DisableWalTopics []string
	WalDirectory     string
	ResendRateLimit  int
}

func (p *T) Init(kafkaParams *kafka.ConfigMap, prom *prometheus.Registry) error {
	var err error
	p.Logger.Info("Creating Kafka producer")
	for k, v := range *kafkaParams {
		p.Logger.Infof("Kafka param %s: %v", k, v)
	}
	p.Producer, err = kafka.NewProducer(kafkaParams)
	cbSettings := gobreaker.Settings{
		Name:          "kafka",
		MaxRequests:   1,
		Interval:      0,
		Timeout:       0,
		OnStateChange: p.setCBState,
	}
	p.cb = gobreaker.NewTwoStepCircuitBreaker(cbSettings)
	p.mutex = new(sync.RWMutex)
	p.resendMutex = new(sync.Mutex)
	p.wal, err = wal.New(p.Config.WalDirectory, prom, p.Logger)
	p.rl = ratelimit.New(p.Config.ResendRateLimit)
	if err != nil {
		p.Logger.Errorf("Could not create kafka producer due to: %v", err)
		return err
	}
	registerMetrics(prom)
	p.Logger.Info("Starting up kafka events tracker")
	go p.producerEventsHander()
	p.Logger.Infof("Starting up kafka resend process with period %s", p.Config.ResendPeriod.String())
	go p.ReSend()
	// monitor CB state every 10 seconds
	go p.trackCBState(10 * time.Second)
	return err
}

func (p *T) ReSend() {
	ticker := time.NewTicker(p.Config.ResendPeriod)

	for _ = range ticker.C {
		p.resendMutex.Lock()
		if p.cb.State() != gobreaker.StateOpen {
			p.Logger.Info("Running resend, as CB is not tripped")
			now := time.Now().Unix()
			for r := range p.wal.Iterate() {
				rtime, err := wal.GetTime(r)
				if err != nil {
					rtime = time.Now()
				}
				if now-rtime.Unix() > int64(p.Config.ResendPeriod.Seconds()) {
					if p.cb.State() != gobreaker.StateOpen {
						p.rl.Take()
						p.produce(r.Topic, r.Payload, FromWAL)
					}
				}
			}

			p.Logger.Info("Running compaction on the database")
			p.wal.CompactAll()
		} else {
			p.Logger.Info("CB is closed, skipping resend")
		}
		p.resendMutex.Unlock()
	}
}

func (p *T) Send(topic string, message []byte) {
	if p.cb.State() != gobreaker.StateOpen {
		p.produce(topic, message, Direct)
		if (p.Config.WalMode == Always && !p.isDisableWal(topic)) || p.isAlwaysWal(topic) {
			p.Logger.Debugf("Storing message to topic: %s into WAL", topic)
			p.wal.SetRecord(topic, message)
		}
		msgSent.With(prometheus.Labels{"topic": topic}).Inc()
	} else {
		p.Logger.Infof("Storing message to topic: %s into WAL as CB is not ready", topic)
		p.wal.SetRecord(topic, message)
	}
}

func (p *T) produce(topic string, message []byte, opaque interface{}) {
	p.Logger.Debugf("Sending to topic: [%s] message %s", topic, string(message))
	p.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:  message,
		Opaque: opaque,
	}
}

func (p *T) producerEventsHander() {
	for e := range p.Producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			success, err := p.cb.Allow()
			m := ev
			if m.TopicPartition.Error != nil {
				msgNOK.With(prometheus.Labels{
					"topic": *m.TopicPartition.Topic,
					"error": m.TopicPartition.Error.Error()}).Inc()
				p.Logger.Infof("could not send message to kafka due to: %s", m.TopicPartition.Error.Error())
				p.wal.SetRecord(*m.TopicPartition.Topic, m.Value)
				if err != nil {
					// We are not allowed to do anything
					break
				}
				success(false)
			} else {
				msgOK.With(prometheus.Labels{"topic": *m.TopicPartition.Topic}).Inc()
				if m.Opaque == FromWAL || p.isAlwaysWal(*m.TopicPartition.Topic) {
					crc := wal.Uint32ToBytes(uint32(wal.CrcSum(m.Value)))
					p.Logger.Debugf("removing CRC: %s", string(crc))
					p.wal.Del(crc)
				}
				if err != nil {
					// We are not allowed to do anything
					break
				}
				success(true)
			}
		default:
			// fmt.Printf("Ignored event: %s\n", ev)
		}
	}
}
func (p *T) isAlwaysWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i, _ := range p.Config.AlwaysWalTopics {
		if p.Config.AlwaysWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) isDisableWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i, _ := range p.Config.DisableWalTopics {
		if p.Config.DisableWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) setCBState(name string, from, to gobreaker.State) {
	switch to {
	case gobreaker.StateClosed:
		cbState.With(prometheus.Labels{"name": name, "state": "closed"}).Inc()
	case gobreaker.StateHalfOpen:
		cbState.With(prometheus.Labels{"name": name, "state": "half"}).Inc()
	case gobreaker.StateOpen:
		cbState.With(prometheus.Labels{"name": name, "state": "open"}).Inc()
	}
}

func (p *T) trackCBState(period time.Duration) {
	ticker := time.NewTicker(p.Config.ResendPeriod)

	for _ = range ticker.C {
		switch s := p.cb.State(); s {
		case gobreaker.StateClosed:
			cbCurrentState.Set(0)
		case gobreaker.StateHalfOpen:
			cbCurrentState.Set(0.5)
		case gobreaker.StateOpen:
			cbCurrentState.Set(1)
		}
	}
}
