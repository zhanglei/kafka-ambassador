package kafka

import (
	"sync"
	"sync/atomic"
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

const defaultMaxInTransit = 500000

type T struct {
	Producer        *kafka.Producer
	Logger          logger.Logger
	Config          *Config
	wal             wal.I
	mutex           *sync.RWMutex
	resendMutex     *sync.Mutex
	cb              *gobreaker.TwoStepCircuitBreaker
	rl              ratelimit.Limiter
	transit         *int64
	halfOpenTransit *uint32
	once            *sync.Once
	lameDuck        bool
}

type Config struct {
	ResendPeriod     time.Duration
	MaxInTransit     uint32
	WalMode          Mode
	AlwaysWalTopics  []string
	DisableWalTopics []string
	WalDirectory     string
	ResendRateLimit  int
	CBTimeout        time.Duration
	CBInterval       time.Duration
	CBMaxRequests    uint32
	CBMaxFailures    uint32
}

func (p *T) Init(kafkaParams *kafka.ConfigMap, prom *prometheus.Registry) error {
	var err error
	p.Logger.Info("Creating Kafka producer")
	// for k, v := range *kafkaParams {
	// 	p.Logger.Infof("Kafka param %s: %v", k, v)
	// }
	p.Producer, err = kafka.NewProducer(kafkaParams)
	if p.Config.CBMaxFailures == 0 {
		p.Config.CBMaxFailures = 5
	}
	cbSettings := gobreaker.Settings{
		Name:          "kafka",
		MaxRequests:   p.Config.CBMaxRequests,
		Timeout:       p.Config.CBTimeout,
		Interval:      p.Config.CBInterval,
		OnStateChange: p.setCBState,
		ReadyToTrip:   p.readyToTrip,
	}
	p.cb = gobreaker.NewTwoStepCircuitBreaker(cbSettings)
	p.mutex = new(sync.RWMutex)
	p.resendMutex = new(sync.Mutex)
	p.wal, err = wal.New(p.Config.WalDirectory, prom, p.Logger)
	p.rl = ratelimit.New(p.Config.ResendRateLimit)
	// p.halfOpenRL = ratelimit.New()
	p.transit = new(int64)
	p.halfOpenTransit = new(uint32)
	p.once = new(sync.Once)
	if p.Config.MaxInTransit == 0 {
		p.Config.MaxInTransit = defaultMaxInTransit

	}
	if err != nil {
		p.Logger.Errorf("Could not create kafka producer due to: %v", err)
		return err
	}
	registerMetrics(prom)
	p.Logger.Info("Starting up kafka events tracker")
	go p.producerEventsHander()

	if p.Config.ResendPeriod != 0 {
		p.Logger.Infof("Starting up kafka resend process with period %s", p.Config.ResendPeriod.String())
		go p.ReSend()
	}
	// monitor CB state every 10 seconds
	go p.trackCBState(10 * time.Second)
	go p.kafkaStats(30 * time.Second)
	return err
}

func (p *T) ReSend() {
	ticker := time.NewTicker(p.Config.ResendPeriod)
	var recordLimit int64

	for range ticker.C {
		switch p.cb.State() {
		// default limit is no limit
		case gobreaker.StateOpen:
			p.Logger.Info("CB is open, skipping resend")
			continue
		case gobreaker.StateHalfOpen:
			recordLimit = 1
			p.Logger.Infof("Running resend with limit %d, as CB is half open", recordLimit)
		default:
			recordLimit = 0
			p.Logger.Infof("Running resend with limit %d, as CB is not open", recordLimit)
		}

		// we should not do resend in lame duck mode
		if !p.lameDuck {
			p.iterateLimit(recordLimit)
		}

		p.Logger.Info("Running compaction on the database")
		p.wal.CompactAll()
	}
}

func (p *T) iterateLimit(limit int64) {
	var c int64
	p.resendMutex.Lock()
	defer p.resendMutex.Unlock()
	now := time.Now().Unix()
	iter := p.wal.Iterator()
	defer iter.Release()
	for iter.Next() {
		// we should not do resend in lame duck mode
		if p.lameDuck {
			return
		}
		c++
		if limit == 0 || c <= limit {
			key := iter.Key()
			r, err := wal.FromBytes(iter.Value())
			if err != nil {
				p.Logger.Warnf("Could not read from record due to error %s", err)
				p.wal.Del(key)
				continue
			}
			rtime, err := wal.GetTime(r)
			if err != nil {
				rtime = time.Now()
			}
			if now-rtime.Unix() > int64(p.Config.ResendPeriod.Seconds()) {
				if p.cb.State() != gobreaker.StateOpen {
					p.rl.Take()
					p.produce(r.Topic, r.Payload, FromWAL)
				} else {
					p.Logger.Infof("We have got state change during retry, current state is %v, abort retry", p.cb.State())
					return
				}
			}
		} else {
			// limit is bigger or equal to counter
			return
		}
	}
}

func (p *T) Send(topic string, message []byte) {
	switch p.cb.State() {
	case gobreaker.StateClosed:
		// we should not process with we reached MaxTransit limit, it seems kafka is not responding
		if int64(p.Config.MaxInTransit) <= *p.transit {
			p.once.Do(p.cbOpen)
		}
		p.produce(topic, message, Direct)
		if (p.Config.WalMode == Always && !p.isDisableWal(topic)) || p.isAlwaysWal(topic) {
			p.Logger.Debugf("Storing message to topic: %s into WAL", topic)
			p.wal.SetRecord(topic, message)
		}
	case gobreaker.StateHalfOpen:
		if uint32(*p.halfOpenTransit) <= p.Config.CBMaxRequests {
			p.produce(topic, message, Direct)
			atomic.AddUint32(p.halfOpenTransit, 1)
		} else {
			p.Logger.Debugf("Storing message to topic: %s into WAL as CB in Half-Open state", topic)
			p.wal.SetRecord(topic, message)
		}
	default:
		p.Logger.Debugf("Storing message to topic: %s into WAL as CB is not ready", topic)
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
	atomic.AddInt64(p.transit, 1)
	msgSent.With(prometheus.Labels{"topic": topic}).Inc()
	msgInTransit.Add(1)
}

func (p *T) producerEventsHander() {
	for e := range p.Producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			success, skipCBUpdate := p.cb.Allow()
			m := ev
			atomic.AddInt64(p.transit, -1)
			msgInTransit.Add(-1)
			if m.TopicPartition.Error != nil {
				msgNOK.With(prometheus.Labels{
					"topic": *m.TopicPartition.Topic,
					"error": m.TopicPartition.Error.Error()}).Inc()
				p.Logger.Debugf("could not send message to kafka due to: %s", m.TopicPartition.Error.Error())
				// we store messages which can be retried only
				if canRetry(m.TopicPartition.Error) {
					p.wal.SetRecord(*m.TopicPartition.Topic, m.Value)
				} else {
					// we could put the message into some malformed topic or similar
					crc := wal.Uint32ToBytes(uint32(wal.CrcSum(m.Value)))
					p.Logger.Infof("Dropped message CRC: %s as we can't retry it due to: %s", string(crc), m.TopicPartition.Error.Error())
					p.wal.Del(crc)
					msgDropped.With(prometheus.Labels{
						"topic": *m.TopicPartition.Topic,
						"error": m.TopicPartition.Error.Error()}).Inc()
				}
				if skipCBUpdate != nil {
					success(false)
				}
			} else {
				msgOK.With(prometheus.Labels{"topic": *m.TopicPartition.Topic}).Inc()
				if m.Opaque == FromWAL || p.isAlwaysWal(*m.TopicPartition.Topic) {
					crc := wal.Uint32ToBytes(uint32(wal.CrcSum(m.Value)))
					p.Logger.Debugf("removing CRC: %s", string(crc))
					p.wal.Del(crc)
				}
				if skipCBUpdate != nil {
					success(true)
				}
			}
		default:
			eventIgnored.Inc()
		}
	}
}
func (p *T) isAlwaysWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i := range p.Config.AlwaysWalTopics {
		if p.Config.AlwaysWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) isDisableWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i := range p.Config.DisableWalTopics {
		if p.Config.DisableWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) kafkaStats(period time.Duration) {
	var ticker *time.Ticker
	if p.Config.ResendPeriod != 0 {
		ticker = time.NewTicker(p.Config.ResendPeriod)
		for range ticker.C {
			producerQueueLen.Set(float64(p.Producer.Len()))
		}
	}
}

func canRetry(err error) bool {
	switch e := err.(kafka.Error); e.Code() {
	// topics are wrong
	case kafka.ErrTopicException, kafka.ErrUnknownTopic:
		return false
	// message is incorrect
	case kafka.ErrMsgSizeTooLarge, kafka.ErrInvalidMsgSize:
		return false
	default:
		return true
	}
}

func (p *T) QueueIsEmpty() bool {
	p.Logger.Infof("Messages in queue: %d", *p.transit)
	if *p.transit <= 0 {
		return true
	}
	return false
}

func (p *T) LameDuckMode() {
	p.lameDuck = true
}
