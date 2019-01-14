package kafka

import (
	"strings"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

var (
	defaults = map[string]interface{}{
		"producer.cb.interval":               "0",     // 0 is disable counters flush
		"producer.cb.timeout":                "20s",   // put CB into half open evry timeout
		"producer.cb.fails":                  "5",     // open CB if fails reached
		"producer.cb.requests":               "3",     // send requests during retry
		"producer.resend.period":             "33s",   // resend from WAL every
		"producer.resend.rate_limit":         "10000", // limit rate from WAL
		"producer.old_producer_kill_timeout": "10m",   // limit rate from WAL
	}
)

func ProducerConfig(c *viper.Viper) *Config {
	p := new(Config)
	// set Defaults
	for k, v := range defaults {
		c.SetDefault(k, v)
	}

	p.ResendPeriod = c.GetDuration("producer.resend.period")
	p.ResendRateLimit = c.GetInt("producer.resend.rate_limit")
	p.CBInterval = c.GetDuration("producer.cb.interval")
	p.CBTimeout = c.GetDuration("producer.cb.timeout")
	p.CBMaxFailures = uint32(c.GetInt32("producer.cb.fails"))
	p.CBMaxRequests = uint32(c.GetInt32("producer.cb.requests"))
	p.AlwaysWalTopics = c.GetStringSlice("producer.wal.always_topics")
	p.DisableWalTopics = c.GetStringSlice("producer.wal.disable_topics")
	p.WalDirectory = c.GetString("producer.wal.path")
	p.OldProducerKillTimeout = c.GetDuration("producer.old_producer_kill_timeout")
	switch mode := c.GetString("producer.wal.mode"); mode {
	case "fallback":
		p.WalMode = Fallback
	case "always":
		p.WalMode = Always
	case "disable":
		p.WalMode = Disable
	default:
		p.WalMode = Fallback
	}
	return p
}

func Viper2Config(c *viper.Viper) (kafka.ConfigMap, error) {
	sub := c.Sub("kafka")
	var cm, t kafka.ConfigMap
	var brokers string
	err := sub.Unmarshal(&cm)
	if err != nil {
		return nil, err
	}
	err = sub.Unmarshal(&t)
	if err != nil {
		return nil, err
	}
	// explicitly set bootstrap.servers has priority over brokers config
	if sub.IsSet("bootstrap.severs") {
		brokers = sub.GetString("bootstrap.servers")
	} else {
		brokers = strings.Join(sub.GetStringSlice("brokers"), ",")
	}

	flatten := config.Flatten(cm)
	flatten["bootstrap.servers"] = brokers

	// TODO: I believe it is possible to simplify this part, but so far it is 2 loops.
	for k, v := range flatten {
		cm[k] = v
	}

	// cleanup the raw data
	for k := range t {
		delete(cm, k)
	}
	return cm, nil
}

func KafkaParamsPathExists(kafkaParams *kafka.ConfigMap, path string) bool {
	value, err := kafkaParams.Get(path, "")
	if err != nil {
		return false
	}
	if value != nil && len(value.(string)) > 0 {
		return true
	}
	return false
}
