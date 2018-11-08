package kafka

import (
	"strings"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

func ProducerConfig(c *viper.Viper) *Config {
	p := new(Config)
	p.ResendPeriod = c.GetDuration("producer.resend.period")
	p.ResendRateLimit = c.GetInt("producer.resend.rate_limit")
	p.AlwaysWalTopics = c.GetStringSlice("producer.wal.always_topics")
	p.DisableWalTopics = c.GetStringSlice("producer.wal.disable_topics")
	p.WalDirectory = c.GetString("producer.wal.path")
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

func KafkaParams(c *viper.Viper) (kafka.ConfigMap, error) {
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
	for k, _ := range t {
		delete(cm, k)
	}
	return cm, nil
}
