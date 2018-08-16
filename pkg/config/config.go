package config

import (
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

// Config is a handler for possible configuration parameters, such as RemoteConfig and ConfigWatch.
type T struct {
	Filename  string
	EnvPrefix string
}

func (c *T) ReadConfig(defaults map[string]interface{}) (*viper.Viper, error) {
	v := viper.New()
	v.SetEnvPrefix(c.EnvPrefix)

	for key, value := range defaults {
		v.SetDefault(key, value)
	}

	v.AutomaticEnv()
	if c.Filename == "" {
		c.Filename = v.GetString("config")
	}
	v.SetConfigFile(c.Filename)

	err := v.ReadInConfig()
	return v, err
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

	flatten := Flatten(cm)
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

func Flatten(value interface{}) map[string]interface{} {
	return flattenPrefixed(value, "")
}

func flattenPrefixed(value interface{}, prefix string) map[string]interface{} {
	m := make(map[string]interface{})
	flattenPrefixedToResult(value, prefix, m)
	return m
}

func flattenPrefixedToResult(value interface{}, prefix string, m map[string]interface{}) {
	base := ""
	if prefix != "" {
		base = prefix + "."
	}

	original := reflect.ValueOf(value)
	kind := original.Kind()
	if kind == reflect.Ptr || kind == reflect.Interface {
		original = reflect.Indirect(original)
		kind = original.Kind()
	}
	t := original.Type()

	switch kind {
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			break
		}
		for _, childKey := range original.MapKeys() {
			childValue := original.MapIndex(childKey)
			flattenPrefixedToResult(childValue.Interface(), base+childKey.String(), m)
		}
	case reflect.Struct:
		for i := 0; i < original.NumField(); i += 1 {
			childValue := original.Field(i)
			childKey := t.Field(i).Name
			flattenPrefixedToResult(childValue.Interface(), base+childKey, m)
		}
	default:
		if prefix != "" {
			m[prefix] = value
		}
	}
}
