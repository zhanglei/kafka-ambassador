package kafka

import (
	"github.com/anchorfree/data-go/pkg/promutils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fastjson"
)

type MetricVec interface {
	prometheus.Collector
	Delete(labels prometheus.Labels) bool
}

func B2f(b bool) float64 {
	if b {
		return float64(1)
	}
	return float64(0)
}

func (p *T) dropProducerMetrics(producer_id string) {
	for _, m := range []MetricVec{msgInTransit, activeProducer, metricRDKafkaGlobal, metricRDKafkaBroker, metricRDKafkaTopic, metricRDKafkaPartition} {
		for _, l := range promutils.GetVectorLabels(m, prometheus.Labels{"producer_id": producer_id}) {
			m.Delete(l)
		}
	}
}

func (p *T) populateRDKafkaMetrics(stats *kafka.Stats) {
	var parser fastjson.Parser
	values, err := parser.Parse(stats.String())
	if err != nil {
		p.Logger.Errorf("Could not parse librdkafka stats event: %v", err)
		return
	}
	histoMetrics := []string{"min", "max", "avg", "p50", "p95", "p99"}
	producerID := string(values.GetStringBytes("name"))
	//librdkafka global metrics
	globalMetrics := []string{"replyq", "msg_cnt", "msg_size", "tx", "tx_bytes", "rx", "rx_bytes", "txmsgs", "txmsgs_bytes", "rxmsgs", "rxmsgs_bytes"}
	for _, m := range globalMetrics {
		metricRDKafkaGlobal.With(prometheus.Labels{
			"metric":      m,
			"producer_id": producerID,
		}).Set(values.GetFloat64(m))
	}
	//librdkafka broker metrics
	brokerMetrics := []string{"outbuf_cnt", "outbuf_msg_cnt", "waitresp_cnt", "waitresp_msg_cnt", "tx", "tx_bytes", "req_timeouts", "rx", "rx_bytes", "connects", "disconnects"}
	values.GetObject("brokers").Visit(func(key []byte, v *fastjson.Value) {
		brokerID := string(v.GetStringBytes("name"))
		metricRDKafkaBroker.With(prometheus.Labels{
			"metric":      "state_up",
			"producer_id": producerID,
			"broker":      brokerID,
			"window":      "",
		}).Set(B2f(string(values.GetStringBytes("state")) == "UP"))
		for _, m := range brokerMetrics {
			metricRDKafkaBroker.With(prometheus.Labels{
				"metric":      m,
				"producer_id": producerID,
				"broker":      brokerID,
				"window":      "",
			}).Set(values.GetFloat64(m))
		}
		for _, m := range []string{"int_latency", "outbuf_latency", "rtt"} {
			for _, window := range histoMetrics {
				metricRDKafkaBroker.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"broker":      brokerID,
					"window":      window,
				}).Set(v.GetFloat64(m, window))
			}
		}
	})
	//librdkafka topic metrics
	topicMetrics := []string{"batchsize", "batchcnt"}
	partitionMetrics := []string{"msgq_cnt", "bsgq_bytes", "xmit_msgq_cnt", "msgs_inflight"}
	values.GetObject("topics").Visit(func(key []byte, v *fastjson.Value) {
		topic := string(v.GetStringBytes("topic"))
		for _, m := range topicMetrics {
			for _, window := range histoMetrics {
				metricRDKafkaTopic.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"topic":       topic,
					"window":      window,
				}).Set(v.GetFloat64(m, window))
			}
		}
		//librdkafka topic-partition metrics
		for _, m := range partitionMetrics {
			v.GetObject("partitions").Visit(func(key []byte, pv *fastjson.Value) {
				metricRDKafkaPartition.With(prometheus.Labels{
					"metric":      m,
					"producer_id": producerID,
					"topic":       topic,
					"partition":   string(key),
				}).Set(pv.GetFloat64(m))
			})
		}
	})
}

var (
	msgSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_requests_cnt",
			Help: "Number of kafka requests sent",
		},
		[]string{"topic"},
	)
	msgOK = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_ack_cnt",
			Help: "Number of kafka ACKed requests received",
		},
		[]string{"topic"},
	)
	msgNOK = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_err_cnt",
			Help: "Number of kafka Errored requests",
		},
		[]string{"topic", "error"},
	)
	msgInTransit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_client_messages_in_transit",
			Help: "Number of kafka messages in transit",
		},
		[]string{"producer_id"},
	)
	msgDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_client_dropped_cnt",
			Help: "Number of kafka Errored requests which are dropped",
		},
		[]string{"topic", "error"},
	)
	eventIgnored = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_client_events_ignored_cnt",
			Help: "Number of kafka events which are ignored",
		},
	)
	cbState = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "producer_cb_state",
			Help: "Circuit Breaker state of Kafka",
		},
		[]string{"name", "state"},
	)
	cbCurrentState = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_cb_current_state",
			Help: "Circuit Breaker current state of Kafka",
		},
	)
	producerQueueLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "producer_kafka_queue_len",
			Help: "Number of messages and requests waiting to be transmitted to the broker as well as delivery reports queued for the application",
		},
	)
	libVersion = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "producer_kafka_librdkafka_version",
			Help: "Version of underlying librdkafka library",
		},
		[]string{"version"},
	)
	activeProducer = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_active_producer",
			Help: "Current active producer",
		},
		[]string{"producer_id"},
	)
	lastProducerStartTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_last_producer_start_time",
			Help: "Time when the freshest producer was started",
		},
	)
	metricCertExpirationTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_cert_expiration_time",
			Help: "Kafka producer certificat NotAfter",
		},
	)
	metricCaExpirationTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_ca_expiration_time",
			Help: "Kafka producer CA NotAfter",
		},
	)
	metricKafkaEventsQueueLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_events_queue_len",
			Help: "Kafka driver events queue length",
		},
	)
	metricRDKafkaGlobal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_global",
			Help: "librdkafka internal global metrics",
		},
		[]string{"producer_id", "metric"},
	)
	metricRDKafkaBroker = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_broker",
			Help: "librdkafka internal broker metrics",
		},
		[]string{"producer_id", "metric", "broker", "window"},
	)
	metricRDKafkaTopic = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_topic",
			Help: "librdkafka internal topic metrics",
		},
		[]string{"producer_id", "metric", "topic", "window"},
	)
	metricRDKafkaPartition = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rdkafka_partition",
			Help: "librdkafka internal partition metrics",
		},
		[]string{"producer_id", "metric", "topic", "partition"},
	)
)

func registerMetrics(prom *prometheus.Registry) {
	prom.MustRegister(msgSent,
		msgOK,
		msgNOK,
		msgDropped,
		cbState,
		cbCurrentState,
		producerQueueLen,
		eventIgnored,
		msgInTransit,
		libVersion,
		activeProducer,
		lastProducerStartTime,
		metricCertExpirationTime,
		metricCaExpirationTime,
		metricKafkaEventsQueueLen,
		metricRDKafkaGlobal,
		metricRDKafkaBroker,
		metricRDKafkaTopic,
		metricRDKafkaPartition,
	)
}
