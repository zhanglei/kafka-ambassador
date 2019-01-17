package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

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
	)
}
