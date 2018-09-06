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
	cbState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
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
)

func registerMetrics(prom *prometheus.Registry) {
	prom.MustRegister(msgSent)
	prom.MustRegister(msgOK)
	prom.MustRegister(msgNOK)
	prom.MustRegister(cbState)
	prom.MustRegister(cbCurrentState)
}
