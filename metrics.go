package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricRequestsIn = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_requests_in",
			Help: "Number of kafka-ambassador requests received",
		},
		[]string{"host", "topic", "status"},
	)
	metricBytesIn = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_bytes_in",
			Help: "Kafka-ambassador bytes received",
		},
		[]string{"host", "topic", "status"},
	)
	metricRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_ambassador_request_duration",
			Help:    "kafka-ambassador request duration in seconds",
			Buckets: []float64{0.005, 0.05, 0.100, 1, 10, 30, 100, 600, 1800},
		},
		[]string{"topic", "status"},
	)
	metricKafkaRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_ambassador_kafka_request_duration",
			Help:    "Kafka-ambassador kafka request duration in seconds",
			Buckets: []float64{0.001, 0.005, 0.05, 0.100, 1, 10, 30, 100, 600, 1800},
		},
		[]string{"topic", "status"},
	)
	metricMessagesIn = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_messages_in",
			Help: "kafka-ambassador report messages processed",
		},
		[]string{"host", "topic"},
	)
	metricRequestsOut = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_requests_out",
			Help: "Number of kafka requests sent",
		},
		[]string{"host", "topic", "status"},
	)
	metricBytesOut = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_bytes_out",
			Help: "Kafka-ambassador bytes sent",
		},
		[]string{"host", "topic", "status"},
	)
	metricMessagesOut = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_ambassador_messages_out",
			Help: "kafka-ambassador messages sent",
		},
		[]string{"host", "topic"},
	)
)

func initMetrics() {
	prometheus.MustRegister(metricRequestsIn)
	prometheus.MustRegister(metricBytesIn)
	prometheus.MustRegister(metricRequestDuration)
	prometheus.MustRegister(metricKafkaRequestDuration)
	prometheus.MustRegister(metricRequestsOut)
	prometheus.MustRegister(metricBytesOut)
	prometheus.MustRegister(metricMessagesOut)
}
