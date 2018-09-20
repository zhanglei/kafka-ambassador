package wal

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	msgReads = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "wal_reads",
			Help: "Number of wal reads",
		},
		[]string{"topic"},
	)
	msgWrites = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "wal_writes",
			Help: "Number of wal records written",
		},
		[]string{"topic"},
	)
	msgDeletes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "wal_deletes",
			Help: "Number of wal records deleted",
		},
	)
)

func registerMetrics(prom *prometheus.Registry) {
	prom.MustRegister(msgReads)
	prom.MustRegister(msgWrites)
	prom.MustRegister(msgDeletes)
}
