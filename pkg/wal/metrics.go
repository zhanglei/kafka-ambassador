package wal

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
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
	walMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_messages",
			Help: "Number of wal records remaining in WAL",
		},
	)
	ldbStatsWriteDelayCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_write_delay_cnt",
			Help: "Write Delay counter",
		},
	)
	ldbStatsWriteDelayDuration = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_write_delay_duration",
			Help: "Write delay milliseconds",
		},
	)
	ldbStatsWritePaused = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_writes_paused",
			Help: "Paused 1, non-paused = 0",
		},
	)
	ldbStatsAliveSnapshots = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_alive_snapshots",
			Help: "Amount of alive leveldb snapshots",
		},
	)
	ldbStatsAliveIterators = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_alive_iterators",
			Help: "Amount of alive iterators over leveldb",
		},
	)
	ldbStatsIOWrite = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "wal_leveldb_io_write",
			Help: "IO stats for write operation",
		},
	)
	ldbStatsIORead = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "wal_leveldb_io_read",
			Help: "IO stats for read operations",
		},
	)
	ldbStatsBlockCacheSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_block_cache_size",
			Help: "Block cache size",
		},
	)
	ldbStatsOpenedTablesCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_opened_tables",
			Help: "Number of leveldb opened tables",
		},
	)
	ldbStatsLevelSizes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_level_size",
			Help: "Size of level table",
		},
		[]string{"id"},
	)
	ldbStatsLevelTablesCounts = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_level_tables",
			Help: "Number of leveldb tables",
		},
		[]string{"id"},
	)
	ldbStatsLevelRead = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_level_reads",
			Help: "Number of wal leveldb reads",
		},
		[]string{"id"},
	)
	ldbStatsLevelWrite = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_level_writes",
			Help: "Number of wal leveldb writes",
		},
		[]string{"id"},
	)
	ldbStatsLevelDurations = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wal_leveldb_level_durations",
			Help: "Time spent on leveldb actions",
		},
		[]string{"id"},
	)
)

func registerMetrics(prom *prometheus.Registry) {
	prom.MustRegister(
		msgReads,
		msgWrites,
		msgDeletes,
		walMessages,
		ldbStatsWriteDelayCount,
		ldbStatsWriteDelayDuration,
		ldbStatsWritePaused,
		ldbStatsAliveSnapshots,
		ldbStatsAliveIterators,
		ldbStatsIOWrite,
		ldbStatsIORead,
		ldbStatsBlockCacheSize,
		ldbStatsOpenedTablesCount,
		ldbStatsLevelSizes,
		ldbStatsLevelTablesCounts,
		ldbStatsLevelRead,
		ldbStatsLevelWrite,
		ldbStatsLevelDurations,
	)
}

func (w *Wal) collectPeriodically(period time.Duration) {
	ticker := time.NewTicker(period)

	stats := new(leveldb.DBStats)
	for _ = range ticker.C {
		err := w.storage.Stats(stats)
		if err != nil {
			//
		}
		statsToProm(stats)
		w.setWALMessages()
	}
}

func statsToProm(stats *leveldb.DBStats) {
	ldbStatsWriteDelayCount.Set(float64(stats.WriteDelayCount))
	ldbStatsWriteDelayDuration.Set(float64(stats.WriteDelayDuration))
	if stats.WritePaused {
		ldbStatsWritePaused.Set(1)
	} else {
		ldbStatsWritePaused.Set(0)
	}
	ldbStatsAliveSnapshots.Set(float64(stats.AliveSnapshots))
	ldbStatsAliveIterators.Set(float64(stats.AliveIterators))
	ldbStatsIOWrite.Set(float64(stats.IOWrite))
	ldbStatsIORead.Set(float64(stats.IORead))
	ldbStatsBlockCacheSize.Set(float64(stats.BlockCacheSize))
	ldbStatsOpenedTablesCount.Set(float64(stats.OpenedTablesCount))
	// these are array metrics
	for i, _ := range stats.LevelSizes {
		levelMetric(ldbStatsLevelSizes, i, stats.LevelSizes[i])
	}
	for i, _ := range stats.LevelTablesCounts {
		levelMetric(ldbStatsLevelSizes, i, stats.LevelTablesCounts[i])
	}
	for i, _ := range stats.LevelRead {
		levelMetric(ldbStatsLevelSizes, i, stats.LevelRead[i])
	}
	for i, _ := range stats.LevelWrite {
		levelMetric(ldbStatsLevelSizes, i, stats.LevelWrite[i])
	}
	for i, _ := range stats.LevelDurations {
		levelMetric(ldbStatsLevelSizes, i, stats.LevelDurations[i].Seconds)
	}
}

func levelMetric(m *prometheus.GaugeVec, i int, v interface{}) {
	switch v.(type) {
	case int64:
		m.WithLabelValues(fmt.Sprintf("%d", i)).Set(float64(v.(int64)))
	case float64:
		m.WithLabelValues(fmt.Sprintf("%d", i)).Set(v.(float64))
	}
}
