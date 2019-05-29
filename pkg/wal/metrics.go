package wal

import (
	"time"

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
	walMessages = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_messages",
			Help: "Number of wal records remaining in WAL",
		},
	)
	writeBatchChLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_write_batch_ch_len",
			Help: "Number of objects in wal write channel",
		},
	)
	deleteBatchChLen = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "wal_delete_batch_ch_len",
			Help: "Number of objects in wal delete channel",
		},
	)
)

func registerMetrics(prom *prometheus.Registry) {
	prom.MustRegister(
		msgReads,
		msgWrites,
		msgDeletes,
		walMessages,
		writeBatchChLen,
		deleteBatchChLen,
	)
	prom.MustRegister(expvarCollector())
}

func (w *Wal) setWALMessagesMetric() {
	cnt := w.MessageCount()
	walMessages.Set(float64(cnt))
}

func (w *Wal) setWriteBatchChLenMetric() {
	writeBatchChLen.Set(float64(len(w.writeCh)))
}

func (w *Wal) setDeleteBatchChLenMetric() {
	deleteBatchChLen.Set(float64(len(w.deleteCh)))
}

func (w *Wal) collectPeriodically(period time.Duration) {
	for range time.Tick(period) {
		w.setWALMessagesMetric()
		w.setWriteBatchChLenMetric()
		w.setDeleteBatchChLenMetric()
	}
}

func expvarCollector() prometheus.Collector {
	return prometheus.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_disk_reads_total": prometheus.NewDesc(
			"badger_disk_reads_total",
			"NumReads has cumulative number of reads",
			nil,
			nil,
		),
		"badger_disk_writes_total": prometheus.NewDesc(
			"badger_disk_writes_total",
			"NumWrites has cumulative number of writes",
			nil,
			nil,
		),
		"badger_read_bytes": prometheus.NewDesc(
			"badger_read_bytes",
			"NumBytesRead has cumulative number of bytes read",
			nil,
			nil,
		),
		"badger_written_bytes": prometheus.NewDesc(
			"badger_written_bytes",
			"NumBytesWritten has cumulative number of bytes written",
			nil,
			nil,
		),
		"badger_lsm_level_gets_total": prometheus.NewDesc(
			"badger_lsm_level_gets_total",
			"NumLSMGets is number of LMS gets",
			[]string{"level"},
			nil,
		),
		"badger_lsm_bloom_hits_total": prometheus.NewDesc(
			"badger_lsm_bloom_hits_total",
			"NumLSMBloomHits is number of LMS bloom hits",
			[]string{"level"},
			nil,
		),
		"badger_gets_total": prometheus.NewDesc(
			"badger_gets_total",
			"NumGets is number of gets",
			nil,
			nil,
		),
		"badger_puts_total": prometheus.NewDesc(
			"badger_puts_total",
			"NumPuts is number of puts",
			nil,
			nil,
		),
		"badger_blocked_puts_total": prometheus.NewDesc(
			"badger_blocked_puts_total",
			"NumBlockedPuts is number of blocked puts",
			nil,
			nil,
		),
		"badger_memtable_gets_total": prometheus.NewDesc(
			"badger_memtable_gets_total",
			"NumMemtableGets is number of memtable gets",
			nil,
			nil,
		),
		"badger_lsm_size_bytes": prometheus.NewDesc(
			"badger_lsm_size_bytes",
			"LSMSize has size of the LSM in bytes",
			[]string{"dir"},
			nil,
		),
		"badger_vlog_size_bytes": prometheus.NewDesc(
			"badger_vlog_size_bytes",
			"VlogSize has size of the value log in bytes",
			[]string{"dir"},
			nil,
		),
		"badger_pending_writes_total": prometheus.NewDesc(
			"badger_pending_writes_total",
			"PendingWrites tracks the number of pending writes.",
			[]string{"dir"},
			nil,
		),
	})
}
