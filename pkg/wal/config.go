package wal

import (
	"time"
)

type Config struct {
	Path                 string        `yaml:"path"`
	CollectMetricsPeriod time.Duration `yaml:"collect_metrics_period"`
	DeleteBatchTimeout   time.Duration `yaml:"delete_batch_timeout"`
	WriteBatchTimeout    time.Duration `yaml:"write_batch_timeout"`
	DeleteBatchSize      int           `yaml:"delete_batch_size"`
	WriteBatchSize       int           `yaml:"write_batch_size"`
	LogGCIdleTime        time.Duration `yaml:"log_gc_idle_time"`
	LogGCDiscardRatio    float64       `yaml:"log_gc_discard_ratio"`
	IteratorPrefetchSize int           `yaml:"iterator_prefetch_size"`
	WriteChSize          int           `yaml:"write_ch_size"`
	DeleteChSize         int           `yaml:"delete_ch_size"`
}

var DefaultConfig = Config{
	Path:                 "/tmp/wal",
	CollectMetricsPeriod: 30 * time.Second,
	WriteBatchTimeout:    5 * time.Second,
	DeleteBatchTimeout:   5 * time.Second,
	WriteBatchSize:       100,
	DeleteBatchSize:      100,
	LogGCIdleTime:        5 * time.Minute,
	LogGCDiscardRatio:    0.5,
	IteratorPrefetchSize: 1000,
	WriteChSize:          1000,
	DeleteChSize:         1000,
}
