package wal

import (
	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal/pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type I interface {
	Close()
	Set([]byte, []byte) error
	Del([]byte) error
	Get([]byte) (*pb.Record, error)
	CompactAll() error
	Iterate() chan *pb.Record
	SetRecord(string, []byte) error
}
type Wal struct {
	logger  logger.Logger
	storage *leveldb.DB
	stopCh  chan bool
}

func New(dir string, prom *prometheus.Registry, logger logger.Logger) (*Wal, error) {
	var s storage.Storage
	var err error
	if dir != "" {
		s, err = storage.OpenFile(dir, false)
		if err != nil {
			return nil, err
		}
	} else {
		s = storage.NewMemStorage()
	}

	db, err := leveldb.Open(s, nil)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		storage: db,
		logger:  logger,
		stopCh:  make(chan bool),
	}

	registerMetrics(prom)
	return wal, nil
}

func (w *Wal) Close() {
	w.storage.Close()
}

func (w *Wal) SetRecord(topic string, value []byte) error {
	r := new(pb.Record)
	// set current timestamp
	r.Now()
	r.SetPayload(value)
	key := pb.Uint32ToBytes(r.Crc)
	r.Topic = topic
	b, err := r.ToBytes()
	if err != nil {
		return err
	}
	msgWrites.With(prometheus.Labels{"topic": topic}).Inc()
	return w.Set(key, b)
}

func (w *Wal) Set(key, payload []byte) error {
	return w.storage.Put(key, payload, nil)
}

func (w *Wal) Get(key []byte) (*pb.Record, error) {
	r := new(pb.Record)
	b, err := w.storage.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	err = r.FromBytes(b)
	if err != nil {
		return nil, err
	}

	msgReads.With(prometheus.Labels{"topic": r.Topic}).Inc()
	return r, nil
}

func (w *Wal) Del(key []byte) error {
	msgDeletes.Inc()
	return w.storage.Delete(key, nil)
}

func (w *Wal) CompactAll() error {
	r := util.Range{
		Start: nil,
		Limit: nil,
	}
	return w.storage.CompactRange(r)
}

func (w *Wal) Iterate() chan *pb.Record {
	r := new(pb.Record)
	c := make(chan *pb.Record)
	iter := w.storage.NewIterator(nil, nil)

	go func() {
		for iter.Next() {
			key := iter.Key()
			err := r.FromBytes(iter.Value())
			if err != nil {
				w.logger.Warnf("Could not read from record due to error %s", err)
				w.Del(key)
				continue
			}
			c <- r
		}
		iter.Release()
		close(c)
	}()
	return c
}
