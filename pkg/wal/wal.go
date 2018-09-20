package wal

import (
	"io/ioutil"
	"os"

	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal/pb"
	"github.com/golang/protobuf/ptypes"
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
		ok, err := isWritable(dir)
		if !ok {
			logger.Fatalf("The WAL folder does not work due to: %s", err)
		}
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
	r.Timestamp = ptypes.TimestampNow()
	r.Crc = CrcSum(value)
	r.Payload = value
	r.Topic = topic

	key := Uint32ToBytes(r.Crc)
	b, err := ToBytes(r)
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
	b, err := w.storage.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	r, err := FromBytes(b)
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
	c := make(chan *pb.Record)
	iter := w.storage.NewIterator(nil, nil)

	go func() {
		for iter.Next() {
			key := iter.Key()
			r, err := FromBytes(iter.Value())
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

func isWritable(path string) (bool, error) {
	content := []byte("temporary file's content")
	tmpfile, err := ioutil.TempFile(path, "wal-test")
	if err != nil {
		return false, err
	}
	if _, err := tmpfile.Write(content); err != nil {
		return false, err
	}
	if err := tmpfile.Close(); err != nil {
		return false, err
	}
	// this must be after all exception handling
	defer os.Remove(tmpfile.Name()) // clean up
	return true, nil
}
