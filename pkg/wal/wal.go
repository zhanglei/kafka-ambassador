package wal

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal/pb"
	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type I interface {
	Close()
	Set([]byte, []byte) error
	Del([]byte) error
	Get([]byte) (*pb.Record, error)
	CompactAll() error
	Iterate(int64) chan *pb.Record
	Iterator() iterator.Iterator
	SetRecord(string, []byte) error
	Messages() int64
}
type Wal struct {
	logger  logger.Logger
	storage *leveldb.DB
	stopCh  chan bool
	batch   *leveldb.Batch
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

	batch := new(leveldb.Batch)

	wal := &Wal{
		storage: db,
		logger:  logger,
		stopCh:  make(chan bool),
		batch:   batch,
	}

	registerMetrics(prom)
	go wal.collectPeriodically(30 * time.Second)
	return wal, nil
}

func (w *Wal) Close() {
	w.storage.Close()
}

func (w *Wal) SetRecord(topic string, value []byte) error {
	r := pb.Record{
		Timestamp: ptypes.TimestampNow(),
		Crc:       CrcSum(value),
		Payload:   value,
		Topic:     topic,
	}

	key := Uint32ToBytes(r.Crc)
	b, err := ToBytes(r)
	if err != nil {
		return err
	}
	msgWrites.With(prometheus.Labels{"topic": topic}).Inc()
	return w.Set(key, b)
}

func (w *Wal) Set(key, payload []byte) error {
	// batch size is hardcoded at the moment, as we are considering switch over to some
	// other DB, which may or may not change the interface.
	// TODO: create leak-less interface for WAL
	batchSize := 100
	var err error

	if w.batch.Len() < batchSize {
		w.batch.Put(key, payload)
	} else {
		err = w.storage.Write(w.batch, nil)
		w.batch.Reset()
	}
	return err
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
func (w *Wal) Messages() (cnt int64) {
	iter := w.storage.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		cnt++
	}
	return cnt
}

func (w *Wal) setWALMessages() {
	cnt := w.Messages()
	walMessages.Set(float64(cnt))
}

func (w *Wal) Iterator() iterator.Iterator {
	return w.storage.NewIterator(nil, nil)
}

func (w *Wal) Iterate(limit int64) chan *pb.Record {
	var cnt int64
	c := make(chan *pb.Record)
	iter := w.storage.NewIterator(nil, nil)

	go func() {
		for iter.Next() {
			if limit != 0 && cnt <= limit {
				key := iter.Key()
				r, err := FromBytes(iter.Value())
				if err != nil {
					w.logger.Warnf("Could not read from record due to error %s", err)
					w.Del(key)
					continue
				}
				c <- r
			} else {
				break
			}
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
	defer os.Remove(tmpfile.Name()) // clean up
	if _, err := tmpfile.Write(content); err != nil {
		return false, err
	}
	if err := tmpfile.Close(); err != nil {
		return false, err
	}
	return true, nil
}
