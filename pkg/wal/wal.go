package wal

import (
	"fmt"
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
	"github.com/syndtr/goleveldb/leveldb/opt"
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

type KV struct {
	K []byte
	V []byte
}

type Wal struct {
	logger  logger.Logger
	storage *leveldb.DB
	stopCh  chan bool
	batch   *leveldb.Batch
	ch      chan KV
}

func New(dir string, prom *prometheus.Registry, logger logger.Logger) (*Wal, error) {
	var s storage.Storage
	var err error
	if dir != "" {
		ok, err := isWritable(dir)
		if !ok {
			logger.Fatalf("The WAL folder does not work due to: %s", err)
		}
		readOnly := false
		s, err = storage.OpenFile(dir, readOnly)
		if err != nil {
			return nil, err
		}
	} else {
		s = storage.NewMemStorage()
	}

	o := &opt.Options{}

	o.BlockSize = 32 * opt.KiB
	o.BlockCacheCapacity = 20 * opt.MiB
	o.CompactionTableSize = 20 * opt.MiB
	o.CompactionL0Trigger = 4
	//o.CompactionTableSizeMultiplier = 2
	o.CompactionTotalSize = 100 * opt.MiB
	//o.CompactionTotalSizeMultiplier = 2
	o.WriteBuffer = 40 * opt.MiB
	o.WriteL0PauseTrigger = 36
	o.WriteL0SlowdownTrigger = 24
	o.DisableCompactionBackoff = true

	db, err := leveldb.Open(s, o)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)

	wal := &Wal{
		storage: db,
		logger:  logger,
		stopCh:  make(chan bool),
		batch:   batch,
		ch:      make(chan KV, 3000),
	}

	registerMetrics(prom)
	go wal.collectPeriodically(30 * time.Second)
	go func() {
		for {
			time.Sleep(3 * time.Second)
			for _, s := range []string{
				//"iostats",
				//"leveldb.num-files-at-level0",
				//"leveldb.num-files-at-level1",
				//"leveldb.num-files-at-level2",
				//"leveldb.num-files-at-level3",
				//"leveldb.num-files-at-level4",
				//"leveldb.stats",
				//"leveldb.iostats",
				//"leveldb.writedelay",
				//"leveldb.sstables",
				"leveldb.blockpool",
				//"leveldb.cachedblock",
				//"leveldb.openedtables",
				//"leveldb.alivesnaps",
				//"leveldb.aliveiters",
			} {
				v, _ := db.GetProperty(s)
				if err == nil {
					wal.logger.Infof("--- property %s", s)
					fmt.Println(v)
				} else {
					wal.logger.Infof("--- property err %v", err)
				}

			}
		}
	}()
	for _ = range []int{1, 2, 3} {
		go func() {
			batchSize := 1000
			b := new(leveldb.Batch)
			for kv := range wal.ch {
				//wal.Set(kv.K, kv.V)
				if b.Len() < batchSize {
					b.Put(kv.K, kv.V)
				} else {
					wal.storage.Write(b, nil)
					b.Reset()
				}
			}
		}()
	}
	return wal, nil
}

func (w *Wal) Close() {
	w.storage.Close()
}

func (w *Wal) SetRecord(topic string, value []byte) error {
	r := pb.Record{
		Timestamp: ptypes.TimestampNow(),
		//Crc:       CrcSum(value),
		Crc:     CrcSum(value) + uint32(time.Now().UnixNano()%4000000000), // +++++++++++++++++++++++++++++ DEBUG !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		Payload: value,
		Topic:   topic,
	}

	key := Uint32ToBytes(r.Crc)
	b, err := ToBytes(r)
	if err != nil {
		return err
	}
	msgWrites.With(prometheus.Labels{"topic": topic}).Inc()
	return w.Set(key, b) //debug
	//w.ch <- KV{K: key, V: b} //debug
	return nil //debug
}

func (w *Wal) Set(key, payload []byte) error {
	// batch size is hardcoded at the moment, as we are considering switch over to some
	// other DB, which may or may not change the interface.
	// TODO: create leak-less interface for WAL
	var err error
	/*
		batchSize := 1000
		if w.batch.Len() < batchSize {
			w.batch.Put(key, payload)
		} else {
			err = w.storage.Write(w.batch, nil)
			w.batch.Reset()
		}
	*/
	err = w.storage.Put(key, payload, &opt.WriteOptions{})
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
