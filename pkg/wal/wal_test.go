package wal

import (
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func crcSum(data []byte) int32 {
	crc := crc32.New(crcTable)
	crc.Write(data)
	return int32(crc.Sum32())
}

const (
	folder = "/tmp"
	topic  = "test"
)

var (
	testpayload = []byte("test value")
	crc         = crcSum(testpayload)
)

// func TestWriteReadMessage(t *testing.T) {
// 	assert := assert.New(t)
// 	s, err := New(folder)
// 	assert.Nil(err)
// 	s.SetRecord(topic, testpayload)
// 	r, err := s.Get([]byte("1234567"))
// 	assert.NotNil(r)
// 	assert.Equal(r.Topic, topic)
// 	s.Close()
// 	// Open data folder
// 	s, err = New(topic)
// 	assert.Nil(err)
// 	r, err = s.Get([]byte("1234567"))
// 	assert.NotNil(r)
// 	assert.Equal(r.Topic, "test")
// 	assert.Equal(r.Payload, testpayload)

// 	err = s.Del([]byte("1234567"))
// 	assert.Nil(err)

// 	r, err = s.Get([]byte("1234567"))
// 	assert.Nil(r)
// }

func TestWALIterator(t *testing.T) {
	var key, val []byte
	cnt := 0
	assert := assert.New(t)
	storage := storage.NewMemStorage()
	db, err := leveldb.Open(storage, nil)
	assert.Nil(err)
	db.Put([]byte("key1"), []byte("val1"), nil)
	db.Put([]byte("key2"), []byte("val2"), nil)
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		cnt++
		key = iter.Key()
		val = iter.Value()
		fmt.Printf("%s: %s\n", string(key), string(val))
	}
	assert.Equal(2, cnt)
	db.Put([]byte("key3"), []byte("val3"), nil)
	iter.Release()
	r := util.Range{
		Start: nil,
		Limit: nil,
	}
	db.CompactRange(r)

	db.Put([]byte("key4"), []byte("val4"), nil)
	iter = db.NewIterator(nil, nil)
	cnt = 0
	for iter.Next() {
		cnt++
		key = iter.Key()
		val = iter.Value()
		fmt.Printf("%s: %s\n", string(key), string(val))
	}
	assert.Equal(4, cnt)
	assert.Equal([]byte("key4"), key)
	assert.Equal([]byte("val4"), val)
	iter.Release()
}

func getTimeString() string {
	now := time.Now()
	t := now.UnixNano()
	return fmt.Sprintf("%d", t)
}

func BenchmarkSetSingle(b *testing.B) {
	s := storage.NewMemStorage()
	db, _ := leveldb.Open(s, nil)
	for n := 0; n < b.N; n++ {
		db.Put([]byte(getTimeString()), []byte("value"), nil)
	}
	db.Close()
}

func BenchmarkSetWithOptions(b *testing.B) {
	s := storage.NewMemStorage()
	dbOpts := &opt.Options{
		NoWriteMerge: true,
		Compression:  opt.NoCompression,
	}
	db, _ := leveldb.Open(s, dbOpts)
	for n := 0; n < b.N; n++ {
		db.Put([]byte(getTimeString()), []byte("value"), nil)
	}
	db.Close()
}

func BenchmarkSetBatch(b *testing.B) {
	batchSize := 1
	s := storage.NewMemStorage()
	db, _ := leveldb.Open(s, nil)
	batch := new(leveldb.Batch)
	for n := 0; n < b.N; n++ {
		if batch.Len() < batchSize {
			batch.Put([]byte(getTimeString()), []byte("value"))
		} else {
			db.Write(batch, nil)
			batch.Reset()
		}
	}
	db.Close()
}
