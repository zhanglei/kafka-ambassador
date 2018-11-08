package wal

import (
	"hash/crc32"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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

func TestIterator(t *testing.T) {
	logger := zap.NewExample()
	var key, val []byte
	cnt := 0
	assert := assert.New(t)
	w, err := New("", prometheus.NewRegistry(), logger.Sugar())
	assert.Nil(err)
	w.Set([]byte("key1"), []byte("val1"))
	w.Set([]byte("key2"), []byte("val2"))
	iter := w.Iterator()
	for iter.Next() {
		cnt++
		key = iter.Key()
		val = iter.Value()
	}
	assert.Equal(2, cnt)
	w.Set([]byte("key3"), []byte("val3"))
	iter.Release()
	w.CompactAll()

	w.Set([]byte("key4"), []byte("val4"))
	iter = w.Iterator()
	cnt = 0
	for iter.Next() {
		cnt++
		key = iter.Key()
		val = iter.Value()
	}
	assert.Equal(4, cnt)
	assert.Equal([]byte("key4"), key)
	assert.Equal([]byte("val4"), val)
	iter.Release()
}

func TestMessages(t *testing.T) {
	logger := zap.NewExample()
	assert := assert.New(t)
	w, err := New("", prometheus.NewRegistry(), logger.Sugar())
	assert.Nil(err)
	w.Set([]byte("key1"), []byte("val1"))
	w.Set([]byte("key2"), []byte("val2"))
	assert.Equal(int64(2), w.Messages())
}
