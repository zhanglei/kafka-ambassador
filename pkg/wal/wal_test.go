package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

const (
	topic = "test"
)

var messages = []string{
	"message1",
	"a second message",
	"little more",
	"444444444444",
	"fifth element",
	"six is a nice number",
	"seven is good as well",
	"ocheinta y ocho",
	"noveinta y nueve",
	"and now is the ten",
}

func TestInAndOut(t *testing.T) {
	dir := helperMkWalDir(t)
	defer os.RemoveAll(dir)
	registry := prometheus.NewRegistry()
	conf := Config{
		Path: dir,
	}
	w, err := New(conf, registry, zap.NewExample().Sugar())
	assert.NoError(t, err)
	for _, m := range messages {
		err = w.Set(topic, []byte(m))
		assert.NoError(t, err)
	}
	helperWaitForEmptyCh(t, w.writeCh, 1*time.Second)
	assert.NoError(t, w.FlushWrites())
	assert.Equal(t, int64(len(messages)), w.MessageCount())

	//Try to pull all records with Iterate first
	looped := []string{}
	for r := range w.Iterate(0) {
		looped = append(looped, string(r.Payload))
		assert.Equal(t, topic, r.Topic)
	}
	assert.ElementsMatch(t, messages, looped, "Looped messages should match original test table")
	//Try to pull limited amount with Iterate()
	looped = []string{}
	limit := 5
	assert.True(t, len(messages) >= limit, "The limit should be less or equal than the test message count")
	for r := range w.Iterate(int64(limit)) {
		looped = append(looped, string(r.Payload))
		assert.Equal(t, topic, r.Topic)
	}
	assert.Equal(t, limit, len(looped))
	assert.Subset(t, messages, looped)
	//Re-open database
	w.storage.Sync()
	w.Close()
	registry = prometheus.NewRegistry()
	w, err = New(conf, registry, zap.NewExample().Sugar())
	//Try to pull all records with Iterate first
	looped = []string{}
	for _, m := range messages {
		k := Uint32ToBytes(CrcSum([]byte(m)))
		r, err := w.Get(k)
		assert.NoError(t, err)
		looped = append(looped, string(r.Payload))
		assert.Equal(t, topic, r.Topic)
	}
	assert.ElementsMatch(t, messages, looped, "Looped messages should match original test table")
	//still same amount should be in the WAL
	assert.Equal(t, int64(len(messages)), w.MessageCount())
	for _, m := range messages {
		err = w.Del(Uint32ToBytes(CrcSum([]byte(m))))
		assert.NoError(t, err)
	}
	helperWaitForEmptyCh(t, w.deleteCh, 1*time.Second)
	assert.NoError(t, w.FlushDeletes())
	assert.Equal(t, int64(0), w.MessageCount())

}

func helperMkWalDir(t *testing.T) string {
	t.Helper()
	dir := fmt.Sprintf("/tmp/wal-test-%d", time.Now().UnixNano())
	err := os.MkdirAll(dir, 0777)
	assert.NoError(t, err)
	return dir
}

func helperWaitForEmptyCh(t *testing.T, ch interface{}, d time.Duration) {
	t.Helper()
	loops := 10
	for c := 0; c < loops; c++ {
		time.Sleep(d / time.Duration(loops))
		switch ch.(type) {
		case chan KV:
			if len(ch.(chan KV)) == 0 {
				return
			}
		case chan []byte:
			if len(ch.(chan []byte)) == 0 {
				return
			}
		}
	}
	t.Error("Wait for empty channel timed out")
	t.FailNow()
}
