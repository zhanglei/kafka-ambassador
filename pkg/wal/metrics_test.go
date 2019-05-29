package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/anchorfree/data-go/pkg/promutils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMetrics(t *testing.T) {
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

	expect := fmt.Sprintf(`# HELP wal_writes Number of wal records written
# TYPE wal_writes counter
wal_writes{topic="test"} %d
`, len(messages))
	rendered, err := promutils.Collect(msgWrites)
	assert.Equal(t, expect, rendered, "msgWrites metric does NOT match")
	assert.NoError(t, err)

	w.setWALMessagesMetric()
	expect = fmt.Sprintf(`# HELP wal_messages Number of wal records remaining in WAL
# TYPE wal_messages gauge
wal_messages %d
`, len(messages))
	rendered, err = promutils.Collect(walMessages)
	assert.Equal(t, expect, rendered, "msgMessages metric does NOT match")
	assert.NoError(t, err)

	reads := 0
	for r := range w.Iterate(0) {
		if r != nil {
			reads++
		}
	}

	msgReadsTemplate := `# HELP wal_reads Number of wal reads
# TYPE wal_reads counter
wal_reads{topic="test"} %d
`
	expect = fmt.Sprintf(msgReadsTemplate, reads)
	rendered, err = promutils.Collect(msgReads)
	assert.Equal(t, expect, rendered, "msgReads metric does NOT match after iterating")
	assert.NoError(t, err)

	for _, m := range messages {
		k := Uint32ToBytes(CrcSum([]byte(m)))
		_, err := w.Get(k)
		assert.NoError(t, err)
		reads++
	}

	expect = fmt.Sprintf(msgReadsTemplate, reads)
	rendered, err = promutils.Collect(msgReads)
	assert.Equal(t, expect, rendered, "msgReads metric does NOT match after reading with Get()")
	assert.NoError(t, err)

	for _, m := range messages {
		err = w.Del(Uint32ToBytes(CrcSum([]byte(m))))
		assert.NoError(t, err)
	}
	helperWaitForEmptyCh(t, w.deleteCh, 1*time.Second)

	expect = fmt.Sprintf(`# HELP wal_deletes Number of wal records deleted
# TYPE wal_deletes counter
wal_deletes %d
`, len(messages))
	rendered, err = promutils.Collect(msgDeletes)
	assert.Equal(t, expect, rendered, "msgDeletes metric does NOT match")
	assert.NoError(t, err)
}
