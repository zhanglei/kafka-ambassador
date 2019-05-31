package kafka

import (
	"testing"

	"github.com/anchorfree/data-go/pkg/promutils"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

var rdStatsJson = `
{
  "name": "rdkafka#producer-1",
  "client_id": "rdkafka",
  "type": "producer",
  "ts": 19658460818,
  "time": 1557318351,
  "replyq": 7,
  "msg_cnt": 2,
  "msg_size": 210,
  "msg_max": 100000,
  "msg_size_max": 1073741824,
  "simple_cnt": 0,
  "metadata_cache_cnt": 99,
  "brokers": {
    "kafka3.data.afdevops.com:9092/3": {
      "name": "kafka3.data.afdevops.com:9092/3",
      "nodeid": 3,
      "nodename": "kafka3.data.afdevops.com:9092",
      "source": "learned",
      "state": "UP",
      "stateage": 18295130,
      "outbuf_cnt": 1,
      "outbuf_msg_cnt": 2,
      "waitresp_cnt": 3,
      "waitresp_msg_cnt": 4,
      "tx": 402,
      "txbytes": 68802,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 423,
      "rxbytes": 28426,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "wakeups": 13,
      "connects": 11,
      "disconnects": 2,
      "int_latency": {"min": 1, "max": 230, "avg": 22, "sum": 20343, "stddev": 23, "p50": 10, "p75": 20, "p90": 25, "p95": 30, "p99": 40, "p99_99": 45, "outofrange": 3, "hdrsize": 11376, "cnt": 10},
      "outbuf_latency": {"min": 1, "max": 230, "avg": 22, "sum": 20343, "stddev": 23, "p50": 10, "p75": 20, "p90": 25, "p95": 30, "p99": 40, "p99_99": 45, "outofrange": 3, "hdrsize": 11376, "cnt": 10},
      "rtt": {"min": 1, "max": 230, "avg": 22, "sum": 20343, "stddev": 23, "p50": 10, "p75": 20, "p90": 25, "p95": 30, "p99": 40, "p99_99": 45, "outofrange": 3, "hdrsize": 11376, "cnt": 10},
      "throttle": {"min": 1, "max": 230, "avg": 22, "sum": 20343, "stddev": 23, "p50": 10, "p75": 20, "p90": 25, "p95": 30, "p99": 40, "p99_99": 45, "outofrange": 3, "hdrsize": 11376, "cnt": 10},
      "req": {"Produce": 3, "Offset": 0, "Metadata": 0, "SaslHandshake": 0, "ApiVersion": 1, "InitProducerId": 0 },
      "toppars": {
        "test-4": {"topic": "test", "partition": 4},
        "test-7": {"topic": "test", "partition": 7}
      }
    }
  },
  "topics": {
    "test": {
      "topic": "test",
      "metadata_age": 18815,
      "batchsize": {"min": 192, "max": 232, "avg": 212, "sum": 424, "stddev": 20, "p50": 192, "p75": 232, "p90": 232, "p95": 232, "p99": 232, "p99_99": 232, "outofrange": 0, "hdrsize": 14448, "cnt": 2},
      "batchcnt": {"min": 2, "max": 4, "avg": 3, "sum": 6, "stddev": 1, "p50": 2, "p75": 4, "p90": 4, "p95": 4, "p99": 4, "p99_99": 4, "outofrange": 0, "hdrsize": 11376, "cnt": 2},
      "partitions": {
        "0": {
          "partition": 0,
          "leader": 2,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "none",
          "query_offset": -1001,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 8,
          "txbytes": 205,
          "rxmsgs": 20,
          "rxbytes": 700,
          "msgs": 2,
          "rx_ver_drops": 9,
          "msgs_inflight": 5,
          "next_ack_seq": 3,
          "next_err_seq": 2,
          "acked_msgid": 1
        },
        "1": {
          "partition": 1,
          "leader": 1,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 2,
          "msgq_bytes": 5,
          "xmit_msgq_cnt": 7,
          "xmit_msgq_bytes": 8,
          "fetchq_cnt": 1,
          "fetchq_size": 2,
          "fetch_state": "none",
          "query_offset": -1001,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 9,
          "txbytes": 1105,
          "rxmsgs": 10,
          "rxbytes": 20,
          "msgs": 9,
          "rx_ver_drops": 1,
          "msgs_inflight": 2,
          "next_ack_seq": 3,
          "next_err_seq": 4,
          "acked_msgid": 7
        }
      }
    }
  },
  "tx": 19,
  "tx_bytes": 2934,
  "rx": 19,
  "rx_bytes": 46061,
  "txmsgs": 17,
  "txmsg_bytes": 1785,
  "rxmsgs": 0,
  "rxmsg_bytes": 0
}
`

func TestPopulateRDKafkaMetrics(t *testing.T) {
	metricRDKafkaGlobal.Reset()
	metricRDKafkaBroker.Reset()
	metricRDKafkaTopic.Reset()
	metricRDKafkaPartition.Reset()

	r := prom.NewRegistry()
	registerMetrics(r)
	populateRDKafkaMetrics(rdStatsJson)

	expect := `# HELP rdkafka_global librdkafka internal global metrics
# TYPE rdkafka_global gauge
rdkafka_global{metric="msg_cnt",producer_id="rdkafka#producer-1"} 2
rdkafka_global{metric="msg_size",producer_id="rdkafka#producer-1"} 210
rdkafka_global{metric="replyq",producer_id="rdkafka#producer-1"} 7
rdkafka_global{metric="rx",producer_id="rdkafka#producer-1"} 19
rdkafka_global{metric="rx_bytes",producer_id="rdkafka#producer-1"} 46061
rdkafka_global{metric="rxmsgs",producer_id="rdkafka#producer-1"} 0
rdkafka_global{metric="rxmsgs_bytes",producer_id="rdkafka#producer-1"} 0
rdkafka_global{metric="tx",producer_id="rdkafka#producer-1"} 19
rdkafka_global{metric="tx_bytes",producer_id="rdkafka#producer-1"} 2934
rdkafka_global{metric="txmsgs",producer_id="rdkafka#producer-1"} 17
rdkafka_global{metric="txmsgs_bytes",producer_id="rdkafka#producer-1"} 0
`

	rendered, err := promutils.Collect(metricRDKafkaGlobal)
	assert.Equal(t, expect, rendered, "global librdkafka metrics do NOT match")
	assert.NoError(t, err)

	expect = `# HELP rdkafka_broker librdkafka internal broker metrics
# TYPE rdkafka_broker gauge
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="connects",producer_id="rdkafka#producer-1",window=""} 11
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="disconnects",producer_id="rdkafka#producer-1",window=""} 2
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="avg"} 22
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="max"} 230
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="min"} 1
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="p50"} 10
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="p95"} 30
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="int_latency",producer_id="rdkafka#producer-1",window="p99"} 40
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_cnt",producer_id="rdkafka#producer-1",window=""} 1
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="avg"} 22
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="max"} 230
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="min"} 1
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="p50"} 10
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="p95"} 30
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_latency",producer_id="rdkafka#producer-1",window="p99"} 40
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="outbuf_msg_cnt",producer_id="rdkafka#producer-1",window=""} 2
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="req_timeouts",producer_id="rdkafka#producer-1",window=""} 0
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="avg"} 22
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="max"} 230
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="min"} 1
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="p50"} 10
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="p95"} 30
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rtt",producer_id="rdkafka#producer-1",window="p99"} 40
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rx",producer_id="rdkafka#producer-1",window=""} 423
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="rxbytes",producer_id="rdkafka#producer-1",window=""} 28426
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="state_up",producer_id="rdkafka#producer-1",window=""} 1
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="tx",producer_id="rdkafka#producer-1",window=""} 402
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="txbytes",producer_id="rdkafka#producer-1",window=""} 68802
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="waitresp_cnt",producer_id="rdkafka#producer-1",window=""} 3
rdkafka_broker{broker="kafka3.data.afdevops.com:9092/3",metric="waitresp_msg_cnt",producer_id="rdkafka#producer-1",window=""} 4
`

	rendered, err = promutils.Collect(metricRDKafkaBroker)
	assert.Equal(t, expect, rendered, "broker metrics do NOT match")
	assert.NoError(t, err)

	expect = `# HELP rdkafka_topic librdkafka internal topic metrics
# TYPE rdkafka_topic gauge
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="avg"} 3
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="max"} 4
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="min"} 2
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="p50"} 2
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="p95"} 4
rdkafka_topic{metric="batchcnt",producer_id="rdkafka#producer-1",topic="test",window="p99"} 4
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="avg"} 212
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="max"} 232
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="min"} 192
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="p50"} 192
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="p95"} 232
rdkafka_topic{metric="batchsize",producer_id="rdkafka#producer-1",topic="test",window="p99"} 232
`

	rendered, err = promutils.Collect(metricRDKafkaTopic)
	assert.Equal(t, expect, rendered, "topic metrics do NOT match")
	assert.NoError(t, err)

	expect = `# HELP rdkafka_partition librdkafka internal partition metrics
# TYPE rdkafka_partition gauge
rdkafka_partition{metric="msgq_bytes",partition="0",producer_id="rdkafka#producer-1",topic="test"} 0
rdkafka_partition{metric="msgq_bytes",partition="1",producer_id="rdkafka#producer-1",topic="test"} 5
rdkafka_partition{metric="msgq_cnt",partition="0",producer_id="rdkafka#producer-1",topic="test"} 0
rdkafka_partition{metric="msgq_cnt",partition="1",producer_id="rdkafka#producer-1",topic="test"} 2
rdkafka_partition{metric="msgs_inflight",partition="0",producer_id="rdkafka#producer-1",topic="test"} 5
rdkafka_partition{metric="msgs_inflight",partition="1",producer_id="rdkafka#producer-1",topic="test"} 2
rdkafka_partition{metric="xmit_msgq_cnt",partition="0",producer_id="rdkafka#producer-1",topic="test"} 0
rdkafka_partition{metric="xmit_msgq_cnt",partition="1",producer_id="rdkafka#producer-1",topic="test"} 7
`

	rendered, err = promutils.Collect(metricRDKafkaPartition)
	assert.Equal(t, expect, rendered, "partition metrics do NOT match")
	assert.NoError(t, err)

	rendered, err = promutils.Gather(r)
	assert.Contains(t, rendered, expect)
}
