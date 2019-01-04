package kafka

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type EmptyResponse struct{}

func (er *EmptyResponse) Bytes() ([]byte, error) {
	return []byte{}, nil
}

func TestLifeCycle(t *testing.T) {
	assert := assert.New(t)
	var host string
	var port int64

	//host := "127.0.0.1"
	//port := 12345
	//address := fmt.Sprintf("%s:%d", host, port)
	messageReceiver := func(nodeID int32, reqKind int16, content []byte) kafkatest.Response {
		fmt.Printf("Request ID: %d\n", reqKind)
		switch reqKind {
		case proto.APIVersionsReqKind:
			fmt.Println("Kafka broker: Got an APIVersionReq")
			req, err := proto.ReadAPIVersionsReq(bytes.NewReader(content))
			assert.NotNil(err, "Could not parse APIVersionReq")
			//To get your broker version run something like: docker exec kafka kafka-broker-api-versions.sh --bootstrap-server 192.168.12.54:9092
			supportedBrokerAPIVersions := []proto.SupportedVersion{
				proto.SupportedVersion{APIKey: 0, MinVersion: 0, MaxVersion: 2},  //Produce
				proto.SupportedVersion{APIKey: 1, MinVersion: 0, MaxVersion: 3},  //Fetch
				proto.SupportedVersion{APIKey: 2, MinVersion: 0, MaxVersion: 1},  //Offsets
				proto.SupportedVersion{APIKey: 3, MinVersion: 0, MaxVersion: 2},  //Metadata
				proto.SupportedVersion{APIKey: 4, MinVersion: 0, MaxVersion: 0},  //LeaderAndIsr
				proto.SupportedVersion{APIKey: 5, MinVersion: 0, MaxVersion: 0},  //StopReplica
				proto.SupportedVersion{APIKey: 6, MinVersion: 0, MaxVersion: 3},  //UpdateMetadata
				proto.SupportedVersion{APIKey: 7, MinVersion: 1, MaxVersion: 1},  //ControlledShutdown
				proto.SupportedVersion{APIKey: 8, MinVersion: 0, MaxVersion: 2},  //OffsetCommit
				proto.SupportedVersion{APIKey: 9, MinVersion: 0, MaxVersion: 2},  //OffsetFetch
				proto.SupportedVersion{APIKey: 10, MinVersion: 0, MaxVersion: 0}, //GroupCoordinator
				proto.SupportedVersion{APIKey: 11, MinVersion: 0, MaxVersion: 1}, //JoinGroup
				proto.SupportedVersion{APIKey: 12, MinVersion: 0, MaxVersion: 0}, //Heartbeat
				proto.SupportedVersion{APIKey: 13, MinVersion: 0, MaxVersion: 0}, //LeaveGroup
				proto.SupportedVersion{APIKey: 14, MinVersion: 0, MaxVersion: 0}, //SyncGroup
				proto.SupportedVersion{APIKey: 15, MinVersion: 0, MaxVersion: 0}, //DescribeGroups
				proto.SupportedVersion{APIKey: 16, MinVersion: 0, MaxVersion: 0}, //ListGroups
				proto.SupportedVersion{APIKey: 17, MinVersion: 0, MaxVersion: 0}, //SaslHandshake
				proto.SupportedVersion{APIKey: 18, MinVersion: 0, MaxVersion: 0}, //ApiVersions
				proto.SupportedVersion{APIKey: 19, MinVersion: 0, MaxVersion: 1}, //CreateTopics
				proto.SupportedVersion{APIKey: 20, MinVersion: 0, MaxVersion: 0}, //DeleteTopics
			}
			avResp := proto.APIVersionsResp{
				Version:       proto.KafkaV0,
				CorrelationID: req.GetCorrelationID(),
				APIVersions:   supportedBrokerAPIVersions,
				ThrottleTime:  10 * time.Millisecond,
			}
			return &avResp

		case proto.MetadataReqKind:
			req, err := proto.ReadMetadataReq(bytes.NewBuffer(content))
			assert.NotNil(err, "Could not parse MetadataReq")

			mdResp := &proto.MetadataResp{
				Version:       proto.KafkaV2,
				CorrelationID: req.GetCorrelationID(),
				Topics: []proto.MetadataRespTopic{
					proto.MetadataRespTopic{
						Name: "test",
						Partitions: []proto.MetadataRespPartition{
							proto.MetadataRespPartition{
								ID:              0,
								Leader:          0,
								Replicas:        []int32{1},
								Isrs:            []int32{1},
								OfflineReplicas: []int32{},
							},
						},
					},
				},
				Brokers: []proto.MetadataRespBroker{
					proto.MetadataRespBroker{
						NodeID: nodeID,
						Host:   host,
						Port:   int32(port),
					},
				},
			}
			return mdResp

		default:
			assert.Failf("unknown request", "Request kind: %d\n", reqKind)
		}
		if reqKind != proto.ProduceReqKind {
			return nil
		}
		time.Sleep(time.Millisecond * 500)
		return nil
	}
	server := kafkatest.NewServer(messageReceiver)
	//go server.Run(address)
	server.MustSpawn()
	address := server.Addr()
	proxy := P{
		FromAddr: "localhost:12345",
		ToAddr:   address,
	}
	err := proxy.Run()
	assert.NotNil(err, "Could not create kafka proxy: %v", err)

	host, strPort, err := net.SplitHostPort(address)
	port, err = strconv.ParseInt(strPort, 10, 32)
	assert.NotNil(err, "Could not split address to host and port: %v", err)

	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()

	flatten := config.Flatten(configMap)
	//flatten["bootstrap.servers"] = address
	flatten["bootstrap.servers"] = "localhost:12345"
	flatten["socket.timeout.ms"] = 10
	flatten["message.timeout.ms"] = 2

	for k, v := range flatten {
		configMap[k] = v
	}

	p := &T{
		Logger: logger.Sugar(),
		Config: &Config{
			WalDirectory:    "",
			ResendRateLimit: 10,
			ResendPeriod:    0,
			CBTimeout:       2 * time.Second,
			CBMaxRequests:   5,
			CBMaxFailures:   3,
		},
	}
	fmt.Println("Going to sleep for a while")
	time.Sleep(time.Second * 5)
	p.Init(&configMap, prometheus.NewRegistry())

	assert.Equal(int64(0), p.wal.Messages(), "Wall should be empty")
	assert.Equal(gobreaker.StateClosed, p.cb.State())

	p.Send("test", []byte("message for WAL 1"))
	p.Send("test", []byte("message for WAL 2"))
	assert.Equal(int64(0), p.wal.Messages(), "Wall still should be empty")

	err = server.Close()
	assert.NotNil(err, "Could not close Test Kafka Server: %v ", err)
	proxy.Close()
	p.Send("test", []byte("message for WAL 3"))
	fmt.Println("Going to sleep for a while to let kafka server close")
	time.Sleep(time.Second * 35)

	assert.Equal(gobreaker.StateOpen, p.cb.State(), "We should immediatelly set to Open after all brokers are down")
	for i := 0; uint32(i) <= p.Config.CBMaxRequests; i++ {
		p.Send("test", []byte(fmt.Sprintf("message for WAL: %d\n", i)))
	}
	p.Send("test", []byte("message for WAL 3"))

	for i := 0; i < 10; i++ {
		//fmt.Printf("messages in wal: %d", p.wal.Messages())
		//time.Sleep(time.Second * 5)
	}

	assert.Equal(int64(3), p.wal.Messages(), "Messages have to hit wal already")

	p.cbClose()
	assert.Equal(gobreaker.StateClosed, p.cb.State(), "Circuit breaker should be closed (green light)")
	p.Send("test", []byte("message for WAL 4"))
	assert.Equal(int64(3), p.wal.Messages())

	p.GetProducer().Producer.Close()
	server.Close()
}
