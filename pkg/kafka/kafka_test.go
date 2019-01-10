package kafka

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/anchorfree/kafka-ambassador/pkg/testproxy"
	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var messagesOut = make(chan []byte)
var brokerHost string = "127.0.0.1"
var proxyPort = int64(12345)
var testTopic = "test"

func messageReceiver(nodeID int32, reqKind int16, content []byte) kafkatest.Response {
	//fmt.Printf("Request ID: %d\n", reqKind)
	switch reqKind {
	case proto.ProduceReqKind:
		req, err := proto.ReadProduceReq(bytes.NewReader(content))
		if err != nil {
			fmt.Println("Could not parse ProduceReq")
		}
		prodRespTopics := []proto.ProduceRespTopic{}
		for _, t := range req.Topics {
			prodRespParts := []proto.ProduceRespPartition{}
			for _, p := range t.Partitions {
				prodRespParts = append(prodRespParts, proto.ProduceRespPartition{
					ID:     0,
					Offset: 0,
				})
				for _, m := range p.Messages {
					fmt.Printf("KAFKA BROKER got message: %s\n", m.Value)
					messagesOut <- m.Value
				}
			}
			prodRespTopics = append(prodRespTopics, proto.ProduceRespTopic{
				Name:       testTopic,
				Partitions: prodRespParts,
			})
		}
		produceResp := proto.ProduceResp{
			Version:       req.GetVersion(),
			CorrelationID: req.GetCorrelationID(),
			Topics:        prodRespTopics,
		}
		return &produceResp

	case proto.APIVersionsReqKind:
		//fmt.Println("Kafka broker: Got an APIVersionReq")
		req, err := proto.ReadAPIVersionsReq(bytes.NewReader(content))
		if err != nil {
			fmt.Println("Could not parse APIVersionReq")
		}
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
		if err != nil {
			fmt.Println("Could not parse MetadataReq")
		}

		mdResp := &proto.MetadataResp{
			Version:       proto.KafkaV2,
			CorrelationID: req.GetCorrelationID(),
			Topics: []proto.MetadataRespTopic{
				proto.MetadataRespTopic{
					Name: testTopic,
					Partitions: []proto.MetadataRespPartition{
						proto.MetadataRespPartition{
							ID:              0,
							Leader:          nodeID,
							Replicas:        []int32{nodeID},
							Isrs:            []int32{nodeID},
							OfflineReplicas: []int32{},
						},
					},
				},
			},
			Brokers: []proto.MetadataRespBroker{
				proto.MetadataRespBroker{
					NodeID: nodeID,
					Host:   brokerHost,
					Port:   int32(proxyPort),
				},
			},
		}
		return mdResp

	default:
		fmt.Printf("unknown request. Request kind: %d\n", reqKind)
	}
	if reqKind != proto.ProduceReqKind {
		return nil
	}
	time.Sleep(time.Millisecond * 5)
	return nil
}

type EmptyResponse struct{}

func (er *EmptyResponse) Bytes() ([]byte, error) {
	return []byte{}, nil
}

func TestLifeCycle(t *testing.T) {
	helperDrainOutChan(2 * time.Second)
	var err error
	assert := assert.New(t)
	server := kafkatest.NewServer(messageReceiver)
	server.MustSpawn()
	address := server.Addr()
	fmt.Printf("Kafka server listening at %s\n", address)
	brokerHost, _, err = net.SplitHostPort(address)
	assert.Nil(err, "Could not split address to host and port: %v", err)
	proxy := testproxy.T{
		FromAddr: fmt.Sprintf("%s:%d", brokerHost, proxyPort),
		ToAddr:   address,
	}
	err = proxy.Run()
	assert.Nil(err, "Could not create kafka proxy: %v", err)
	defer proxy.Close()

	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()

	flatten := config.Flatten(configMap)
	flatten["bootstrap.servers"] = fmt.Sprintf("%s:%d", brokerHost, proxyPort)
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
	p.Init(&configMap, prometheus.NewRegistry())
	fmt.Println("Going to sleep for a while to let producer init")
	time.Sleep(time.Second * 5)

	assert.Equal(int64(0), p.wal.Messages(), "WAL should be empty")
	assert.Equal(gobreaker.StateClosed, p.cb.State())
	p.Send(testTopic, []byte("test message 1"))
	p.Send(testTopic, []byte("test message 2"))
	assert.Equal(int64(0), p.wal.Messages(), "WAL still should be empty")

	fmt.Println("Sleep to flush librdkafka queue")
	time.Sleep(time.Second * 5)
	fmt.Println("Closing proxy")
	proxy.Close()
	p.Send(testTopic, []byte("first message to fail"))
	fmt.Println("Going to sleep for a while to let kafka server close")
	time.Sleep(time.Second * 5)

	assert.Contains([]gobreaker.State{gobreaker.StateOpen, gobreaker.StateHalfOpen}, p.cb.State(), "We should immediatelly set to Open after all brokers are down")
	for i := 1; uint32(i) <= p.Config.CBMaxFailures; i++ {
		m := []byte(fmt.Sprintf("message for WAL: %d\n", i))
		fmt.Printf("Sending test message: %s", m)
		p.Send(testTopic, m)
	}
	assert.True(p.wal.Messages() >= int64(p.Config.CBMaxRequests), "Messages have to hit wal already")

	fmt.Println("Reopen proxy")
	proxy.Run()
	fmt.Println("Closing circuit breaker")
	if p.cb.State() != gobreaker.StateClosed {
		p.cbClose()
	}
	assert.Equal(gobreaker.StateClosed, p.cb.State(), "Circuit breaker should be closed (green light)")
	fmt.Println("Send extra message 3")
	p.Send(testTopic, []byte("test message 3"))
	assert.True(p.wal.Messages() >= int64(p.Config.CBMaxRequests))

	p.GetProducer().Producer.Close()
	server.Close()
	proxy.Close()
}

func helperGenerateKey(t *testing.T) *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key

}

func helperGenerateCA(t *testing.T, caKey *rsa.PrivateKey, nva time.Time) *x509.Certificate {
	nvb := time.Now().Add(-1 * time.Hour)
	caTemplate := &x509.Certificate{
		Subject:               pkix.Name{CommonName: "ca"},
		Issuer:                pkix.Name{CommonName: "ca"},
		SerialNumber:          big.NewInt(0),
		NotAfter:              nva,
		NotBefore:             nvb,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFinalCert, err := x509.ParseCertificate(caDer)
	if err != nil {
		t.Fatal(err)
	}
	return caFinalCert
}

func helperGenerateServerCert(t *testing.T, key *rsa.PrivateKey, caCert *x509.Certificate, caKey *rsa.PrivateKey, nva time.Time) []byte {
	nvb := time.Now().Add(-1 * time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		Subject:      pkix.Name{CommonName: "host"},
		Issuer:       pkix.Name{CommonName: "ca"},
		SerialNumber: big.NewInt(0),
		NotAfter:     nva,
		NotBefore:    nvb,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
func helperGenerateClientCert(t *testing.T, key *rsa.PrivateKey, caCert *x509.Certificate, caKey *rsa.PrivateKey, nva time.Time) []byte {
	nvb := time.Now().Add(-1 * time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		Subject:      pkix.Name{CommonName: "client"},
		Issuer:       pkix.Name{CommonName: "ca"},
		SerialNumber: big.NewInt(0),
		NotAfter:     nva,
		NotBefore:    nvb,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestTLS(t *testing.T) {
	helperDrainOutChan(2 * time.Second)
	var err error
	assert := assert.New(t)
	caKey := helperGenerateKey(t)
	clientKey := helperGenerateKey(t)
	serverKey := helperGenerateKey(t)
	clientCertExpireAfter := 5 * time.Second

	nva := time.Now().Add(1 * time.Hour)
	caCert := helperGenerateCA(t, caKey, nva)

	serverDer := helperGenerateServerCert(t, serverKey, caCert, caKey, nva)
	clientDer := helperGenerateClientCert(t, clientKey, caCert, caKey, time.Now().Add(clientCertExpireAfter))

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tls.Certificate{
			Certificate: [][]byte{serverDer},
			PrivateKey:  serverKey,
		}},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		//Renegotiation: tls.RenegotiateFreelyAsClient, //keep to fore re-auth once proxy restarted and we are re-connecting
		Renegotiation: tls.RenegotiateNever, //keep to fore re-auth once proxy restarted and we are re-connecting
	}

	server := kafkatest.NewServer(messageReceiver)
	server.MustSpawn()
	address := server.Addr()
	fmt.Printf("Kafka server listening at %s\n", address)

	proxy := testproxy.T{
		FromAddr:  "localhost:12345",
		ToAddr:    address,
		TLSConfig: tlsConfig,
	}
	//fmt.Println(tlsConfig)
	fmt.Printf("proxy: %+v\n", proxy)
	err = proxy.Run()
	assert.Nilf(err, "Could not create kafka proxy: %v", err)

	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()

	clientCertPath := "/tmp/client.cert"
	clientKeyPath := "/tmp/client.key"
	caPath := "/tmp/ca.cert"

	pemClientKey := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(clientKey),
		},
	)
	pemClientCert := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: clientDer,
		},
	)
	pemCA := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: caCert.Raw,
		},
	)

	ioutil.WriteFile(clientCertPath, pemClientCert, 0700)
	ioutil.WriteFile(clientKeyPath, pemClientKey, 0700)
	ioutil.WriteFile(caPath, pemCA, 0700)

	flatten := config.Flatten(configMap)
	flatten["bootstrap.servers"] = fmt.Sprintf("%s:%d", "127.0.0.1", proxyPort)
	flatten["queue.buffering.max.ms"] = 1
	flatten["security.protocol"] = "ssl"
	flatten["ssl.ca.location"] = caPath
	flatten["ssl.certificate.location"] = clientCertPath
	flatten["ssl.key.location"] = clientKeyPath

	for k, v := range flatten {
		configMap[k] = v
	}

	p := &T{
		Logger: logger.Sugar(),
		Config: &Config{
			WalDirectory:           "",
			ResendRateLimit:        10,
			ResendPeriod:           0,
			CBTimeout:              2 * time.Second,
			CBMaxRequests:          5,
			CBMaxFailures:          3,
			OldProducerKillTimeout: 1 * time.Second,
		},
	}

	p.Init(&configMap, prometheus.NewRegistry())
	timeout := 5 * time.Second
	/////////////////////////
	m := []byte("test message to go through")
	p.Send(testTopic, m)
	assert.Equalf(m, helperGetOutput(timeout), "Initial looped message should match within %s", timeout)
	time.Sleep(clientCertExpireAfter)
	proxy.Close()
	proxy.Run()
	if p.cb.State() != gobreaker.StateClosed {
		go p.cbClose()
		for i := 1; uint32(i) <= p.Config.CBMaxRequests; i++ {
			p.Send(testTopic, []byte(fmt.Sprintf("push cb close req #%d", i)))
		}
		helperDrainOutChan(3 * time.Second)
	}
	timeout = 5 * time.Second
	/////////////////////////
	m = []byte("This message should NOT loop")
	p.Send(testTopic, m)
	timeout = 10 * time.Second
	out := helperGetOutput(timeout)
	assert.Nilf(out, "No message should reach test kafka broker since our client auth cert has to be expired: %s", out)

	//generate new client auth cert
	//fmt.Printf(" -- Generate new client certificate\n")
	clientDer = helperGenerateClientCert(t, clientKey, caCert, caKey, time.Now().Add(1*time.Hour))
	pemClientCert = pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: clientDer,
		},
	)
	//fmt.Printf(" -- Write new client certificate\n")
	ioutil.WriteFile(clientCertPath, pemClientCert, 0700)
	//fmt.Printf(" -- AddActiveProducer\n")
	p.AddActiveProducer(&configMap)
	//most probably CB is open by now, force close it
	if p.cb.State() != gobreaker.StateClosed {
		p.cbClose()
		for i := 1; uint32(i) <= p.Config.CBMaxRequests; i++ {
			p.Send(testTopic, []byte(fmt.Sprintf("another push cb close message #%d", i)))
		}
		helperDrainOutChan(3 * time.Second)
	}
	helperDrainOutChan(3 * time.Second)
	m = []byte("+++ A new message to go through 1")
	p.Send(testTopic, m)
	timeout = 5 * time.Second
	assert.Equalf(m, helperGetOutput(timeout), "New producer looped message should match within %s", timeout)
	p.AddActiveProducer(&configMap)
	m = []byte("+++ A new message to go through 2")
	p.Send(testTopic, m)
	assert.Equalf(m, helperGetOutput(timeout), "Yet another new producer looped message should match within %s", timeout)
	p.GetProducer().Producer.Close()
	proxy.Close()
}

func TestAddingActiveProducer(t *testing.T) {
	helperDrainOutChan(2 * time.Second)
	assert := assert.New(t)
	server := kafkatest.NewServer(messageReceiver)
	//No proxy going to use
	address := fmt.Sprintf("%s:%d", brokerHost, proxyPort)
	go server.Run(address)
	defer server.Close()
	fmt.Printf("Kafka server listening at %s\n", address)

	configMap := make(kafka.ConfigMap)
	logger := zap.NewExample()
	flatten := config.Flatten(configMap)
	flatten["bootstrap.servers"] = address
	flatten["batch.num.messages"] = 1

	for k, v := range flatten {
		configMap[k] = v
	}

	oldProducerKillTimeout := 3 * time.Second
	p := &T{
		Logger: logger.Sugar(),
		Config: &Config{
			WalDirectory:           "",
			ResendRateLimit:        10,
			ResendPeriod:           0,
			CBTimeout:              2 * time.Second,
			CBMaxRequests:          5,
			CBMaxFailures:          3,
			OldProducerKillTimeout: oldProducerKillTimeout,
		},
	}
	p.Init(&configMap, prometheus.NewRegistry())
	assert.Equal(1, p.GetProducersCount(), "Should start with a single producer")
	assert.Equal(uint(1), p.GetActiveProducerID(), "ActiveProducerID should be 1")
	helperDrainOutChan(3 * time.Second)
	m := []byte("another test message 1")
	p.Send(testTopic, m)
	assert.Equal(m, helperGetOutput(5*time.Second), "Looped message should match")

	p.AddActiveProducer(&configMap)
	assert.Equal(2, p.GetProducersCount(), "There should be 2 producers now")
	assert.Equal(uint(2), p.GetActiveProducerID(), "ActiveProducerID should be 2")
	m = []byte("another test message 2")
	p.Send(testTopic, m)
	assert.Equal(m, helperGetOutput(5*time.Second), "Looped message though new producer should match")
	time.Sleep(oldProducerKillTimeout)
	assert.Equal(1, p.GetProducersCount(), "Old producer should be either stopped or killed already")
	p.GetProducer().Producer.Close()
}

func helperDrainOutChan(timeout time.Duration) {
	startAt := time.Now()
	for {
		select {
		case <-messagesOut:
		default:
		}
		time.Sleep(10 * time.Millisecond)
		if time.Since(startAt) > timeout {
			break
		}
	}
}

func helperGetOutput(timeout time.Duration) []byte {
	out := []byte("")
	select {
	case out = <-messagesOut:
	case <-time.After(timeout):
		return nil
	}
	return out
}
